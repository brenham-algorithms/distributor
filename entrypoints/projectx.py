import argparse
import json
import logging
import threading
import time as t
from typing import Callable, Dict, List

import redis
import yaml
from projectx_client import Auth
from pydantic import BaseModel
from signalrcore.hub_connection_builder import HubConnectionBuilder
from signalrcore.messages.message_type import MessageType


class ProjectXSubscriberParams(BaseModel):
    base_url: str
    market_hub_base_url: str
    user_hub_base_url: str
    username: str
    api_key: str
    contract_ids: List[str]
    account_ids: List[int]
    redis_host: str
    redis_port: int

    @classmethod
    def load_from_config(cls, args) -> "ProjectXSubscriberParams":
        with open(args.config, "r") as f:
            raw = yaml.safe_load(f) or {}

        data = raw.get("projectx", {})

        return cls(
            base_url=data.get("base_url"),
            market_hub_base_url=data.get("market_hub_base_url"),
            user_hub_base_url=data.get("user_hub_base_url"),
            username=data.get("username"),
            api_key=data.get("api_key"),
            contract_ids=data.get("contract_ids", []),
            account_ids=data.get("account_ids", []),
            redis_host=data.get("redis_host"),
            redis_port=data.get("redis_port"),
        )


# If a hub receives nothing (events or server keep-alive pings) for this long,
# the connection is presumed dead and is rebuilt from scratch.
STALE_AFTER_SECONDS = 60
WATCHDOG_INTERVAL_SECONDS = 5


class ProjectXSubscriber:
    def __init__(
        self,
        logger: logging.Logger,
        params: ProjectXSubscriberParams,
    ):
        self.logger = logger
        self.params = params
        self.contract_ids = params.contract_ids
        self.account_ids = params.account_ids

        # Redis

        self.redis = redis.Redis(host=params.redis_host, port=params.redis_port, db=0)

        # State
        self._stopping = False
        self._last_message: Dict[str, float] = {}

        self._login()
        self.market_hub = self._build_market_hub()
        self.user_hub = self._build_user_hub()

    def start(self):
        self.market_hub.start()
        self.user_hub.start()
        try:
            while True:
                t.sleep(WATCHDOG_INTERVAL_SECONDS)
                self._check_hub("market")
                self._check_hub("user")
        except KeyboardInterrupt:
            self._stopping = True
            self.logger.info("shutting down")

            # Unsubscribe market hub
            for contract_id in self.contract_ids:
                self.market_hub.send(
                    "UnsubscribeContractTrades",
                    [contract_id],
                )

            # Unsubscribe user hub
            self.user_hub.send("UnsubscribeAccounts", [])
            for account_id in self.account_ids:
                self.user_hub.send("UnsubscribeOrders", [account_id])
                self.user_hub.send("UnsubscribePositions", [account_id])
                self.user_hub.send("UnsubscribeTrades", [account_id])

            self.market_hub.stop()
            self.user_hub.stop()
            self.redis.close()

    # Connection management

    def _login(self):
        self.jwt_token = Auth(
            base_url=self.params.base_url,
            username=self.params.username,
            api_key=self.params.api_key,
        ).login()

    def _build_hub(self, name: str, url: str, events: Dict[str, Callable]):
        hub = (
            HubConnectionBuilder()
            .with_url(
                f"{url}?access_token={self.jwt_token}",
                options={
                    "access_token_factory": lambda: self.jwt_token,
                    "headers": {},
                    "verify_ssl": True,
                },
            )
            .configure_logging(logging.WARNING)
            .with_automatic_reconnect(
                {
                    "type": "raw",
                    "keep_alive_interval": 10,
                    "reconnect_interval": 5,
                    "max_attempts": 5,
                }
            )
            .build()
        )

        for event, handler in events.items():
            hub.on(event, self._guarded(name, event, handler))

        # Record every inbound message, including server pings, so the watchdog
        # can tell a dead connection from a quiet market. BaseHubConnection.start()
        # hands self.on_message to the transport, so this override takes effect.
        dispatch = hub.on_message

        def on_message(messages):
            self._last_message[name] = t.monotonic()
            for message in messages:
                if message.type == MessageType.ping:
                    self.logger.debug(f"{name} hub: keep-alive ping received")
            dispatch(messages)

        hub.on_message = on_message
        self._last_message[name] = t.monotonic()

        return hub

    def _guarded(self, name: str, event: str, handler: Callable) -> Callable:
        # Exceptions raised in event handlers propagate into signalrcore's
        # receive thread and kill it, so never let one escape.
        def wrapper(args):
            try:
                handler(args)
            except Exception:
                self.logger.exception(f"{name} hub: failed to handle {event}")

        return wrapper

    def _build_market_hub(self):
        hub = self._build_hub(
            "market",
            self.params.market_hub_base_url,
            {"GatewayTrade": self._on_trade},
        )
        hub.on_open(self._on_market_hub_open)
        hub.on_reconnect(self._on_market_hub_reconnect)
        hub.on_close(self._on_market_hub_close)
        hub.on_error(self._on_market_hub_error)
        return hub

    def _build_user_hub(self):
        hub = self._build_hub(
            "user",
            self.params.user_hub_base_url,
            {
                "GatewayUserOrder": self._on_order_event,
                "GatewayUserPosition": self._on_position_event,
                "GatewayUserAccount": self._on_account_event,
                "GatewayUserTrade": self._on_user_trade_event,
            },
        )
        hub.on_open(self._on_user_hub_open)
        hub.on_reconnect(self._on_user_hub_reconnect)
        hub.on_close(self._on_user_hub_close)
        hub.on_error(self._on_user_hub_error)
        return hub

    def _check_hub(self, name: str):
        silent_for = t.monotonic() - self._last_message[name]
        if silent_for < STALE_AFTER_SECONDS:
            return

        self.logger.warning(
            f"{name} hub: no messages for {silent_for:.0f}s, rebuilding connection"
        )

        old_hub = self.market_hub if name == "market" else self.user_hub

        # stop() joins the receive thread, which can block indefinitely on a
        # half-open socket, so don't let it stall the watchdog.
        threading.Thread(
            target=self._stop_hub, args=(name, old_hub), daemon=True
        ).start()

        # Resetting the timestamp also spaces out retries if the rebuild fails
        self._last_message[name] = t.monotonic()

        try:
            # The JWT is baked into the hub URL and expires, so get a fresh one
            self._login()
            if name == "market":
                self.market_hub = self._build_market_hub()
                self.market_hub.start()
            else:
                self.user_hub = self._build_user_hub()
                self.user_hub.start()
        except Exception:
            self.logger.exception(
                f"{name} hub: rebuild failed, retrying in {STALE_AFTER_SECONDS}s"
            )

    def _stop_hub(self, name: str, hub):
        try:
            hub.stop()
        except Exception:
            self.logger.exception(f"{name} hub: error stopping stale connection")

    # Market hub handlers

    def _subscribe_market(self):
        for contract_id in self.contract_ids:
            self.market_hub.send(
                "SubscribeContractTrades",
                [contract_id],
            )

        self.logger.info(f"subscribed to contracts: {', '.join(self.contract_ids)}")

    def _on_market_hub_open(self):
        self.logger.info("market hub connected")
        self._subscribe_market()

    def _on_market_hub_reconnect(self):
        # signalrcore fires on_reconnect, not on_open, after an automatic
        # reconnect, and the new connection starts with no subscriptions
        self.logger.warning("market hub reconnected, resubscribing")
        self._subscribe_market()

    def _on_market_hub_close(self):
        self.logger.info("market hub disconnected")

    def _on_market_hub_error(self, error):
        self.logger.error(f"market hub error: {error.error}")

    def _on_trade(self, args):
        if self._stopping:
            return

        contract_id, trades = args

        for trade in trades:
            self.redis.publish(f"ticks:{contract_id}", json.dumps(trade))

        self.logger.debug(f"published {len(trades)} trades for {contract_id}")

    # User hub handlers

    def _subscribe_user(self):
        self.user_hub.send("SubscribeAccounts", [])
        for account_id in self.account_ids:
            self.user_hub.send("SubscribeOrders", [account_id])
            self.user_hub.send("SubscribePositions", [account_id])
            self.user_hub.send("SubscribeTrades", [account_id])

        self.logger.info(
            f"subscribed to user events for accounts: "
            f"{', '.join(str(a) for a in self.account_ids)}"
        )

    def _on_user_hub_open(self):
        self.logger.info("user hub connected")
        self._subscribe_user()

    def _on_user_hub_reconnect(self):
        self.logger.warning("user hub reconnected, resubscribing")
        self._subscribe_user()

    def _on_user_hub_close(self):
        self.logger.info("user hub disconnected")

    def _on_user_hub_error(self, error):
        self.logger.error(f"user hub error: {error}")

    def _on_order_event(self, args):
        if self._stopping:
            return

        self.redis.publish("user:orders", json.dumps(args))
        self.logger.debug(f"published order event")

    def _on_position_event(self, args):
        if self._stopping:
            return

        self.redis.publish("user:positions", json.dumps(args))
        self.logger.debug(f"published position event")

    def _on_account_event(self, args):
        if self._stopping:
            return

        self.redis.publish("user:accounts", json.dumps(args))
        self.logger.debug(f"published account event")

    def _on_user_trade_event(self, args):
        if self._stopping:
            return

        self.redis.publish("user:trades", json.dumps(args))
        self.logger.debug(f"published user trade event")


def main(args) -> None:
    logger = logging.getLogger("projectx")
    logging.basicConfig(level=getattr(logging, args.level.upper(), logging.INFO))

    params = ProjectXSubscriberParams.load_from_config(args)
    subscriber = ProjectXSubscriber(logger=logger, params=params)
    subscriber.start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Market data feed distributor",
    )

    parser.add_argument(
        "--config", type=str, default="config.yaml", help="Config file path"
    )

    parser.add_argument(
        "--level",
        type=str,
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )

    args = parser.parse_args()
    main(args)
