import argparse
import json
import logging
import time as t
from typing import List, Optional

import redis
import yaml
from projectx_client import Auth
from pydantic import BaseModel
from signalrcore.hub_connection_builder import HubConnectionBuilder


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


class ProjectXSubscriber:
    def __init__(
        self,
        logger: logging.Logger,
        params: ProjectXSubscriberParams,
    ):
        self.logger = logger
        self.contract_ids = params.contract_ids
        self.account_ids = params.account_ids

        self.jwt_token = Auth(
            base_url=params.base_url,
            username=params.username,
            api_key=params.api_key,
        ).login()

        # Market hub

        self.market_hub = (
            HubConnectionBuilder()
            .with_url(
                f"{params.market_hub_base_url}?access_token={self.jwt_token}",
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

        self.market_hub.on_open(self._on_market_hub_open)
        self.market_hub.on_close(self._on_market_hub_close)
        self.market_hub.on_error(self._on_market_hub_error)
        self.market_hub.on("GatewayTrade", self._on_trade)

        # User hub

        self.user_hub = (
            HubConnectionBuilder()
            .with_url(
                f"{params.user_hub_base_url}?access_token={self.jwt_token}",
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

        self.user_hub.on_open(self._on_user_hub_open)
        self.user_hub.on_close(self._on_user_hub_close)
        self.user_hub.on_error(self._on_user_hub_error)
        self.user_hub.on("GatewayUserOrder", self._on_order_event)
        self.user_hub.on("GatewayUserPosition", self._on_position_event)
        self.user_hub.on("GatewayUserAccount", self._on_account_event)
        self.user_hub.on("GatewayUserTrade", self._on_user_trade_event)

        # Redis

        self.redis = redis.Redis(host=params.redis_host, port=params.redis_port, db=0)

        # State
        self._stopping = False

    def start(self):
        self.market_hub.start()
        self.user_hub.start()
        try:
            while True:
                t.sleep(1)
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

    # Market hub handlers

    def _on_market_hub_open(self):
        self.logger.info("market hub connected")

        for contract_id in self.contract_ids:
            self.market_hub.send(
                "SubscribeContractTrades",
                [contract_id],
            )

        self.logger.info(f"subscribed to contracts: {', '.join(self.contract_ids)}")

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

    def _on_user_hub_open(self):
        self.logger.info("user hub connected")

        self.user_hub.send("SubscribeAccounts", [])
        for account_id in self.account_ids:
            self.user_hub.send("SubscribeOrders", [account_id])
            self.user_hub.send("SubscribePositions", [account_id])
            self.user_hub.send("SubscribeTrades", [account_id])

        self.logger.info(
            f"subscribed to user events for accounts: "
            f"{', '.join(str(a) for a in self.account_ids)}"
        )

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
