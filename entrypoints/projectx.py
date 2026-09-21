import argparse
import json
import logging
import time as t
from typing import List

import redis
import yaml
from projectx_client import Auth
from pydantic import BaseModel
from signalrcore.hub_connection_builder import HubConnectionBuilder


class ProjectXSubscriberParams(BaseModel):
    base_url: str
    market_hub_base_url: str
    username: str
    api_key: str
    contract_ids: List[str]
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
            username=data.get("username"),
            api_key=data.get("api_key"),
            contract_ids=data.get("contract_ids", []),
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

        self.jwt_token = Auth(
            base_url=params.base_url,
            username=params.username,
            api_key=params.api_key,
        ).login()

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
            .configure_logging(logging.INFO)
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

        self.redis = redis.Redis(host=params.redis_host, port=params.redis_port, db=0)

        # State attributes
        self._stopping = False

        # Register market hub handlers
        self.market_hub.on_open(self.on_open)
        self.market_hub.on_close(self.on_close)
        self.market_hub.on_error(self.on_error)
        self.market_hub.on("GatewayTrade", self.on_trade)

    def start(self):
        self.market_hub.start()
        try:
            while True:
                t.sleep(1)
        except KeyboardInterrupt:
            self._stopping = True
            self.logger.info(
                "user stopped market hub", extra={"event": "market_hub_stop"}
            )

            for contract_id in self.contract_ids:
                self.market_hub.send(
                    "UnsubscribeContractTrades",
                    [contract_id],
                )

            self.market_hub.stop()
            self.redis.close()

    def on_open(self):
        self.logger.info(
            "user opened connection to market hub",
            extra={"event": "market_hub_connect"},
        )

        # Subscribe to the configured futures contracts
        for contract_id in self.contract_ids:
            self.market_hub.send(
                "SubscribeContractTrades",
                [contract_id],
            )

        self.logger.info(f"subscribed to contracts {', '.join(self.contract_ids)}")

    def on_close(self):
        self.logger.info("user disconnected from market hub")

    def on_error(self, error):
        self.logger.error(f"market hub error: {error.error}")

    def on_trade(self, args):
        if self._stopping:
            return

        contract_id, trades = args

        for trade in trades:
            self.redis.publish(f"ticks:{contract_id}", json.dumps(trade))

        self.logger.debug(f"published {len(trades)} trades for {contract_id}")


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
