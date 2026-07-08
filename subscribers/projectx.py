import logging
import time as t
from typing import Dict, List

from projectx_client import Auth
from signalrcore.hub_connection_builder import HubConnectionBuilder


class ProjectXSubscriber:
    def __init__(
        self,
        logger: logging.Logger,
        base_url: str,
        market_hub_base_url: str,
        username: str,
        api_key: str,
        contract_ids: List[str],
    ):
        self.logger = logger
        self.contract_ids = contract_ids

        self.jwt_token = Auth(
            base_url=base_url,
            username=username,
            api_key=api_key,
        ).login()

        self.market_hub = (
            HubConnectionBuilder()
            .with_url(
                f"{market_hub_base_url}?access_token={self.jwt_token}",
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
            self.logger.info(
                "user stopped market hub", extra={"event": "market_hub_stop"}
            )
            self.market_hub.send(
                "UnsubscribeContractTrades",
                self.contract_ids,
            )
            self.market_hub.stop()

    def on_open(self):
        self.logger.info(
            "user opened connection to market hub",
            extra={"event": "market_hub_connect"},
        )

        # Subscribe to the configured futures contracts
        self.market_hub.send(
            "SubscribeContractTrades",
            self.contract_ids,
        )

        self.logger.info(
            f"subscribed to contracts {', '.join(self.contract_ids)}",
            extra={"event": "market_hub_subscribe"},
        )

    def on_close(self):
        self.logger.info(
            "user disconnected from market hub",
            extra={"event": "market_hub_disconnect"},
        )

    def on_error(self, error):
        self.logger.error(
            "market hub error",
            extra={"event": "market_hub_error", "error": error.error},
        )

    def on_trade(self, args):
        contract_id, trades = args
        self.logger.debug(f"received {len(trades)} trades for {contract_id}")
