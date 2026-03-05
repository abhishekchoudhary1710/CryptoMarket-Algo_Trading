"""
Order execution and position management via ccxt.
"""

from __future__ import annotations

import logging
from typing import Optional

import ccxt

from config.settings import ExchangeConfig
from core.exchange_client import ExchangeClient

log = logging.getLogger(__name__)


class OrderManager:
    def __init__(self, client: ExchangeClient, cfg: ExchangeConfig, comment: str) -> None:
        self.client = client
        self.exchange = client.exchange
        self.symbol = cfg.symbol
        self.comment = comment

    # ── position query ────────────────────────────────────────────────
    def get_open_position(self) -> Optional[dict]:
        try:
            positions = self.exchange.fetch_positions([self.symbol])
            for pos in positions:
                amt = float(pos.get("contracts", 0) or 0)
                if amt != 0:
                    return pos
            return None
        except ccxt.BaseError as e:
            log.error("Failed to fetch positions: %s", e)
            return None

    # ── market order ──────────────────────────────────────────────────
    def send_market_order(
        self, side: str, amount: float, sl: float, tp: float
    ) -> bool:
        try:
            params = {}

            # Set stop loss and take profit (exchange-specific params)
            params["stopLoss"] = {"triggerPrice": sl}
            params["takeProfit"] = {"triggerPrice": tp}

            order = self.exchange.create_order(
                symbol=self.symbol,
                type="market",
                side=side,
                amount=amount,
                params=params,
            )
            log.info(
                "Order filled | side=%s amount=%.6f price=%s sl=%.2f tp=%.2f id=%s",
                side,
                amount,
                order.get("average", order.get("price", "N/A")),
                sl,
                tp,
                order.get("id", "N/A"),
            )
            return True
        except ccxt.BaseError as e:
            log.error("Order FAILED | side=%s amount=%.6f error=%s", side, amount, e)
            return False

    # ── close position ────────────────────────────────────────────────
    def close_position(self, position: dict) -> bool:
        try:
            side = position.get("side", "")
            contracts = abs(float(position.get("contracts", 0) or 0))
            if contracts == 0:
                return False

            close_side = "sell" if side == "long" else "buy"

            self.exchange.create_order(
                symbol=self.symbol,
                type="market",
                side=close_side,
                amount=contracts,
                params={"reduceOnly": True},
            )
            log.info("Closed position side=%s contracts=%.6f", side, contracts)
            return True
        except ccxt.BaseError as e:
            log.error("Close FAILED: %s", e)
            return False
