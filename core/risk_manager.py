"""
Risk management: spread filter, position sizing, daily-loss guard.
"""

from __future__ import annotations

import logging
import math
from typing import Optional

from config.settings import ExchangeConfig, RiskConfig
from core.exchange_client import ExchangeClient

log = logging.getLogger(__name__)


class RiskManager:
    def __init__(self, client: ExchangeClient, cfg: ExchangeConfig, risk_cfg: RiskConfig) -> None:
        self.client = client
        self.exchange = client.exchange
        self.symbol = cfg.symbol
        self.risk_cfg = risk_cfg
        self._daily_pnl = 0.0  # track realised P&L from closed trades this session

    # ── spread guard ──────────────────────────────────────────────────
    def spread_ok(self) -> bool:
        ticker = self.client.fetch_ticker()
        if ticker is None:
            return False
        bid = ticker.get("bid")
        ask = ticker.get("ask")
        if bid is None or ask is None or bid <= 0:
            return False
        spread_pct = ((ask - bid) / bid) * 100.0
        if spread_pct > self.risk_cfg.max_spread_pct:
            log.info("Spread too high: %.4f%% (limit %.4f%%)", spread_pct, self.risk_cfg.max_spread_pct)
            return False
        return True

    # ── daily-loss guard ──────────────────────────────────────────────
    def update_daily_pnl(self, pnl: float) -> None:
        self._daily_pnl += pnl

    def reset_daily_pnl(self) -> None:
        self._daily_pnl = 0.0

    def daily_loss_ok(self, balance: float) -> bool:
        limit = balance * (self.risk_cfg.max_daily_loss_pct / 100.0)
        if self._daily_pnl <= -limit:
            log.warning(
                "Daily loss guard hit: pnl=%.2f, limit=-%.2f. No new entries.",
                self._daily_pnl,
                limit,
            )
            return False
        return True

    # ── position sizing ───────────────────────────────────────────────
    def calc_amount(
        self, side: str, entry: float, stop: float, balance: float
    ) -> Optional[float]:
        if entry <= 0 or stop <= 0:
            return None

        risk_per_unit = abs(entry - stop)
        if risk_per_unit <= 0:
            return None

        risk_amount = balance * (self.risk_cfg.risk_per_trade_pct / 100.0)
        raw_amount = risk_amount / risk_per_unit

        # Respect exchange market minimums
        try:
            market = self.exchange.market(self.symbol)
            min_amount = market.get("limits", {}).get("amount", {}).get("min", 0) or 0
            precision = market.get("precision", {}).get("amount", 8)

            if isinstance(precision, int):
                raw_amount = math.floor(raw_amount * (10 ** precision)) / (10 ** precision)
            else:
                step = float(precision)
                raw_amount = math.floor(raw_amount / step) * step

            if raw_amount < min_amount:
                log.warning("Calculated amount %.8f below minimum %.8f", raw_amount, min_amount)
                return None

            return raw_amount
        except Exception as e:
            log.error("Amount calculation error: %s", e)
            return None
