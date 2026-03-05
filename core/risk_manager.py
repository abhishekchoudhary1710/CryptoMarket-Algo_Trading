"""
Risk management: spread filter, lot sizing, daily-loss guard.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timezone
from typing import Optional

import MetaTrader5 as mt5

from config.settings import MT5Config, RiskConfig

log = logging.getLogger(__name__)


def volume_precision(step: float) -> int:
    txt = f"{step:.10f}".rstrip("0")
    if "." not in txt:
        return 0
    return len(txt.split(".")[1])


class RiskManager:
    def __init__(self, mt5_cfg: MT5Config, risk_cfg: RiskConfig, magic: int) -> None:
        self.symbol = mt5_cfg.symbol
        self.risk_cfg = risk_cfg
        self.magic = magic

    # ── spread guard ─────────────────────────────────────────────────
    def spread_ok(self) -> bool:
        info = mt5.symbol_info(self.symbol)
        tick = mt5.symbol_info_tick(self.symbol)
        if info is None or tick is None:
            return False
        spread_pts = (tick.ask - tick.bid) / info.point
        if spread_pts > self.risk_cfg.max_spread_points:
            log.info("Spread too high: %.1f points (limit %.1f)", spread_pts, self.risk_cfg.max_spread_points)
            return False
        return True

    # ── daily-loss guard ─────────────────────────────────────────────
    def daily_pnl(self) -> float:
        now = datetime.now(timezone.utc)
        day_start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
        deals = mt5.history_deals_get(day_start, now)
        if not deals:
            return 0.0
        total = 0.0
        for deal in deals:
            if deal.magic == self.magic:
                total += deal.profit + deal.commission + deal.swap
        return total

    def daily_loss_ok(self, balance: float) -> bool:
        limit = balance * (self.risk_cfg.max_daily_loss_pct / 100.0)
        pnl = self.daily_pnl()
        if pnl <= -limit:
            log.warning(
                "Daily loss guard hit: pnl=%.2f, limit=-%.2f. No new entries.",
                pnl,
                limit,
            )
            return False
        return True

    # ── lot sizing ───────────────────────────────────────────────────
    def calc_volume(
        self, side: str, entry: float, stop: float, balance: float
    ) -> Optional[float]:
        info = mt5.symbol_info(self.symbol)
        if info is None:
            return None

        order_type = mt5.ORDER_TYPE_BUY if side == "buy" else mt5.ORDER_TYPE_SELL
        one_lot_loss = mt5.order_calc_profit(order_type, self.symbol, 1.0, entry, stop)
        if one_lot_loss is None:
            return None

        one_lot_loss = abs(one_lot_loss)
        if one_lot_loss <= 0:
            return None

        risk_amount = balance * (self.risk_cfg.risk_per_trade_pct / 100.0)
        raw = risk_amount / one_lot_loss
        step = info.volume_step
        rounded = math.floor(raw / step) * step
        rounded = max(info.volume_min, min(rounded, info.volume_max))

        if rounded < info.volume_min:
            return None
        return round(rounded, volume_precision(step))
