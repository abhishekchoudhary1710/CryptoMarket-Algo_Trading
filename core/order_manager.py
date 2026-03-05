"""
Order execution and position management via MT5.
"""

from __future__ import annotations

import logging
from typing import Optional

import MetaTrader5 as mt5

from config.settings import MT5Config

log = logging.getLogger(__name__)


class OrderManager:
    def __init__(self, mt5_cfg: MT5Config, magic: int, comment: str) -> None:
        self.symbol = mt5_cfg.symbol
        self.deviation = mt5_cfg.deviation
        self.magic = magic
        self.comment = comment

    # ── position query ───────────────────────────────────────────────
    def get_open_position(self):
        positions = mt5.positions_get(symbol=self.symbol)
        if not positions:
            return None
        for pos in positions:
            if pos.magic == self.magic:
                return pos
        return None

    # ── market order ─────────────────────────────────────────────────
    def send_market_order(
        self, side: str, volume: float, sl: float, tp: float
    ) -> bool:
        tick = mt5.symbol_info_tick(self.symbol)
        if tick is None:
            return False

        order_type = mt5.ORDER_TYPE_BUY if side == "buy" else mt5.ORDER_TYPE_SELL
        price = tick.ask if side == "buy" else tick.bid
        fill_modes = [
            mt5.ORDER_FILLING_IOC,
            mt5.ORDER_FILLING_FOK,
            mt5.ORDER_FILLING_RETURN,
        ]

        for fill in fill_modes:
            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": self.symbol,
                "volume": volume,
                "type": order_type,
                "price": price,
                "sl": sl,
                "tp": tp,
                "deviation": self.deviation,
                "magic": self.magic,
                "comment": self.comment,
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": fill,
            }
            result = mt5.order_send(request)
            if result is not None and result.retcode == mt5.TRADE_RETCODE_DONE:
                log.info(
                    "Order filled | side=%s vol=%.2f price=%.3f sl=%.3f tp=%.3f deal=%s",
                    side,
                    volume,
                    price,
                    sl,
                    tp,
                    result.deal,
                )
                return True

        log.error("Order FAILED | side=%s vol=%.2f", side, volume)
        return False

    # ── close position ───────────────────────────────────────────────
    def close_position(self, position) -> bool:
        tick = mt5.symbol_info_tick(self.symbol)
        if tick is None:
            return False

        if position.type == mt5.POSITION_TYPE_BUY:
            close_type = mt5.ORDER_TYPE_SELL
            price = tick.bid
        else:
            close_type = mt5.ORDER_TYPE_BUY
            price = tick.ask

        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "position": position.ticket,
            "symbol": self.symbol,
            "volume": position.volume,
            "type": close_type,
            "price": price,
            "deviation": self.deviation,
            "magic": self.magic,
            "comment": f"{self.comment}_close",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        result = mt5.order_send(request)
        if result is not None and result.retcode == mt5.TRADE_RETCODE_DONE:
            log.info("Closed position ticket=%s", position.ticket)
            return True
        log.error("Close FAILED ticket=%s", position.ticket)
        return False
