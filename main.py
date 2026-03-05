"""
Crypto Swing-Structure Trading Bot
───────────────────────────────────
Entry point.  Connects to exchange via ccxt, bootstraps the swing-structure
strategy, and loops on each new closed bar.

Usage:
    python main.py
"""

from __future__ import annotations

import logging
import time

from config.settings import ExchangeConfig, RiskConfig, SwingStructureConfig
from core.exchange_client import ExchangeClient
from core.order_manager import OrderManager
from core.risk_manager import RiskManager
from strategies.swing_structure import SwingStructureStrategy
from utils.logger import setup_logging

log = logging.getLogger(__name__)


def run() -> None:
    # ── configuration ─────────────────────────────────────────────────
    exchange_cfg = ExchangeConfig()
    risk_cfg = RiskConfig()
    strat_cfg = SwingStructureConfig()

    # ── initialise components ─────────────────────────────────────────
    setup_logging()
    client = ExchangeClient(exchange_cfg)
    client.connect()

    order_mgr = OrderManager(client, exchange_cfg, comment=strat_cfg.comment)
    risk_mgr = RiskManager(client, exchange_cfg, risk_cfg)
    strategy = SwingStructureStrategy(strat_cfg)

    balance = client.account_balance()
    if balance is None:
        raise RuntimeError("Cannot read account balance after connection.")
    log.info("Risk guard | max_daily_loss=%.2f", balance * (risk_cfg.max_daily_loss_pct / 100.0))

    # ── main loop ─────────────────────────────────────────────────────
    try:
        while True:
            df = client.get_rates(strat_cfg.bars_to_load)
            if df is None:
                time.sleep(exchange_cfg.loop_sleep_seconds)
                continue

            signal = strategy.next_signal(df)
            if signal is None:
                time.sleep(exchange_cfg.loop_sleep_seconds)
                continue

            log.info(
                "Signal | side=%s entry=%.3f sl=%.3f tp=%.3f reason=%s",
                signal["side"],
                signal["entry"],
                signal["sl"],
                signal["tp"],
                signal["reason"],
            )

            # ── risk checks ───────────────────────────────────────────
            balance = client.account_balance()
            if balance is None:
                time.sleep(exchange_cfg.loop_sleep_seconds)
                continue

            if not risk_mgr.daily_loss_ok(balance):
                time.sleep(exchange_cfg.loop_sleep_seconds)
                continue

            # ── position management ───────────────────────────────────
            position = order_mgr.get_open_position()
            if position is not None:
                pos_side = position.get("side", "")
                if signal["side"] == "buy" and pos_side == "short":
                    order_mgr.close_position(position)
                elif signal["side"] == "sell" and pos_side == "long":
                    order_mgr.close_position(position)
                else:
                    log.info("Same-side position already open. Skipping new entry.")
                    time.sleep(exchange_cfg.loop_sleep_seconds)
                    continue

            if not risk_mgr.spread_ok():
                time.sleep(exchange_cfg.loop_sleep_seconds)
                continue

            # ── entry price validation ────────────────────────────────
            ticker = client.fetch_ticker()
            if ticker is None:
                time.sleep(exchange_cfg.loop_sleep_seconds)
                continue

            market_entry = ticker["ask"] if signal["side"] == "buy" else ticker["bid"]
            sl = signal["sl"]
            tp = signal["tp"]

            if signal["side"] == "buy" and not (sl < market_entry < tp):
                log.warning("Invalid buy levels for market execution. Skipping.")
                time.sleep(exchange_cfg.loop_sleep_seconds)
                continue
            if signal["side"] == "sell" and not (tp < market_entry < sl):
                log.warning("Invalid sell levels for market execution. Skipping.")
                time.sleep(exchange_cfg.loop_sleep_seconds)
                continue

            # ── position sizing & order ───────────────────────────────
            amount = risk_mgr.calc_amount(signal["side"], market_entry, sl, balance)
            if amount is None:
                log.error("Position sizing failed.")
                time.sleep(exchange_cfg.loop_sleep_seconds)
                continue

            order_mgr.send_market_order(signal["side"], amount, sl, tp)
            time.sleep(exchange_cfg.loop_sleep_seconds)

    except KeyboardInterrupt:
        log.info("Stopped by user.")
    finally:
        client.shutdown()


if __name__ == "__main__":
    run()
