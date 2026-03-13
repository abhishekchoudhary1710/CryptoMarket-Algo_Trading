"""
Simple live tick listener for GOLD MCX futures via Angel One.

Usage:
    python main_mcx_gold_ticks.py
"""

from __future__ import annotations

import time

from brokers.angelone import AngelOneBroker
from data.futures import resolve_gold_mcx_futures_token
from utils.logger import logger, setup_logging
from config import settings


def run() -> None:
    setup_logging(level=settings.LOG_LEVEL)
    broker = AngelOneBroker()

    if not broker.connect():
        raise RuntimeError("Angel One login failed. Check ANGEL_* credentials in .env.")

    scripmaster = broker.fetch_scripmaster_data()
    if not scripmaster:
        raise RuntimeError("Could not download ScripMaster data.")

    contract = resolve_gold_mcx_futures_token(scripmaster)
    if not contract:
        raise RuntimeError("Could not resolve GOLD MCX futures token from ScripMaster.")

    token = contract["token"]
    symbol = contract.get("symbol", "GOLD_MCX_FUT")
    logger.info("Subscribing to %s (%s)", symbol, token)

    def on_ws_data(message):
        tick = broker.extract_ltp(message, expected_token=token)
        if tick:
            logger.info("[MCX TICK] %s token=%s ltp=%.2f", symbol, token, tick["ltp"])

    ws = broker.start_websocket(
        token_list=[{"exchangeType": settings.MCX_WS_EXCHANGE_TYPE, "tokens": [token]}],
        on_data_callback=on_ws_data,
        correlation_id="gold_mcx_live",
        mode=1,
    )
    if ws is None:
        raise RuntimeError("Failed to start Angel websocket.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping MCX live tick listener.")
    finally:
        broker.close()


if __name__ == "__main__":
    run()
