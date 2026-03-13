"""
Run dual live-feed ingestion for XAUUSD + MCX GOLD futures and build
1m/3m/5m/10m/15m candles.

Usage:
    python main_dual_feed.py
"""

from __future__ import annotations

from config import settings
from core.dual_feed_pipeline import run_dual_feed_pipeline
from utils.logger import setup_logging


if __name__ == "__main__":
    setup_logging(level=settings.LOG_LEVEL)
    run_dual_feed_pipeline()
