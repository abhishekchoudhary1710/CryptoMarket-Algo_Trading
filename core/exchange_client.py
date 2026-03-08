"""
Exchange connection manager and market-data provider via ccxt.

Wraps ccxt so the rest of the codebase never calls exchange APIs directly.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Optional

import ccxt
import pandas as pd

from config.settings import ExchangeConfig

log = logging.getLogger(__name__)


class ExchangeClient:
    """Thin wrapper around a ccxt exchange instance."""

    def __init__(self, cfg: ExchangeConfig) -> None:
        self.cfg = cfg
        self.exchange: Optional[ccxt.Exchange] = None
        self._last_tick_heartbeat_log_monotonic = 0.0
        self._last_tick_snapshot: Optional[tuple[Optional[float], Optional[float], Optional[float]]] = None
        self._last_tick_change_monotonic = 0.0

    # ── lifecycle ─────────────────────────────────────────────────────
    def connect(self) -> None:
        exchange_class = getattr(ccxt, self.cfg.exchange_id)
        self.exchange = exchange_class({
            "apiKey": self.cfg.api_key,
            "secret": self.cfg.api_secret,
            "password": self.cfg.password,
            "enableRateLimit": True,
        })

        if self.cfg.sandbox:
            self.exchange.set_sandbox_mode(True)

        self.exchange.load_markets()

        if self.cfg.symbol not in self.exchange.markets:
            raise RuntimeError(f"Symbol {self.cfg.symbol} not found on {self.cfg.exchange_id}")

        balance = self.account_balance()
        log.info(
            "Connected | exchange=%s  sandbox=%s  balance=%.2f USDT",
            self.cfg.exchange_id,
            self.cfg.sandbox,
            balance or 0.0,
        )

    def shutdown(self) -> None:
        if self.exchange and hasattr(self.exchange, "close"):
            self.exchange.close()
        log.info("Exchange client shutdown complete.")

    # ── market data ───────────────────────────────────────────────────
    def get_rates(self, bars: int) -> Optional[pd.DataFrame]:
        """Return OHLCV dataframe with UTC ``time`` column, or *None*."""
        try:
            ohlcv = self.exchange.fetch_ohlcv(
                self.cfg.symbol, self.cfg.timeframe, limit=bars
            )
        except ccxt.BaseError as e:
            log.error("Failed to fetch OHLCV: %s", e)
            return None

        if ohlcv is None or len(ohlcv) < 50:
            return None

        df = pd.DataFrame(ohlcv, columns=["time", "open", "high", "low", "close", "volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        return df

    # ── account helpers ───────────────────────────────────────────────
    def account_balance(self) -> Optional[float]:
        try:
            balance = self.exchange.fetch_balance()
            return float(balance.get("total", {}).get("USDT", 0.0))
        except ccxt.BaseError as e:
            log.error("Failed to fetch balance: %s", e)
            return None

    def fetch_ticker(self) -> Optional[dict]:
        try:
            return self.exchange.fetch_ticker(self.cfg.symbol)
        except ccxt.BaseError as e:
            log.error("Failed to fetch ticker: %s", e)
            return None

    def log_live_tick_heartbeat(self) -> None:
        """Log periodic ticker snapshots so runtime feed activity is visible."""
        interval = self.cfg.tick_log_interval_seconds
        if interval <= 0:
            return

        now = time.monotonic()
        if now - self._last_tick_heartbeat_log_monotonic < interval:
            return

        ticker = self.fetch_ticker()
        if ticker is None:
            return

        self._last_tick_heartbeat_log_monotonic = now
        timestamp = ticker.get("timestamp")
        tick_time = "n/a"
        if isinstance(timestamp, (int, float)) and timestamp > 0:
            tick_time = datetime.fromtimestamp(timestamp / 1000.0, tz=timezone.utc).isoformat()

        bid = float(ticker["bid"]) if isinstance(ticker.get("bid"), (int, float)) else None
        ask = float(ticker["ask"]) if isinstance(ticker.get("ask"), (int, float)) else None
        last = float(ticker["last"]) if isinstance(ticker.get("last"), (int, float)) else None

        current_snapshot = (bid, ask, last)
        changed = current_snapshot != self._last_tick_snapshot
        if changed:
            self._last_tick_snapshot = current_snapshot
            self._last_tick_change_monotonic = now

        stale_for_seconds = 0.0
        if self._last_tick_change_monotonic > 0:
            stale_for_seconds = now - self._last_tick_change_monotonic

        log.info(
            "Live tick | symbol=%s status=%s stale_for=%.0fs bid=%s ask=%s last=%s tick_time=%s",
            self.cfg.symbol,
            "LIVE" if changed else "STALE",
            stale_for_seconds,
            f"{bid:.6f}" if bid is not None else "n/a",
            f"{ask:.6f}" if ask is not None else "n/a",
            f"{last:.6f}" if last is not None else "n/a",
            tick_time,
        )
