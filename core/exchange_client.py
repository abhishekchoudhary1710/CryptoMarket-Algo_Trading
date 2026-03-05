"""
Exchange connection manager and market-data provider via ccxt.

Wraps ccxt so the rest of the codebase never calls exchange APIs directly.
"""

from __future__ import annotations

import logging
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
        if self.exchange:
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
