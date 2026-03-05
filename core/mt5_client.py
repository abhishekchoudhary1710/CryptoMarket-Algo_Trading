"""
MT5 connection manager and market-data provider.

Wraps MetaTrader5 so the rest of the codebase never calls ``mt5.*`` directly.
Supports both local (interactive MT5) and headless (GCP) initialisation.
"""

from __future__ import annotations

import logging
from typing import Optional

import MetaTrader5 as mt5
import pandas as pd

from config.settings import MT5Config

log = logging.getLogger(__name__)


class MT5Client:
    """Thin wrapper around the MetaTrader5 Python package."""

    def __init__(self, cfg: MT5Config) -> None:
        self.cfg = cfg
        self._connected = False

    # ── lifecycle ────────────────────────────────────────────────────
    def connect(self) -> None:
        """
        Initialise MT5 terminal.

        If ``MT5_LOGIN`` / ``MT5_PASSWORD`` / ``MT5_SERVER`` are set in the
        environment (or .env file), they are passed to ``mt5.initialize()``
        so the bot can run headless on a GCP VM with no interactive login.
        """
        init_kwargs: dict = {}

        if self.cfg.path:
            init_kwargs["path"] = self.cfg.path
        if self.cfg.login and self.cfg.password and self.cfg.server:
            init_kwargs["login"] = self.cfg.login
            init_kwargs["password"] = self.cfg.password
            init_kwargs["server"] = self.cfg.server

        if not mt5.initialize(**init_kwargs):
            raise RuntimeError(f"MT5 initialize failed: {mt5.last_error()}")

        account = mt5.account_info()
        if account is None:
            raise RuntimeError(
                "MT5 initialised but no account is logged in. "
                "Set MT5_LOGIN, MT5_PASSWORD, MT5_SERVER in your .env file."
            )

        if not mt5.symbol_select(self.cfg.symbol, True):
            raise RuntimeError(
                f"Unable to select symbol {self.cfg.symbol}: {mt5.last_error()}"
            )

        self._connected = True
        log.info(
            "Connected | login=%s  server=%s  balance=%.2f",
            account.login,
            account.server,
            account.balance,
        )

    def shutdown(self) -> None:
        mt5.shutdown()
        self._connected = False
        log.info("MT5 shutdown complete.")

    # ── market data ──────────────────────────────────────────────────
    def get_rates(self, bars: int) -> Optional[pd.DataFrame]:
        """Return OHLCV dataframe with UTC ``time`` column, or *None*."""
        rates = mt5.copy_rates_from_pos(
            self.cfg.symbol, self.cfg.timeframe, 0, bars
        )
        if rates is None or len(rates) < 50:
            return None
        df = pd.DataFrame(rates)
        df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
        return df

    # ── account helpers ──────────────────────────────────────────────
    @staticmethod
    def account_balance() -> Optional[float]:
        acc = mt5.account_info()
        return acc.balance if acc else None

    @staticmethod
    def symbol_info(symbol: str):
        return mt5.symbol_info(symbol)

    @staticmethod
    def symbol_tick(symbol: str):
        return mt5.symbol_info_tick(symbol)
