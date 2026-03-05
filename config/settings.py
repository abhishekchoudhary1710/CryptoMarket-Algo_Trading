"""
Centralised configuration for the XAUUSD trading system.

All tuneable parameters live here.  Credentials are loaded from
environment variables (or a .env file) so secrets never touch code.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional

import MetaTrader5 as mt5
from dotenv import load_dotenv

load_dotenv()  # reads .env in project root (if present)


# ── MT5 connection & credentials ────────────────────────────────────
@dataclass
class MT5Config:
    # Credentials — loaded from env vars for GCP / headless deploys
    login: int = int(os.getenv("MT5_LOGIN", "0"))
    password: str = os.getenv("MT5_PASSWORD", "")
    server: str = os.getenv("MT5_SERVER", "")
    path: str = os.getenv("MT5_PATH", "")  # e.g. "C:/Program Files/MetaTrader 5/terminal64.exe"

    symbol: str = os.getenv("MT5_SYMBOL", "XAUUSD")
    timeframe: int = int(os.getenv("MT5_TIMEFRAME", str(mt5.TIMEFRAME_M5)))
    deviation: int = 20
    loop_sleep_seconds: int = 2


# ── Risk management ─────────────────────────────────────────────────
@dataclass
class RiskConfig:
    risk_per_trade_pct: float = float(os.getenv("RISK_PER_TRADE_PCT", "0.5"))
    max_daily_loss_pct: float = float(os.getenv("MAX_DAILY_LOSS_PCT", "2.0"))
    max_spread_points: float = float(os.getenv("MAX_SPREAD_POINTS", "120.0"))


# ── Swing structure strategy (H1-L1-A-B-C-D) ──────────────────────
@dataclass
class SwingStructureConfig:
    bars_to_load: int = 500

    entry_buffer: float = 0.05
    rr_ratio: float = 2.0

    magic: int = 260405
    comment: str = "xau_swing_struct_v1"
