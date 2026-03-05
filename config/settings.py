"""
Centralised configuration for the crypto trading system.

All tuneable parameters live here.  Credentials are loaded from
environment variables (or a .env file) so secrets never touch code.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()  # reads .env in project root (if present)


# ── Exchange connection & credentials ─────────────────────────────────
@dataclass
class ExchangeConfig:
    # Which ccxt exchange to use (e.g. "binance", "bybit", "okx")
    exchange_id: str = os.getenv("EXCHANGE_ID", "bybit")

    api_key: str = os.getenv("EXCHANGE_API_KEY", "")
    api_secret: str = os.getenv("EXCHANGE_API_SECRET", "")
    password: str = os.getenv("EXCHANGE_PASSWORD", "")  # some exchanges need this

    # Set True for testnet/sandbox
    sandbox: bool = os.getenv("EXCHANGE_SANDBOX", "true").lower() == "true"

    symbol: str = os.getenv("SYMBOL", "BTC/USDT")
    timeframe: str = os.getenv("TIMEFRAME", "5m")
    loop_sleep_seconds: int = int(os.getenv("LOOP_SLEEP_SECONDS", "2"))


# ── Risk management ───────────────────────────────────────────────────
@dataclass
class RiskConfig:
    risk_per_trade_pct: float = float(os.getenv("RISK_PER_TRADE_PCT", "0.5"))
    max_daily_loss_pct: float = float(os.getenv("MAX_DAILY_LOSS_PCT", "2.0"))
    max_spread_pct: float = float(os.getenv("MAX_SPREAD_PCT", "0.15"))


# ── Swing structure strategy (H1-L1-A-B-C-D) ────────────────────────
@dataclass
class SwingStructureConfig:
    bars_to_load: int = int(os.getenv("BARS_TO_LOAD", "500"))

    entry_buffer_pct: float = float(os.getenv("ENTRY_BUFFER_PCT", "0.01"))
    rr_ratio: float = float(os.getenv("RR_RATIO", "2.0"))

    magic: str = "swing_struct_v1"
    comment: str = "swing_struct_v1"
