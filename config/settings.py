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

# ── Divergence strategy symbols (primary vs secondary chart) ─────────
# Primary/secondary naming keeps the strategy reusable across markets
# (e.g. XAUUSDT vs GOLD MCX futures, spot vs perpetuals, etc.).
DIVERGENCE_PRIMARY_SYMBOL: str = os.getenv("DIVERGENCE_PRIMARY_SYMBOL", os.getenv("SYMBOL", "XAUUSD"))
DIVERGENCE_SECONDARY_SYMBOL: str = os.getenv("DIVERGENCE_SECONDARY_SYMBOL", "MCX:GOLD_FUT")
DIVERGENCE_PRIMARY_LABEL: str = os.getenv("DIVERGENCE_PRIMARY_LABEL", DIVERGENCE_PRIMARY_SYMBOL)
DIVERGENCE_SECONDARY_LABEL: str = os.getenv("DIVERGENCE_SECONDARY_LABEL", DIVERGENCE_SECONDARY_SYMBOL)

# Divergence strategy runtime parameters.
DIVERGENCE_THRESHOLD_MINUTES: int = int(os.getenv("DIVERGENCE_THRESHOLD_MINUTES", "15"))
MARKET_OPEN_HOUR: int = int(os.getenv("MARKET_OPEN_HOUR", "9"))
MARKET_OPEN_MINUTE: int = int(os.getenv("MARKET_OPEN_MINUTE", "15"))
MARKET_CLOSE_HOUR: int = int(os.getenv("MARKET_CLOSE_HOUR", "23"))
MARKET_CLOSE_MINUTE: int = int(os.getenv("MARKET_CLOSE_MINUTE", "30"))
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
LOGS_DIR: str = os.getenv("LOGS_DIR", "logs")
DATA_DIR: str = os.getenv("DATA_DIR", "logs/data")
ORDER_HISTORY_DIR: str = os.getenv("ORDER_HISTORY_DIR", "logs/order_history")
RAW_TICKS_DIR: str = os.getenv("RAW_TICKS_DIR", "logs/raw_ticks")
DEFAULT_LOT_SIZE: int = int(os.getenv("DEFAULT_LOT_SIZE", "1"))
LOT_SIZE: int = int(os.getenv("LOT_SIZE", "1"))
MAX_LOTS: int = int(os.getenv("MAX_LOTS", "10"))
TARGET_RISK_MIN: float = float(os.getenv("TARGET_RISK_MIN", "500"))
TARGET_RISK_MID: float = float(os.getenv("TARGET_RISK_MID", "1000"))
TARGET_RISK_MAX: float = float(os.getenv("TARGET_RISK_MAX", "1500"))
MAX_REQUESTS_PER_MINUTE: int = int(os.getenv("MAX_REQUESTS_PER_MINUTE", "500"))
MIN_REQUEST_INTERVAL_MS: int = int(os.getenv("MIN_REQUEST_INTERVAL_MS", "50"))

# Angel One / MCX bridge configuration.
ANGEL_API_KEY: str = os.getenv("ANGEL_API_KEY", "")
ANGEL_USERNAME: str = os.getenv("ANGEL_USERNAME", "")
ANGEL_PASSWORD: str = os.getenv("ANGEL_PASSWORD", "")
ANGEL_TOTP_KEY: str = os.getenv("ANGEL_TOTP_KEY", "")
ANGEL_SCRIPMASTER_URL: str = os.getenv(
    "ANGEL_SCRIPMASTER_URL",
    "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json",
)

# MCX Gold futures token discovery filters.
MCX_EXCHANGE_SEGMENT: str = os.getenv("MCX_EXCHANGE_SEGMENT", "MCX")
MCX_INSTRUMENT_TYPE: str = os.getenv("MCX_INSTRUMENT_TYPE", "FUTCOM")
MCX_NAME: str = os.getenv("MCX_NAME", "GOLD")
MCX_TRADINGSYMBOL_CONTAINS: str = os.getenv("MCX_TRADINGSYMBOL_CONTAINS", "GOLD")
MCX_EXCLUDE_KEYWORDS: tuple[str, ...] = tuple(
    keyword.strip().upper()
    for keyword in os.getenv("MCX_EXCLUDE_KEYWORDS", "MINI,PETAL")
    .split(",")
    if keyword.strip()
)
MCX_WS_EXCHANGE_TYPE: int = int(os.getenv("MCX_WS_EXCHANGE_TYPE", "5"))

# Dual-feed (XAUUSD + MCX) runtime config.
PRIMARY_PROVIDER: str = os.getenv("PRIMARY_PROVIDER", "oanda").lower()
PRIMARY_SYMBOL: str = os.getenv("PRIMARY_SYMBOL", os.getenv("DIVERGENCE_PRIMARY_SYMBOL", "XAU_USD"))
PRIMARY_TICK_POLL_SECONDS: float = float(os.getenv("PRIMARY_TICK_POLL_SECONDS", "1"))
CANDLE_TIMEFRAMES_MINUTES: str = os.getenv("CANDLE_TIMEFRAMES_MINUTES", "1,3,5,10,15")
DIVERGENCE_TIMEFRAME_MINUTES: int = int(os.getenv("DIVERGENCE_TIMEFRAME_MINUTES", "10"))
ENTRY_TIMEFRAME_MINUTES: int = int(os.getenv("ENTRY_TIMEFRAME_MINUTES", "3"))
DUAL_FEED_HEARTBEAT_SECONDS: int = int(os.getenv("DUAL_FEED_HEARTBEAT_SECONDS", "15"))
SIGNAL_JOURNAL_PATH: str = os.getenv("SIGNAL_JOURNAL_PATH", "logs/signal_journal.csv")
SIGNAL_MAX_AGE_MINUTES: int = int(os.getenv("SIGNAL_MAX_AGE_MINUTES", "90"))
STALE_FEED_WARN_SECONDS: int = int(os.getenv("STALE_FEED_WARN_SECONDS", "20"))

# OANDA practice/live config for XAUUSD primary feed.
OANDA_ENV: str = os.getenv("OANDA_ENV", "practice").lower()
OANDA_API_TOKEN: str = os.getenv("OANDA_API_TOKEN", "")
OANDA_ACCOUNT_ID: str = os.getenv("OANDA_ACCOUNT_ID", "")
OANDA_INSTRUMENT: str = os.getenv("OANDA_INSTRUMENT", "XAU_USD")
OANDA_REQUEST_TIMEOUT_SECONDS: int = int(os.getenv("OANDA_REQUEST_TIMEOUT_SECONDS", "10"))

# Restrict execution symbol to XAUUSD market only.
TRADE_SYMBOL_LOCK: str = os.getenv("TRADE_SYMBOL_LOCK", "XAUUSD")


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

    symbol: str = os.getenv("SYMBOL", "XAUUSD")
    timeframe: str = os.getenv("TIMEFRAME", "5m")
    loop_sleep_seconds: int = int(os.getenv("LOOP_SLEEP_SECONDS", "2"))
    tick_log_interval_seconds: int = int(os.getenv("TICK_LOG_INTERVAL_SECONDS", "15"))


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
