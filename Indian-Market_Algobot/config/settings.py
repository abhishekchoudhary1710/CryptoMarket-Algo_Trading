# config/settings.py
"""
Configuration settings for the Algo-Trading application.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# -----------------------------
# Broker Credentials (Angel One)
# -----------------------------
API_KEY = os.getenv('ANGEL_API_KEY', 'zCjdRuaC')
USERNAME = os.getenv('ANGEL_USERNAME', 'A1389496')
PASSWORD = os.getenv('ANGEL_PASSWORD', '6969')
TOTP_KEY = os.getenv('ANGEL_TOTP_KEY', 'EGT7K2YIJUMQFL3L4PPCEVYDDQ')
# -----------------------------
# Base Directories
# -----------------------------
BASE_DIR = Path(
    os.getenv("APP_BASE_DIR", Path(__file__).resolve().parent.parent)
)

# Root output folder (ONE place)
OUTPUT_DIR = BASE_DIR / os.getenv("OUTPUT_DIR", "outputs")

# Structured subfolders
LOGS_DIR = OUTPUT_DIR / "logs"
DATA_DIR = OUTPUT_DIR / "data"

ORDER_HISTORY_DIR = DATA_DIR / "order_history"
OPTIONS_DATA_DIR = DATA_DIR / "options_data"
RAW_TICKS_DIR = DATA_DIR / "raw_ticks"
HISTORICAL_DIR = DATA_DIR / "historical"

# Ensure directories exist
for d in [
    OUTPUT_DIR,
    LOGS_DIR,
    DATA_DIR,
    ORDER_HISTORY_DIR,
    OPTIONS_DATA_DIR,
    RAW_TICKS_DIR,
    HISTORICAL_DIR,
]:
    d.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Logging
# -----------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = str(LOGS_DIR / os.getenv("LOG_FILE", "algo_trading.log"))

# Historical fetch retry settings (when market is open)
HISTORICAL_FETCH_RETRIES = int(os.getenv("HISTORICAL_FETCH_RETRIES", "5"))
HISTORICAL_FETCH_RETRY_DELAY = int(os.getenv("HISTORICAL_FETCH_RETRY_DELAY", "10"))
HISTORICAL_FETCH_RETRY_BACKOFF = int(os.getenv("HISTORICAL_FETCH_RETRY_BACKOFF", "2"))

# -----------------------------
# Market Hours (IST)
# -----------------------------
MARKET_OPEN_HOUR = int(os.getenv("MARKET_OPEN_HOUR", "9"))
MARKET_OPEN_MINUTE = int(os.getenv("MARKET_OPEN_MINUTE", "15"))
MARKET_CLOSE_HOUR = int(os.getenv("MARKET_CLOSE_HOUR", "15"))
MARKET_CLOSE_MINUTE = int(os.getenv("MARKET_CLOSE_MINUTE", "30"))

# -----------------------------
# Trading Configuration
# -----------------------------
SPOT_TOKEN = str(os.getenv("SPOT_TOKEN", "99926000"))
DEFAULT_LOT_SIZE = int(os.getenv("DEFAULT_LOT_SIZE", "75"))

DEFAULT_MAX_RISK = int(os.getenv("DEFAULT_MAX_RISK", "1000"))
DEFAULT_ACCOUNT_BALANCE = int(os.getenv("DEFAULT_ACCOUNT_BALANCE", "100000"))

TARGET_RISK_MIN = int(os.getenv("TARGET_RISK_MIN", "5000"))
TARGET_RISK_MAX = int(os.getenv("TARGET_RISK_MAX", "5500"))
TARGET_RISK_MID = int(os.getenv("TARGET_RISK_MID", "5250"))

MAX_SIGNAL_ATTEMPTS = int(os.getenv("MAX_SIGNAL_ATTEMPTS", "1"))
MAX_LOTS = int(os.getenv("MAX_LOTS", "50"))  # Maximum lots per option
LOT_SIZE = int(os.getenv("LOT_SIZE", "75"))  # NIFTY standard lot size

# -----------------------------
# API Rate Limits
# -----------------------------
MAX_REQUESTS_PER_MINUTE = int(os.getenv("MAX_REQUESTS_PER_MINUTE", "500"))
MAX_REQUESTS_PER_SECOND = int(os.getenv("MAX_REQUESTS_PER_SECOND", "20"))
MIN_REQUEST_INTERVAL_MS = int(os.getenv("MIN_REQUEST_INTERVAL_MS", "50"))

# Greeks API Rate Limits
GREEKS_DAILY_LIMIT = int(os.getenv("GREEKS_DAILY_LIMIT", "3000"))
GREEKS_MINUTE_LIMIT = int(os.getenv("GREEKS_MINUTE_LIMIT", "180"))
GREEKS_PER_EXPIRY_MINUTE_LIMIT = int(os.getenv("GREEKS_PER_EXPIRY_MINUTE_LIMIT", "90"))
GREEKS_PER_SECOND_LIMIT = int(os.getenv("GREEKS_PER_SECOND_LIMIT", "3"))

# -----------------------------
# Strategy Constants
# -----------------------------
RISK_REWARD = float(os.getenv("RISK_REWARD", "2.0"))
OPTION_STRIKE_STEP = int(os.getenv("OPTION_STRIKE_STEP", "50"))
MAX_OPTION_PICK = int(os.getenv("MAX_OPTION_PICK", "3"))

# Greeks refresh settings
GREEKS_REFRESH_INTERVAL = int(os.getenv("GREEKS_REFRESH_INTERVAL", "10"))  # seconds
GREEKS_CACHE_DURATION = int(os.getenv("GREEKS_CACHE_DURATION", "10"))  # seconds

# Order execution
AUTO_ORDER_EXECUTION = os.getenv("AUTO_ORDER_EXECUTION", "True").lower() in ("true", "1", "yes")

# Signal check settings
SIGNAL_CHECK_INTERVAL = int(os.getenv("SIGNAL_CHECK_INTERVAL", "60"))  # seconds
MIN_ORDER_GAP = int(os.getenv("MIN_ORDER_GAP", "300"))  # seconds (5 minutes)

# Data export intervals
CSV_EXPORT_INTERVAL = int(os.getenv("CSV_EXPORT_INTERVAL", "1800"))  # seconds (30 minutes)
TICKS_EXPORT_INTERVAL = int(os.getenv("TICKS_EXPORT_INTERVAL", "900"))  # seconds (15 minutes)
STATS_REPORT_INTERVAL = int(os.getenv("STATS_REPORT_INTERVAL", "300"))  # seconds (5 minutes)

# -----------------------------
# Divergence Strategy Settings
# -----------------------------
# Divergence detection threshold (in minutes)
DIVERGENCE_THRESHOLD_MINUTES = float(os.getenv("DIVERGENCE_THRESHOLD_MINUTES", "0.5"))

# Maximum number of divergences to track simultaneously
MAX_DIVERGENCES_TRACKED = int(os.getenv("MAX_DIVERGENCES_TRACKED", "10"))

# Entry setup timeout (seconds) - abandon if no entry after this time
ENTRY_SETUP_TIMEOUT_SECONDS = int(os.getenv("ENTRY_SETUP_TIMEOUT_SECONDS", "300"))  # 5 minutes

# Red candle/pullback timeout (seconds) - abandon if no red candle/pullback after this time
RED_CANDLE_TIMEOUT_SECONDS = int(os.getenv("RED_CANDLE_TIMEOUT_SECONDS", "180"))  # 3 minutes
PULLBACK_TIMEOUT_SECONDS = int(os.getenv("PULLBACK_TIMEOUT_SECONDS", "180"))  # 3 minutes

# Maximum risk per trade (rupees)
MAX_RISK_PER_TRADE = int(os.getenv("MAX_RISK_PER_TRADE", "2000"))

# Maximum open positions
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "3"))
