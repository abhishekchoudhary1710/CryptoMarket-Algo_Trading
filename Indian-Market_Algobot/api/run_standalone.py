"""
Standalone API server for testing the dashboard without the trading bot.
Populates shared state with dummy data so the dashboard has something to show.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from api.state import shared_state
from api.server import app

# Populate dummy data for testing
shared_state.bot_running = True
shared_state.bot_start_time = datetime.now()
shared_state.spot_ltp = 24450.50
shared_state.fut_ltp = 24475.25
shared_state.last_tick_time = datetime.now()
shared_state.tick_count = 1234
shared_state.spot_1m_candles = 45
shared_state.spot_5m_candles = 9
shared_state.fut_1m_candles = 45
shared_state.fut_5m_candles = 9

shared_state.strategies = {
    "bullish_divergence": {
        "active": True,
        "active_divergences": [
            {"pivot_number": 3, "divergence_type": "RED_CANDLE", "start_time": "2026-03-04 14:20:00"},
        ],
        "entry_setups": "1 active setup",
    },
    "bearish_divergence": {
        "active": False,
        "active_divergences": [],
        "entry_setups": "",
    },
}

if __name__ == "__main__":
    import uvicorn
    print("=" * 50)
    print("  STANDALONE API SERVER (TEST MODE)")
    print("  Dashboard: http://localhost:3000")
    print("  API Docs:  http://localhost:8000/docs")
    print("=" * 50)
    uvicorn.run(app, host="0.0.0.0", port=8000)
