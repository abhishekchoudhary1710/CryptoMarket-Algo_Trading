"""
FastAPI server exposing trading bot state via REST and WebSocket APIs.
Runs in a background thread alongside the trading bot.
"""

import os
import asyncio
import json
import threading
from datetime import datetime
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from api.state import shared_state
from config import settings

app = FastAPI(title="Algo-Trading API", version="1.0.0")

# Allow dashboard to connect from any origin (restrict in production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ──────────────────────────── REST ENDPOINTS ────────────────────────────


@app.get("/api/status")
def get_status():
    """Bot running status, uptime, kill switch state."""
    snapshot = shared_state.get_snapshot()
    uptime = None
    if shared_state.bot_start_time:
        uptime = str(datetime.now() - shared_state.bot_start_time)
    return {
        "bot_running": snapshot["bot_running"],
        "kill_switch": snapshot["kill_switch"],
        "uptime": uptime,
        "bot_start_time": snapshot["bot_start_time"],
        "tick_count": snapshot["tick_count"],
        "last_tick_time": snapshot["last_tick_time"],
    }


@app.get("/api/prices")
def get_prices():
    """Current spot and futures LTP."""
    return {
        "spot_ltp": shared_state.spot_ltp,
        "fut_ltp": shared_state.fut_ltp,
        "premium": round(shared_state.fut_ltp - shared_state.spot_ltp, 2) if shared_state.fut_ltp else 0,
        "last_tick_time": str(shared_state.last_tick_time) if shared_state.last_tick_time else None,
    }


@app.get("/api/strategies")
def get_strategies():
    """Strategy states — active/inactive, D points, pending setups, divergences."""
    return shared_state.strategies


@app.get("/api/orders/today")
def get_today_orders():
    """Today's order history from CSV."""
    date_str = datetime.now().strftime("%Y%m%d")
    csv_path = settings.ORDER_HISTORY_DIR / f"order_history_{date_str}.csv"

    if not csv_path.exists():
        return {"orders": [], "count": 0}

    try:
        df = pd.read_csv(csv_path)
        orders = df.to_dict(orient="records")
        return {"orders": orders, "count": len(orders)}
    except Exception as e:
        return {"orders": [], "count": 0, "error": str(e)}


@app.get("/api/orders/history")
def get_order_history(days: int = 30):
    """Order history for the last N days."""
    all_orders = []
    order_dir = settings.ORDER_HISTORY_DIR

    if not order_dir.exists():
        return {"orders": [], "count": 0}

    try:
        csv_files = sorted(order_dir.glob("order_history_*.csv"), reverse=True)
        for csv_file in csv_files[:days]:
            try:
                df = pd.read_csv(csv_file)
                all_orders.extend(df.to_dict(orient="records"))
            except Exception:
                continue
        return {"orders": all_orders, "count": len(all_orders)}
    except Exception as e:
        return {"orders": [], "count": 0, "error": str(e)}


@app.get("/api/entries/today")
def get_today_entries():
    """Today's entry signals from CSV."""
    date_str = datetime.now().strftime("%Y%m%d")
    csv_path = settings.RAW_TICKS_DIR / f"entries_{date_str}.csv"

    if not csv_path.exists():
        return {"entries": [], "count": 0}

    try:
        df = pd.read_csv(csv_path)
        entries = df.to_dict(orient="records")
        return {"entries": entries, "count": len(entries)}
    except Exception as e:
        return {"entries": [], "count": 0, "error": str(e)}


@app.get("/api/pnl/today")
def get_today_pnl():
    """Today's P&L calculated from orders."""
    date_str = datetime.now().strftime("%Y%m%d")
    csv_path = settings.ORDER_HISTORY_DIR / f"order_history_{date_str}.csv"

    if not csv_path.exists():
        return {"pnl": 0, "trades": 0, "wins": 0, "losses": 0, "orders": []}

    try:
        df = pd.read_csv(csv_path)
        # Filter only successful orders
        placed = df[df["status"].isin(["PLACED", "COMPLETE"])] if "status" in df.columns else df
        return {
            "trades": len(placed),
            "orders": placed.to_dict(orient="records"),
        }
    except Exception as e:
        return {"pnl": 0, "trades": 0, "error": str(e)}


@app.get("/api/pnl/monthly")
def get_monthly_pnl():
    """Monthly P&L breakdown from historical order CSVs."""
    order_dir = settings.ORDER_HISTORY_DIR
    if not order_dir.exists():
        return {"daily_pnl": [], "total_trades": 0}

    daily_summary = []
    try:
        csv_files = sorted(order_dir.glob("order_history_*.csv"))
        for csv_file in csv_files:
            try:
                date_str = csv_file.stem.replace("order_history_", "")
                df = pd.read_csv(csv_file)
                trade_count = len(df)
                daily_summary.append({
                    "date": f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}",
                    "trades": trade_count,
                    "orders": df.to_dict(orient="records"),
                })
            except Exception:
                continue

        total_trades = sum(d["trades"] for d in daily_summary)
        return {"daily_pnl": daily_summary, "total_trades": total_trades}
    except Exception as e:
        return {"daily_pnl": [], "total_trades": 0, "error": str(e)}


@app.get("/api/candles")
def get_candle_counts():
    """Current candle counts across timeframes."""
    return shared_state.get_snapshot()["candles"]


@app.post("/api/kill")
def kill_switch():
    """Activate kill switch to stop all trading."""
    shared_state.set_kill_switch(True)
    return {"status": "kill_switch_activated", "message": "Trading will stop after current tick processing."}


@app.post("/api/resume")
def resume_trading():
    """Deactivate kill switch to resume trading."""
    shared_state.set_kill_switch(False)
    return {"status": "kill_switch_deactivated", "message": "Trading resumed."}


# ──────────────────────────── WEBSOCKET ────────────────────────────


class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, data: dict):
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(data)
            except Exception:
                disconnected.append(connection)
        for conn in disconnected:
            self.active_connections.remove(conn)


ws_manager = ConnectionManager()


@app.websocket("/ws/live")
async def websocket_live(websocket: WebSocket):
    """WebSocket endpoint for real-time price and state streaming."""
    await ws_manager.connect(websocket)
    try:
        while True:
            snapshot = shared_state.get_snapshot()
            snapshot["prices"] = {
                "spot_ltp": shared_state.spot_ltp,
                "fut_ltp": shared_state.fut_ltp,
                "premium": round(shared_state.fut_ltp - shared_state.spot_ltp, 2) if shared_state.fut_ltp else 0,
            }
            await websocket.send_json(snapshot)
            await asyncio.sleep(1)  # Send updates every 1 second
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception:
        ws_manager.disconnect(websocket)


# ──────────────────────────── SERVER STARTUP ────────────────────────────


def start_api_server(host="0.0.0.0", port=8000):
    """Start the FastAPI server in a background thread."""
    import uvicorn

    def _run():
        uvicorn.run(app, host=host, port=port, log_level="warning")

    thread = threading.Thread(target=_run, daemon=True, name="api-server")
    thread.start()
    return thread
