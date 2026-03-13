"""
Thread-safe shared state between the trading bot and the API server.
The bot writes state, the API reads it.
"""

import threading
from datetime import datetime


class SharedState:
    """Singleton shared state accessible by both bot and API."""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._data_lock = threading.Lock()

        # Bot status
        self.bot_running = False
        self.bot_start_time = None
        self.kill_switch = False

        # Prices
        self.spot_ltp = 0.0
        self.fut_ltp = 0.0
        self.last_tick_time = None

        # Strategy states
        self.strategies = {
            "bullish_divergence": {"active": False, "active_divergences": [], "entry_setups": ""},
            "bearish_divergence": {"active": False, "active_divergences": [], "entry_setups": ""},
        }

        # Orders placed today
        self.today_orders = []

        # Signals detected today
        self.today_signals = []

        # Candle counts
        self.spot_1m_candles = 0
        self.spot_5m_candles = 0
        self.fut_1m_candles = 0
        self.fut_5m_candles = 0

        # Tick count
        self.tick_count = 0

    def update_prices(self, spot_ltp, fut_ltp, tick_count):
        with self._data_lock:
            self.spot_ltp = spot_ltp
            self.fut_ltp = fut_ltp
            self.tick_count = tick_count
            self.last_tick_time = datetime.now()

    def update_strategies(self, engine):
        with self._data_lock:
            if hasattr(engine, 'divergence_strategy') and engine.divergence_strategy:
                strat = engine.divergence_strategy
                divs = self._serialize_divergences(strat)
                entry_summaries = self._collect_entry_summaries_bullish(strat)
                pivot_data = self._collect_pivot_data(strat, pivot_type='lows')
                first_candle = self._serialize_first_candle(strat, direction='bullish')
                self.strategies["bullish_divergence"] = {
                    "active": len(divs) > 0,
                    "active_divergences": divs,
                    "entry_setups": entry_summaries,
                    "pivots": pivot_data,
                    "first_candle": first_candle,
                }
            if hasattr(engine, 'bearish_divergence_strategy') and engine.bearish_divergence_strategy:
                strat = engine.bearish_divergence_strategy
                divs = self._serialize_divergences(strat)
                entry_summaries = self._collect_entry_summaries_bearish(strat)
                pivot_data = self._collect_pivot_data(strat, pivot_type='highs')
                first_candle = self._serialize_first_candle(strat, direction='bearish')
                self.strategies["bearish_divergence"] = {
                    "active": len(divs) > 0,
                    "active_divergences": divs,
                    "entry_setups": entry_summaries,
                    "pivots": pivot_data,
                    "first_candle": first_candle,
                }

    def update_candle_counts(self, engine):
        with self._data_lock:
            self.spot_1m_candles = len(engine.spot_series["1m"].completed_candles)
            self.spot_5m_candles = len(engine.spot_series["5m"].completed_candles)
            self.fut_1m_candles = len(engine.fut_series["1m"].completed_candles)
            self.fut_5m_candles = len(engine.fut_series["5m"].completed_candles)

    def add_order(self, order_record):
        with self._data_lock:
            self.today_orders.append(order_record)

    def add_signal(self, signal_record):
        with self._data_lock:
            self.today_signals.append(signal_record)

    def set_kill_switch(self, value):
        with self._data_lock:
            self.kill_switch = value

    def is_killed(self):
        with self._data_lock:
            return self.kill_switch

    def get_snapshot(self):
        with self._data_lock:
            return {
                "bot_running": self.bot_running,
                "bot_start_time": str(self.bot_start_time) if self.bot_start_time else None,
                "kill_switch": self.kill_switch,
                "spot_ltp": self.spot_ltp,
                "fut_ltp": self.fut_ltp,
                "last_tick_time": str(self.last_tick_time) if self.last_tick_time else None,
                "strategies": dict(self.strategies),
                "today_orders_count": len(self.today_orders),
                "today_signals_count": len(self.today_signals),
                "tick_count": self.tick_count,
                "candles": {
                    "spot_1m": self.spot_1m_candles,
                    "spot_5m": self.spot_5m_candles,
                    "fut_1m": self.fut_1m_candles,
                    "fut_5m": self.fut_5m_candles,
                },
            }

    @staticmethod
    def _serialize_setup(setup):
        if not setup:
            return None
        return {
            "entry_price": setup.get("entry_price"),
            "stop_loss": setup.get("stop_loss"),
            "target": setup.get("target"),
        }

    @staticmethod
    def _serialize_divergences(strat):
        divs = []
        for d in getattr(strat, 'active_divergences', []):
            divs.append({
                "pivot_number": d.get('pivot_number'),
                "divergence_type": d.get('divergence_type'),
                "start_time": str(d.get('start_time', '')),
                "candle_time": str(d.get('candle_time', '')),
                "spot_broken": d.get('spot_broken', False),
                "fut_broken": d.get('fut_broken', False),
            })
        return divs

    @staticmethod
    def _collect_entry_summaries_bearish(strat):
        summaries = {}
        for name, attr in [
            ("green_candle", "green_candle_manager"),
            ("pullback", "entry_manager"),
            ("mother_child", "mother_child_manager"),
            ("three_top", "three_top_manager"),
        ]:
            mgr = getattr(strat, attr, None)
            if mgr:
                summaries[name] = mgr.get_active_setups_summary()
        return summaries

    @staticmethod
    def _collect_entry_summaries_bullish(strat):
        summaries = {}
        for name, attr in [
            ("red_candle", "entry_manager"),
            ("pullback", "pullback_manager"),
            ("mother_child", "mother_child_manager"),
            ("three_bottom", "three_bottom_manager"),
        ]:
            mgr = getattr(strat, attr, None)
            if mgr:
                summaries[name] = mgr.get_active_setups_summary()
        return summaries

    @staticmethod
    def _collect_pivot_data(strat, pivot_type='highs'):
        data = {"total": 0, "unbroken": [], "broken": []}
        spot_5m = getattr(strat, 'spot_5m', None)
        if not spot_5m:
            return data

        if pivot_type == 'highs':
            all_pivots = getattr(spot_5m, 'pivot_highs', [])
            broken_list = getattr(spot_5m, 'broken_pivot_highs', [])
            broken_times = {b['pivot_time'] for b in broken_list}
        else:
            all_pivots = getattr(spot_5m, 'pivot_lows', [])
            broken_list = getattr(spot_5m, 'broken_pivot_lows', [])
            broken_times = {b['pivot_time'] for b in broken_list}

        data["total"] = len(all_pivots)
        for p in all_pivots:
            if p.get('removed', False):
                continue
            entry = {
                "pivot_number": p.get('pivot_number', 0),
                "time": str(p.get('time', '')),
                "price": p.get('price', 0),
            }
            if p['time'] in broken_times:
                bp = next((b for b in broken_list if b['pivot_time'] == p['time']), None)
                if bp:
                    if pivot_type == 'highs':
                        entry["breakout_price"] = bp.get('breakout_price', 0)
                        entry["breakout_time"] = str(bp.get('breakout_time', ''))
                    else:
                        entry["breakdown_price"] = bp.get('breakdown_price', 0)
                        entry["breakdown_time"] = str(bp.get('breakdown_time', ''))
                data["broken"].append(entry)
            else:
                data["unbroken"].append(entry)
        return data

    @staticmethod
    def _serialize_first_candle(strat, direction='bearish'):
        fc = getattr(strat, '_first_candle_pivot', None)
        if not fc:
            return None
        result = {
            "time": str(fc.get('time', '')),
            "price": fc.get('price', 0),
            "broken": getattr(strat, '_first_candle_broken', False),
        }
        if direction == 'bearish':
            result["breakdown_price"] = getattr(strat, '_first_candle_breakdown_price', None)
        else:
            result["breakout_price"] = getattr(strat, '_first_candle_breakout_price', None)
        return result


# Global singleton
shared_state = SharedState()
