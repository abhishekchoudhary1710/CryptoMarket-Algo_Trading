"""
Dual live-feed ingestion and unified multi-timeframe signal pipeline.

Sources:
- Primary: XAUUSD from OANDA pricing endpoint
- Secondary: MCX GOLD futures from Angel One websocket
"""

from __future__ import annotations

import csv
import os
import queue
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from threading import Lock
from typing import Any

from brokers.angelone import AngelOneBroker
from config import settings
from core.execution_interface import NoOpExecutionClient
from core.oanda_client import OandaPricingClient
from data.futures import resolve_gold_mcx_futures_token
from data.ohlcv import LiveOHLCVData
from utils.logger import logger


@dataclass
class NormalizedTick:
    source: str
    symbol: str
    ltp: float
    timestamp: datetime
    token: str | None = None
    raw: Any = None


def parse_timeframes(raw: str) -> list[int]:
    values: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            minute = int(part)
        except ValueError:
            continue
        if minute <= 0:
            continue
        values.append(minute)
    deduped = sorted(set(values))
    return deduped or [1, 3, 5, 10, 15]


class SignalJournal:
    def __init__(self, path: str) -> None:
        self.path = path
        self._ensure_file()

    def _ensure_file(self) -> None:
        parent = os.path.dirname(self.path)
        if parent:
            os.makedirs(parent, exist_ok=True)

        if os.path.exists(self.path):
            return

        with open(self.path, "w", newline="", encoding="utf-8") as fp:
            writer = csv.writer(fp)
            writer.writerow(
                [
                    "event_time",
                    "event_type",
                    "direction",
                    "pivot_time",
                    "xau_value",
                    "mcx_value",
                    "entry_level",
                    "stop_level",
                    "message",
                ]
            )

    def write(
        self,
        event_type: str,
        direction: str,
        pivot_time: datetime | None,
        xau_value: float | None,
        mcx_value: float | None,
        entry_level: float | None,
        stop_level: float | None,
        message: str,
    ) -> None:
        with open(self.path, "a", newline="", encoding="utf-8") as fp:
            writer = csv.writer(fp)
            writer.writerow(
                [
                    datetime.now().isoformat(),
                    event_type,
                    direction,
                    pivot_time.isoformat() if pivot_time else "",
                    f"{xau_value:.6f}" if xau_value is not None else "",
                    f"{mcx_value:.6f}" if mcx_value is not None else "",
                    f"{entry_level:.6f}" if entry_level is not None else "",
                    f"{stop_level:.6f}" if stop_level is not None else "",
                    message,
                ]
            )


class DivergenceSignalEngine:
    """Phase-1 signal engine: divergence on 10m, entry trigger on 3m."""

    def __init__(
        self,
        divergence_tf: int,
        entry_tf: int,
        signal_max_age_minutes: int,
        journal: SignalJournal,
    ) -> None:
        self.divergence_tf = divergence_tf
        self.entry_tf = entry_tf
        self.signal_max_age = timedelta(minutes=signal_max_age_minutes)
        self.journal = journal

        self._low_pivots: list[dict[str, Any]] = []
        self._high_pivots: list[dict[str, Any]] = []
        self._processed_low_times: set[datetime] = set()
        self._processed_high_times: set[datetime] = set()
        self._active_setup: dict[str, Any] | None = None

    @staticmethod
    def _last_confirmed_pivot(candles: list[dict[str, Any]], field: str, mode: str) -> dict[str, Any] | None:
        if len(candles) < 3:
            return None

        prev_candle = candles[-3]
        pivot_candle = candles[-2]
        next_candle = candles[-1]

        pivot_value = float(pivot_candle[field])
        prev_value = float(prev_candle[field])
        next_value = float(next_candle[field])

        if mode == "low" and pivot_value < prev_value and pivot_value < next_value:
            return {"time": pivot_candle["time"], "value": pivot_value}
        if mode == "high" and pivot_value > prev_value and pivot_value > next_value:
            return {"time": pivot_candle["time"], "value": pivot_value}
        return None

    def _mcx_value_for_pivot(self, mcx_candles: list[dict[str, Any]], pivot_time: datetime, mode: str) -> float | None:
        if not mcx_candles:
            return None

        window = {
            pivot_time - timedelta(minutes=self.divergence_tf),
            pivot_time,
            pivot_time + timedelta(minutes=self.divergence_tf),
        }
        selected = [c for c in mcx_candles if c["time"] in window]
        if not selected:
            return None

        if mode == "low":
            return min(float(c["low"]) for c in selected)
        return max(float(c["high"]) for c in selected)

    def _start_setup(self, direction: str, pivot: dict[str, Any], entry_series: LiveOHLCVData) -> None:
        candles = entry_series.completed_candles
        if candles:
            anchor = candles[-1]
            entry_level = float(anchor["high"] if direction == "buy" else anchor["low"])
            stop_level = float(anchor["low"] if direction == "buy" else anchor["high"])
            anchor_time = anchor["time"]
        else:
            entry_level = None
            stop_level = None
            anchor_time = None

        self._active_setup = {
            "direction": direction,
            "pivot_time": pivot["time"],
            "detected_at": datetime.now(),
            "xau_value": float(pivot["xau"]),
            "mcx_value": float(pivot["mcx"]),
            "entry_level": entry_level,
            "stop_level": stop_level,
            "entry_anchor_time": anchor_time,
            "triggered": False,
        }

        logger.info(
            "Divergence detected | direction=%s pivot_time=%s xau=%.4f mcx=%.4f entry_level=%s stop=%s",
            direction,
            pivot["time"],
            pivot["xau"],
            pivot["mcx"],
            f"{entry_level:.4f}" if entry_level is not None else "n/a",
            f"{stop_level:.4f}" if stop_level is not None else "n/a",
        )
        self.journal.write(
            event_type="divergence_detected",
            direction=direction,
            pivot_time=pivot["time"],
            xau_value=pivot["xau"],
            mcx_value=pivot["mcx"],
            entry_level=entry_level,
            stop_level=stop_level,
            message="Divergence setup opened",
        )

    def evaluate_divergence(self, xau_series: LiveOHLCVData, mcx_series: LiveOHLCVData, entry_series: LiveOHLCVData) -> None:
        xau_candles = xau_series.completed_candles
        mcx_candles = mcx_series.completed_candles

        low_pivot = self._last_confirmed_pivot(xau_candles, field="low", mode="low")
        if low_pivot and low_pivot["time"] not in self._processed_low_times:
            self._processed_low_times.add(low_pivot["time"])
            mcx_low = self._mcx_value_for_pivot(mcx_candles, low_pivot["time"], mode="low")
            if mcx_low is not None:
                pivot = {"time": low_pivot["time"], "xau": low_pivot["value"], "mcx": mcx_low}
                self._low_pivots.append(pivot)
                if len(self._low_pivots) >= 2:
                    prev, curr = self._low_pivots[-2], self._low_pivots[-1]
                    if curr["xau"] < prev["xau"] and curr["mcx"] > prev["mcx"]:
                        self._start_setup("buy", curr, entry_series)

        high_pivot = self._last_confirmed_pivot(xau_candles, field="high", mode="high")
        if high_pivot and high_pivot["time"] not in self._processed_high_times:
            self._processed_high_times.add(high_pivot["time"])
            mcx_high = self._mcx_value_for_pivot(mcx_candles, high_pivot["time"], mode="high")
            if mcx_high is not None:
                pivot = {"time": high_pivot["time"], "xau": high_pivot["value"], "mcx": mcx_high}
                self._high_pivots.append(pivot)
                if len(self._high_pivots) >= 2:
                    prev, curr = self._high_pivots[-2], self._high_pivots[-1]
                    if curr["xau"] > prev["xau"] and curr["mcx"] < prev["mcx"]:
                        self._start_setup("sell", curr, entry_series)

    def evaluate_entry_trigger(self, current_price: float) -> dict[str, Any] | None:
        setup = self._active_setup
        if setup is None or setup.get("triggered"):
            return None

        if datetime.now() - setup["detected_at"] > self.signal_max_age:
            logger.info("Setup expired | direction=%s pivot_time=%s", setup["direction"], setup["pivot_time"])
            self.journal.write(
                event_type="setup_expired",
                direction=setup["direction"],
                pivot_time=setup["pivot_time"],
                xau_value=setup["xau_value"],
                mcx_value=setup["mcx_value"],
                entry_level=setup.get("entry_level"),
                stop_level=setup.get("stop_level"),
                message="Signal expired before trigger",
            )
            self._active_setup = None
            return None

        entry_level = setup.get("entry_level")
        if entry_level is None:
            return None

        direction = setup["direction"]
        if direction == "buy" and current_price > float(entry_level):
            setup["triggered"] = True
        elif direction == "sell" and current_price < float(entry_level):
            setup["triggered"] = True

        if not setup["triggered"]:
            return None

        signal = {
            "direction": direction,
            "pivot_time": setup["pivot_time"],
            "entry_level": float(entry_level),
            "stop_level": float(setup["stop_level"]) if setup["stop_level"] is not None else None,
            "trigger_price": float(current_price),
            "detected_at": setup["detected_at"],
            "triggered_at": datetime.now(),
        }
        logger.info(
            "Entry trigger | direction=%s trigger_price=%.4f entry_level=%.4f pivot_time=%s",
            signal["direction"],
            signal["trigger_price"],
            signal["entry_level"],
            signal["pivot_time"],
        )
        self.journal.write(
            event_type="entry_triggered",
            direction=setup["direction"],
            pivot_time=setup["pivot_time"],
            xau_value=setup["xau_value"],
            mcx_value=setup["mcx_value"],
            entry_level=signal["entry_level"],
            stop_level=signal["stop_level"],
            message=f"Triggered at price {signal['trigger_price']:.6f}",
        )
        self._active_setup = None
        return signal


class MultiTimeframeCandlePipeline:
    """Maintains per-source candle series across multiple timeframes."""

    def __init__(self, timeframes: list[int], signal_engine: DivergenceSignalEngine) -> None:
        self.timeframes = sorted(set(timeframes))
        self.signal_engine = signal_engine

        self.series: dict[str, dict[int, LiveOHLCVData]] = {
            "xauusd": {},
            "mcx_gold_fut": {},
        }
        self._lock = Lock()

        self._last_tick_time: dict[str, datetime | None] = {
            "xauusd": None,
            "mcx_gold_fut": None,
        }

        for source in self.series:
            for tf in self.timeframes:
                series_name = f"{source}_{tf}m"
                self.series[source][tf] = LiveOHLCVData(
                    timeframe_minutes=tf,
                    name=series_name,
                    on_candle_close=self._on_candle_close,
                )

        for s in self.series["xauusd"].values():
            s.is_spot_data = True

    def _source_from_name(self, name: str) -> str | None:
        if name.startswith("xauusd"):
            return "xauusd"
        if name.startswith("mcx_gold_fut"):
            return "mcx_gold_fut"
        return None

    def _on_candle_close(self, tf: int, name: str) -> None:
        source = self._source_from_name(name)
        if not source:
            return

        series = self.series[source].get(tf)
        if not series or not series.completed_candles:
            return

        closed = series.completed_candles[-1]
        logger.info(
            "Candle close | source=%s tf=%sm time=%s o=%.2f h=%.2f l=%.2f c=%.2f v=%s",
            source,
            tf,
            closed["time"],
            closed["open"],
            closed["high"],
            closed["low"],
            closed["close"],
            closed["volume"],
        )

        if source == "xauusd" and tf == self.signal_engine.divergence_tf:
            xau_div = self.series["xauusd"][self.signal_engine.divergence_tf]
            mcx_div = self.series["mcx_gold_fut"][self.signal_engine.divergence_tf]
            xau_entry = self.series["xauusd"][self.signal_engine.entry_tf]
            self.signal_engine.evaluate_divergence(xau_div, mcx_div, xau_entry)

    def ingest_tick(self, tick: NormalizedTick) -> None:
        with self._lock:
            bucket = self.series.get(tick.source)
            if not bucket:
                return
            for tf in self.timeframes:
                bucket[tf].update_from_tick(tick.ltp, tick.timestamp)
            self._last_tick_time[tick.source] = tick.timestamp

    def latest_close(self, source: str, tf: int) -> float | None:
        with self._lock:
            bucket = self.series.get(source)
            if not bucket or tf not in bucket:
                return None
            series = bucket[tf]
            if series.current_candle:
                return float(series.current_candle["close"])
            if series.completed_candles:
                return float(series.completed_candles[-1]["close"])
            return None

    def stale_seconds(self, source: str) -> float | None:
        with self._lock:
            ts = self._last_tick_time.get(source)
            if ts is None:
                return None
            return (datetime.now(ts.tzinfo) - ts).total_seconds()


class PrimaryOandaTickSource:
    """OANDA pricing source for XAUUSD."""

    def __init__(self) -> None:
        self.client = OandaPricingClient(
            api_token=settings.OANDA_API_TOKEN,
            account_id=settings.OANDA_ACCOUNT_ID,
            instrument=settings.OANDA_INSTRUMENT,
            env=settings.OANDA_ENV,
            timeout_seconds=settings.OANDA_REQUEST_TIMEOUT_SECONDS,
        )

    def connect(self) -> None:
        self.client.connect()
        logger.info(
            "Primary feed connected | provider=oanda env=%s instrument=%s",
            settings.OANDA_ENV,
            settings.OANDA_INSTRUMENT,
        )

    def fetch_tick(self) -> NormalizedTick | None:
        price_tick = self.client.fetch_latest_price()
        if price_tick is None:
            return None

        return NormalizedTick(
            source="xauusd",
            symbol=price_tick.instrument,
            ltp=float(price_tick.mid),
            timestamp=price_tick.timestamp,
            raw=price_tick.raw,
        )

    def close(self) -> None:
        return


class MCXGoldTickSource:
    """Angel One websocket source for MCX GOLD futures ticks."""

    def __init__(self, broker: AngelOneBroker) -> None:
        self.broker = broker
        self._queue: queue.SimpleQueue[NormalizedTick] = queue.SimpleQueue()
        self.token: str | None = None
        self.symbol: str = "MCX_GOLD_FUT"

    def connect(self) -> None:
        if not self.broker.connect():
            raise RuntimeError("Angel One login failed for MCX source")

        scripmaster = self.broker.fetch_scripmaster_data()
        if not scripmaster:
            raise RuntimeError("Could not fetch ScripMaster data")

        contract = resolve_gold_mcx_futures_token(scripmaster)
        if not contract:
            raise RuntimeError("Could not resolve MCX GOLD futures token")

        self.token = str(contract["token"])
        self.symbol = str(contract.get("symbol") or self.symbol)

        def on_ws_data(message: dict[str, Any] | str) -> None:
            tick = self.broker.extract_ltp(message, expected_token=self.token)
            if not tick:
                return
            self._queue.put(
                NormalizedTick(
                    source="mcx_gold_fut",
                    symbol=self.symbol,
                    token=self.token,
                    ltp=float(tick["ltp"]),
                    timestamp=datetime.now(),
                    raw=tick.get("raw", message),
                )
            )

        ws = self.broker.start_websocket(
            token_list=[{"exchangeType": settings.MCX_WS_EXCHANGE_TYPE, "tokens": [self.token]}],
            on_data_callback=on_ws_data,
            correlation_id="xauusd_mcx_dual_feed",
            mode=1,
        )
        if ws is None:
            raise RuntimeError("Failed to start Angel websocket for MCX source")

        logger.info("MCX feed connected | symbol=%s token=%s", self.symbol, self.token)

    def drain_ticks(self) -> list[NormalizedTick]:
        ticks: list[NormalizedTick] = []
        while True:
            try:
                ticks.append(self._queue.get_nowait())
            except queue.Empty:
                break
        return ticks

    def close(self) -> None:
        self.broker.close()


def run_dual_feed_pipeline() -> None:
    timeframes = parse_timeframes(settings.CANDLE_TIMEFRAMES_MINUTES)
    journal = SignalJournal(settings.SIGNAL_JOURNAL_PATH)
    signal_engine = DivergenceSignalEngine(
        divergence_tf=settings.DIVERGENCE_TIMEFRAME_MINUTES,
        entry_tf=settings.ENTRY_TIMEFRAME_MINUTES,
        signal_max_age_minutes=settings.SIGNAL_MAX_AGE_MINUTES,
        journal=journal,
    )
    pipeline = MultiTimeframeCandlePipeline(timeframes=timeframes, signal_engine=signal_engine)

    primary = PrimaryOandaTickSource()
    mcx = MCXGoldTickSource(AngelOneBroker())
    execution_client = NoOpExecutionClient()

    heartbeat_interval = max(1, int(settings.DUAL_FEED_HEARTBEAT_SECONDS))
    stale_warn_seconds = max(1, int(settings.STALE_FEED_WARN_SECONDS))
    poll_seconds = max(0.2, float(settings.PRIMARY_TICK_POLL_SECONDS))

    last_heartbeat = 0.0
    logger.info(
        "Dual-feed pipeline starting | provider=%s primary=%s timeframes=%s divergence_tf=%sm entry_tf=%sm",
        settings.PRIMARY_PROVIDER,
        settings.PRIMARY_SYMBOL,
        timeframes,
        settings.DIVERGENCE_TIMEFRAME_MINUTES,
        settings.ENTRY_TIMEFRAME_MINUTES,
    )

    primary.connect()
    mcx.connect()

    try:
        while True:
            try:
                primary_tick = primary.fetch_tick()
                if primary_tick:
                    pipeline.ingest_tick(primary_tick)
            except Exception as exc:
                logger.error("Primary tick fetch failed: %s", exc)

            for tick in mcx.drain_ticks():
                pipeline.ingest_tick(tick)

            xau_last = pipeline.latest_close("xauusd", 1)
            if xau_last is not None:
                signal = signal_engine.evaluate_entry_trigger(xau_last)
                if signal:
                    _ = execution_client.place_order(
                        side=signal["direction"],
                        quantity=1.0,
                        sl=signal["stop_level"],
                        tp=None,
                    )

            now = time.monotonic()
            if now - last_heartbeat >= heartbeat_interval:
                last_heartbeat = now
                xau = pipeline.latest_close("xauusd", 1)
                mcx_ltp = pipeline.latest_close("mcx_gold_fut", 1)
                xau_stale = pipeline.stale_seconds("xauusd")
                mcx_stale = pipeline.stale_seconds("mcx_gold_fut")

                logger.info(
                    "Heartbeat | xauusd_1m_close=%s stale=%ss mcx_1m_close=%s stale=%ss",
                    f"{xau:.2f}" if xau is not None else "n/a",
                    f"{xau_stale:.0f}" if xau_stale is not None else "n/a",
                    f"{mcx_ltp:.2f}" if mcx_ltp is not None else "n/a",
                    f"{mcx_stale:.0f}" if mcx_stale is not None else "n/a",
                )
                if xau_stale is not None and xau_stale >= stale_warn_seconds:
                    logger.warning("XAUUSD feed appears stale for %.0fs", xau_stale)
                if mcx_stale is not None and mcx_stale >= stale_warn_seconds:
                    logger.warning("MCX feed appears stale for %.0fs", mcx_stale)

            time.sleep(poll_seconds)
    finally:
        primary.close()
        mcx.close()
        logger.info("Dual-feed pipeline stopped")
