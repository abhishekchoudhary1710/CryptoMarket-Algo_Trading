"""
Swing-structure strategy (H1-L1-A-B-C-D pattern).

Detects both bullish and bearish swing setups and emits a signal dict
when price breaks above point D (bullish) or below point D (bearish).
"""

from __future__ import annotations

import logging
from typing import Dict, Optional

import pandas as pd

from config.settings import SwingStructureConfig
from strategies.base import BaseStrategy

log = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════════
#  Swing-point detection helpers
# ════════════════════════════════════════════════════════════════════

def _is_swing_high(df: pd.DataFrame, idx: int) -> bool:
    if idx <= 0 or idx >= len(df) - 1:
        return False

    cur = df.iloc[idx]["high"]
    prev_h = df.iloc[idx - 1]["high"]
    next_h = df.iloc[idx + 1]["high"]

    i = idx - 1
    while i >= 0 and df.iloc[i]["high"] == cur:
        if i > 0:
            prev_h = df.iloc[i - 1]["high"]
        i -= 1

    i = idx + 1
    while i < len(df) and df.iloc[i]["high"] == cur:
        if i < len(df) - 1:
            next_h = df.iloc[i + 1]["high"]
        i += 1

    return cur > prev_h and cur > next_h


def _is_swing_low(df: pd.DataFrame, idx: int) -> bool:
    if idx <= 0 or idx >= len(df) - 1:
        return False

    cur = df.iloc[idx]["low"]
    prev_l = df.iloc[idx - 1]["low"]
    next_l = df.iloc[idx + 1]["low"]

    i = idx - 1
    while i >= 0 and df.iloc[i]["low"] == cur:
        if i > 0:
            prev_l = df.iloc[i - 1]["low"]
        i -= 1

    i = idx + 1
    while i < len(df) and df.iloc[i]["low"] == cur:
        if i < len(df) - 1:
            next_l = df.iloc[i + 1]["low"]
        i += 1

    return cur < prev_l and cur < next_l


# ════════════════════════════════════════════════════════════════════
#  Bullish swing state machine
# ════════════════════════════════════════════════════════════════════

class _BullishState:
    """Tracks L1 → H1 → A → B → C → D for bullish breakout."""

    def __init__(self, cfg: SwingStructureConfig) -> None:
        self.cfg = cfg
        self.L1 = self.H1 = self.A = self.B = self.C = self.D = None
        self.L1_idx = self.H1_idx = self.A_idx = self.B_idx = self.C_idx = self.D_idx = None
        self.pending_setup: Optional[Dict[str, float]] = None

    def _reset(self, points: list[str]) -> None:
        for p in points:
            setattr(self, p, None)
            setattr(self, f"{p}_idx", None)
            if p == "D":
                self.pending_setup = None

    def _init_l1(self, df: pd.DataFrame) -> None:
        if self.L1 is None and len(df) > 0:
            self.L1 = float(df.iloc[0]["low"])
            self.L1_idx = 0
            self.H1 = None
            self.H1_idx = None

    def _calc_d(self, df: pd.DataFrame) -> None:
        if self.H1 is None or self.B is None or self.C is None:
            return
        highest = float(df.iloc[self.B_idx]["high"])
        highest_idx = self.B_idx
        for i in range(self.B_idx + 1, self.C_idx + 1):
            cur = float(df.iloc[i]["high"])
            if cur > highest:
                highest = cur
                highest_idx = i
        self.D, self.D_idx = highest, highest_idx

        entry = self.D * (1 + self.cfg.entry_buffer_pct / 100.0)
        sl = self.C * (1 - self.cfg.entry_buffer_pct / 100.0)
        risk = entry - sl
        tp = entry + (risk * self.cfg.rr_ratio)
        self.pending_setup = {"entry": entry, "sl": sl, "tp": tp}

    def process_bar(self, df: pd.DataFrame, i: int) -> Optional[Dict[str, float]]:
        if i <= 0 or i >= len(df):
            return None

        self._init_l1(df)
        current = df.iloc[i]
        prev = df.iloc[i - 1]

        # ── update L1 (deepest swing low) ───────────────────────────
        if _is_swing_low(df, i) and current["low"] < self.L1:
            self.L1 = float(current["low"])
            self.L1_idx = i
            self._reset(["H1", "A", "B", "C", "D"])

        # ── update H1 ──────────────────────────────────────────────
        if self.L1 is not None and _is_swing_high(df, i):
            if i > self.L1_idx and current["high"] > self.L1:
                if self.H1 is None or current["high"] > self.H1:
                    self.H1 = float(current["high"])
                    self.H1_idx = i
                    self._reset(["A", "B", "C", "D"])

        # ── update A ──────────────────────────────────────────────
        if self.H1 is not None and self.L1 is not None:
            if self.A is None and _is_swing_low(df, i):
                if i > self.H1_idx and current["low"] > self.L1:
                    self.A = float(current["low"])
                    self.A_idx = i
            elif self.A is not None and _is_swing_low(df, i):
                if i > self.A_idx and current["low"] < self.A and current["low"] > self.L1:
                    self.A = float(current["low"])
                    self.A_idx = i
                    self._reset(["B", "C", "D"])

        # ── update B ──────────────────────────────────────────────
        if self.A is not None:
            if self.B is None and _is_swing_low(df, i):
                if i > self.A_idx + 1 and current["low"] > self.A:
                    self.B = float(current["low"])
                    self.B_idx = i
            elif self.B is not None and _is_swing_low(df, i):
                if i > self.B_idx and current["low"] < self.B and current["low"] > self.A:
                    self.B = float(current["low"])
                    self.B_idx = i
                    self._reset(["C", "D"])

        # ── update C & D, check breakout ──────────────────────────
        if self.B is not None:
            if self.C is None:
                if i > self.B_idx and current["low"] < prev["low"] and current["low"] > self.B:
                    self.C = float(current["low"])
                    self.C_idx = i
                    self._calc_d(df)
            else:
                if current["low"] < self.C and current["low"] > self.B:
                    self.C = float(current["low"])
                    self.C_idx = i
                    self._calc_d(df)

            # ── breakout above D ──────────────────────────────────
            if self.pending_setup is not None and current["high"] > self.D:
                signal = {
                    "side": "buy",
                    "entry": self.pending_setup["entry"],
                    "sl": self.pending_setup["sl"],
                    "tp": self.pending_setup["tp"],
                    "reason": f"bullish_breakout_D_{self.D:.3f}",
                }
                self._reset(["H1", "L1", "A", "B", "C", "D"])
                self.L1 = float(current["low"])
                self.L1_idx = i
                return signal

            # ── invalidation ──────────────────────────────────────
            if self.pending_setup is not None:
                if current["low"] <= self.B:
                    self._reset(["B", "C", "D"])
                if current["low"] <= self.A:
                    self._reset(["A", "B", "C", "D"])
                if current["low"] <= self.L1:
                    self._reset(["L1", "H1", "A", "B", "C", "D"])
                    self.L1 = float(current["low"])
                    self.L1_idx = i
            elif self.C is not None:
                if current["low"] <= self.B:
                    self._reset(["B", "C"])
                if current["low"] <= self.A:
                    self._reset(["A", "B", "C"])
                if current["low"] <= self.L1:
                    self._reset(["L1", "H1", "A", "B", "C"])
                    self.L1 = float(current["low"])
                    self.L1_idx = i
            elif self.B is not None and current["low"] <= self.A:
                self._reset(["A", "B", "C"])
            elif self.A is not None and current["low"] <= self.L1:
                self._reset(["L1", "H1", "A", "B", "C"])
                self.L1 = float(current["low"])
                self.L1_idx = i

        return None


# ════════════════════════════════════════════════════════════════════
#  Bearish swing state machine
# ════════════════════════════════════════════════════════════════════

class _BearishState:
    """Tracks H1 → L1 → A → B → C → D for bearish breakout."""

    def __init__(self, cfg: SwingStructureConfig) -> None:
        self.cfg = cfg
        self.H1 = self.L1 = self.A = self.B = self.C = self.D = None
        self.H1_idx = self.L1_idx = self.A_idx = self.B_idx = self.C_idx = self.D_idx = None
        self.pending_setup: Optional[Dict[str, float]] = None

    def _reset(self, points: list[str]) -> None:
        for p in points:
            setattr(self, p, None)
            setattr(self, f"{p}_idx", None)
            if p == "D":
                self.pending_setup = None

    def _init_h1(self, df: pd.DataFrame) -> None:
        if self.H1 is None and len(df) > 0:
            self.H1 = float(df.iloc[0]["high"])
            self.H1_idx = 0
            self.L1 = None
            self.L1_idx = None

    def _calc_d(self, df: pd.DataFrame) -> None:
        if self.H1 is None or self.L1 is None or self.A is None or self.B is None or self.C is None:
            return
        lowest = float(df.iloc[self.B_idx]["low"])
        lowest_idx = self.B_idx
        for i in range(self.B_idx + 1, self.C_idx + 1):
            cur = float(df.iloc[i]["low"])
            if cur < lowest:
                lowest = cur
                lowest_idx = i
        self.D, self.D_idx = lowest, lowest_idx

        entry = self.D * (1 - self.cfg.entry_buffer_pct / 100.0)
        sl = self.C * (1 + self.cfg.entry_buffer_pct / 100.0)
        risk = sl - entry
        tp = entry - (risk * self.cfg.rr_ratio)
        self.pending_setup = {"entry": entry, "sl": sl, "tp": tp}

    def process_bar(self, df: pd.DataFrame, i: int) -> Optional[Dict[str, float]]:
        if i <= 0 or i >= len(df):
            return None

        self._init_h1(df)
        current = df.iloc[i]
        prev = df.iloc[i - 1]

        # ── update H1 (highest swing high) ──────────────────────────
        if _is_swing_high(df, i) and (self.H1 is None or current["high"] > self.H1):
            self.H1 = float(current["high"])
            self.H1_idx = i
            self._reset(["L1", "A", "B", "C", "D"])

        # ── update L1 ──────────────────────────────────────────────
        if self.H1 is not None and _is_swing_low(df, i):
            if i > self.H1_idx and current["low"] < self.H1:
                if self.L1 is None or current["low"] < self.L1:
                    self.L1 = float(current["low"])
                    self.L1_idx = i
                    self._reset(["A", "B", "C", "D"])

        # ── update A ──────────────────────────────────────────────
        if self.H1 is not None and self.L1 is not None:
            if self.A is None and _is_swing_high(df, i):
                if i > self.L1_idx and current["high"] < self.H1:
                    self.A = float(current["high"])
                    self.A_idx = i
            elif self.A is not None and _is_swing_high(df, i):
                if i > self.A_idx and current["high"] > self.A and current["high"] < self.H1:
                    self.A = float(current["high"])
                    self.A_idx = i
                    self._reset(["B", "C", "D"])

        # ── update B ──────────────────────────────────────────────
        if self.A is not None:
            if self.B is None and _is_swing_high(df, i):
                if i > self.A_idx + 1 and current["high"] < self.A:
                    self.B = float(current["high"])
                    self.B_idx = i
            elif self.B is not None and _is_swing_high(df, i):
                if i > self.B_idx and current["high"] > self.B and current["high"] < self.A:
                    self.B = float(current["high"])
                    self.B_idx = i
                    self._reset(["C", "D"])

        # ── update C & D, check breakout ──────────────────────────
        if self.B is not None:
            if self.C is None:
                if i > self.B_idx and current["high"] > prev["high"] and current["high"] < self.B:
                    self.C = float(current["high"])
                    self.C_idx = i
                    self._calc_d(df)
            else:
                if current["high"] > self.C and current["high"] < self.B:
                    self.C = float(current["high"])
                    self.C_idx = i
                    self._calc_d(df)

            # ── breakout below D ──────────────────────────────────
            if self.pending_setup is not None and current["low"] < self.D:
                signal = {
                    "side": "sell",
                    "entry": self.pending_setup["entry"],
                    "sl": self.pending_setup["sl"],
                    "tp": self.pending_setup["tp"],
                    "reason": f"bearish_breakout_D_{self.D:.3f}",
                }
                self._reset(["H1", "L1", "A", "B", "C", "D"])
                self.H1 = float(current["high"])
                self.H1_idx = i
                return signal

            # ── invalidation ──────────────────────────────────────
            if self.pending_setup is not None:
                if self.H1 is not None and current["high"] >= self.H1:
                    self._reset(["H1", "L1", "A", "B", "C", "D"])
                    self.H1 = float(current["high"])
                    self.H1_idx = i
                elif self.A is not None and current["high"] >= self.A:
                    self._reset(["A", "B", "C", "D"])
                elif self.B is not None and current["high"] >= self.B:
                    self._reset(["B", "C", "D"])
            elif self.C is not None:
                if self.H1 is not None and current["high"] >= self.H1:
                    self._reset(["H1", "L1", "A", "B", "C"])
                    self.H1 = float(current["high"])
                    self.H1_idx = i
                elif self.A is not None and current["high"] >= self.A:
                    self._reset(["A", "B", "C"])
                elif self.B is not None and current["high"] >= self.B:
                    self._reset(["B", "C"])
            elif self.B is not None and current["high"] >= self.A:
                self._reset(["A", "B"])
            elif self.A is not None and current["high"] >= self.H1:
                self._reset(["H1", "L1", "A"])
                self.H1 = float(current["high"])
                self.H1_idx = i

        return None


# ════════════════════════════════════════════════════════════════════
#  Public strategy class
# ════════════════════════════════════════════════════════════════════

class SwingStructureStrategy(BaseStrategy):
    """
    Runs both bullish and bearish swing-structure state machines in
    parallel.  Bootstraps on first call, then processes only new
    closed bars.
    """

    def __init__(self, cfg: SwingStructureConfig) -> None:
        self.cfg = cfg
        self._bull = _BullishState(cfg)
        self._bear = _BearishState(cfg)
        self._last_closed_time = None
        self._bootstrapped = False

    def _process_idx(self, df: pd.DataFrame, i: int) -> Optional[Dict[str, float]]:
        bull_sig = self._bull.process_bar(df, i)
        bear_sig = self._bear.process_bar(df, i)

        if bull_sig and bear_sig:
            log.warning("Conflicting bull+bear signals on same bar — skipping.")
            return None
        return bull_sig or bear_sig

    def _bootstrap(self, df: pd.DataFrame) -> None:
        if len(df) < 3:
            return
        for i in range(1, len(df) - 1):
            self._process_idx(df, i)
        self._last_closed_time = df.iloc[-2]["time"]
        self._bootstrapped = True
        log.info("Swing engine bootstrapped with %d bars.", len(df) - 1)

    # ── BaseStrategy interface ───────────────────────────────────────
    def next_signal(self, df: pd.DataFrame) -> Optional[Dict[str, float]]:
        if len(df) < 3:
            return None

        closed_idx = len(df) - 2
        closed_time = df.iloc[closed_idx]["time"]

        if not self._bootstrapped:
            self._bootstrap(df)
            return None

        if closed_time == self._last_closed_time:
            return None

        signal = self._process_idx(df, closed_idx)
        self._last_closed_time = closed_time
        return signal
