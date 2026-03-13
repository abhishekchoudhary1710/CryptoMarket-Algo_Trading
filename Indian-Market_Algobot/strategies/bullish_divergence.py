"""
Bullish Divergence Strategy module.
Implements a trading strategy based on spot-futures divergence with red candle entry.

Strategy Logic:
- Detects pivot lows in spot 5-minute data
- Monitors for divergence between spot and futures pivot low breakdown times
- When divergence detected, looks for red candle entry setup
- Enters on breakout above red candle high with stop loss at red candle low
"""

import pandas as pd
import numpy as np
import os
import time
from datetime import datetime, timedelta

from utils.logger import logger, get_strategy_logger
from models.option import OptionData
from models.order_manager import OrderManager
from config import settings


class _PrefixedLogger:
    """Wraps a logger to auto-prefix all messages with a data source tag (e.g. [SPOT], [FUTURES])."""
    def __init__(self, logger, prefix):
        self._logger = logger
        self._prefix = prefix

    def info(self, msg, *args, **kwargs):
        self._logger.info(f"[{self._prefix}] {msg}", *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self._logger.warning(f"[{self._prefix}] {msg}", *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self._logger.error(f"[{self._prefix}] {msg}", *args, **kwargs)

    def debug(self, msg, *args, **kwargs):
        self._logger.debug(f"[{self._prefix}] {msg}", *args, **kwargs)


class RedCandleEntryManager:
    """Manages the red candle entry logic within divergence windows using 1-minute candles"""

    def __init__(self, logger, data_source="SPOT"):
        self.active_entry_setups = []  # Track active entry setups during divergence
        self.data_source = data_source
        self.logger = _PrefixedLogger(logger, data_source) if data_source != "SPOT" else logger

    def add_entry_setup(self, pivot_number, start_time, candle_time=None, prev_candle=None):
        """Add a new entry setup when divergence starts.
        prev_candle: the last completed 1m candle before divergence, used as left neighbor for L1 detection."""
        setup = {
            'pivot_number': pivot_number,
            'start_time': start_time,
            'candle_time': candle_time,
            'setup_candles': [],        # Per-setup 1m candles for L1 detection
            'l1_price': None,           # L1 pivot low price
            'l1_time': None,            # L1 candle time
            'l1_candle': None,          # L1 candle data
            'red_candle': None,  # Will store the red candle details
            'entry_level': None,  # High of red candle
            'stop_loss': None,   # Low of red candle
            'entry_triggered': False,
            'status': 'looking_for_l1'
        }
        # Seed with previous candle so L1 can be detected 1 candle sooner
        if prev_candle is not None:
            setup['setup_candles'].append(prev_candle)
        self.active_entry_setups.append(setup)
        self.logger.info("\n" + "-"*40)
        candle_str = f" (Candle: {candle_time})" if candle_time else ""
        self.logger.info(f"🔍 ENTRY SETUP ADDED for Pivot {pivot_number}{candle_str} 🔍")
        self.logger.info("🔄 Looking for L1 (Pivot Low) before red candle search")
        self.logger.info("-"*40)

    def remove_entry_setup(self, pivot_number):
        """Remove entry setup when divergence ends"""
        # Find the setup to get candle_time before removing
        setup_to_remove = next((s for s in self.active_entry_setups if s['pivot_number'] == pivot_number), None)
        candle_time = setup_to_remove.get('candle_time') if setup_to_remove else None

        self.active_entry_setups = [s for s in self.active_entry_setups if s['pivot_number'] != pivot_number]
        self.logger.info("\n" + "-"*40)
        candle_str = f" (Candle: {candle_time})" if candle_time else ""
        self.logger.info(f"🚫 ENTRY SETUP REMOVED for Pivot {pivot_number}{candle_str} 🚫")
        self.logger.info("-"*40)

    def update_with_new_candle(self, candle, current_price):
        """Update entry setups when a new 1-minute candle is completed"""
        for setup in self.active_entry_setups:
            if setup['status'] == 'looking_for_l1':
                # Skip candles that ended before divergence start
                if candle['time'] + timedelta(minutes=1) <= setup['start_time']:
                    continue

                # Track candles for L1 (pivot low) detection
                setup['setup_candles'].append(candle)

                # Need at least 3 candles: prev + candidate + confirmation
                if len(setup['setup_candles']) >= 3:
                    prev_c = setup['setup_candles'][-3]
                    candidate = setup['setup_candles'][-2]
                    confirm_c = setup['setup_candles'][-1]

                    # Pivot low: candidate low < both neighbors' lows
                    if candidate['low'] < prev_c['low'] and candidate['low'] < confirm_c['low']:
                        # L1 found!
                        setup['l1_price'] = candidate['low']
                        setup['l1_time'] = candidate['time']
                        setup['l1_candle'] = candidate
                        setup['status'] = 'looking_for_red_candle'

                        candle_str = f" (Candle: {setup['candle_time']})" if setup.get('candle_time') else ""
                        self.logger.info("\n" + "=="*25)
                        self.logger.info("📊 RED CANDLE L1 (PIVOT LOW) DETECTED 📊")
                        self.logger.info("=="*25)
                        self.logger.info(f"📊 Pivot {setup['pivot_number']}{candle_str}")
                        self.logger.info(f"  📉 L1 Price: {setup['l1_price']:.2f}")
                        self.logger.info(f"  ⏰ L1 Time: {setup['l1_time']}")
                        self.logger.info(f"  🔴 Now looking for red candle after L1")
                        self.logger.info("=="*25)

            elif setup['status'] == 'looking_for_red_candle':
                # Track candles for L1 break detection
                setup['setup_candles'].append(candle)

                # Check if L1 is broken (candle low < L1) — reset to find new L1
                if candle['low'] < setup['l1_price']:
                    old_l1 = setup['l1_price']
                    setup['l1_price'] = None
                    setup['l1_time'] = None
                    setup['l1_candle'] = None
                    setup['status'] = 'looking_for_l1'
                    # Keep last 2 candles for next L1 detection
                    setup['setup_candles'] = setup['setup_candles'][-2:]
                    candle_str = f" (Candle: {setup['candle_time']})" if setup.get('candle_time') else ""
                    self.logger.info(
                        f"⚠️ RED CANDLE L1 BROKEN for Pivot {setup['pivot_number']}{candle_str}: "
                        f"candle low {candle['low']:.2f} < L1 {old_l1:.2f} — resetting, scanning for new L1"
                    )
                    continue

                # Check if this candle comes after the L1 candle
                if candle['time'] > setup['l1_time']:
                    # Check if this is a red candle
                    if candle['close'] < candle['open']:
                        # This is our red candle!
                        setup['red_candle'] = candle
                        setup['entry_level'] = candle['high']
                        setup['stop_loss'] = candle['low']
                        setup['status'] = 'waiting_for_breakout'

                        self.logger.info("\n" + "=="*25)
                        self.logger.info("🔴 RED CANDLE DETECTED (after L1) 🔴")
                        self.logger.info("=="*25)
                        candle_str = f" (Candle: {setup['candle_time']})" if setup.get('candle_time') else ""
                        self.logger.info(f"📊 Pivot {setup['pivot_number']}{candle_str} - Red candle found! at {candle['time']}")
                        self.logger.info(f"  📊 L1 Reference: {setup['l1_price']:.2f} at {setup['l1_time']}")
                        self.logger.info(f"  ⏰ Red Candle Time: {candle['time']}")
                        self.logger.info(
                            f"  📈 OHLC: O={candle['open']:.2f}, H={candle['high']:.2f}, "
                            f"L={candle['low']:.2f}, C={candle['close']:.2f}"
                        )
                        self.logger.info(f"  ➡️ Entry Level: {setup['entry_level']:.2f}")
                        self.logger.info(f"  🛑 Stop Loss: {setup['stop_loss']:.2f}")
                        self.logger.info(
                            f"  ⚠️ Risk/Reward Ratio: {(setup['entry_level'] - setup['stop_loss']):.2f} points risk"
                        )
                        self.logger.info("=="*25 + "\n")

    def check_for_entry_signals(self, current_price, current_time):
        """Check if current price triggers any entry signals"""
        signals = []

        invalidated_pivots = []

        for setup in self.active_entry_setups:
            if setup['status'] == 'waiting_for_breakout' and not setup['entry_triggered']:
                # L1 BREAK CHECK: if price breaks below L1, reset entire setup
                if setup['l1_price'] is not None and current_price < setup['l1_price']:
                    old_l1 = setup['l1_price']
                    candle_str = f" (Candle: {setup['candle_time']})" if setup.get('candle_time') else ""
                    self.logger.info("\n" + "!!"*25)
                    self.logger.info(f"⚠️ RED CANDLE L1 BROKEN (tick) for Pivot {setup['pivot_number']}{candle_str} ⚠️")
                    self.logger.info(f"  📊 Price {current_price:.2f} broke below L1 {old_l1:.2f}")
                    self.logger.info(f"  🔄 Resetting to look for new L1")
                    self.logger.info("!!"*25 + "\n")
                    # Reset to looking_for_l1
                    setup['l1_price'] = None
                    setup['l1_time'] = None
                    setup['l1_candle'] = None
                    setup['red_candle'] = None
                    setup['entry_level'] = None
                    setup['stop_loss'] = None
                    setup['status'] = 'looking_for_l1'
                    setup['setup_candles'] = setup['setup_candles'][-2:] if len(setup['setup_candles']) >= 2 else setup['setup_candles']
                    continue

                # INVALIDATION: price breaks BELOW red candle LOW (stop loss level)
                # This means red candle entry has become a pullback scenario
                if current_price < setup['stop_loss']:
                    candle_str = f" (Candle: {setup['candle_time']})" if setup.get('candle_time') else ""
                    self.logger.info("\n" + "!!"*25)
                    self.logger.info(f"🚫 RED CANDLE INVALIDATED for Pivot {setup['pivot_number']}{candle_str} 🚫")
                    self.logger.info(f"  📊 Price {current_price:.2f} broke below red candle LOW {setup['stop_loss']:.2f}")
                    self.logger.info(f"  📝 Reason: Red candle entry becomes pullback entry")
                    self.logger.info(f"  ➡️ Pullback, mother-child and 3-bottom entry methods continue")
                    self.logger.info("!!"*25 + "\n")
                    invalidated_pivots.append(setup['pivot_number'])
                    continue

                if current_price > setup['entry_level']:
                    # Entry signal triggered!
                    setup['entry_triggered'] = True
                    setup['status'] = 'entry_triggered'

                    signal = {
                        'type': 'BUY',
                        'data_source': self.data_source,
                        'pivot_number': setup['pivot_number'],
                        'candle_time': setup.get('candle_time'),
                        'entry_price': setup['entry_level'],
                        'current_price': current_price,
                        'stop_loss': setup['stop_loss'],
                        'entry_time': current_time,
                        'red_candle_time': setup['red_candle']['time'],
                        'risk_points': setup['entry_level'] - setup['stop_loss']
                    }
                    signals.append(signal)

                    self.logger.info("\n" + "*"*50)
                    self.logger.info("🔥🔥🔥 ENTRY SIGNAL TRIGGERED 🔥🔥🔥")
                    self.logger.info("*"*50)
                    candle_str = f" (Candle: {setup['candle_time']})" if setup.get('candle_time') else ""
                    self.logger.info(f"🚀 [BUY SIGNAL] ENTRY TRIGGERED for Pivot {setup['pivot_number']}{candle_str}! 🚀")
                    self.logger.info(f"  ➡️ Entry Price: {setup['entry_level']:.2f}")
                    self.logger.info(f"  📊 Current Price: {current_price:.2f}")
                    self.logger.info(f"  🛑 Stop Loss: {setup['stop_loss']:.2f}")
                    self.logger.info(f"  ⚠️ Risk: {signal['risk_points']:.2f} points")
                    self.logger.info(f"  ⏰ Entry Time: {current_time}")
                    self.logger.info(f"  📈 Red Candle: {setup['red_candle']['time']}")
                    self.logger.info("*"*50 + "\n")

        # Remove invalidated setups (only red candle, 3-bottom continues)
        for pivot_num in invalidated_pivots:
            self.active_entry_setups = [s for s in self.active_entry_setups if s['pivot_number'] != pivot_num]

        return signals

    def correct_last_candle(self, corrected_candle):
        """Update derived values in active setups when a candle is corrected by historical data."""
        candle_time = corrected_candle['time']
        for setup in self.active_entry_setups:
            if setup['entry_triggered']:
                continue
            if setup.get('h1_candle') and setup['h1_candle'].get('time') == candle_time:
                old_h1 = setup['h1_price']
                setup['h1_price'] = corrected_candle['low']
                self.logger.info(
                    f"[CANDLE CORRECTION] Red candle H1 updated for Pivot {setup['pivot_number']}: "
                    f"{old_h1:.2f} -> {corrected_candle['low']:.2f}"
                )
            if setup.get('red_candle') and setup['red_candle'].get('time') == candle_time:
                if corrected_candle['close'] < corrected_candle['open']:
                    old_entry = setup['entry_level']
                    old_sl = setup['stop_loss']
                    setup['entry_level'] = corrected_candle['high']
                    setup['stop_loss'] = corrected_candle['low']
                    self.logger.info(
                        f"[CANDLE CORRECTION] Red candle levels updated for Pivot {setup['pivot_number']}: "
                        f"entry {old_entry:.2f} -> {corrected_candle['high']:.2f}, "
                        f"SL {old_sl:.2f} -> {corrected_candle['low']:.2f}"
                    )
                else:
                    self.logger.info(
                        f"[CANDLE CORRECTION] Candle NO LONGER RED for Pivot {setup['pivot_number']}! "
                        f"Resetting to look for red candle"
                    )
                    setup['red_candle'] = None
                    setup['entry_level'] = None
                    setup['stop_loss'] = None
                    setup['status'] = 'looking_for_red_candle'

    def get_active_setups_summary(self):
        """Get summary of all active entry setups"""
        if not self.active_entry_setups:
            return "No active entry setups"

        summary = []
        for setup in self.active_entry_setups:
            candle_str = f" (Candle: {setup['candle_time']})" if setup.get('candle_time') else ""
            if setup['status'] == 'looking_for_l1':
                summary.append(f"  [RED CANDLE] Pivot {setup['pivot_number']}{candle_str}: Looking for L1 pivot low (started at {setup['start_time']})")
            elif setup['status'] == 'looking_for_red_candle':
                l1_str = f", L1={setup['l1_price']:.2f}" if setup.get('l1_price') else ""
                summary.append(f"  [RED CANDLE] Pivot {setup['pivot_number']}{candle_str}: Looking for red candle after L1{l1_str}")
            elif setup['status'] == 'waiting_for_breakout':
                summary.append(f"  [RED CANDLE] Pivot {setup['pivot_number']}{candle_str}: Waiting for breakout above {setup['entry_level']:.2f} (SL: {setup['stop_loss']:.2f})")
            elif setup['status'] == 'entry_triggered':
                summary.append(f"  [RED CANDLE] Pivot {setup['pivot_number']}{candle_str}: RED CANDLE ENTRY TRIGGERED! Above {setup['entry_level']:.2f}")

        return "\n".join(summary)


class PullBackEntryManager:
    """Manages the pullback entry logic within bullish divergence windows using 1-minute candles.
    Flow: Divergence starts -> Find L1 (pivot low) -> Look for pullback above L1 -> Entry on breakout"""

    def __init__(self, logger, data_source="SPOT"):
        self.active_entry_setups = []  # Track active entry setups during divergence
        self.data_source = data_source
        self.logger = _PrefixedLogger(logger, data_source) if data_source != "SPOT" else logger

    def add_entry_setup(self, pivot_number, start_time, candle_time=None, prev_candle=None):
        """Add a new entry setup when divergence starts.
        prev_candle: the last completed 1m candle before divergence, used as left neighbor for L1 detection."""
        setup = {
            'pivot_number': pivot_number,
            'start_time': start_time,
            'candle_time': candle_time,
            'setup_candles': [],        # Per-setup 1m candles (seeded with prev_candle if available)
            'l1_price': None,           # L1 pivot low price (reference point)
            'l1_time': None,            # L1 candle time
            'l1_candle': None,          # L1 candle data
            'pullback_candle': None,
            'highest_price': None,      # Highest price after L1 (for entry)
            'lowest_price': None,       # Lowest price from divergence to current (for SL)
            'entry_level': None,        # Entry at highest price after L1
            'stop_loss': None,          # Lowest price from divergence to current
            'entry_triggered': False,
            'status': 'looking_for_l1'
        }
        # Seed with previous candle so L1 can be detected 1 candle sooner
        if prev_candle is not None:
            setup['setup_candles'].append(prev_candle)
        self.active_entry_setups.append(setup)
        self.logger.info("\n" + "-"*40)
        candle_str = f" (Candle: {candle_time})" if candle_time else ""
        self.logger.info(f"🔍 PULLBACK ENTRY SETUP ADDED for Pivot {pivot_number}{candle_str} 🔍")
        self.logger.info("🔄 Looking for L1 (Pivot Low)")
        self.logger.info("-"*40)

    def remove_entry_setup(self, pivot_number):
        """Remove entry setup when divergence ends"""
        setup_to_remove = next((s for s in self.active_entry_setups if s['pivot_number'] == pivot_number), None)
        candle_time = setup_to_remove.get('candle_time') if setup_to_remove else None

        self.active_entry_setups = [s for s in self.active_entry_setups if s['pivot_number'] != pivot_number]
        self.logger.info("\n" + "-"*40)
        candle_str = f" (Candle: {candle_time})" if candle_time else ""
        self.logger.info(f"🚫 PULLBACK ENTRY SETUP REMOVED for Pivot {pivot_number}{candle_str} 🚫")
        self.logger.info("-"*40)

    def update_with_new_candle(self, candle, current_price):
        """Update entry setups when a new 1-minute candle is completed"""
        for setup in self.active_entry_setups:
            if setup['status'] == 'looking_for_l1':
                # Skip candles that ended before divergence start
                # Include candle forming during divergence for proper pivot detection
                if candle['time'] + timedelta(minutes=1) <= setup['start_time']:
                    continue

                # Track candles for L1 (pivot low) detection
                setup['setup_candles'].append(candle)

                # Need at least 3 candles: prev + candidate + confirmation
                # With seeded prev_candle, L1 detected after just 2 post-divergence candles
                if len(setup['setup_candles']) >= 3:
                    prev_c = setup['setup_candles'][-3]
                    candidate = setup['setup_candles'][-2]
                    confirm_c = setup['setup_candles'][-1]

                    # Pivot low: candidate low < both neighbors' lows
                    if candidate['low'] < prev_c['low'] and candidate['low'] < confirm_c['low']:
                        # L1 found!
                        setup['l1_price'] = candidate['low']
                        setup['l1_time'] = candidate['time']
                        setup['l1_candle'] = candidate
                        setup['lowest_price'] = candidate['low']
                        setup['highest_price'] = confirm_c['high']
                        setup['status'] = 'looking_for_pullback'

                        candle_str = f" (Candle: {setup['candle_time']})" if setup.get('candle_time') else ""
                        self.logger.info("\n" + "=="*25)
                        self.logger.info("📊 L1 (PIVOT LOW) DETECTED 📊")
                        self.logger.info("=="*25)
                        self.logger.info(f"📊 Pivot {setup['pivot_number']}{candle_str}")
                        self.logger.info(f"  📉 L1 Price: {setup['l1_price']:.2f}")
                        self.logger.info(f"  ⏰ L1 Time: {setup['l1_time']}")
                        self.logger.info(f"  🔄 Now looking for pullback above L1")
                        self.logger.info("=="*25)

            elif setup['status'] == 'looking_for_pullback':
                # Track candles and look for pullback above L1
                setup['setup_candles'].append(candle)

                # Check if L1 is broken (candle low < L1) — reset to find new L1
                if candle['low'] < setup['l1_price']:
                    old_l1 = setup['l1_price']
                    setup['l1_price'] = None
                    setup['l1_time'] = None
                    setup['l1_candle'] = None
                    setup['highest_price'] = None
                    setup['lowest_price'] = None
                    setup['status'] = 'looking_for_l1'
                    # Keep [K-1, K] so when K+1 arrives the check is:
                    # prev=K-1 > candidate=K < confirm=K+1 → K is new L1
                    setup['setup_candles'] = setup['setup_candles'][-2:]
                    candle_str = f" (Candle: {setup['candle_time']})" if setup.get('candle_time') else ""
                    self.logger.info(
                        f"⚠️ L1 BROKEN for Pivot {setup['pivot_number']}{candle_str}: "
                        f"candle low {candle['low']:.2f} < L1 {old_l1:.2f} — resetting, scanning fresh for new L1"
                    )
                    continue

                # Update highest and lowest prices
                if candle['high'] > setup['highest_price']:
                    setup['highest_price'] = candle['high']
                if candle['low'] < setup['lowest_price']:
                    setup['lowest_price'] = candle['low']

                # Pullback: current candle LOW < previous candle LOW (dip down in bullish trend)
                if len(setup['setup_candles']) >= 2:
                    prev_candle = setup['setup_candles'][-2]

                    if candle['low'] < prev_candle['low']:
                        # Pullback detected above L1!
                        setup['pullback_candle'] = candle
                        setup['entry_level'] = setup['highest_price']
                        setup['stop_loss'] = candle['low']  # Pullback candle low, not L1
                        setup['status'] = 'waiting_for_breakout'

                        candle_str = f" (Candle: {setup['candle_time']})" if setup.get('candle_time') else ""
                        self.logger.info("\n" + "=="*25)
                        self.logger.info("🔄 PULLBACK DETECTED 🔄")
                        self.logger.info("=="*25)
                        self.logger.info(f"📊 Pivot {setup['pivot_number']}{candle_str} - Pullback found above L1!")
                        self.logger.info(f"  ⏰ Time: {candle['time']}")
                        self.logger.info(
                            f"  📈 Current OHLC: O={candle['open']:.2f}, H={candle['high']:.2f}, "
                            f"L={candle['low']:.2f}, C={candle['close']:.2f}"
                        )
                        self.logger.info(f"  📊 Previous Low: {prev_candle['low']:.2f}")
                        self.logger.info(f"  📊 L1 (Pivot Low): {setup['l1_price']:.2f}")
                        self.logger.info(f"  ➡️ Entry Level: {setup['entry_level']:.2f}")
                        self.logger.info(f"  🛑 Stop Loss: {setup['stop_loss']:.2f}")
                        self.logger.info(
                            f"  ⚠️ Risk: {(setup['entry_level'] - setup['stop_loss']):.2f} points"
                        )
                        self.logger.info("=="*25 + "\n")

            elif setup['status'] == 'waiting_for_breakout' and not setup['entry_triggered']:
                # Check if L1 is broken (candle low < L1) — reset to find new L1
                if candle['low'] < setup['l1_price']:
                    old_l1 = setup['l1_price']
                    setup['l1_price'] = None
                    setup['l1_time'] = None
                    setup['l1_candle'] = None
                    setup['pullback_candle'] = None
                    setup['highest_price'] = None
                    setup['lowest_price'] = None
                    setup['entry_level'] = None
                    setup['stop_loss'] = None
                    setup['status'] = 'looking_for_l1'
                    # Keep [K-1, K] so when K+1 arrives the check is:
                    # prev=K-1 > candidate=K < confirm=K+1 → K is new L1
                    k_minus_1 = setup['setup_candles'][-1] if setup['setup_candles'] else None
                    setup['setup_candles'] = [k_minus_1, candle] if k_minus_1 else [candle]
                    candle_str = f" (Candle: {setup['candle_time']})" if setup.get('candle_time') else ""
                    self.logger.info(
                        f"⚠️ L1 BROKEN for Pivot {setup['pivot_number']}{candle_str}: "
                        f"candle low {candle['low']:.2f} < L1 {old_l1:.2f} — "
                        f"resetting, scanning fresh for new L1"
                    )
                    continue

                # Dynamically update SL to lowest candle low while waiting for breakout
                if candle['low'] < setup['stop_loss']:
                    old_sl = setup['stop_loss']
                    setup['stop_loss'] = candle['low']
                    setup['lowest_price'] = candle['low']
                    candle_str = f" (Candle: {setup['candle_time']})" if setup.get('candle_time') else ""
                    self.logger.info(
                        f"🔄 SL UPDATED for Pivot {setup['pivot_number']}{candle_str}: "
                        f"{old_sl:.2f} -> {setup['stop_loss']:.2f} "
                        f"(candle low at {candle['time']})"
                    )

    def check_for_entry_signals(self, current_price, current_time):
        """Check if current price triggers any entry signals"""
        signals = []

        for setup in self.active_entry_setups:
            if setup['status'] == 'waiting_for_breakout' and not setup['entry_triggered']:
                # Dynamically update SL to lowest tick price while waiting for breakout
                if current_price < setup['stop_loss']:
                    old_sl = setup['stop_loss']
                    setup['stop_loss'] = current_price
                    setup['lowest_price'] = current_price
                    candle_str = f" (Candle: {setup['candle_time']})" if setup.get('candle_time') else ""
                    self.logger.info(
                        f"🔄 SL UPDATED (tick) for Pivot {setup['pivot_number']}{candle_str}: "
                        f"{old_sl:.2f} -> {setup['stop_loss']:.2f}"
                    )

                if current_price > setup['entry_level']:  # Bullish: price breaks ABOVE entry level
                    # Entry signal triggered!
                    setup['entry_triggered'] = True
                    setup['status'] = 'entry_triggered'

                    signal = {
                        'type': 'BUY',
                        'data_source': self.data_source,
                        'pivot_number': setup['pivot_number'],
                        'candle_time': setup.get('candle_time'),
                        'entry_price': setup['entry_level'],
                        'current_price': current_price,
                        'stop_loss': setup['stop_loss'],
                        'entry_time': current_time,
                        'pullback_candle_time': setup['pullback_candle']['time'],
                        'risk_points': setup['entry_level'] - setup['stop_loss'],
                        'entry_method': 'pullback'
                    }
                    signals.append(signal)

                    self.logger.info("\n" + "*"*50)
                    self.logger.info("🔥🔥🔥 PULLBACK ENTRY SIGNAL TRIGGERED 🔥🔥🔥")
                    self.logger.info("*"*50)
                    candle_str = f" (Candle: {setup['candle_time']})" if setup.get('candle_time') else ""
                    self.logger.info(f"🚀 [BUY SIGNAL] PULLBACK ENTRY TRIGGERED for Pivot {setup['pivot_number']}{candle_str}! 🚀")
                    self.logger.info(f"  ➡️ Entry Price: {setup['entry_level']:.2f}")
                    self.logger.info(f"  📊 Current Price: {current_price:.2f}")
                    self.logger.info(f"  🛑 Stop Loss: {setup['stop_loss']:.2f}")
                    self.logger.info(f"  ⚠️ Risk: {signal['risk_points']:.2f} points")
                    self.logger.info(f"  ⏰ Entry Time: {current_time}")
                    self.logger.info(f"  📉 L1 (Pivot Low): {setup['l1_price']:.2f} at {setup['l1_time']}")
                    self.logger.info(f"  📈 Pullback Candle: {setup['pullback_candle']['time']}")
                    self.logger.info("*"*50 + "\n")

        return signals

    def correct_last_candle(self, corrected_candle):
        """Update derived values in active setups when a candle is corrected by historical data."""
        candle_time = corrected_candle['time']
        for setup in self.active_entry_setups:
            if setup['entry_triggered']:
                continue
            if setup.get('h1_candle') and setup['h1_candle'].get('time') == candle_time:
                old_h1 = setup['h1_price']
                setup['h1_price'] = corrected_candle['low']
                if setup.get('lowest_price') is not None and corrected_candle['low'] < setup['lowest_price']:
                    setup['lowest_price'] = corrected_candle['low']
                self.logger.info(
                    f"[CANDLE CORRECTION] Pullback H1 updated for Pivot {setup['pivot_number']}: "
                    f"{old_h1:.2f} -> {corrected_candle['low']:.2f}"
                )
            if setup.get('pullback_candle') and setup['pullback_candle'].get('time') == candle_time:
                old_sl = setup['stop_loss']
                setup['stop_loss'] = corrected_candle['low']
                if setup.get('highest_price') is not None and corrected_candle['high'] > setup['highest_price']:
                    setup['highest_price'] = corrected_candle['high']
                    setup['entry_level'] = corrected_candle['high']
                self.logger.info(
                    f"[CANDLE CORRECTION] Pullback levels updated for Pivot {setup['pivot_number']}: "
                    f"SL {old_sl:.2f} -> {corrected_candle['low']:.2f}, entry={setup['entry_level']:.2f}"
                )
            if setup['status'] == 'looking_for_pullback' and setup.get('highest_price') is not None:
                if corrected_candle['high'] > setup['highest_price']:
                    setup['highest_price'] = corrected_candle['high']

    def get_active_setups_summary(self):
        """Get summary of all active pullback entry setups"""
        if not self.active_entry_setups:
            return "No active pullback entry setups"

        summary = []
        for setup in self.active_entry_setups:
            candle_str = f" (Candle: {setup['candle_time']})" if setup.get('candle_time') else ""
            if setup['status'] == 'looking_for_l1':
                summary.append(f"  [PULLBACK] Pivot {setup['pivot_number']}{candle_str}: Looking for L1 (started at {setup['start_time']})")
            elif setup['status'] == 'looking_for_pullback':
                summary.append(f"  [PULLBACK] Pivot {setup['pivot_number']}{candle_str}: L1 at {setup['l1_price']:.2f}, looking for pullback")
            elif setup['status'] == 'waiting_for_breakout':
                summary.append(f"  [PULLBACK] Pivot {setup['pivot_number']}{candle_str}: Waiting for breakout above {setup['entry_level']:.2f} (SL: {setup['stop_loss']:.2f})")
            elif setup['status'] == 'entry_triggered':
                summary.append(f"  [PULLBACK] Pivot {setup['pivot_number']}{candle_str}: PULLBACK ENTRY TRIGGERED! Above {setup['entry_level']:.2f}")

        return "\n".join(summary)


class MotherChildEntryManager:
    """Manages the mother-child candle entry logic within bullish divergence windows using 1-minute candles.

    Mother candle: The candle with the lowest low since divergence detected, AND its low must be
                   <= the lowest price in the divergence window (from divergence detection to now).
    Child candle: The very next candle after mother that is completely inside mother
                  (child high < mother high AND child low > mother low).
    Entry: BUY when price > child candle HIGH
    SL: mother candle LOW
    Reset: If a new candle has lower or equal low than mother, it becomes new mother (while divergence active).
    Window low: Tracks the lowest price from divergence window start. Mother must have low <= window_low.
    Invalidation: price < mother candle LOW (SL level) - other entry methods continue.
    """

    def __init__(self, logger, data_source="SPOT"):
        self.active_entry_setups = []
        self.data_source = data_source
        self.logger = _PrefixedLogger(logger, data_source) if data_source != "SPOT" else logger

    def add_entry_setup(self, pivot_number, start_time, candle_time=None, current_candle=None, window_low=None):
        """Add a new entry setup when divergence starts"""
        setup = {
            'pivot_number': pivot_number,
            'start_time': start_time,
            'candle_time': candle_time,
            'mother_candle': None,
            'child_candle': None,
            'prev_candle': None,  # Track previous candle to check if it's the mother
            'entry_level': None,  # Child candle HIGH
            'stop_loss': None,    # Mother candle LOW
            'entry_triggered': False,
            'window_low': window_low if window_low is not None else float('inf'),  # Lowest price in divergence window
            'status': 'looking_for_mother_child'
        }
        # If the current candle (at divergence detection) is provided, use it as the initial mother candidate
        if current_candle is not None:
            # Only accept as mother if its low <= window_low
            if current_candle['low'] <= setup['window_low']:
                setup['mother_candle'] = current_candle
                setup['prev_candle'] = current_candle
                setup['status'] = 'waiting_for_child'
            else:
                # Candle doesn't qualify as mother, keep looking
                setup['prev_candle'] = current_candle
                setup['status'] = 'waiting_for_child'
        self.active_entry_setups.append(setup)
        self.logger.info("\n" + "-"*40)
        candle_str = f" (Candle: {candle_time})" if candle_time else ""
        self.logger.info(f"MOTHER-CHILD ENTRY SETUP ADDED for Pivot {pivot_number}{candle_str}")
        self.logger.info(f"Window LOW (divergence window): {setup['window_low']:.2f}")
        if current_candle is not None:
            if setup['mother_candle'] is not None:
                self.logger.info(
                    f"Initial MOTHER CANDIDATE (divergence candle): "
                    f"low={current_candle['low']:.2f} at {current_candle['time']} (<= window low {setup['window_low']:.2f})"
                )
            else:
                self.logger.info(
                    f"Initial candle low={current_candle['low']:.2f} at {current_candle['time']} "
                    f"> window low {setup['window_low']:.2f} - NOT a valid mother, waiting for lower candle"
                )
        self.logger.info("Looking for mother-child candle pattern (lowest candle + inside bar)")
        self.logger.info("-"*40)

    def remove_entry_setup(self, pivot_number):
        """Remove entry setup when divergence ends"""
        setup_to_remove = next((s for s in self.active_entry_setups if s['pivot_number'] == pivot_number), None)
        candle_time = setup_to_remove.get('candle_time') if setup_to_remove else None

        self.active_entry_setups = [s for s in self.active_entry_setups if s['pivot_number'] != pivot_number]
        self.logger.info("\n" + "-"*40)
        candle_str = f" (Candle: {candle_time})" if candle_time else ""
        self.logger.info(f"MOTHER-CHILD ENTRY SETUP REMOVED for Pivot {pivot_number}{candle_str}")
        self.logger.info("-"*40)

    def update_with_new_candle(self, candle, current_price):
        """Update entry setups when a new 1-minute candle is completed"""
        for setup in self.active_entry_setups:
            if setup['entry_triggered']:
                continue

            # Use the initial candle's time (not wall-clock start_time) to filter candles
            # This ensures candles completed after the initial mother candidate are processed
            # (start_time is wall-clock which can be mid-candle, causing the next candle to be skipped)
            filter_time = setup['mother_candle']['time'] if setup['mother_candle'] else (setup['prev_candle']['time'] if setup['prev_candle'] else setup['start_time'])
            if candle['time'] <= filter_time:
                continue

            # Update window_low with each new candle's low
            if candle['low'] < setup['window_low']:
                setup['window_low'] = candle['low']

            if setup['status'] in ('looking_for_mother_child', 'waiting_for_child'):
                if setup['mother_candle'] is None:
                    # First candle after divergence - only accept as mother if low <= window_low
                    if candle['low'] <= setup['window_low']:
                        setup['mother_candle'] = candle
                        setup['prev_candle'] = candle
                        setup['status'] = 'waiting_for_child'
                        self.logger.info(
                            f"MOTHER CANDIDATE for Pivot {setup['pivot_number']}: "
                            f"low={candle['low']:.2f} at {candle['time']} (<= window low {setup['window_low']:.2f})"
                        )
                    else:
                        setup['prev_candle'] = candle
                        self.logger.info(
                            f"Candle low={candle['low']:.2f} at {candle['time']} "
                            f"> window low {setup['window_low']:.2f} - NOT a valid mother for Pivot {setup['pivot_number']}"
                        )
                    continue

                # Check if this candle has a LOWER or EQUAL low to current mother
                if candle['low'] <= setup['mother_candle']['low']:
                    old_low = setup['mother_candle']['low']
                    setup['mother_candle'] = candle
                    setup['child_candle'] = None
                    setup['entry_level'] = None
                    setup['stop_loss'] = None
                    setup['prev_candle'] = candle
                    setup['status'] = 'waiting_for_child'
                    self.logger.info(
                        f"NEW MOTHER CANDLE for Pivot {setup['pivot_number']}: "
                        f"low {old_low:.2f} -> {candle['low']:.2f} at {candle['time']} - child reset"
                    )
                    continue

                # Check if previous candle is the mother and this candle is inside
                if setup['prev_candle'] is setup['mother_candle']:
                    mother = setup['mother_candle']
                    # Mother must have low <= window_low to be valid for child detection
                    if mother['low'] <= setup['window_low'] and candle['high'] < mother['high'] and candle['low'] > mother['low']:
                        # Child candle found!
                        setup['child_candle'] = candle
                        setup['entry_level'] = candle['high']   # Entry above child HIGH
                        setup['stop_loss'] = mother['low']      # SL below mother LOW
                        setup['status'] = 'waiting_for_breakout'

                        self.logger.info("\n" + "=="*25)
                        self.logger.info("MOTHER-CHILD PATTERN DETECTED")
                        self.logger.info("=="*25)
                        candle_str = f" (Candle: {setup['candle_time']})" if setup.get('candle_time') else ""
                        self.logger.info(f"Pivot {setup['pivot_number']}{candle_str} - Mother-Child pattern found!")
                        self.logger.info(
                            f"  Mother: {mother['time']} O={mother['open']:.2f} H={mother['high']:.2f} "
                            f"L={mother['low']:.2f} C={mother['close']:.2f}"
                        )
                        self.logger.info(
                            f"  Child:  {candle['time']} O={candle['open']:.2f} H={candle['high']:.2f} "
                            f"L={candle['low']:.2f} C={candle['close']:.2f}"
                        )
                        self.logger.info(f"  Window LOW: {setup['window_low']:.2f}")
                        self.logger.info(f"  Entry Level: {setup['entry_level']:.2f} (above child HIGH)")
                        self.logger.info(f"  Stop Loss: {setup['stop_loss']:.2f} (below mother LOW)")
                        self.logger.info(
                            f"  Risk: {(setup['entry_level'] - setup['stop_loss']):.2f} points"
                        )
                        self.logger.info("=="*25 + "\n")

                setup['prev_candle'] = candle

            elif setup['status'] == 'waiting_for_breakout':
                # While waiting for breakout, if a new lower or equal candle appears, reset
                if candle['low'] <= setup['mother_candle']['low']:
                    old_low = setup['mother_candle']['low']
                    setup['mother_candle'] = candle
                    setup['child_candle'] = None
                    setup['entry_level'] = None
                    setup['stop_loss'] = None
                    setup['prev_candle'] = candle
                    setup['status'] = 'waiting_for_child'
                    candle_str = f" (Candle: {setup['candle_time']})" if setup.get('candle_time') else ""
                    self.logger.info(
                        f"NEW MOTHER CANDLE (reset) for Pivot {setup['pivot_number']}{candle_str}: "
                        f"low {old_low:.2f} -> {candle['low']:.2f} at {candle['time']} - pattern reset"
                    )

    def check_for_entry_signals(self, current_price, current_time):
        """Check if current price triggers any entry signals"""
        signals = []
        invalidated_pivots = []

        for setup in self.active_entry_setups:
            # Update window_low with current tick price
            if current_price < setup['window_low']:
                setup['window_low'] = current_price

            if setup['status'] == 'waiting_for_breakout' and not setup['entry_triggered']:
                # Check if mother still qualifies (low <= window_low)
                if setup['mother_candle']['low'] > setup['window_low']:
                    candle_str = f" (Candle: {setup['candle_time']})" if setup.get('candle_time') else ""
                    self.logger.info(
                        f"MOTHER-CHILD PAUSED for Pivot {setup['pivot_number']}{candle_str}: "
                        f"mother low {setup['mother_candle']['low']:.2f} > window low {setup['window_low']:.2f} "
                        f"- waiting for new lower candle to become mother"
                    )
                    # Reset to waiting_for_child since mother is no longer the lowest
                    setup['child_candle'] = None
                    setup['entry_level'] = None
                    setup['stop_loss'] = None
                    setup['status'] = 'waiting_for_child'
                    continue

                # INVALIDATION: price breaks BELOW mother candle LOW (SL level)
                if current_price < setup['stop_loss']:
                    candle_str = f" (Candle: {setup['candle_time']})" if setup.get('candle_time') else ""
                    self.logger.info("\n" + "!!"*25)
                    self.logger.info(f"MOTHER-CHILD INVALIDATED for Pivot {setup['pivot_number']}{candle_str}")
                    self.logger.info(f"  Price {current_price:.2f} broke below mother candle LOW {setup['stop_loss']:.2f}")
                    self.logger.info(f"  Other entry methods continue")
                    self.logger.info("!!"*25 + "\n")
                    invalidated_pivots.append(setup['pivot_number'])
                    continue

                if current_price > setup['entry_level']:
                    # Entry signal triggered!
                    setup['entry_triggered'] = True
                    setup['status'] = 'entry_triggered'

                    signal = {
                        'type': 'BUY',
                        'data_source': self.data_source,
                        'pivot_number': setup['pivot_number'],
                        'candle_time': setup.get('candle_time'),
                        'entry_price': setup['entry_level'],
                        'current_price': current_price,
                        'stop_loss': setup['stop_loss'],
                        'entry_time': current_time,
                        'mother_candle_time': setup['mother_candle']['time'],
                        'child_candle_time': setup['child_candle']['time'],
                        'risk_points': setup['entry_level'] - setup['stop_loss'],
                        'entry_method': 'mother_child'
                    }
                    signals.append(signal)

                    self.logger.info("\n" + "*"*50)
                    self.logger.info("MOTHER-CHILD ENTRY SIGNAL TRIGGERED")
                    self.logger.info("*"*50)
                    candle_str = f" (Candle: {setup['candle_time']})" if setup.get('candle_time') else ""
                    self.logger.info(f"[BUY SIGNAL] MOTHER-CHILD ENTRY for Pivot {setup['pivot_number']}{candle_str}!")
                    self.logger.info(f"  Entry Price: {setup['entry_level']:.2f}")
                    self.logger.info(f"  Current Price: {current_price:.2f}")
                    self.logger.info(f"  Stop Loss: {setup['stop_loss']:.2f}")
                    self.logger.info(f"  Risk: {signal['risk_points']:.2f} points")
                    self.logger.info(f"  Entry Time: {current_time}")
                    self.logger.info(f"  Mother: {setup['mother_candle']['time']}")
                    self.logger.info(f"  Child: {setup['child_candle']['time']}")
                    self.logger.info("*"*50 + "\n")

        # Remove invalidated setups (only mother-child, other methods continue)
        for pivot_num in invalidated_pivots:
            self.active_entry_setups = [s for s in self.active_entry_setups if s['pivot_number'] != pivot_num]

        return signals

    def correct_last_candle(self, corrected_candle):
        """Update derived values in active setups when a candle is corrected by historical data."""
        candle_time = corrected_candle['time']
        for setup in self.active_entry_setups:
            if setup['entry_triggered']:
                continue
            if setup.get('mother_candle') and setup['mother_candle'].get('time') == candle_time:
                old_low = setup['stop_loss'] if setup.get('stop_loss') else setup['mother_candle']['low']
                setup['stop_loss'] = corrected_candle['low'] if setup.get('child_candle') else None
                self.logger.info(
                    f"[CANDLE CORRECTION] Mother candle updated for Pivot {setup['pivot_number']}: "
                    f"low {old_low:.2f} -> {corrected_candle['low']:.2f}"
                )
            if setup.get('child_candle') and setup['child_candle'].get('time') == candle_time:
                old_entry = setup['entry_level']
                setup['entry_level'] = corrected_candle['high']
                self.logger.info(
                    f"[CANDLE CORRECTION] Child candle updated for Pivot {setup['pivot_number']}: "
                    f"entry {old_entry:.2f} -> {corrected_candle['high']:.2f}"
                )

    def get_active_setups_summary(self):
        """Get summary of all active mother-child entry setups"""
        if not self.active_entry_setups:
            return "No active mother-child entry setups"

        summary = []
        for setup in self.active_entry_setups:
            candle_str = f" (Candle: {setup['candle_time']})" if setup.get('candle_time') else ""
            if setup['status'] in ('looking_for_mother_child', 'waiting_for_child'):
                mother_str = f", mother low={setup['mother_candle']['low']:.2f}" if setup['mother_candle'] else ""
                summary.append(
                    f"  [MOTHER-CHILD] Pivot {setup['pivot_number']}{candle_str}: Looking for mother-child pattern{mother_str}"
                )
            elif setup['status'] == 'waiting_for_breakout':
                summary.append(
                    f"  [MOTHER-CHILD] Pivot {setup['pivot_number']}{candle_str}: Waiting for breakout above "
                    f"{setup['entry_level']:.2f} (SL: {setup['stop_loss']:.2f})"
                )
            elif setup['status'] == 'entry_triggered':
                summary.append(
                    f"  [MOTHER-CHILD] Pivot {setup['pivot_number']}{candle_str}: MOTHER-CHILD ENTRY TRIGGERED! "
                    f"Above {setup['entry_level']:.2f}"
                )

        return "\n".join(summary)


class ThreeBottomEntryManager:
    """
    Manages the 3-bottom entry logic within divergence windows using 1-minute candles.

    Pattern: L1 < A < B (three ascending bottoms)
    - L1: Initial reference low (spot pivot low at divergence detection, dynamically updated)
    - H1: First swing high after L1
    - A: Swing low > L1 (1st higher bottom)
    - B: Swing low > A (2nd higher bottom)
    - Entry: highest high between A and B
    - SL: B - 0.05
    - Invalidation: price falls below B
    """

    def __init__(self, logger, data_source="SPOT"):
        self.active_entry_setups = []
        self.data_source = data_source
        self.logger = _PrefixedLogger(logger, data_source) if data_source != "SPOT" else logger

    def add_entry_setup(self, pivot_number, start_time, candle_time=None, initial_low=None):
        """Add a new 3-bottom entry setup when divergence starts."""
        setup = {
            'pivot_number': pivot_number,
            'start_time': start_time,
            'candle_time': candle_time,
            'candles': [],
            'L1': initial_low,
            'L1_idx': None,       # None means L1 is from pivot, not from 1m candles
            'H1': None, 'H1_idx': None,
            'A': None, 'A_idx': None,
            'B': None, 'B_idx': None,
            'entry_price': None,
            'stop_loss': None,
            'target': None,
            'entry_triggered': False,
            'status': 'searching'
        }
        self.active_entry_setups.append(setup)
        self.logger.info("\n" + "-"*40)
        candle_str = f" (Candle: {candle_time})" if candle_time else ""
        l1_str = f" L1={initial_low:.2f}" if initial_low else ""
        self.logger.info(f"3-BOTTOM ENTRY SETUP ADDED for Pivot {pivot_number}{candle_str}{l1_str}")
        self.logger.info("Searching for L1 < H1 > A < B pattern on 1m candles")
        self.logger.info("-"*40)

    def remove_entry_setup(self, pivot_number):
        """Remove 3-bottom entry setup when divergence ends."""
        setup_to_remove = next((s for s in self.active_entry_setups if s['pivot_number'] == pivot_number), None)
        candle_time = setup_to_remove.get('candle_time') if setup_to_remove else None
        self.active_entry_setups = [s for s in self.active_entry_setups if s['pivot_number'] != pivot_number]
        self.logger.info("\n" + "-"*40)
        candle_str = f" (Candle: {candle_time})" if candle_time else ""
        self.logger.info(f"3-BOTTOM ENTRY SETUP REMOVED for Pivot {pivot_number}{candle_str}")
        self.logger.info("-"*40)

    def _is_swing_high(self, candles, idx):
        """Check if candle at idx is a swing high (same swing-logic rules)."""
        if idx <= 0 or idx >= len(candles) - 1:
            return False
        current_high = candles[idx]['high']
        prev_high = candles[idx - 1]['high']
        next_high = candles[idx + 1]['high']

        i = idx - 1
        while i >= 0 and candles[i]['high'] == current_high:
            if i > 0:
                prev_high = candles[i - 1]['high']
            i -= 1

        i = idx + 1
        while i < len(candles) and candles[i]['high'] == current_high:
            if i < len(candles) - 1:
                next_high = candles[i + 1]['high']
            i += 1

        return current_high > prev_high and current_high > next_high

    def _is_swing_low(self, candles, idx):
        """Check if candle at idx is a swing low (same swing-logic rules)."""
        if idx <= 0 or idx >= len(candles) - 1:
            return False
        current_low = candles[idx]['low']
        prev_low = candles[idx - 1]['low']
        next_low = candles[idx + 1]['low']

        i = idx - 1
        while i >= 0 and candles[i]['low'] == current_low:
            if i > 0:
                prev_low = candles[i - 1]['low']
            i -= 1

        i = idx + 1
        while i < len(candles) and candles[i]['low'] == current_low:
            if i < len(candles) - 1:
                next_low = candles[i + 1]['low']
            i += 1

        return current_low < prev_low and current_low < next_low

    def _calculate_entry_level(self, setup):
        """Calculate entry level once B forms: highest high between A and B."""
        candles = setup['candles']
        a_idx = setup['A_idx']
        b_idx = setup['B_idx']

        highest_high = candles[a_idx]['high']
        for i in range(a_idx + 1, b_idx + 1):
            if candles[i]['high'] > highest_high:
                highest_high = candles[i]['high']

        risk_reward = getattr(settings, 'RISK_REWARD', 2.0)
        setup['entry_price'] = highest_high
        setup['stop_loss'] = setup['A'] - 0.05
        risk = setup['entry_price'] - setup['stop_loss']
        setup['target'] = setup['entry_price'] + (risk * risk_reward)
        setup['status'] = 'setup_ready'

        candle_str = f" (Candle: {setup['candle_time']})" if setup.get('candle_time') else ""
        self.logger.info("\n" + "=="*25)
        self.logger.info(f"3-BOTTOM ENTRY LEVEL CALCULATED for Pivot {setup['pivot_number']}{candle_str}")
        self.logger.info("=="*25)
        self.logger.info(f"  L1: {setup['L1']:.2f}")
        self.logger.info(f"  H1: {setup['H1']:.2f} at {candles[setup['H1_idx']]['time']}")
        self.logger.info(f"  A: {setup['A']:.2f} at {candles[setup['A_idx']]['time']}")
        self.logger.info(f"  B: {setup['B']:.2f} at {candles[setup['B_idx']]['time']}")
        self.logger.info(f"  Entry Level: {setup['entry_price']:.2f} (highest high between A and B)")
        self.logger.info(f"  Stop Loss: {setup['stop_loss']:.2f} (below A)")
        self.logger.info(f"  Target: {setup['target']:.2f}")
        self.logger.info(f"  Risk: {risk:.2f} points")
        self.logger.info("=="*25 + "\n")

    def _detect_pattern(self, setup):
        """Scan collected 1m candles to identify/update L1 < H1 > A < B pattern."""
        candles = setup['candles']
        if len(candles) < 3:
            return

        scan_start = 0

        # --- L1 dynamic update ---
        # If any candle low goes below L1, update L1 and reset everything
        for i in range(scan_start, len(candles)):
            if candles[i]['low'] < setup['L1']:
                prev_l1 = setup['L1']
                setup['L1'] = candles[i]['low']
                setup['L1_idx'] = i
                setup['H1'] = None; setup['H1_idx'] = None
                setup['A'] = None; setup['A_idx'] = None
                setup['B'] = None; setup['B_idx'] = None
                setup['entry_price'] = None; setup['stop_loss'] = None; setup['target'] = None
                setup['status'] = 'searching'
                self.logger.info(
                    f"[3-BOTTOM Pivot {setup['pivot_number']}] L1 updated from {prev_l1:.2f} to {setup['L1']:.2f} "
                    f"at {candles[i]['time']} - resetting pattern"
                )

        # --- H1 detection (swing high after L1) ---
        l1_start = (setup['L1_idx'] + 1) if setup['L1_idx'] is not None else 0
        for i in range(max(1, l1_start), len(candles) - 1):
            if self._is_swing_high(candles, i) and candles[i]['high'] > setup['L1']:
                if setup['H1'] is None or candles[i]['high'] > setup['H1']:
                    prev_h1 = setup['H1']
                    setup['H1'] = candles[i]['high']
                    setup['H1_idx'] = i
                    setup['A'] = None; setup['A_idx'] = None
                    setup['B'] = None; setup['B_idx'] = None
                    setup['entry_price'] = None; setup['stop_loss'] = None; setup['target'] = None
                    setup['status'] = 'searching'
                    if prev_h1 is None:
                        self.logger.info(
                            f"[3-BOTTOM Pivot {setup['pivot_number']}] H1 detected at {setup['H1']:.2f} "
                            f"at {candles[i]['time']}"
                        )
                    else:
                        self.logger.info(
                            f"[3-BOTTOM Pivot {setup['pivot_number']}] H1 updated from {prev_h1:.2f} to {setup['H1']:.2f} "
                            f"at {candles[i]['time']}"
                        )

        if setup['H1'] is None:
            return

        # --- A detection (swing low > L1, after H1) ---
        for i in range(setup['H1_idx'] + 1, len(candles) - 1):
            if self._is_swing_low(candles, i) and candles[i]['low'] > setup['L1']:
                if setup['A'] is None:
                    setup['A'] = candles[i]['low']
                    setup['A_idx'] = i
                    self.logger.info(
                        f"[3-BOTTOM Pivot {setup['pivot_number']}] A detected at {setup['A']:.2f} "
                        f"at {candles[i]['time']}"
                    )
                elif candles[i]['low'] < setup['A'] and candles[i]['low'] > setup['L1'] and i > setup['A_idx']:
                    # Dynamic update: lower swing low still above L1
                    prev_a = setup['A']
                    setup['A'] = candles[i]['low']
                    setup['A_idx'] = i
                    setup['B'] = None; setup['B_idx'] = None
                    setup['entry_price'] = None; setup['stop_loss'] = None; setup['target'] = None
                    setup['status'] = 'searching'
                    self.logger.info(
                        f"[3-BOTTOM Pivot {setup['pivot_number']}] A updated from {prev_a:.2f} to {setup['A']:.2f} "
                        f"at {candles[i]['time']} - B reset"
                    )

        if setup['A'] is None:
            return

        # --- B detection (swing low > A, after A) ---
        for i in range(setup['A_idx'] + 1, len(candles) - 1):
            if self._is_swing_low(candles, i) and candles[i]['low'] > setup['A']:
                if setup['B'] is None:
                    setup['B'] = candles[i]['low']
                    setup['B_idx'] = i
                    self.logger.info(
                        f"[3-BOTTOM Pivot {setup['pivot_number']}] B detected at {setup['B']:.2f} "
                        f"at {candles[i]['time']}"
                    )
                    self._calculate_entry_level(setup)
                elif candles[i]['low'] < setup['B'] and candles[i]['low'] > setup['A'] and i > setup['B_idx']:
                    # Dynamic update: lower swing low still above A
                    prev_b = setup['B']
                    setup['B'] = candles[i]['low']
                    setup['B_idx'] = i
                    self.logger.info(
                        f"[3-BOTTOM Pivot {setup['pivot_number']}] B updated from {prev_b:.2f} to {setup['B']:.2f} "
                        f"at {candles[i]['time']} - recalculating entry"
                    )
                    self._calculate_entry_level(setup)

    def update_with_new_candle(self, candle, current_price):
        """Update 3-bottom entry setups when a new 1-minute candle is completed."""
        for setup in self.active_entry_setups:
            if setup['entry_triggered']:
                continue

            # Only consider candles after divergence start
            if candle['time'] <= setup['start_time']:
                continue

            setup['candles'].append(candle)
            self._detect_pattern(setup)

    def check_for_entry_signals(self, current_price, current_time):
        """Check if current price triggers any 3-bottom entry signals."""
        signals = []

        for setup in self.active_entry_setups:
            if setup['status'] == 'setup_ready' and not setup['entry_triggered']:
                # Invalidation: price falls below B
                if current_price < setup['B']:
                    candle_str = f" (Candle: {setup['candle_time']})" if setup.get('candle_time') else ""
                    self.logger.info(
                        f"[3-BOTTOM Pivot {setup['pivot_number']}{candle_str}] "
                        f"INVALIDATED: price {current_price:.2f} < B {setup['B']:.2f} - resetting B"
                    )
                    setup['B'] = None; setup['B_idx'] = None
                    setup['entry_price'] = None; setup['stop_loss'] = None; setup['target'] = None
                    setup['status'] = 'searching'
                    continue

                # Entry trigger: price breaks above entry level
                if current_price > setup['entry_price']:
                    setup['entry_triggered'] = True
                    setup['status'] = 'entry_triggered'

                    signal = {
                        'type': 'BUY',
                        'data_source': self.data_source,
                        'pivot_number': setup['pivot_number'],
                        'candle_time': setup.get('candle_time'),
                        'entry_price': setup['entry_price'],
                        'current_price': current_price,
                        'stop_loss': setup['stop_loss'],
                        'entry_time': current_time,
                        'risk_points': setup['entry_price'] - setup['stop_loss'],
                        'entry_method': 'three_bottom'
                    }
                    signals.append(signal)

                    candle_str = f" (Candle: {setup['candle_time']})" if setup.get('candle_time') else ""
                    self.logger.info("\n" + "*"*50)
                    self.logger.info("3-BOTTOM ENTRY SIGNAL TRIGGERED")
                    self.logger.info("*"*50)
                    self.logger.info(f"Pivot {setup['pivot_number']}{candle_str}")
                    self.logger.info(f"  L1: {setup['L1']:.2f}")
                    self.logger.info(f"  A: {setup['A']:.2f}")
                    self.logger.info(f"  B: {setup['B']:.2f}")
                    self.logger.info(f"  Entry Price: {setup['entry_price']:.2f}")
                    self.logger.info(f"  Current Price: {current_price:.2f}")
                    self.logger.info(f"  Stop Loss: {setup['stop_loss']:.2f}")
                    self.logger.info(f"  Target: {setup['target']:.2f}")
                    self.logger.info(f"  Risk: {signal['risk_points']:.2f} points")
                    self.logger.info(f"  Entry Time: {current_time}")
                    self.logger.info("*"*50 + "\n")

        return signals

    def correct_last_candle(self, corrected_candle):
        """Update derived values in active setups when a candle is corrected by historical data."""
        candle_time = corrected_candle['time']
        for setup in self.active_entry_setups:
            if setup['entry_triggered']:
                continue
            for i, c in enumerate(setup.get('candles', [])):
                if c.get('time') == candle_time:
                    setup['candles'][i] = corrected_candle
                    break

    def get_active_setups_summary(self):
        """Get summary of all active 3-bottom entry setups."""
        if not self.active_entry_setups:
            return "No active 3-bottom entry setups"

        summary = ["3-Bottom Entry Setups:"]
        for setup in self.active_entry_setups:
            candle_str = f" (Candle: {setup['candle_time']})" if setup.get('candle_time') else ""
            points = []
            if setup['L1'] is not None:
                points.append(f"L1={setup['L1']:.2f}")
            if setup['H1'] is not None:
                points.append(f"H1={setup['H1']:.2f}")
            if setup['A'] is not None:
                points.append(f"A={setup['A']:.2f}")
            if setup['B'] is not None:
                points.append(f"B={setup['B']:.2f}")
            points_str = ", ".join(points) if points else "no points yet"

            if setup['status'] == 'searching':
                summary.append(
                    f"  [3-BOTTOM] Pivot {setup['pivot_number']}{candle_str}: Searching ({points_str}, "
                    f"{len(setup['candles'])} candles)"
                )
            elif setup['status'] == 'setup_ready':
                summary.append(
                    f"  [3-BOTTOM] Pivot {setup['pivot_number']}{candle_str}: Waiting for break above "
                    f"{setup['entry_price']:.2f} (SL: {setup['stop_loss']:.2f})"
                )
            elif setup['status'] == 'entry_triggered':
                summary.append(
                    f"  [3-BOTTOM] Pivot {setup['pivot_number']}{candle_str}: 3-BOTTOM ENTRY TRIGGERED!"
                )

        return "\n".join(summary)


class BullishDivergenceStrategy:
    """
    Strategy that identifies and trades bullish divergence between spot and futures.
    The strategy looks for pivot low breakdowns with timing divergence and enters
    using red candle breakout pattern.
    """

    def __init__(self, spot_1m_data, spot_5m_data, futures_5m_data, order_manager=None, broker=None, futures_1m_data=None):
        """
        Initialize the strategy with spot and futures data.

        Args:
            spot_1m_data: LiveOHLCVData instance for spot 1-minute candles
            spot_5m_data: LiveOHLCVData instance for spot 5-minute candles
            futures_5m_data: LiveOHLCVData instance for futures 5-minute candles
            order_manager: OrderManager instance for handling orders
            broker: Broker instance for fetching market/options data
            futures_1m_data: LiveOHLCVData instance for futures 1-minute candles
        """
        self.spot_1m = spot_1m_data
        self.spot_5m = spot_5m_data
        self.futures_5m = futures_5m_data
        self.futures_1m = futures_1m_data
        self.broker = broker

        # Strategy-specific logger
        self.logger = get_strategy_logger("bullish_divergence")

        # Set strategy logger on OHLCV data instances for pivot breakdown logs
        self.spot_5m.set_strategy_logger(self.logger, strategy_type='bullish')
        self.futures_5m.set_strategy_logger(self.logger, strategy_type='bullish')

        # OrderManager is injected from engine
        self.order_manager = order_manager if order_manager else (OrderManager(broker) if broker else None)

        # Spot entry managers
        self.entry_manager = RedCandleEntryManager(self.logger, data_source="SPOT")
        self.pullback_manager = PullBackEntryManager(self.logger, data_source="SPOT")
        self.mother_child_manager = MotherChildEntryManager(self.logger, data_source="SPOT")
        self.three_bottom_manager = ThreeBottomEntryManager(self.logger, data_source="SPOT")

        # Futures entry managers (run independently on futures 1m candles)
        self.fut_entry_manager = RedCandleEntryManager(self.logger, data_source="FUTURES")
        self.fut_pullback_manager = PullBackEntryManager(self.logger, data_source="FUTURES")
        self.fut_mother_child_manager = MotherChildEntryManager(self.logger, data_source="FUTURES")
        self.fut_three_bottom_manager = ThreeBottomEntryManager(self.logger, data_source="FUTURES")

        # Active divergences tracking
        self.active_divergences = []
        self._log_state_cache = {}

        # Divergence threshold from settings
        self.divergence_threshold_minutes = settings.DIVERGENCE_THRESHOLD_MINUTES

        # First candle pivot high tracking for divergence validation
        self._first_candle_pivot = None   # {'time': ..., 'price': ...} once detected
        self._first_candle_broken = False
        self._first_candle_breakout_price = None
        self._first_candle_breakout_time = None

        # Setup & counters
        self.pending_setup = None
        self.executed_patterns = []
        self.signal_counter = 0
        self.order_counter = 0

        # Greeks/options caching
        self.last_greeks_refresh = None
        self.greeks_refresh_interval = getattr(settings, "GREEKS_REFRESH_INTERVAL", 600)
        self.cached_options_data = None

        # Stop loss calculation flags
        self.sl_calculated_once = False
        self._cached_options_with_sl = None

        # Ensure dirs exist
        os.makedirs(settings.ORDER_HISTORY_DIR, exist_ok=True)
        os.makedirs(settings.OPTIONS_DATA_DIR, exist_ok=True)

    def generate_signals(self):
        """
        Generate initial signals from historical data.
        For divergence strategy, we primarily work with live data.
        """
        self.logger.info("BullishDivergenceStrategy: generate_signals() called")
        self.logger.info("Monitoring spot-futures divergence in real-time")

        # Data gap warning: check if first candle starts from market open
        if hasattr(self.spot_5m, 'completed_candles') and self.spot_5m.completed_candles:
            first_time = self.spot_5m.completed_candles[0]['time']
            if hasattr(first_time, 'hour'):
                mkt_h = settings.MARKET_OPEN_HOUR
                mkt_m = settings.MARKET_OPEN_MINUTE
                if first_time.hour > mkt_h or (first_time.hour == mkt_h and first_time.minute > mkt_m + 5):
                    self.logger.warning(
                        f"DATA GAP: First candle at {first_time}, expected ~{mkt_h:02d}:{mkt_m:02d}. "
                        f"Historical data may be incomplete after restart."
                    )

        # Log spot 5m data status
        spot_candles = len(self.spot_5m.completed_candles) if hasattr(self.spot_5m, 'completed_candles') else 0
        spot_pivot_lows = len(self.spot_5m.pivot_lows) if hasattr(self.spot_5m, 'pivot_lows') else 0
        self.logger.info(f"[DATA STATUS] Spot 5m candles: {spot_candles}, Pivot lows: {spot_pivot_lows}")

        # Log ALL pivot lows if any
        if hasattr(self.spot_5m, 'pivot_lows') and self.spot_5m.pivot_lows:
            self.logger.info(f"[PIVOT LOWS] Found {len(self.spot_5m.pivot_lows)} pivot lows in spot data")
            for i, pivot in enumerate(self.spot_5m.pivot_lows):
                self.logger.info(f"Pivot Low {i+1} (Candle: {pivot['time']}) @ {pivot['price']:.2f}")
        else:
            self.logger.info("[PIVOT LOWS] No pivot lows detected yet")

        # Post-scan: check ALL candles after each pivot to detect breakdowns missed during disconnect
        self._check_all_historical_pivot_breakdowns()

    def _check_all_historical_pivot_breakdowns(self):
        """
        Check ALL candles after each pivot to detect breakdowns missed during disconnect.
        The default _check_historical_pivot_breakdowns in ohlcv.py only checks the last candle's close,
        which misses breakdowns that happened mid-session.
        """
        if not hasattr(self.spot_5m, 'completed_candles') or not self.spot_5m.completed_candles:
            return

        candles = self.spot_5m.completed_candles

        # Check pivot lows — if any candle's low went below pivot price, it was broken
        for i, pivot_low in enumerate(self.spot_5m.pivot_lows):
            if pivot_low.get('removed', False):
                continue
            already_broken = any(
                broken['pivot_time'] == pivot_low['time']
                for broken in self.spot_5m.broken_pivot_lows
            )
            if already_broken:
                continue

            pivot_time = pivot_low['time']
            for candle in candles:
                if candle['time'] > pivot_time and candle['low'] <= pivot_low['price']:
                    broken_pivot = {
                        'pivot_time': pivot_low['time'],
                        'pivot_price': pivot_low['price'],
                        'breakdown_time': candle['time'],
                        'breakdown_price': candle['low'],
                        'pivot_number': i + 1
                    }
                    self.spot_5m.broken_pivot_lows.append(broken_pivot)
                    self.logger.info(
                        f"MISSED BREAKDOWN: Pivot Low {i+1} (Candle: {pivot_low['time']}) "
                        f"@ {pivot_low['price']:.2f} was broken by candle at {candle['time']} "
                        f"(low: {candle['low']:.2f}). Breakdown was missed during disconnect."
                    )
                    break

        # Check pivot highs — if any candle's high went above pivot price, it was broken
        for i, pivot_high in enumerate(self.spot_5m.pivot_highs):
            if pivot_high.get('removed', False):
                continue
            already_broken = any(
                broken['pivot_time'] == pivot_high['time']
                for broken in self.spot_5m.broken_pivot_highs
            )
            if already_broken:
                continue

            pivot_time = pivot_high['time']
            for candle in candles:
                if candle['time'] > pivot_time and candle['high'] >= pivot_high['price']:
                    broken_pivot = {
                        'pivot_number': pivot_high.get('pivot_number', i + 1),
                        'pivot_price': pivot_high['price'],
                        'pivot_time': pivot_high['time'],
                        'breakout_price': candle['high'],
                        'breakout_time': candle['time']
                    }
                    self.spot_5m.broken_pivot_highs.append(broken_pivot)
                    self.logger.info(
                        f"MISSED BREAKOUT: Pivot High {pivot_high.get('pivot_number', i+1)} "
                        f"(Candle: {pivot_high['time']}) @ {pivot_high['price']:.2f} was broken "
                        f"by candle at {candle['time']} (high: {candle['high']:.2f}). "
                        f"Breakout was missed during disconnect."
                    )
                    break

    def _log_state(self, key, value, message):
        """Log only when the tracked value changes."""
        if self._log_state_cache.get(key) != value:
            self._log_state_cache[key] = value
            self.logger.info(message)

    def check_live_tick(self, price, tick_time=None):
        """
        Check for entry signals based on current live price tick.

        Args:
            price: Current spot price
            tick_time: Current timestamp

        Returns:
            Signal dictionary if entry triggered, None otherwise
        """
        if tick_time is None:
            tick_time = datetime.now()

        # Check pivot breakdowns for spot
        self.spot_5m.check_pivot_low_breakdown(price, tick_time)

        # Track first candle as pivot high for divergence validation
        if self._first_candle_pivot is None and self.spot_5m.completed_candles:
            fc = self.spot_5m.completed_candles[0]
            fc_time = fc['time']
            mkt_h = settings.MARKET_OPEN_HOUR
            mkt_m = settings.MARKET_OPEN_MINUTE
            if hasattr(fc_time, 'hour') and fc_time.hour == mkt_h and fc_time.minute == mkt_m:
                self._first_candle_pivot = {'time': fc_time, 'price': fc['high']}
                self.logger.info(f"[FIRST CANDLE PIVOT] Tracking {mkt_h:02d}:{mkt_m:02d} candle high as pivot high @ {fc['high']:.2f}")

        # Check if first candle pivot high is broken (price goes above it)
        if self._first_candle_pivot and not self._first_candle_broken:
            if price >= self._first_candle_pivot['price']:
                self._first_candle_broken = True
                self._first_candle_breakout_price = price
                self._first_candle_breakout_time = tick_time
                self.logger.info(
                    f"[BREAKOUT] First candle pivot high BROKEN! "
                    f"Pivot: {self._first_candle_pivot['time']} @ {self._first_candle_pivot['price']:.2f} -> "
                    f"Breakout: {tick_time} @ {price:.2f}"
                )

        # Derive and update futures pivots from spot
        self._update_futures_pivot_lows(tick_time)

        # Check divergence conditions (including "both broken" ending condition)
        self._check_for_divergence(price, tick_time)
        active_count = len(self.active_divergences)
        status_message = (
            f"[DIVERGENCE STATUS] Tracking {active_count} active divergence(s)"
            if active_count
            else "[DIVERGENCE STATUS] No active divergences"
        )
        self._log_state("active_divergence_count", active_count, status_message)

        # Check for entry signals from red candle, pullback, mother-child, and 3-bottom methods
        entry_signals = self.entry_manager.check_for_entry_signals(price, tick_time)
        pullback_signals = self.pullback_manager.check_for_entry_signals(price, tick_time)
        mother_child_signals = self.mother_child_manager.check_for_entry_signals(price, tick_time)
        three_bottom_signals = self.three_bottom_manager.check_for_entry_signals(price, tick_time)

        # Combine signals: if multiple methods trigger with the SAME entry price for a pivot,
        # pick the one with the biggest SL (widest risk). Otherwise, first triggered wins.
        all_signals = entry_signals + pullback_signals + mother_child_signals + three_bottom_signals
        if all_signals:
            pivot_signals = {}
            for sig in all_signals:
                pn = sig['pivot_number']
                if pn not in pivot_signals:
                    pivot_signals[pn] = sig
                elif sig.get('entry_price') == pivot_signals[pn].get('entry_price'):
                    # Same entry price - pick the one with bigger SL (wider risk)
                    if sig.get('risk_points', 0) > pivot_signals[pn].get('risk_points', 0):
                        pivot_signals[pn] = sig
                # Different entry price - first triggered wins (already stored)
            entry_signals = list(pivot_signals.values())
        else:
            entry_signals = []

        entry_count = len(entry_signals)
        entry_message = (
            f"[ENTRY SIGNAL] Found {entry_count} entry signal(s)"
            if entry_signals
            else "[ENTRY SIGNAL] No entry signals"
        )
        self._log_state("entry_signal_count", entry_count, entry_message)

        if entry_signals:
            for signal in entry_signals:
                # Remove from active divergences after entry
                pivot_number = signal['pivot_number']
                candle_time = signal.get('candle_time')
                candle_str = f" (Candle: {candle_time})" if candle_time else ""
                entry_method = signal.get('entry_method', 'red_candle')

                self.active_divergences = [
                    div for div in self.active_divergences
                    if div['pivot_number'] != pivot_number
                ]
                self.logger.info(
                    f"[DIVERGENCE TRACKING] Stopped tracking pivot {pivot_number}{candle_str} "
                    f"after {entry_method} entry"
                )

                # Clean up ALL entry managers (both spot and futures)
                self.entry_manager.remove_entry_setup(pivot_number)
                self.pullback_manager.remove_entry_setup(pivot_number)
                self.mother_child_manager.remove_entry_setup(pivot_number)
                self.three_bottom_manager.remove_entry_setup(pivot_number)
                self.fut_entry_manager.remove_entry_setup(pivot_number)
                self.fut_pullback_manager.remove_entry_setup(pivot_number)
                self.fut_mother_child_manager.remove_entry_setup(pivot_number)
                self.fut_three_bottom_manager.remove_entry_setup(pivot_number)

                # Remove pivot from tracking in both spot and futures
                self.spot_5m.remove_pivot_from_tracking(pivot_number)
                self.futures_5m.remove_pivot_from_tracking(pivot_number)
                self.logger.info(f"[PIVOT TRACKING] Removed pivot {pivot_number}{candle_str} after entry")

                # Convert to options signal format
                return self._convert_to_options_signal(signal, price)

        return None

    def check_futures_tick(self, price, tick_time=None):
        """
        Check futures entry managers for signals using futures price.

        Args:
            price: Current futures price
            tick_time: Current timestamp

        Returns:
            Signal dictionary if entry triggered, None otherwise
        """
        if tick_time is None:
            tick_time = datetime.now()

        # Check for entry signals from futures managers only
        entry_signals = self.fut_entry_manager.check_for_entry_signals(price, tick_time)
        pullback_signals = self.fut_pullback_manager.check_for_entry_signals(price, tick_time)
        mother_child_signals = self.fut_mother_child_manager.check_for_entry_signals(price, tick_time)
        three_bottom_signals = self.fut_three_bottom_manager.check_for_entry_signals(price, tick_time)

        # Same signal-combining logic as check_live_tick
        all_signals = entry_signals + pullback_signals + mother_child_signals + three_bottom_signals
        if all_signals:
            pivot_signals = {}
            for sig in all_signals:
                pn = sig['pivot_number']
                # Skip if pivot already consumed by spot entry
                if not any(div['pivot_number'] == pn for div in self.active_divergences):
                    continue
                if pn not in pivot_signals:
                    pivot_signals[pn] = sig
                elif sig.get('entry_price') == pivot_signals[pn].get('entry_price'):
                    if sig.get('risk_points', 0) > pivot_signals[pn].get('risk_points', 0):
                        pivot_signals[pn] = sig
            entry_signals = list(pivot_signals.values())
        else:
            entry_signals = []

        if entry_signals:
            for signal in entry_signals:
                pivot_number = signal['pivot_number']
                candle_time = signal.get('candle_time')
                candle_str = f" (Candle: {candle_time})" if candle_time else ""
                entry_method = signal.get('entry_method', 'red_candle')

                self.active_divergences = [
                    div for div in self.active_divergences
                    if div['pivot_number'] != pivot_number
                ]
                self.logger.info(
                    f"[FUTURES] Stopped tracking pivot {pivot_number}{candle_str} "
                    f"after {entry_method} entry"
                )

                # Clean up ALL entry managers (both spot and futures)
                self.entry_manager.remove_entry_setup(pivot_number)
                self.pullback_manager.remove_entry_setup(pivot_number)
                self.mother_child_manager.remove_entry_setup(pivot_number)
                self.three_bottom_manager.remove_entry_setup(pivot_number)
                self.fut_entry_manager.remove_entry_setup(pivot_number)
                self.fut_pullback_manager.remove_entry_setup(pivot_number)
                self.fut_mother_child_manager.remove_entry_setup(pivot_number)
                self.fut_three_bottom_manager.remove_entry_setup(pivot_number)

                # Remove pivot from tracking
                if hasattr(self.spot_5m, 'remove_pivot_from_tracking'):
                    self.spot_5m.remove_pivot_from_tracking(pivot_number)
                if hasattr(self.futures_5m, 'remove_pivot_from_tracking'):
                    self.futures_5m.remove_pivot_from_tracking(pivot_number)
                self.logger.info(f"[PIVOT TRACKING] Removed pivot {pivot_number}{candle_str} after futures entry")

                return self._convert_to_options_signal(signal, price)

        return None

    def on_candle_close(self, candle, timeframe='1m', data_source='spot'):
        """
        Called when a new candle closes. Used to update entry logic.

        Args:
            candle: Dictionary with time, open, high, low, close, volume
            timeframe: Timeframe of the candle ('1m', '5m', etc.)
            data_source: 'spot' or 'futures' - routes to the correct entry managers
        """
        if timeframe == '1m':
            current_price = candle.get('close', 0)
            if data_source == 'spot':
                self.entry_manager.update_with_new_candle(candle, current_price)
                self.pullback_manager.update_with_new_candle(candle, current_price)
                self.mother_child_manager.update_with_new_candle(candle, current_price)
                self.three_bottom_manager.update_with_new_candle(candle, current_price)
            elif data_source == 'futures':
                self.fut_entry_manager.update_with_new_candle(candle, current_price)
                self.fut_pullback_manager.update_with_new_candle(candle, current_price)
                self.fut_mother_child_manager.update_with_new_candle(candle, current_price)
                self.fut_three_bottom_manager.update_with_new_candle(candle, current_price)

    def on_candle_correction(self, corrected_candle, data_source='spot'):
        """
        Called when a 1m candle's OHLC is corrected by historical API validation.
        Updates derived values (entry levels, stop losses) in all entry managers.
        """
        if data_source == 'spot':
            managers = [
                self.entry_manager, self.pullback_manager,
                self.mother_child_manager, self.three_bottom_manager
            ]
        else:
            managers = [
                self.fut_entry_manager, self.fut_pullback_manager,
                self.fut_mother_child_manager, self.fut_three_bottom_manager
            ]

        for manager in managers:
            if hasattr(manager, 'correct_last_candle'):
                manager.correct_last_candle(corrected_candle)

    def _update_futures_pivot_lows(self, current_time):
        """
        Update futures pivot lows based on spot pivot times.
        Derives futures pivots from spot pivots (same time, different price).
        """
        try:
            if not hasattr(self, '_futures_pivots_initialized'):
                self.futures_5m.pivot_lows = []
                self.futures_5m.broken_pivot_lows = []
                self._futures_pivots_initialized = True
                self.logger.info("Initialized futures pivot lows tracking based on spot pivot times")

            newly_added = False
            for spot_pivot in self.spot_5m.pivot_lows:
                # Check if futures pivot already exists at this time
                existing_fut_pivot = next(
                    (p for p in self.futures_5m.pivot_lows if p['time'] == spot_pivot['time']),
                    None
                )
                if not existing_fut_pivot:
                    # Get futures low at spot pivot time
                    fut_price = self.spot_5m.get_futures_low_at_time(spot_pivot['time'])
                    if fut_price is not None:
                        fut_pivot = {
                            'time': spot_pivot['time'],
                            'price': fut_price,
                            'removed': False
                        }
                        self.futures_5m.pivot_lows.append(fut_pivot)
                        pivot_number = len(self.futures_5m.pivot_lows)
                        self.logger.info(
                            f"[DERIVED] Futures Pivot Low {pivot_number} (Candle: {fut_pivot['time']}) "
                            f"from spot pivot - Price: {fut_pivot['price']:.2f}"
                        )
                        newly_added = True

            # Check for historical breakdowns if we just added futures pivots
            if newly_added and hasattr(self, '_futures_pivots_initialized'):
                if self.futures_5m.completed_candles:
                    last_candle = self.futures_5m.completed_candles[-1]
                    last_price = last_candle['close']
                    last_time = last_candle['time']

                    # Check which futures pivots are already broken
                    for fut_pivot in self.futures_5m.pivot_lows:
                        if fut_pivot.get('removed', False):
                            continue

                        # Check if already marked as broken
                        already_broken = any(
                            broken['pivot_time'] == fut_pivot['time']
                            for broken in self.futures_5m.broken_pivot_lows
                        )

                        if not already_broken and last_price <= fut_pivot['price']:
                            pivot_number = self.futures_5m.pivot_lows.index(fut_pivot) + 1
                            broken_pivot = {
                                'pivot_time': fut_pivot['time'],
                                'pivot_price': fut_pivot['price'],
                                'breakdown_time': last_time,
                                'breakdown_price': last_price,
                                'pivot_number': pivot_number
                            }
                            self.futures_5m.broken_pivot_lows.append(broken_pivot)
                            self.logger.info(
                                f"[HISTORICAL BREAKDOWN] FUTURES Pivot Low {pivot_number} "
                                f"was already broken in historical data! "
                                f"Pivot: {broken_pivot['pivot_time']} @ {broken_pivot['pivot_price']:.2f} -> "
                                f"Last historical: {last_time} @ {last_price:.2f}"
                            )

        except Exception as e:
            self.logger.error(f"Error updating futures pivot lows: {e}")

    def _check_for_divergence(self, current_price, current_time):
        """
        Check for divergence between spot and futures pivot low breakdowns.
        Also checks for divergence ending condition (both pivots broken).
        """
        threshold = timedelta(minutes=self.divergence_threshold_minutes)

        # Get pivot lows from spot and futures
        spot_pivots = self.spot_5m.pivot_lows if hasattr(self.spot_5m, 'pivot_lows') else []
        futures_pivots = self.futures_5m.pivot_lows if hasattr(self.futures_5m, 'pivot_lows') else []

        max_pivots = max(len(spot_pivots), len(futures_pivots))

        # Check for divergence ending first (for already tracked divergences)
        for idx, div in enumerate(self.active_divergences[:]):  # Use copy to allow removal during iteration
            pivot_number = div['pivot_number']

            # Get breakdown information for this pivot
            spot_broken = self._get_broken_pivot(self.spot_5m, pivot_number)
            futures_broken = self._get_broken_pivot(self.futures_5m, pivot_number)

            spot_time = spot_broken['breakdown_time'] if spot_broken else None
            futures_time = futures_broken['breakdown_time'] if futures_broken else None

            # DIVERGENCE ENDING CONDITION: Both pivots now broken
            if spot_time and futures_time:
                # Get candle time from the pivot
                pivot_data = next((p for p in spot_pivots if p.get('pivot_number') == pivot_number or spot_pivots.index(p) + 1 == pivot_number), None)
                if not pivot_data:
                    pivot_data = next((p for p in futures_pivots if p.get('pivot_number') == pivot_number or futures_pivots.index(p) + 1 == pivot_number), None)
                candle_time = pivot_data['time'] if pivot_data else 'N/A'

                duration_sec = (current_time - div['start_time']).total_seconds()
                self.logger.info("\n" + "!"*60)
                self.logger.info("🛑 DIVERGENCE ENDED 🛑")
                self.logger.info("!"*60)
                self.logger.info(f"📊 Pivot {pivot_number} (Candle: {candle_time}) divergence has ended at {current_time}")
                self.logger.info(f"⏹️ ENTRY SIGNAL DEACTIVATED at {current_time}")
                self.logger.info("📝 Reason: Both spot and futures pivot lows are now broken")
                self.logger.info(f"⏱️ Divergence duration: {duration_sec:.0f} seconds")

                # Stop looking for entry (all managers - spot and futures)
                self.entry_manager.remove_entry_setup(pivot_number)
                self.pullback_manager.remove_entry_setup(pivot_number)
                self.mother_child_manager.remove_entry_setup(pivot_number)
                self.three_bottom_manager.remove_entry_setup(pivot_number)
                self.fut_entry_manager.remove_entry_setup(pivot_number)
                self.fut_pullback_manager.remove_entry_setup(pivot_number)
                self.fut_mother_child_manager.remove_entry_setup(pivot_number)
                self.fut_three_bottom_manager.remove_entry_setup(pivot_number)

                # Mark the pivot as removed in both spot and futures data
                self.spot_5m.remove_pivot_from_tracking(pivot_number)
                self.futures_5m.remove_pivot_from_tracking(pivot_number)

                self.logger.info("\n" + "-"*50)
                self.logger.info(f"📍 PIVOT TRACKING STOPPED for Pivot {pivot_number} (Candle: {candle_time}) 📍")
                self.logger.info("📝 Reason: Both spot and futures pivots are broken")
                self.logger.info("-"*50)

                # Remove from active divergences
                self.active_divergences.pop(idx)

        # Now check for NEW divergences
        self._log_state(
            "divergence_scan_count",
            max_pivots,
            f"[DIVERGENCE SCAN] Scanning {max_pivots} pivot(s) for new divergences"
        )

        for i in range(max_pivots):
            # Skip if pivot is removed
            spot_pivot = spot_pivots[i] if i < len(spot_pivots) else None
            futures_pivot = futures_pivots[i] if i < len(futures_pivots) else None

            spot_removed = bool(spot_pivot and spot_pivot.get('removed', False))
            futures_removed = bool(futures_pivot and futures_pivot.get('removed', False))
            if spot_removed or futures_removed:
                # Get candle time from the pivot
                candle_time = spot_pivot['time'] if spot_pivot else (futures_pivot['time'] if futures_pivot else 'N/A')
                self._log_state(
                    f"pivot_{i+1}_removed",
                    (spot_removed, futures_removed),
                    f"[PIVOT REMOVED] Pivot Low {i+1} (Candle: {candle_time}) - Spot removed: {spot_removed}, Futures removed: {futures_removed}"
                )
                continue

            # Check if already tracking this pivot
            if any(div['pivot_number'] == i+1 for div in self.active_divergences):
                continue

            # Get breakdown information
            spot_broken = self._get_broken_pivot(self.spot_5m, i+1)
            futures_broken = self._get_broken_pivot(self.futures_5m, i+1)

            spot_time = spot_broken['breakdown_time'] if spot_broken else None
            futures_time = futures_broken['breakdown_time'] if futures_broken else None

            # Skip logging for broken pivots (both spot and futures broken)
            if spot_time and futures_time:
                continue

            # Log pivot prices
            spot_price = spot_pivot['price'] if spot_pivot else None
            futures_price = futures_pivot['price'] if futures_pivot else None

            # Get candle time from the pivot
            candle_time = spot_pivot['time'] if spot_pivot else (futures_pivot['time'] if futures_pivot else 'N/A')

            pivot_state = (spot_time, futures_time, spot_price, futures_price)
            spot_price_str = f"{spot_price:.2f}" if spot_price is not None else "N/A"
            futures_price_str = f"{futures_price:.2f}" if futures_price is not None else "N/A"
            spot_time_str = spot_time if spot_time else "N/A"
            futures_time_str = futures_time if futures_time else "N/A"
            self._log_state(
                f"pivot_{i+1}_status",
                pivot_state,
                (
                    f"[PIVOT STATUS] Pivot Low {i+1} (Candle: {candle_time}) - Spot broken: {bool(spot_time)}, "
                    f"Futures broken: {bool(futures_time)}, "
                    f"Spot price: {spot_price_str}, Futures price: {futures_price_str}, "
                    f"Spot time: {spot_time_str}, Futures time: {futures_time_str}"
                ),
            )

            diverging = False
            divergence_type = ""

            # Check for divergence conditions
            # Immediate divergence when one side breaks and other hasn't — no time threshold
            if spot_time and not futures_time:
                # Spot broken but futures not — immediate divergence
                diverging = True
                divergence_type = "UNBROKEN DIVERGENCE (Futures holding)"

            elif futures_time and not spot_time:
                # Futures broken but spot not — immediate divergence
                diverging = True
                divergence_type = "UNBROKEN DIVERGENCE (Spot holding)"

            if diverging:
                # Validate: at least one unbroken pivot high (formed BEFORE the pivot low) must exist
                candle_time = spot_pivot['time'] if spot_pivot else (futures_pivot['time'] if futures_pivot else 'N/A')
                pivot_low_time = spot_pivot['time'] if spot_pivot else None
                unbroken_pivot_highs = [
                    ph for ph in self.spot_5m.get_unbroken_pivot_highs()
                    if pivot_low_time and ph['time'] < pivot_low_time
                ]

                # Include first candle HIGH as a pivot high for validation (if before pivot low)
                first_candle_included = False
                fc_label = f"{settings.MARKET_OPEN_HOUR:02d}:{settings.MARKET_OPEN_MINUTE:02d}"
                if self._first_candle_pivot:
                    fc_high = self._first_candle_pivot['price']
                    fc_time = self._first_candle_pivot['time']
                    fc_before_pivot = pivot_low_time and fc_time < pivot_low_time
                    already_pivot = any(ph['time'] == fc_time for ph in self.spot_5m.pivot_highs)
                    if not fc_before_pivot:
                        self.logger.info(f"[FIRST CANDLE] {fc_label} candle high @ {fc_high:.2f} skipped (not before pivot low {candle_time})")
                    elif self._first_candle_broken:
                        self.logger.info(
                            f"[FIRST CANDLE] {fc_label} candle high @ {fc_high:.2f} is BROKEN "
                            f"(broken at {self._first_candle_breakout_time} @ {self._first_candle_breakout_price:.2f})"
                        )
                    elif not already_pivot:
                        first_candle_entry = {
                            'pivot_number': 'FC',
                            'time': fc_time,
                            'price': fc_high,
                            'source': 'first_candle'
                        }
                        unbroken_pivot_highs.append(first_candle_entry)
                        first_candle_included = True
                        self.logger.info(f"[FIRST CANDLE] {fc_label} candle high @ {fc_high:.2f} included as pivot high for validation")
                    else:
                        self.logger.info(f"[FIRST CANDLE] {fc_label} candle high @ {fc_high:.2f} already exists as a detected pivot high")

                total_pivot_highs = len(self.spot_5m.pivot_highs) + (1 if first_candle_included else 0)
                broken_pivot_highs = len(self.spot_5m.broken_pivot_highs) + (1 if self._first_candle_broken and self._first_candle_pivot else 0)

                self.logger.info("\n" + "-"*50)
                self.logger.info(f"[PIVOT HIGH CHECK] Validating divergence for Pivot Low {i+1} (Candle: {candle_time}) — checking pivot highs before {candle_time}")
                self.logger.info(f"  Total pivot highs (incl. first candle): {total_pivot_highs}")
                self.logger.info(f"  Broken pivot highs: {broken_pivot_highs}")
                self.logger.info(f"  Unbroken pivot highs for validation: {len(unbroken_pivot_highs)}")

                if not unbroken_pivot_highs:
                    self.logger.info(f"[DIVERGENCE INVALIDATED] Pivot Low {i+1} (Candle: {candle_time}) - No unbroken pivot highs exist, skipping")
                    self.logger.info("-"*50)
                    continue
                else:
                    for ph in unbroken_pivot_highs:
                        ph_num = ph.get('pivot_number', 'N/A')
                        source = " (first candle)" if ph.get('source') == 'first_candle' else ""
                        self.logger.info(f"  Unbroken Pivot High {ph_num}: {ph['time']} @ {ph['price']:.2f}{source}")
                    self.logger.info(f"[DIVERGENCE VALIDATED] {len(unbroken_pivot_highs)} unbroken pivot high(s) found - divergence is valid")
                    self.logger.info("-"*50)

                self.logger.info("\n" + "#"*60)
                self.logger.info("⚠️⚠️⚠️ DIVERGENCE DETECTED ⚠️⚠️⚠️")
                self.logger.info("#"*60)
                self.logger.info(f"📊 Type: {divergence_type}")
                self.logger.info(f"📊 Pivot Low {i+1} (Candle: {candle_time})")

                # Start tracking divergence
                divergence_record = {
                    'pivot_number': i+1,
                    'start_time': current_time,
                    'candle_time': candle_time,
                    'spot_broken': bool(spot_time),
                    'fut_broken': bool(futures_time),
                    'divergence_type': divergence_type
                }
                self.active_divergences.append(divergence_record)

                # --- SPOT entry managers ---
                prev_1m_candle = self.spot_1m.completed_candles[-1] if hasattr(self.spot_1m, 'completed_candles') and self.spot_1m.completed_candles else None
                self.entry_manager.add_entry_setup(i+1, current_time, candle_time, prev_candle=prev_1m_candle)
                self.pullback_manager.add_entry_setup(i+1, current_time, candle_time, prev_candle=prev_1m_candle)

                last_1m_candle = self.spot_1m.completed_candles[-1] if hasattr(self.spot_1m, 'completed_candles') and self.spot_1m.completed_candles else None
                window_low = last_1m_candle['low'] if last_1m_candle else current_price
                self.mother_child_manager.add_entry_setup(i+1, current_time, candle_time, current_candle=last_1m_candle, window_low=window_low)

                initial_low = spot_pivot['price'] if spot_pivot else current_price
                self.three_bottom_manager.add_entry_setup(i+1, current_time, candle_time, initial_low)

                # --- FUTURES entry managers ---
                fut_prev_1m = self.futures_1m.completed_candles[-1] if self.futures_1m and hasattr(self.futures_1m, 'completed_candles') and self.futures_1m.completed_candles else None
                self.fut_entry_manager.add_entry_setup(i+1, current_time, candle_time, prev_candle=fut_prev_1m)
                self.fut_pullback_manager.add_entry_setup(i+1, current_time, candle_time, prev_candle=fut_prev_1m)

                fut_last_1m = self.futures_1m.completed_candles[-1] if self.futures_1m and hasattr(self.futures_1m, 'completed_candles') and self.futures_1m.completed_candles else None
                fut_window_low = fut_last_1m['low'] if fut_last_1m else current_price
                self.fut_mother_child_manager.add_entry_setup(i+1, current_time, candle_time, current_candle=fut_last_1m, window_low=fut_window_low)

                self.fut_three_bottom_manager.add_entry_setup(i+1, current_time, candle_time, initial_low)

    def _get_broken_pivot(self, data_series, pivot_number):
        """Get broken pivot information from data series."""
        if hasattr(data_series, 'get_broken_pivot_lows'):
            broken_pivots = data_series.get_broken_pivot_lows()
            for broken in broken_pivots:
                if broken['pivot_number'] == pivot_number:
                    return broken
        return None

    def _convert_to_options_signal(self, signal, current_price):
        """
        Convert divergence signal to options trading signal format.
        Similar to bullish swing strategy format.
        """
        entry_price = signal['entry_price']
        stop_loss = signal['stop_loss']
        risk_points = signal['risk_points']

        # Calculate target based on risk-reward ratio
        risk_reward = getattr(settings, 'RISK_REWARD', 2.0)
        target = entry_price + (risk_points * risk_reward)

        self.logger.info(
            f"[DIVERGENCE SIGNAL] Entry {entry_price:.2f} SL {stop_loss:.2f} Target {target:.2f}"
        )

        # Get options data if broker available
        if self.broker:
            options_data = self._fetch_and_select_option(current_price, entry_price, stop_loss, target)
            if options_data:
                options_data['pivot_number'] = signal.get('pivot_number')
                return options_data

        # Return basic signal without options data
        return {
            'strategy': 'bullish_divergence',
            'signal_type': 'BUY',
            'entry_price': entry_price,
            'stop_loss': stop_loss,
            'target': target,
            'pivot_number': signal['pivot_number'],
            'risk_points': risk_points
        }

    def _calculate_option_stop_loss(self, option_data, underlying_entry, underlying_sl):
        """
        Calculate option-specific stop loss using Greeks and underlying SL.
        """
        try:
            delta = float(option_data.get('delta', 0) or 0)
            gamma = float(option_data.get('gamma', 0) or 0)
            theta = float(option_data.get('theta', 0) or 0)

            underlying_entry = float(underlying_entry)
            underlying_sl = float(underlying_sl)

            underlying_move = abs(underlying_entry - underlying_sl)

            delta_impact = delta * underlying_move
            gamma_impact = 0.5 * gamma * (underlying_move ** 2)
            theta_impact = (abs(theta) / (24 * 6)) * (10 / 60)

            total_price_change = delta_impact + gamma_impact + theta_impact

            return {
                'total_sl': total_price_change,
                'components': {
                    'delta_impact': delta_impact,
                    'gamma_impact': gamma_impact,
                    'theta_impact': theta_impact
                }
            }
        except Exception as e:
            self.logger.error(f"Error calculating option stop loss: {e}")
            return None

    def _fetch_and_select_option(self, spot_price, entry_price, stop_loss, target):
        """
        Fetch and select the best CALL option for the divergence trade using
        Greeks-based risk calculation and multi-lot optimization to find the
        (strike, lot) combination closest to TARGET_RISK_MID.
        """
        try:
            if not self.broker or not hasattr(self.broker, 'fetch_options_chain'):
                self.logger.warning("Broker not available or missing fetch_options_chain")
                return None

            # Fetch options chain with Greeks from broker
            options_df = self.broker.fetch_options_chain(spot_price)
            if options_df is None or options_df.empty:
                self.logger.warning("No options data available from broker")
                return None

            # Filter CE options only
            ce_options = options_df[options_df['option_type'] == 'CE'].copy()
            if ce_options.empty:
                self.logger.warning("No CE options available")
                return None

            # Filter strikes near ATM (within 200 points)
            atm_strike = round(entry_price / 50) * 50
            ce_options = ce_options[abs(ce_options['strike_float'] - atm_strike) <= 200]

            if ce_options.empty:
                self.logger.warning("No CE options found near ATM")
                return None

            # Get nearest 2 expiries
            unique_expiries = sorted(ce_options['expiry_date'].unique())[:2]
            ce_options = ce_options[ce_options['expiry_date'].isin(unique_expiries)]

            # Calculate SL for each option using Greeks
            options_with_sl = []
            for _, option in ce_options.iterrows():
                sl_calc = self._calculate_option_stop_loss(option, entry_price, stop_loss)
                if sl_calc:
                    option_data = option.to_dict()
                    option_data['option_sl_points'] = sl_calc['total_sl']
                    option_data['sl_components'] = sl_calc['components']
                    options_with_sl.append(option_data)

            if not options_with_sl:
                self.logger.warning("No options with valid SL calculation")
                return None

            # Display all options with their Greeks and stop losses
            self.logger.info("\n=== ALL SELECTED OPTIONS WITH GREEKS AND STOP LOSSES ===")
            for i, option in enumerate(options_with_sl):
                self.logger.info(f"\nOption {i+1}:")
                self.logger.info(f"  Strike: {option['strike_float']} CE")
                self.logger.info(f"  Expiry: {option.get('expiry_date', 'N/A')}")
                self.logger.info(f"  Greeks:")
                self.logger.info(f"    Delta: {option.get('delta', 'N/A')}")
                self.logger.info(f"    Gamma: {option.get('gamma', 'N/A')}")
                self.logger.info(f"    Theta: {option.get('theta', 'N/A')}")
                self.logger.info(f"    Vega: {option.get('vega', 'N/A')}")
                self.logger.info(f"    IV: {option.get('impliedVolatility', 'N/A')}")
                self.logger.info(f"  Stop Loss: {option['option_sl_points']:.2f}")
                self.logger.info(f"  SL Components:")
                self.logger.info(f"    Delta Impact: {option['sl_components']['delta_impact']:.2f}")
                self.logger.info(f"    Gamma Impact: {option['sl_components']['gamma_impact']:.2f}")
                self.logger.info(f"    Theta Impact: {option['sl_components']['theta_impact']:.2f}")

            # Multi-lot risk optimization: find (strike, lot) closest to TARGET_RISK_MID
            self.logger.info(f"\n=== SELECTING BEST OPTION FROM {len(options_with_sl)} SELECTED STRIKES ONLY ===")
            target_risk_range = (settings.TARGET_RISK_MIN, settings.TARGET_RISK_MAX)
            target_risk_mid = settings.TARGET_RISK_MID
            lot_size = settings.LOT_SIZE
            best_option = None
            best_quantity = None
            best_risk = None
            closest_distance = float('inf')

            for option in options_with_sl:
                sl_points = abs(option['option_sl_points'])
                risk_per_lot = sl_points * lot_size

                if risk_per_lot <= 0:
                    continue

                self.logger.info(f"\nAnalyzing {option['strike_float']} CE (SL: {sl_points:.2f} points, Risk per lot: ₹{risk_per_lot:.2f}):")

                lots = 1
                while True:
                    quantity = lots * lot_size
                    total_risk = risk_per_lot * lots

                    if total_risk > target_risk_range[1]:
                        self.logger.info(f"  {lots} lots: ₹{total_risk:.2f} (exceeds ₹{target_risk_range[1]:.0f} limit)")
                        break

                    distance = abs(total_risk - target_risk_mid)

                    self.logger.info(f"  {lots} lots: ₹{total_risk:.2f} (distance from ₹{target_risk_mid:.0f}: ₹{distance:.2f})")

                    if distance < closest_distance:
                        closest_distance = distance
                        best_option = option
                        best_quantity = quantity
                        best_risk = total_risk
                        self.logger.info(f"    -> NEW BEST OPTION! Distance: ₹{distance:.2f}")

                    lots += 1
                    if lots > settings.MAX_LOTS:
                        self.logger.info(f"  Reached maximum {settings.MAX_LOTS} lots limit ({settings.MAX_LOTS * lot_size} quantity)")
                        break

            if best_option is None:
                self.logger.warning("No suitable option found for target risk")
                return None

            self.logger.info(f"\n=== SELECTED OPTION (Closest to ₹{target_risk_mid} target) ===")
            self.logger.info(f"Strike: {best_option['strike_float']} CE")
            self.logger.info(f"Expiry: {best_option.get('expiry_date', 'N/A')}")
            self.logger.info(f"Quantity: {best_quantity} (Lots: {best_quantity//lot_size})")
            self.logger.info(f"Total Risk: ₹{best_risk:.2f}")
            self.logger.info(f"Distance from Target: ₹{closest_distance:.2f}")
            self.logger.info(f"Stop Loss: {best_option['option_sl_points']:.2f}")
            self.logger.info(f"Greeks Profile:")
            self.logger.info(f"  Delta: {best_option.get('delta', 'N/A')}")
            self.logger.info(f"  Gamma: {best_option.get('gamma', 'N/A')}")
            self.logger.info(f"  Theta: {best_option.get('theta', 'N/A')}")
            self.logger.info(f"  Vega: {best_option.get('vega', 'N/A')}")
            self.logger.info(f"  IV: {best_option.get('impliedVolatility', 'N/A')}")

            return {
                'selected_option': best_option,
                'selected_quantity': best_quantity,
                'selected_risk': best_risk,
                'transaction_type': 'BUY',
                'order_type': 'MARKET',
                'option_sl_points': abs(best_option['option_sl_points']),
                'target_multiple': 2,
                'entry_price': entry_price,
                'stop_loss': stop_loss,
                'target': target,
                'strategy': 'bullish_divergence'
            }

        except Exception as e:
            self.logger.error(f"Error fetching options: {e}")
            return None

    def print_current_structure(self):
        """Print current divergence tracking status."""
        self.logger.info("\n===== BULLISH DIVERGENCE STATUS =====")
        self.logger.info(f"Active Divergences: {len(self.active_divergences)}")

        for div in self.active_divergences:
            candle_str = f" (Candle: {div['candle_time']})" if div.get('candle_time') else ""
            self.logger.info(f"  Pivot {div['pivot_number']}{candle_str}: {div['divergence_type']}")
            self.logger.info(f"    Started: {div['start_time']}")

        entry_summary = self.entry_manager.get_active_setups_summary()
        self.logger.info(f"\n{entry_summary}")

        pullback_summary = self.pullback_manager.get_active_setups_summary()
        self.logger.info(f"\n{pullback_summary}")

        mother_child_summary = self.mother_child_manager.get_active_setups_summary()
        self.logger.info(f"\n{mother_child_summary}")

        three_bottom_summary = self.three_bottom_manager.get_active_setups_summary()
        self.logger.info(f"\n{three_bottom_summary}")
        self.logger.info("="*40)
