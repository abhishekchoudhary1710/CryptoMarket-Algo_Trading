"""
FeedHandler
- Receives websocket ticks (spot + futures)
- Updates OHLCV for 1m / 5m / 15m on BOTH tokens
- Sends only spot ticks to strategy via router
"""

import json
import time
import threading
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from utils.logger import logger
from utils.live_capture import log_tick
from utils.helpers import is_before_market_open
from api.state import shared_state


class FeedHandler:
    def __init__(self, spot_token, fut_token, spot_series, fut_series, router, broker=None):
        self.spot_token = str(spot_token)
        self.fut_token = str(fut_token) if fut_token else None

        self.spot_series = spot_series
        self.fut_series = fut_series
        self.router = router
        self.broker = broker  # For historical candle validation

        # Tick counters for periodic logging
        self.data_count = 0
        self.spot_ltp = 0
        self.fut_ltp = 0
        self._premarket_active = None
        self._premarket_cleared = False
        self._premarket_logged = False

        # Set candle close callbacks for real-time structure detection
        for tf_name, tf_series in self.spot_series.items():
            tf_series.on_candle_close = self._on_candle_close
        for tf_name, tf_series in self.fut_series.items():
            tf_series.on_candle_close = self._on_candle_close

    def on_ws_message(self, message):
        """
        AngelOne WS message could be:
        - dict with {data: [ticks]}
        - single tick dict
        - JSON string of either of the above
        """
        try:
            if isinstance(message, str):
                try:
                    message = json.loads(message)
                except Exception:
                    logger.warning(f"WS message not JSON parsable: {message[:120]}")
                    return

            # If batch
            if isinstance(message, dict) and "data" in message and isinstance(message["data"], list):
                for tick in message["data"]:
                    self._process_single_tick(tick)
            else:
                self._process_single_tick(message)

        except Exception as e:
            logger.error(f"FeedHandler error: {e}")

    def _process_single_tick(self, tick):
        try:
            if not isinstance(tick, dict):
                return

            token = str(tick.get("token") or tick.get("symbolToken") or "")
            ltp_raw = tick.get("last_traded_price") or tick.get("ltp") or tick.get("lastTradedPrice")

            if token == "" or ltp_raw is None:
                return

            # AngelOne sends LTP in paise -> convert to rupees
            price = float(ltp_raw) * 0.01
            ts = datetime.now()

            if self._skip_premarket_ticks(ts):
                return
            
            # Increment data counter
            self.data_count += 1

            # ---------------- SPOT TICK ----------------
            if token == self.spot_token:
                self.spot_ltp = price
                # Update shared state for API (every tick for real-time dashboard)
                shared_state.update_prices(self.spot_ltp, self.fut_ltp, self.data_count)
                log_tick("spot", token, price, ts)
                
                for tf_name, tf_series in self.spot_series.items():
                    tf_series.update_from_tick(price, ts)

                # Log every 10th spot tick (matching bullish3t1.py format)
                if self.data_count % 10 == 0:
                    logger.info(f"Spot tick: {price}")

                # Check spot pivot breakouts for divergence strategies
                if hasattr(self.router, 'bearish_divergence_strategy') and self.router.bearish_divergence_strategy:
                    self.spot_series["5m"].check_pivot_high_breakdown(price, ts)
                    self.spot_series["5m"].check_pivot_low_breakdown(price, ts)
                elif hasattr(self.router, 'divergence_strategy') and self.router.divergence_strategy:
                    # Ensure pivot high breakdowns are tracked for bullish divergence validation
                    self.spot_series["5m"].check_pivot_high_breakdown(price, ts)

                # Only SPOT ticks go to strategy/router
                self.router.route_price("spot", price)

                # Check for breakout signals and auto-place orders if enabled
                if hasattr(self.router, 'check_for_breakout_signals'):
                    self.router.check_for_breakout_signals("spot", price)

            # ---------------- FUTURES TICK ----------------
            elif self.fut_token and token == self.fut_token:
                self.fut_ltp = price
                log_tick("fut", token, price, ts)

                for tf_name, tf_series in self.fut_series.items():
                    tf_series.update_from_tick(price, ts)

                # Update futures pivots from spot on futures ticks (for both divergence strategies)
                if hasattr(self.router, 'divergence_strategy') and self.router.divergence_strategy:
                    self.router.divergence_strategy._update_futures_pivot_lows(ts)
                    # Check futures pivot breakdown on futures ticks
                    self.fut_series["5m"].check_pivot_low_breakdown(price, ts)

                if hasattr(self.router, 'bearish_divergence_strategy') and self.router.bearish_divergence_strategy:
                    self.router.bearish_divergence_strategy._update_futures_pivot_highs(ts)
                    # Check futures pivot breakdown on futures ticks (for highs)
                    self.fut_series["5m"].check_pivot_high_breakdown(price, ts)

                # Log every 10th futures tick (matching bullish3t1.py format)
                if self.data_count % 10 == 0:
                    logger.info(f"Futures tick: {price}")

                # Futures strategy not active yet, but router still receives instrument tag
                self.router.route_price("fut", price)

        except Exception as e:
            logger.error(f"Single tick processing failed: {e}")

    def _skip_premarket_ticks(self, ts):
        if not is_before_market_open(ts):
            if self._premarket_active:
                self._clear_ohlcv_state()
                self._premarket_active = False
                self._premarket_cleared = False
                self._premarket_logged = False
                logger.info("[MARKET OPEN] Starting tick processing from 09:15 IST")
            return False

        if self._premarket_active is not True:
            self._premarket_active = True
            self._premarket_cleared = False
            self._premarket_logged = False

        if not self._premarket_logged:
            logger.info("[PREMARKET] Ignoring ticks before 09:15 IST; clearing cached candles")
            self._premarket_logged = True

        if not self._premarket_cleared:
            self._clear_ohlcv_state()
            self._premarket_cleared = True

        return True

    def _clear_ohlcv_state(self):
        for series in self.spot_series.values():
            series.clear_data()
        for series in self.fut_series.values():
            series.clear_data()
        self.data_count = 0
        self.spot_ltp = 0
        self.fut_ltp = 0

    def _on_candle_close(self, timeframe, instrument):
        """
        Callback triggered when a candle closes.
        Updates strategy structures in real-time for 5-minute candles.
        Notifies divergence strategy on 1-minute candle close for red candle detection.

        Args:
            timeframe (int): Timeframe in minutes (1, 5, 15)
            instrument (str): Instrument name (spot/fut)
        """
        try:
            # Update structures on 5-minute candle close for spot
            if timeframe == 5 and instrument == "spot":
                logger.info(f"[CANDLE CLOSE] 5-min candle closed, updating structures...")
                self._update_strategy_structures()

            # Notify divergence strategy on 1-minute candle close for entry logic
            if timeframe == 1 and instrument == "spot":
                self._notify_divergence_strategy_on_candle_close()
                # Validate candle with historical API in background
                self._launch_candle_validation("spot")

            # Notify divergence strategy on futures 1-minute candle close for futures entry logic
            if timeframe == 1 and instrument == "fut":
                self._notify_divergence_strategy_on_futures_candle_close()
                # Validate candle with historical API in background
                self._launch_candle_validation("fut")

        except Exception as e:
            logger.error(f"Error in candle close callback: {e}")

    def _update_strategy_structures(self):
        """Update strategy structures periodically (for both bullish and bearish)"""
        try:
            if not self.router:
                return

            spot_df = self.spot_series["5m"].get_dataframe()

            if spot_df.empty:
                logger.warning("Cannot update structures: 5m dataframe is empty")
                return

            # Log the data processing
            logger.info(f"Processing data with {len(spot_df)} entries.")

            # Update bullish strategy
            if self.router.bullish_strategy:
                self._update_single_strategy(self.router.bullish_strategy, spot_df, "BULLISH")

            # Update bearish strategy
            if self.router.bearish_strategy:
                self._update_single_strategy(self.router.bearish_strategy, spot_df, "BEARISH")

        except Exception as e:
            logger.error(f"Error updating strategy structures: {e}")

    def _update_single_strategy(self, strategy, spot_df, strategy_name):
        """Helper to update a single strategy's structures"""
        try:
            # Preserve current structure points
            current_points = {
                'H1': strategy.H1,
                'H1_idx': strategy.H1_idx,
                'L1': strategy.L1,
                'L1_idx': strategy.L1_idx,
                'A': strategy.A,
                'A_idx': strategy.A_idx,
                'B': strategy.B,
                'B_idx': strategy.B_idx,
                'C': strategy.C,
                'C_idx': strategy.C_idx,
                'D': strategy.D,
                'D_idx': strategy.D_idx,
                'pending_setup': strategy.pending_setup
            }

            # Update data reference
            strategy.data = spot_df

            # Rebuild signals DataFrame to match new data size
            strategy.signals = pd.DataFrame(index=spot_df.index)
            strategy.signals["signal"] = 0
            strategy.signals["entry_price"] = np.nan
            strategy.signals["stop_loss"] = np.nan
            strategy.signals["target"] = np.nan

            # Restore structure points
            for key, value in current_points.items():
                setattr(strategy, key, value)

            # Re-generate signals with full structure details suppressed during refresh
            logger.info(f"Updating {strategy_name} strategy structures")
            strategy.generate_signals(log_structure_details=False)

        except Exception as e:
            logger.error(f"Error updating {strategy_name} strategy structures: {e}")

    def _notify_divergence_strategy_on_candle_close(self):
        """Notify both bullish and bearish divergence strategies when a 1-minute candle closes"""
        try:
            if not self.router:
                return

            # Get the most recently completed 1-minute candle from spot data
            spot_1m_series = self.spot_series.get("1m")
            if not spot_1m_series:
                return

            # Get the last completed candle
            completed_candles = spot_1m_series.completed_candles
            if not completed_candles:
                return

            last_candle = completed_candles[-1]

            # Notify bullish divergence strategy (red candle entry)
            if hasattr(self.router, 'divergence_strategy') and self.router.divergence_strategy:
                if hasattr(self.router.divergence_strategy, 'on_candle_close'):
                    self.router.divergence_strategy.on_candle_close(last_candle, timeframe='1m')

            # Notify bearish divergence strategy (pullback entry)
            if hasattr(self.router, 'bearish_divergence_strategy') and self.router.bearish_divergence_strategy:
                if hasattr(self.router.bearish_divergence_strategy, 'on_candle_close'):
                    self.router.bearish_divergence_strategy.on_candle_close(last_candle, timeframe='1m')

        except Exception as e:
            logger.error(f"Error notifying divergence strategies on candle close: {e}")

    def _notify_divergence_strategy_on_futures_candle_close(self):
        """Notify divergence strategies when a futures 1-minute candle closes"""
        try:
            if not self.router:
                return

            fut_1m_series = self.fut_series.get("1m")
            if not fut_1m_series:
                return

            completed_candles = fut_1m_series.completed_candles
            if not completed_candles:
                return

            last_candle = completed_candles[-1]

            # Notify bullish divergence strategy with futures candle
            if hasattr(self.router, 'divergence_strategy') and self.router.divergence_strategy:
                if hasattr(self.router.divergence_strategy, 'on_candle_close'):
                    self.router.divergence_strategy.on_candle_close(last_candle, timeframe='1m', data_source='futures')

            # Notify bearish divergence strategy with futures candle
            if hasattr(self.router, 'bearish_divergence_strategy') and self.router.bearish_divergence_strategy:
                if hasattr(self.router.bearish_divergence_strategy, 'on_candle_close'):
                    self.router.bearish_divergence_strategy.on_candle_close(last_candle, timeframe='1m', data_source='futures')

        except Exception as e:
            logger.error(f"Error notifying divergence strategies on futures candle close: {e}")

    # ------------------------------------------------------------------
    # Historical candle validation (runs in background thread)
    # ------------------------------------------------------------------

    def _launch_candle_validation(self, instrument):
        """Launch background thread to validate the last completed 1m candle against historical API."""
        if not self.broker:
            return

        series = self.spot_series if instrument == "spot" else self.fut_series
        completed = series["1m"].completed_candles
        if not completed:
            return

        candle_time = completed[-1]['time']
        t = threading.Thread(
            target=self._validate_candle_with_historical,
            args=(instrument, candle_time),
            daemon=True
        )
        t.start()

    def _validate_candle_with_historical(self, instrument, candle_time):
        """
        Fetch 1m historical candle from broker API and compare with tick-based candle.
        If OHLC differs, correct the candle in-place and notify strategies.
        """
        try:
            # Wait for historical API to have the completed candle
            time.sleep(3)

            if instrument == "spot":
                exchange, token = "NSE", self.spot_token
                series = self.spot_series
            else:
                exchange, token = "NFO", self.fut_token
                series = self.fut_series

            if not token:
                return

            # Fetch a small window around the candle
            from_dt = candle_time.strftime("%Y-%m-%d %H:%M")
            to_dt = (candle_time + timedelta(minutes=2)).strftime("%Y-%m-%d %H:%M")

            params = {
                "exchange": exchange,
                "symboltoken": token,
                "interval": "ONE_MINUTE",
                "fromdate": from_dt,
                "todate": to_dt,
            }

            response = self.broker.historical_data(params)
            if not response or not response.get('status'):
                logger.debug(f"[CANDLE VALIDATION] {instrument.upper()} {candle_time} - No historical response")
                return

            data = response.get('data', [])
            if not data:
                logger.debug(f"[CANDLE VALIDATION] {instrument.upper()} {candle_time} - No historical data")
                return

            # Find the matching candle by timestamp
            target_str = candle_time.strftime("%Y-%m-%dT%H:%M")
            hist_candle = None
            for row in data:
                if target_str in row[0]:
                    hist_candle = {
                        'open': float(row[1]),
                        'high': float(row[2]),
                        'low': float(row[3]),
                        'close': float(row[4]),
                        'volume': int(row[5])
                    }
                    break

            if not hist_candle:
                logger.debug(f"[CANDLE VALIDATION] {instrument.upper()} {candle_time} - Candle not found in historical data")
                return

            # Find tick-based candle in completed_candles
            tick_candle = None
            for c in reversed(series["1m"].completed_candles):
                if c['time'] == candle_time:
                    tick_candle = c
                    break

            if not tick_candle:
                return

            # Compare OHLC
            diffs = {}
            for field in ['open', 'high', 'low', 'close']:
                if abs(tick_candle[field] - hist_candle[field]) > 0.01:
                    diffs[field] = (tick_candle[field], hist_candle[field])

            if not diffs:
                return

            # Log differences
            inst_label = instrument.upper()
            logger.warning(
                f"[CANDLE CORRECTION] {inst_label} {candle_time} - OHLC mismatch! "
                f"Tick: O={tick_candle['open']:.2f} H={tick_candle['high']:.2f} "
                f"L={tick_candle['low']:.2f} C={tick_candle['close']:.2f} | "
                f"Historical: O={hist_candle['open']:.2f} H={hist_candle['high']:.2f} "
                f"L={hist_candle['low']:.2f} C={hist_candle['close']:.2f}"
            )
            for field, (tick_val, hist_val) in diffs.items():
                logger.warning(
                    f"[CANDLE CORRECTION] {inst_label} {candle_time} "
                    f"{field}: {tick_val:.2f} -> {hist_val:.2f} (diff={hist_val - tick_val:+.2f})"
                )

            # Correct candle in-place (updates all references including entry manager setup_candles)
            tick_candle['open'] = hist_candle['open']
            tick_candle['high'] = hist_candle['high']
            tick_candle['low'] = hist_candle['low']
            tick_candle['close'] = hist_candle['close']
            tick_candle['volume'] = hist_candle['volume']

            logger.info(f"[CANDLE CORRECTION] {inst_label} {candle_time} - Corrected with historical data")

            # Notify strategies to update derived values (entry levels, stop losses)
            data_source = 'spot' if instrument == 'spot' else 'futures'
            self._notify_candle_correction(tick_candle, data_source)

        except Exception as e:
            logger.error(f"[CANDLE VALIDATION] Error validating {instrument} candle at {candle_time}: {e}")

    def _notify_candle_correction(self, corrected_candle, data_source):
        """Notify divergence strategies that a candle was corrected."""
        try:
            if not self.router:
                return

            if hasattr(self.router, 'divergence_strategy') and self.router.divergence_strategy:
                if hasattr(self.router.divergence_strategy, 'on_candle_correction'):
                    self.router.divergence_strategy.on_candle_correction(corrected_candle, data_source=data_source)

            if hasattr(self.router, 'bearish_divergence_strategy') and self.router.bearish_divergence_strategy:
                if hasattr(self.router.bearish_divergence_strategy, 'on_candle_correction'):
                    self.router.bearish_divergence_strategy.on_candle_correction(corrected_candle, data_source=data_source)

        except Exception as e:
            logger.error(f"[CANDLE CORRECTION] Error notifying strategies: {e}")
