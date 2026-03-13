"""
OHLCV (Open, High, Low, Close, Volume) data module.
Provides classes and functions for working with time series price data.
"""

import os
import pandas as pd
from datetime import datetime
from utils.logger import logger
from config import settings


class LiveOHLCVData:
    """
    Class for managing real-time OHLCV data with multiple timeframes.
    Processes tick data into candles of various timeframes.
    """

    def __init__(self, timeframe_minutes=1, name="default", on_candle_close=None):
        """
        Initialize LiveOHLCVData instance.

        Args:
            timeframe_minutes (int): Timeframe in minutes (1, 5, 15)
            name (str): Identifier for this series (spot/fut)
            on_candle_close (callable): Callback function when a candle completes
        """
        self.tf = timeframe_minutes
        self.name = name
        self.on_candle_close = on_candle_close

        # Store candles
        self.current_candle = None
        self.completed_candles = []

        # Pivot tracking (for divergence strategies)
        self.pivot_lows = []  # List of detected pivot lows (for bullish divergence)
        self.broken_pivot_lows = []  # List of broken pivot lows
        self.pivot_highs = []  # List of detected pivot highs (for bearish divergence)
        self.broken_pivot_highs = []  # List of broken pivot highs
        self.is_spot_data = False  # Set to True for spot data
        self.is_5min_timeframe = (timeframe_minutes == 5)
        self.futures_data_ref = None  # Reference to futures data for spot 5m
        self.strategy_logger = None  # Optional logger for strategy-specific logging
        self.strategy_type = None  # 'bullish' or 'bearish' - controls which pivot logs go to strategy logger

    # ----------------------------------------------------------------------
    # 🔥 FIXED SIGNATURE (price, timestamp)
    # This now matches FeedHandler calls.
    # ----------------------------------------------------------------------
    def update_from_tick(self, price, timestamp):
        """
        Update OHLCV candle from live tick.

        Args:
            price (float): LTP (last traded price)
            timestamp (datetime): Tick timestamp
        """
        try:
            if not isinstance(timestamp, datetime):
                return

            # Normalize candle start time
            candle_start = timestamp.replace(
                minute=(timestamp.minute // self.tf) * self.tf,
                second=0,
                microsecond=0
            )

            # If new candle:
            if not self.current_candle or candle_start > self.current_candle["time"]:
                if self.current_candle:
                    completed_candle = self.current_candle.copy()
                    self.completed_candles.append(completed_candle)

                    # Detect pivots if this is 5-minute timeframe
                    if self.is_5min_timeframe:
                        self.detect_pivot_lows()
                        self.detect_pivot_highs()

                    # Trigger callback when a candle closes
                    if self.on_candle_close:
                        try:
                            self.on_candle_close(self.tf, self.name)
                        except Exception as e:
                            logger.error(f"Error in candle close callback: {e}")

                # Create new candle
                self.current_candle = {
                    "time": candle_start,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": 1
                }

            else:
                # Update existing candle
                self.current_candle["high"] = max(self.current_candle["high"], price)
                self.current_candle["low"] = min(self.current_candle["low"], price)
                self.current_candle["close"] = price
                self.current_candle["volume"] += 1

        except Exception as e:
            logger.error(f"[{self.name} {self.tf}m] update_from_tick error: {e}")

    # ----------------------------------------------------------------------

    def get_latest_candles(self, count=200):
        """Return latest N candles including current one."""
        candles = (
            self.completed_candles + [self.current_candle]
            if self.current_candle
            else self.completed_candles
        )
        return candles[-count:]

    def get_dataframe(self):
        """Return candles as pandas DataFrame."""
        # Return ALL candles for intraday analysis (not limited to latest N)
        candles = (
            self.completed_candles + [self.current_candle]
            if self.current_candle
            else self.completed_candles
        )

        if not candles:
            return pd.DataFrame()

        df = pd.DataFrame(candles)
        df.rename(columns={"time": "timestamp"}, inplace=True)
        df.set_index("timestamp", inplace=True)

        return df

    # ----------------------------------------------------------------------

    def initialize_from_historical(self, historical_df):
        """
        Load historical OHLCV candles.
        Now expects pre-resampled data matching the timeframe.

        Args:
            historical_df (pd.DataFrame): Pre-resampled OHLCV data
        """
        try:
            if historical_df.empty:
                logger.warning(f"No historical data for {self.name} {self.tf}m")
                return False

            historical_df = historical_df.sort_values("timestamp")

            for _, row in historical_df.iterrows():
                # Use timestamp as-is (already resampled by engine)
                ts = row["timestamp"]

                self.completed_candles.append(
                    {
                        "time": ts,
                        "open": row["open"],
                        "high": row["high"],
                        "low": row["low"],
                        "close": row["close"],
                        "volume": row.get("volume", 0),
                    }
                )

            # Log in bullish3t1.py format
            logger.info(
                f"Initialized {len(self.completed_candles)} historical {self.tf}min candles"
            )

            # Detect pivots if this is 5-minute data
            if self.is_5min_timeframe:
                self.detect_pivot_lows()
                self.detect_pivot_highs()

                # Check for already broken pivots in historical data
                if self.completed_candles:
                    last_candle = self.completed_candles[-1]
                    last_price = last_candle['close']
                    last_time = last_candle['time']

                    # Check if any pivot lows are already broken
                    self._check_historical_pivot_breakdowns(last_price, last_time)

            return True

        except Exception as e:
            logger.error(
                f"[{self.name} {self.tf}m] Error loading historical: {e}"
            )
            return False

    # ----------------------------------------------------------------------

    def clear_data(self):
        """Clear candles and pivot state for a fresh session."""
        self.current_candle = None
        self.completed_candles = []
        self.pivot_lows = []
        self.broken_pivot_lows = []
        self.pivot_highs = []
        self.broken_pivot_highs = []

    def export_to_csv(self, prefix=None):
        """Export candles to CSV."""
        try:
            df = self.get_dataframe()
            if df.empty:
                logger.warning(f"No data to export for {self.name} {self.tf}m")
                return None

            os.makedirs(settings.DATA_DIR, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            prefix = prefix or f"{self.name}_{self.tf}m"
            filepath = os.path.join(settings.DATA_DIR, f"{prefix}_{timestamp}.csv")

            df.reset_index().to_csv(filepath, index=False)
            logger.info(f"Exported {len(df)} candles -> {filepath}")
            return filepath

        except Exception as e:
            logger.error(
                f"[{self.name} {self.tf}m] CSV export error: {e}"
            )
            return None

    # ----------------------------------------------------------------------
    # PIVOT DETECTION METHODS (for divergence strategy)
    # ----------------------------------------------------------------------

    def set_futures_data_reference(self, futures_data):
        """Set reference to futures data for mapping pivot lows (for spot 5m data)"""
        self.futures_data_ref = futures_data

    def set_strategy_logger(self, strategy_logger, strategy_type=None):
        """Set strategy-specific logger for divergence-related logs"""
        self.strategy_logger = strategy_logger
        self.strategy_type = strategy_type

    def remove_pivot_from_tracking(self, pivot_number):
        """
        Mark a pivot as removed from tracking after entry is triggered.
        Uses 'removed' flag to maintain pivot numbering.
        """
        if pivot_number <= len(self.pivot_lows) and pivot_number > 0:
            pivot_index = pivot_number - 1
            if pivot_index < len(self.pivot_lows):
                self.pivot_lows[pivot_index]['removed'] = True
                logger.info(f"[PIVOT TRACKING] Marked Pivot {pivot_number} as removed from tracking in {self.name}")

    def detect_pivot_lows(self):
        """
        Detect pivot lows in completed candles with improved handling of equal lows.
        Only runs on SPOT 5-minute data. Futures pivots are derived separately.
        """
        if len(self.completed_candles) < 3:
            return

        if not self.is_spot_data:
            logger.debug("Skipping direct futures pivot low detection - futures pivots should only be derived from spot pivots")
            return

        for idx in range(1, len(self.completed_candles) - 1):
            current_low = self.completed_candles[idx]['low']
            prev_low = self.completed_candles[idx - 1]['low']
            next_low = self.completed_candles[idx + 1]['low']

            # Handle equal lows before current point
            i = idx - 1
            while i >= 0 and self.completed_candles[i]['low'] == current_low:
                if i > 0:
                    prev_low = self.completed_candles[i - 1]['low']
                i -= 1

            # Handle equal lows after current point
            i = idx + 1
            while i < len(self.completed_candles) and self.completed_candles[i]['low'] == current_low:
                if i < len(self.completed_candles) - 1:
                    next_low = self.completed_candles[i + 1]['low']
                i += 1

            # Pivot condition: current low is less than both prev and next
            if current_low < prev_low and current_low < next_low:
                pivot_low = {
                    'time': self.completed_candles[idx]['time'],
                    'price': current_low,
                    'removed': False
                }

                # Check for duplicates
                is_duplicate = any(existing_low['time'] == pivot_low['time'] for existing_low in self.pivot_lows)
                if not is_duplicate:
                    self.pivot_lows.append(pivot_low)
                    pivot_number = len(self.pivot_lows)
                    pivot_low['pivot_number'] = pivot_number
                    log_message = f"[DETECTED] SPOT Pivot Low {pivot_number} detected at {pivot_low['time']} - Price: {pivot_low['price']:.2f}"
                    logger.info(log_message)
                    if self.strategy_logger:
                        self.strategy_logger.info(log_message)

    def get_futures_low_at_time(self, spot_time):
        """
        Get the futures pivot low price at the given spot time.
        Uses a 3-candle window: previous, current, and next candle.
        Returns the LOWEST price among all three candles.
        Used for deriving futures pivots from spot pivot times.

        Example: If spot pivot is at 9:00-9:05, futures will look at:
        - 8:55-9:00 (previous)
        - 9:00-9:05 (current)
        - 9:05-9:10 (next)
        Returns the lowest 'low' among these three candles.
        """
        if not self.futures_data_ref or not self.futures_data_ref.completed_candles:
            return None

        # Calculate timestamps for 3-candle window (5-minute candles = 300 seconds)
        from datetime import timedelta
        prev_time = spot_time - timedelta(seconds=300)
        current_time = spot_time
        next_time = spot_time + timedelta(seconds=300)

        # Collect lows from all three candles
        lows = []
        for candle in self.futures_data_ref.completed_candles:
            if candle['time'] in [prev_time, current_time, next_time]:
                lows.append(candle['low'])

        # Return the lowest price among the three candles
        if lows:
            lowest_price = min(lows)
            logger.debug(f"[3-CANDLE WINDOW] Futures pivot low at spot time {current_time}: "
                        f"Found {len(lows)} candles, lowest price: {lowest_price:.2f}")
            return lowest_price
        return None

    def get_futures_high_at_time(self, spot_time):
        """
        Get the futures pivot high price at the given spot time.
        Uses a 3-candle window: previous, current, and next candle.
        Returns the HIGHEST price among all three candles.
        Used for deriving futures pivots from spot pivot times (bearish divergence).

        Example: If spot pivot is at 9:00-9:05, futures will look at:
        - 8:55-9:00 (previous)
        - 9:00-9:05 (current)
        - 9:05-9:10 (next)
        Returns the highest 'high' among these three candles.
        """
        if not self.futures_data_ref or not self.futures_data_ref.completed_candles:
            return None

        # Calculate timestamps for 3-candle window (5-minute candles = 300 seconds)
        from datetime import timedelta
        prev_time = spot_time - timedelta(seconds=300)
        current_time = spot_time
        next_time = spot_time + timedelta(seconds=300)

        # Collect highs from all three candles
        highs = []
        for candle in self.futures_data_ref.completed_candles:
            if candle['time'] in [prev_time, current_time, next_time]:
                highs.append(candle['high'])

        # Return the highest price among the three candles
        if highs:
            highest_price = max(highs)
            logger.debug(f"[3-CANDLE WINDOW] Futures pivot high at spot time {current_time}: "
                        f"Found {len(highs)} candles, highest price: {highest_price:.2f}")
            return highest_price
        return None

    def check_pivot_low_breakdown(self, current_price, current_time):
        """
        Check if any pivot low has been broken by the current price.
        Skips pivots marked as 'removed'.
        """
        broken_pivots = []

        for pivot_low in self.pivot_lows:
            # Skip pivots that have been marked as removed
            if pivot_low.get('removed', False):
                continue

            already_broken = any(broken['pivot_time'] == pivot_low['time'] for broken in self.broken_pivot_lows)
            if not already_broken and current_price <= pivot_low['price']:
                broken_pivot = {
                    'pivot_time': pivot_low['time'],
                    'pivot_price': pivot_low['price'],
                    'breakdown_time': current_time,
                    'breakdown_price': current_price,
                    'pivot_number': self.pivot_lows.index(pivot_low) + 1
                }

                self.broken_pivot_lows.append(broken_pivot)
                broken_pivots.append(broken_pivot)

                data_type = "SPOT" if self.is_spot_data else "FUTURES"
                log_message = (f"[BREAKDOWN] {data_type} Pivot Low {broken_pivot['pivot_number']} BROKEN! "
                              f"Pivot: {broken_pivot['pivot_time']} @ {broken_pivot['pivot_price']:.2f} -> "
                              f"Breakdown: {broken_pivot['breakdown_time']} @ {broken_pivot['breakdown_price']:.2f}")

                # Log to both module logger and strategy logger if available
                logger.info(log_message)
                if self.strategy_logger:
                    self.strategy_logger.info(log_message)

        return broken_pivots

    def get_broken_pivot_lows(self):
        """Get all broken pivot lows"""
        return self.broken_pivot_lows

    def get_unbroken_pivot_lows(self):
        """
        Get pivot lows that haven't been broken yet and aren't removed.
        Used for active monitoring.
        """
        broken_times = [broken['pivot_time'] for broken in self.broken_pivot_lows]
        return [pivot for pivot in self.pivot_lows
                if pivot['time'] not in broken_times and not pivot.get('removed', False)]

    # ----------------------------------------------------------------------
    # 🔥 PIVOT HIGHS DETECTION (for bearish divergence)
    # ----------------------------------------------------------------------

    def detect_pivot_highs(self):
        """
        Detect pivot highs in completed candles with improved handling of equal highs.
        Only runs on SPOT 5-minute data. Futures pivots are derived separately.
        Mirrors detect_pivot_lows logic but looks for highs.
        """
        if len(self.completed_candles) < 3:
            return

        if not self.is_spot_data:
            logger.debug("Skipping direct futures pivot high detection - futures pivots should only be derived from spot pivots")
            return

        for idx in range(1, len(self.completed_candles) - 1):
            current_high = self.completed_candles[idx]['high']
            prev_high = self.completed_candles[idx - 1]['high']
            next_high = self.completed_candles[idx + 1]['high']

            # Handle equal highs before current point
            i = idx - 1
            while i >= 0 and self.completed_candles[i]['high'] == current_high:
                if i > 0:
                    prev_high = self.completed_candles[i - 1]['high']
                i -= 1

            # Handle equal highs after current point
            i = idx + 1
            while i < len(self.completed_candles) and self.completed_candles[i]['high'] == current_high:
                if i < len(self.completed_candles) - 1:
                    next_high = self.completed_candles[i + 1]['high']
                i += 1

            # Pivot condition: current high is greater than both prev and next
            if current_high > prev_high and current_high > next_high:
                # Check for duplicates first
                is_duplicate = any(existing_high['time'] == self.completed_candles[idx]['time'] for existing_high in self.pivot_highs)
                if not is_duplicate:
                    pivot_number = len(self.pivot_highs) + 1
                    pivot_high = {
                        'pivot_number': pivot_number,
                        'time': self.completed_candles[idx]['time'],
                        'price': current_high,
                        'removed': False
                    }
                    self.pivot_highs.append(pivot_high)
                    log_message = f"[DETECTED] SPOT Pivot High {pivot_number} detected at {pivot_high['time']} - Price: {pivot_high['price']:.2f}"
                    logger.info(log_message)
                    if self.strategy_logger:
                        self.strategy_logger.info(log_message)

    def remove_pivot_high_from_tracking(self, pivot_number):
        """
        Mark a pivot high as 'removed' without deleting it.
        This preserves numbering while excluding it from active tracking.

        Args:
            pivot_number (int): The pivot number to remove
        """
        for pivot in self.pivot_highs:
            if pivot['pivot_number'] == pivot_number:
                pivot['removed'] = True
                logger.info(f"[PIVOT HIGH REMOVED] Pivot High {pivot_number} marked as removed from tracking")
                break

    def check_pivot_high_breakdown(self, current_price, current_time):
        """
        Check if any unbroken pivot highs have been broken upward (price > pivot high).
        For bearish divergence, we look for upward breakouts of pivot highs.

        Args:
            current_price (float): Current market price
            current_time (datetime): Current timestamp

        Returns:
            list: List of newly broken pivots
        """
        if not self.is_5min_timeframe:
            return []

        broken_pivots = []
        broken_times = [broken['pivot_time'] for broken in self.broken_pivot_highs]

        for pivot in self.pivot_highs:
            # Skip if already broken or removed
            if pivot['time'] in broken_times or pivot.get('removed', False):
                continue

            # Check for upward breakout (price >= pivot high)
            if current_price >= pivot['price']:
                broken_pivot = {
                    'pivot_number': pivot['pivot_number'],
                    'pivot_price': pivot['price'],
                    'pivot_time': pivot['time'],
                    'breakout_price': current_price,
                    'breakout_time': current_time
                }

                self.broken_pivot_highs.append(broken_pivot)
                broken_pivots.append(broken_pivot)

                data_type = "SPOT" if self.is_spot_data else "FUTURES"
                log_message = (f"[BREAKOUT] {data_type} Pivot High {broken_pivot['pivot_number']} BROKEN! "
                              f"Pivot: {broken_pivot['pivot_time']} @ {broken_pivot['pivot_price']:.2f} -> "
                              f"Breakout: {broken_pivot['breakout_time']} @ {broken_pivot['breakout_price']:.2f}")

                # Log to both module logger and strategy logger if available
                logger.info(log_message)
                if self.strategy_logger:
                    self.strategy_logger.info(log_message)

        return broken_pivots

    def get_broken_pivot_highs(self):
        """Get all broken pivot highs"""
        return self.broken_pivot_highs

    def get_unbroken_pivot_highs(self):
        """
        Get pivot highs that haven't been broken yet and aren't removed.
        Used for active monitoring.
        """
        broken_times = [broken['pivot_time'] for broken in self.broken_pivot_highs]
        return [pivot for pivot in self.pivot_highs
                if pivot['time'] not in broken_times and not pivot.get('removed', False)]

    def _check_historical_pivot_breakdowns(self, last_price, last_time):
        """
        Check if any pivots were already broken in historical data.
        This is called after loading historical candles to mark pivots that
        were already broken before live trading started.

        Checks ALL candles after each pivot (not just the last close) to catch
        breakdowns that happened mid-session and were followed by a reversal.

        Args:
            last_price (float): Last price from historical data (unused, kept for signature compat)
            last_time (datetime): Last timestamp from historical data (unused, kept for signature compat)
        """
        data_type = "SPOT" if self.is_spot_data else "FUTURES"

        # Check pivot lows for breakdowns — scan ALL candles after each pivot
        for pivot_low in self.pivot_lows:
            if pivot_low.get('removed', False):
                continue
            already_broken = any(broken['pivot_time'] == pivot_low['time'] for broken in self.broken_pivot_lows)
            if already_broken:
                continue

            pivot_time = pivot_low['time']
            for candle in self.completed_candles:
                if candle['time'] > pivot_time and candle['low'] <= pivot_low['price']:
                    broken_pivot = {
                        'pivot_time': pivot_low['time'],
                        'pivot_price': pivot_low['price'],
                        'breakdown_time': candle['time'],
                        'breakdown_price': candle['low'],
                        'pivot_number': self.pivot_lows.index(pivot_low) + 1
                    }
                    self.broken_pivot_lows.append(broken_pivot)

                    logger.info(f"[HISTORICAL BREAKDOWN] {data_type} Pivot Low {broken_pivot['pivot_number']} "
                              f"was already broken in historical data! "
                              f"Pivot: {broken_pivot['pivot_time']} @ {broken_pivot['pivot_price']:.2f} -> "
                              f"Broken at: {candle['time']} (low: {candle['low']:.2f})")
                    break

        # Check pivot highs for breakouts — scan ALL candles after each pivot
        for pivot_high in self.pivot_highs:
            if pivot_high.get('removed', False):
                continue
            already_broken = any(broken['pivot_time'] == pivot_high['time'] for broken in self.broken_pivot_highs)
            if already_broken:
                continue

            pivot_time = pivot_high['time']
            for candle in self.completed_candles:
                if candle['time'] > pivot_time and candle['high'] >= pivot_high['price']:
                    broken_pivot = {
                        'pivot_number': pivot_high['pivot_number'],
                        'pivot_price': pivot_high['price'],
                        'pivot_time': pivot_high['time'],
                        'breakout_price': candle['high'],
                        'breakout_time': candle['time']
                    }
                    self.broken_pivot_highs.append(broken_pivot)

                    logger.info(f"[HISTORICAL BREAKOUT] {data_type} Pivot High {broken_pivot['pivot_number']} "
                              f"was already broken in historical data! "
                              f"Pivot: {broken_pivot['pivot_time']} @ {broken_pivot['pivot_price']:.2f} -> "
                              f"Broken at: {candle['time']} (high: {candle['high']:.2f})")
                    break


# ======================================================================
# RESAMPLING UTILITY
# ======================================================================

def resample_ohlcv(df, timeframe, on="timestamp"):
    """Resample OHLCV to different timeframe."""
    try:
        if df.empty:
            return df

        df = df.copy()
        if not pd.api.types.is_datetime64_any_dtype(df[on]):
            df[on] = pd.to_datetime(df[on])

        if df.index.name != on:
            df = df.set_index(on)

        return (
            df.resample(timeframe)
            .agg(
                {
                    "open": "first",
                    "high": "max",
                    "low": "min",
                    "close": "last",
                    "volume": "sum",
                }
            )
            .dropna()
        )

    except Exception as e:
        logger.error(f"Resample error: {e}")
        return pd.DataFrame()


# ======================================================================
# HISTORICAL FETCH
# ======================================================================

def fetch_historical_data(
    broker, token, exchange, from_date, to_date, interval="ONE_MINUTE"
):
    """
    Fetch historical OHLCV from broker API.
    """
    try:
        logger.info(
            f"Fetching {interval} historical data for {exchange}:{token} from {from_date} to {to_date}"
        )

        if not broker or not hasattr(broker, "api"):
            logger.error("Broker API missing")
            return pd.DataFrame()

        params = {
            "exchange": exchange,
            "symboltoken": token,
            "interval": interval,
            "fromdate": from_date,
            "todate": to_date,
        }

        response = broker.api.getCandleData(params)
        
        # Log API response (matching bullish3t1.py debug format)
        logger.debug(f"API Response: {response}")

        if not isinstance(response, dict):
            logger.error(f"Invalid response format: {type(response)}")
            return pd.DataFrame()

        if not response.get("status"):
            logger.error(f"Broker Error: {response.get('message', 'Unknown')}")
            return pd.DataFrame()

        data = response.get("data", [])
        if not data:
            logger.warning(f"No historical {interval} for {exchange}:{token}")
            return pd.DataFrame()

        df = pd.DataFrame(
            data, columns=["timestamp", "open", "high", "low", "close", "volume"]
        )

        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["timestamp"]).sort_values("timestamp")

        df[["open", "high", "low", "close", "volume"]] = df[
            ["open", "high", "low", "close", "volume"]
        ].apply(pd.to_numeric, errors="coerce")

        logger.info(f"Successfully fetched {len(df)} records")

        return df

    except Exception as e:
        logger.error(f"Historical fetch error: {e}")
        return pd.DataFrame()
