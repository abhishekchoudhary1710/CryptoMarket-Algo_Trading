"""
Application engine to coordinate broker, data, strategies and feed.

Requirements implemented:
- Fetch historical data for BOTH spot and futures
- Build 1m/5m/15m OHLCV for BOTH
- Consume live ticks for BOTH through websocket
- Run configured active strategies on spot data
- Keep futures data ready for future strategies
"""

import os
import time
import pytz
import pandas as pd
from datetime import datetime, timedelta
from utils.logger import logger
from utils.helpers import get_today_date_range, fetch_scripmaster_data, is_market_open
from utils.retry import retry
from api.state import shared_state
from api.server import start_api_server

from data.ohlcv import LiveOHLCVData, fetch_historical_data
from data.futures import get_nifty_futures_token

from strategies.bullish_divergence import BullishDivergenceStrategy
from strategies.bearish_divergence import BearishDivergenceStrategy
from models.order_manager import OrderManager
from core.router import SignalRouter
from core.feed_handler import FeedHandler
from config import settings

class TradingEngine:
    

    def __init__(self, broker):
        self.broker = broker

        self.spot_token = settings.SPOT_TOKEN
        self.fut_token = None

        # OHLCV containers for spot
        self.spot_series = {
            "1m": LiveOHLCVData(1, "spot"),
            "5m": LiveOHLCVData(5, "spot"),
            "15m": LiveOHLCVData(15, "spot"),
        }
        # Set is_spot_data flags for spot series
        for series in self.spot_series.values():
            series.is_spot_data = True

        # OHLCV containers for futures
        self.fut_series = {
            "1m": LiveOHLCVData(1, "fut"),
            "5m": LiveOHLCVData(5, "fut"),
            "15m": LiveOHLCVData(15, "fut"),
        }
        # Futures series is_spot_data remains False (default)

        self.fut_strategy = None  # placeholder for future use

        self.router = None
        self.feed_handler = None
        self.websocket = None
        self._spot_data_verified = False  # Set True once 5m data starts from market open

        # Ensure directories exist
        os.makedirs(settings.DATA_DIR, exist_ok=True)
        os.makedirs(settings.RAW_TICKS_DIR, exist_ok=True)
        os.makedirs(settings.ORDER_HISTORY_DIR, exist_ok=True)
        os.makedirs(settings.OPTIONS_DATA_DIR, exist_ok=True)

    def start(self):
        logger.info("Starting TradingEngine")

        # Start API server for dashboard
        start_api_server(host="0.0.0.0", port=8000)
        logger.info("API server started on port 8000")
        shared_state.bot_running = True
        shared_state.bot_start_time = datetime.now()

        if not self.broker.connect():
            logger.error("Broker connect failed.")
            return False

        # scripmaster download with retry
        scripmaster = retry(fetch_scripmaster_data, retries=3, delay=3, name="fetch_scripmaster_data")
        if not scripmaster:
            return False

        self.fut_token = get_nifty_futures_token(scripmaster)
        if not self.fut_token:
            logger.warning("No futures token found, running spot-only strategy (but futures data will still be prepared if token appears).")

        self._initialize_historical()
        self._initialize_strategies()
        self._start_websocket()

        try:
            while True:
                # Check kill switch
                if shared_state.is_killed():
                    logger.info("Kill switch activated. Stopping trading.")
                    break

                self._periodic_tasks()
                self._update_shared_state()
                time.sleep(300)
        except KeyboardInterrupt:
            logger.info("Engine stopped by user.")
            self.shutdown()
            return True
        except Exception as e:
            logger.error(f"Engine crashed: {e}")
            self.shutdown()
            return False

    def _resample_ohlcv(self, df, timeframe):
        """
        Resample 1-minute OHLCV data to a different timeframe.
        Matching bullish3t.py _resample_and_initialize logic.

        Args:
            df (pd.DataFrame): 1-minute OHLCV data with timestamp column
            timeframe (str): Target timeframe (e.g., "5min", "15min")

        Returns:
            pd.DataFrame: Resampled OHLCV data
        """
        try:
            if df.empty:
                logger.warning(f"Cannot resample empty dataframe to {timeframe}")
                return pd.DataFrame()

            df = df.copy()

            # Ensure timestamp column exists and is datetime
            if 'timestamp' not in df.columns:
                if df.index.name == 'timestamp':
                    df.reset_index(inplace=True)
                else:
                    logger.error("No timestamp column found in dataframe")
                    return pd.DataFrame()

            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)

            # Resample to target timeframe
            resampled = df.resample(timeframe).agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).dropna()

            # Convert back to format expected by initialize_from_historical
            resampled_df = resampled.reset_index()
            logger.info(f"Resampled {len(df)} candles to {len(resampled_df)} {timeframe} candles")

            return resampled_df

        except Exception as e:
            logger.error(f"Error resampling data to {timeframe}: {e}")
            return pd.DataFrame()

    def _initialize_historical(self):
        from_date, to_date, now = get_today_date_range()
        market_open = is_market_open(now)

        # ---------------- SPOT HISTORICAL ----------------
        logger.info("=" * 60)
        logger.info("FETCHING HISTORICAL SPOT DATA")
        logger.info(f"Token: {self.spot_token} | Exchange: NSE | Period: {from_date} to {to_date}")
        logger.info("=" * 60)
        
        spot_hist = self._fetch_historical_with_retry(
            token=self.spot_token,
            exchange="NSE",
            from_date=from_date,
            to_date=to_date,
            market_open=market_open,
        )

        if not spot_hist.empty:
            logger.info(f"[SUCCESS] Fetched {len(spot_hist)} spot candles")

            # Save to CSV (matching bullish3t1.py format)
            timestamp = datetime.now().strftime('%Y%m%d')
            spot_file = settings.OUTPUT_DIR / f"spot_historical_{timestamp}.csv"
            spot_hist.to_csv(spot_file, index=False)
            logger.info(f"Saved historical spot data to {spot_file}")
            logger.info(f"Retrieved {len(spot_hist)} historical spot records")
            logger.info(f"Latest spot data: {spot_hist.iloc[-1].to_dict()}")

            # Initialize each timeframe with properly resampled data
            self.spot_series["1m"].initialize_from_historical(spot_hist.copy())

            # Resample to 5-minute candles
            spot_5m = self._resample_ohlcv(spot_hist.copy(), "5min")
            self.spot_series["5m"].initialize_from_historical(spot_5m)

            # Resample to 15-minute candles
            spot_15m = self._resample_ohlcv(spot_hist.copy(), "15min")
            self.spot_series["15m"].initialize_from_historical(spot_15m)

            logger.info("[SUCCESS] Spot historical data initialized for 1m/5m/15m timeframes")
        else:
            logger.warning("[FAILED] No historical spot data returned (market may be closed or no data available)")

        # ---------------- FUTURES HISTORICAL ----------------
        if self.fut_token:
            logger.info("=" * 60)
            logger.info("FETCHING HISTORICAL FUTURES DATA")
            logger.info(f"Token: {self.fut_token} | Exchange: NFO | Period: {from_date} to {to_date}")
            logger.info("=" * 60)
            
            fut_hist = self._fetch_historical_with_retry(
                token=self.fut_token,
                exchange="NFO",
                from_date=from_date,
                to_date=to_date,
                market_open=market_open,
            )

            if not fut_hist.empty:
                logger.info(f"[SUCCESS] Fetched {len(fut_hist)} futures candles")

                # Save to CSV (matching bullish3t1.py format)
                timestamp = datetime.now().strftime('%Y%m%d')
                fut_file = settings.OUTPUT_DIR / f"futures_historical_{timestamp}.csv"
                fut_hist.to_csv(fut_file, index=False)
                logger.info(f"Saved historical futures data to {fut_file}")
                logger.info(f"Retrieved {len(fut_hist)} historical futures records")
                logger.info(f"Latest futures data: {fut_hist.iloc[-1].to_dict()}")

                # Initialize each timeframe with properly resampled data
                self.fut_series["1m"].initialize_from_historical(fut_hist.copy())

                # Resample to 5-minute candles
                fut_5m = self._resample_ohlcv(fut_hist.copy(), "5min")
                self.fut_series["5m"].initialize_from_historical(fut_5m)

                # Resample to 15-minute candles
                fut_15m = self._resample_ohlcv(fut_hist.copy(), "15min")
                self.fut_series["15m"].initialize_from_historical(fut_15m)

                logger.info("[SUCCESS] Futures historical data initialized for 1m/5m/15m timeframes")
            else:
                logger.warning("[FAILED] No historical futures data returned (market may be closed or no data available)")
        else:
            logger.info("Futures token not available yet; skipping futures historical fetch")

    def _fetch_historical_with_retry(self, token, exchange, from_date, to_date, market_open, interval="ONE_MINUTE"):
        def _attempt():
            df = fetch_historical_data(
                self.broker, token, exchange, from_date, to_date, interval=interval
            )
            if df.empty:
                raise ValueError("empty historical data")
            return df

        if market_open:
            logger.info(
                f"Market is open; retrying historical fetch for {exchange}:{token} up to "
                f"{settings.HISTORICAL_FETCH_RETRIES} times on failure"
            )
            result = retry(
                _attempt,
                retries=settings.HISTORICAL_FETCH_RETRIES,
                delay=settings.HISTORICAL_FETCH_RETRY_DELAY,
                backoff=settings.HISTORICAL_FETCH_RETRY_BACKOFF,
                name=f"fetch_historical_{exchange}_{token}",
            )
            return result if result is not None else pd.DataFrame()

        return fetch_historical_data(
            self.broker, token, exchange, from_date, to_date, interval=interval
        )
    def _initialize_strategies(self):
        # Shared order manager
        om = OrderManager(self.broker)

        spot_df = self.spot_series["5m"].get_dataframe()
        if spot_df.empty:
            logger.warning("[LIVE MODE] Spot 5m dataframe is empty; strategies will warm up from live candles")

        # ---------------- BULLISH DIVERGENCE STRATEGY ----------------
        logger.info("=" * 60)
        logger.info("INITIALIZING BULLISH DIVERGENCE STRATEGY")
        logger.info("=" * 60)

        # Link spot 5m to futures 5m for pivot derivation
        self.spot_series["5m"].set_futures_data_reference(self.fut_series["5m"])

        # Initialize divergence strategy
        self.divergence_strategy = BullishDivergenceStrategy(
            spot_1m_data=self.spot_series["1m"],
            spot_5m_data=self.spot_series["5m"],
            futures_5m_data=self.fut_series["5m"],
            order_manager=om,
            broker=self.broker,
            futures_1m_data=self.fut_series["1m"]
        )

        self.divergence_strategy.generate_signals()
        if not spot_df.empty:
            logger.info("[SUCCESS] BullishDivergenceStrategy initialized and signals generated from historical data")
        else:
            logger.info("[LIVE MODE] BullishDivergenceStrategy initialized; waiting for live pivots")

        # ---------------- BEARISH DIVERGENCE STRATEGY ----------------
        logger.info("=" * 60)
        logger.info("INITIALIZING BEARISH DIVERGENCE STRATEGY")
        logger.info("=" * 60)

        # Note: Bearish divergence uses pivot HIGHS (requires pivot_highs implementation)
        # For now, creating instance for structure - full functionality requires pivot_highs
        self.bearish_divergence_strategy = BearishDivergenceStrategy(
            spot_1m_data=self.spot_series["1m"],
            spot_5m_data=self.spot_series["5m"],
            futures_5m_data=self.fut_series["5m"],
            order_manager=om,
            broker=self.broker,
            futures_1m_data=self.fut_series["1m"]
        )

        self.bearish_divergence_strategy.generate_signals()
        if not spot_df.empty:
            logger.info("[SUCCESS] BearishDivergenceStrategy initialized and signals generated from historical data")
        else:
            logger.info("[LIVE MODE] BearishDivergenceStrategy initialized; waiting for live pivots")
        logger.info("[NOTE] Full functionality requires pivot_highs implementation in LiveOHLCVData")

        # ---------------- FUTURES STRATEGY (NOT USED NOW) ----------------
        self.fut_strategy = None

        # Router
        logger.info("=" * 60)
        logger.info("INITIALIZING SIGNAL ROUTER & FEED HANDLER")
        logger.info("=" * 60)

        self.router = SignalRouter(
            divergence_strategy=self.divergence_strategy,
            bearish_divergence_strategy=self.bearish_divergence_strategy,
            fut_strategy=self.fut_strategy,
            order_manager=om
        )
        logger.info("[SUCCESS] Signal router initialized")

        # Feed handler updates both spot+fut OHLCV, but router will only run spot strategy
        self.feed_handler = FeedHandler(
            spot_token=self.spot_token,
            fut_token=self.fut_token,
            spot_series=self.spot_series,
            fut_series=self.fut_series,
            router=self.router,
            broker=self.broker
        )
        logger.info("[SUCCESS] Feed handler initialized")



    def _start_websocket(self):
        logger.info("=" * 60)
        logger.info("STARTING WEBSOCKET CONNECTION")
        logger.info("=" * 60)
        
        token_list = [{"exchangeType": 1, "tokens": [self.spot_token]}]
        logger.info(f"[SUCCESS] Subscribing to SPOT token: {self.spot_token}")
        
        if self.fut_token:
            token_list.append({"exchangeType": 2, "tokens": [self.fut_token]})
            logger.info(f"[SUCCESS] Subscribing to FUTURES token: {self.fut_token}")

        self.websocket = self.broker.start_websocket(
            token_list,
            self.feed_handler.on_ws_message
        )

        if not self.websocket:
            logger.error("[FAILED] Websocket connection failed to start")
            raise RuntimeError("Websocket failed to start.")
        
        logger.info("[SUCCESS] Websocket connection established")
        logger.info("=" * 60)

    def _periodic_tasks(self):
        self._ensure_data_from_market_open()

    def _ensure_data_from_market_open(self):
        """Ensure 5m spot data starts from market open. Re-fetch if missing.
        Once verified, stops checking for the rest of the session."""
        if self._spot_data_verified:
            return

        try:
            ist = pytz.timezone('Asia/Kolkata')
            now = datetime.now(ist)

            # Only check during market hours
            mkt_h = settings.MARKET_OPEN_HOUR
            mkt_m = settings.MARKET_OPEN_MINUTE
            if now.hour < mkt_h or (now.hour == mkt_h and now.minute < mkt_m):
                return
            if now.hour > settings.MARKET_CLOSE_HOUR:
                return
            if now.hour == settings.MARKET_CLOSE_HOUR and now.minute > settings.MARKET_CLOSE_MINUTE:
                return

            spot_5m = self.spot_series["5m"]
            market_open = now.replace(hour=mkt_h, minute=mkt_m, second=0, microsecond=0)
            market_open_naive = market_open.replace(tzinfo=None)

            if not spot_5m.completed_candles:
                # No data at all — but if we just started before market open,
                # wait at least 10 min for the first candle to form naturally
                now_naive = now.replace(tzinfo=None)
                if now_naive < market_open_naive + timedelta(minutes=10):
                    return
                # After 10 min past open and still no data → initial fetch must have failed
                logger.warning("DATA INTEGRITY: 5m spot data is completely empty. Re-fetching...")
            else:
                first_time = spot_5m.completed_candles[0]["time"]
                first_naive = first_time.replace(tzinfo=None) if hasattr(first_time, 'tzinfo') and first_time.tzinfo else first_time

                if first_naive <= market_open_naive + timedelta(minutes=15):
                    # Data is valid, no need to check again
                    self._spot_data_verified = True
                    logger.info(f"DATA INTEGRITY: 5m data verified, starts from {first_time}")
                    return

                logger.warning(
                    f"DATA INTEGRITY: 5m data starts at {first_time}, "
                    f"expected ~{mkt_h:02d}:{mkt_m:02d}. Re-fetching..."
                )

            # Wait before re-fetch to avoid hitting rate limits again
            time.sleep(5)

            from_date, to_date, _ = get_today_date_range()
            spot_hist = self._fetch_historical_with_retry(
                token=self.spot_token, exchange="NSE",
                from_date=from_date, to_date=to_date, market_open=True
            )

            if not spot_hist.empty:
                spot_5m_hist = self._resample_ohlcv(spot_hist.copy(), "5min")
                if not spot_5m_hist.empty:
                    current_candle = spot_5m.current_candle
                    spot_5m.completed_candles = []
                    spot_5m.initialize_from_historical(spot_5m_hist)
                    spot_5m.current_candle = current_candle
                    self._spot_data_verified = True
                    logger.info(
                        f"DATA INTEGRITY: Re-initialized 5m data with "
                        f"{len(spot_5m.completed_candles)} candles starting from "
                        f"{spot_5m.completed_candles[0]['time']}"
                    )
            else:
                logger.error("DATA INTEGRITY: Re-fetch returned empty data, will retry next cycle")
        except Exception as e:
            logger.error(f"Error in data integrity check: {e}")

    def _update_shared_state(self):
        """Push current bot state to the shared state for the API."""
        try:
            if self.feed_handler:
                shared_state.update_prices(
                    self.feed_handler.spot_ltp,
                    self.feed_handler.fut_ltp,
                    self.feed_handler.data_count,
                )
            shared_state.update_strategies(self)
            shared_state.update_candle_counts(self)
        except Exception as e:
            logger.error(f"Error updating shared state: {e}")

    def shutdown(self):
        try:
            shared_state.bot_running = False
            if self.websocket:
                try:
                    self.websocket.close_connection()
                except Exception:
                    pass
            if self.broker and hasattr(self.broker, "shutdown"):
                try:
                    self.broker.shutdown()
                except Exception:
                    pass
            logger.info("Shutdown complete.")
        except Exception as e:
            logger.error(f"Shutdown error: {e}")
    def _periodic_tasks(self):
        """Periodic maintenance tasks - log stats and check divergence every 5 minutes"""
        self._log_stats()
        self._check_comprehensive_divergence()

    def _log_stats(self):
        """Log current market statistics"""
        try:
            logger.info("\n===== PERIODIC STATS =====")
            logger.info(f"Total ticks processed: Spot: {self.feed_handler.data_count}, Futures: {self.feed_handler.data_count}")
            logger.info(f"Current prices - Spot: {self.feed_handler.spot_ltp:.2f}, Futures: {self.feed_handler.fut_ltp:.2f}")

            premium = self.feed_handler.fut_ltp - self.feed_handler.spot_ltp
            logger.info(f"Futures premium/discount: {premium:.2f} points ({(premium/self.feed_handler.spot_ltp)*100:.2f}%)")

            # Get candle counts
            spot_1m_candles = len(self.spot_series["1m"].completed_candles)
            spot_5m_candles = len(self.spot_series["5m"].completed_candles)
            fut_1m_candles = len(self.fut_series["1m"].completed_candles)
            fut_5m_candles = len(self.fut_series["5m"].completed_candles)

            logger.info(f"1-min candles - Spot: {spot_1m_candles}, Futures: {fut_1m_candles}")
            logger.info(f"5-min candles - Spot: {spot_5m_candles}, Futures: {fut_5m_candles}")

            logger.info("========================\n")

        except Exception as e:
            logger.error(f"Error generating stats report: {e}")

    def _check_comprehensive_divergence(self):
        """
        Periodic comprehensive divergence analysis.
        Matches bullishdiv4.py and bearishdiv2.py show_comprehensive_pivot_summary() functionality.
        Runs every 5 minutes to check ALL pivots for divergence conditions.
        Handles both bullish and bearish divergence strategies.
        """
        try:
            current_spot_price = self.feed_handler.spot_ltp
            current_time = datetime.now()

            # ----- BULLISH DIVERGENCE -----
            if hasattr(self, 'divergence_strategy') and self.divergence_strategy:
                bull_log = self.divergence_strategy.logger

                bull_log.info("\n" + "="*60)
                bull_log.info("PERIODIC COMPREHENSIVE DIVERGENCE ANALYSIS")
                bull_log.info("="*60)
                bull_log.info("\n--- BULLISH DIVERGENCE (Pivot Lows) ---")

                # Manually trigger comprehensive divergence check
                # (in bullishdiv4.py this happens in show_comprehensive_pivot_summary)
                self.divergence_strategy._check_for_divergence(current_spot_price, current_time)

                # Log active divergences status
                if self.divergence_strategy.active_divergences:
                    bull_log.info(f"\nActive Bullish Divergences: {len(self.divergence_strategy.active_divergences)}")
                    for div in self.divergence_strategy.active_divergences:
                        duration = (current_time - div['start_time']).total_seconds() / 60
                        bull_log.info(f"  Pivot {div['pivot_number']}: {div['divergence_type']}")
                        bull_log.info(f"    Started: {div['start_time']}, Duration: {duration:.1f} minutes")
                else:
                    bull_log.info("\nNo active bullish divergences being tracked")

                # Log pivot high status for bullish divergence validation
                spot_5m = self.divergence_strategy.spot_5m
                all_highs = spot_5m.pivot_highs
                broken_highs = spot_5m.broken_pivot_highs
                unbroken_highs = spot_5m.get_unbroken_pivot_highs()
                bull_log.info(f"\n--- PIVOT HIGHS (for divergence validation) ---")

                # Log first candle (9:15-9:20) pivot high status
                bull_strat = self.divergence_strategy
                if bull_strat._first_candle_pivot:
                    fc_high = bull_strat._first_candle_pivot['price']
                    fc_already_pivot = any(ph['time'] == bull_strat._first_candle_pivot['time'] for ph in all_highs)
                    if bull_strat._first_candle_broken:
                        bull_log.info(
                            f"  [FIRST CANDLE] 9:15-9:20 high @ {fc_high:.2f} - BROKEN "
                            f"(at {bull_strat._first_candle_breakout_time} @ {bull_strat._first_candle_breakout_price:.2f})"
                        )
                    else:
                        fc_note = " (also a detected pivot)" if fc_already_pivot else " (first candle only)"
                        bull_log.info(f"  [FIRST CANDLE] 9:15-9:20 high @ {fc_high:.2f} - UNBROKEN{fc_note}")

                bull_log.info(f"Total: {len(all_highs)} | Broken: {len(broken_highs)} | Unbroken: {len(unbroken_highs)}")
                if unbroken_highs:
                    for ph in unbroken_highs:
                        ph_num = ph.get('pivot_number', 'N/A')
                        bull_log.info(f"  [UNBROKEN] Pivot High {ph_num}: {ph['time']} @ {ph['price']:.2f}")
                else:
                    bull_log.info("  No unbroken pivot highs")
                if broken_highs:
                    for bh in broken_highs[-5:]:
                        bull_log.info(f"  [BROKEN]   Pivot High {bh.get('pivot_number', 'N/A')}: {bh['pivot_time']} @ {bh['pivot_price']:.2f} -> broken @ {bh['breakout_price']:.2f}")

                # Log entry setups status
                entry_summary = self.divergence_strategy.entry_manager.get_active_setups_summary()
                bull_log.info(f"\n{entry_summary}")
                bull_log.info("="*60 + "\n")

            # ----- BEARISH DIVERGENCE -----
            if hasattr(self, 'bearish_divergence_strategy') and self.bearish_divergence_strategy:
                bear_log = self.bearish_divergence_strategy.logger

                bear_log.info("\n" + "="*60)
                bear_log.info("PERIODIC COMPREHENSIVE DIVERGENCE ANALYSIS")
                bear_log.info("="*60)
                bear_log.info("\n--- BEARISH DIVERGENCE (Pivot Highs) ---")

                # Manually trigger comprehensive divergence check
                # (in bearishdiv2.py this happens in show_comprehensive_pivot_summary)
                self.bearish_divergence_strategy._check_for_divergence(current_spot_price, current_time)

                # Log active divergences status
                if self.bearish_divergence_strategy.active_divergences:
                    bear_log.info(f"\nActive Bearish Divergences: {len(self.bearish_divergence_strategy.active_divergences)}")
                    for div in self.bearish_divergence_strategy.active_divergences:
                        duration = (current_time - div['start_time']).total_seconds() / 60
                        bear_log.info(f"  Pivot {div['pivot_number']}: {div['divergence_type']}")
                        bear_log.info(f"    Started: {div['start_time']}, Duration: {duration:.1f} minutes")
                else:
                    bear_log.info("\nNo active bearish divergences being tracked")

                # Log pivot low status for bearish divergence validation
                spot_5m = self.bearish_divergence_strategy.spot_5m
                all_lows = spot_5m.pivot_lows
                broken_lows = spot_5m.broken_pivot_lows
                unbroken_lows = spot_5m.get_unbroken_pivot_lows()
                bear_log.info(f"\n--- PIVOT LOWS (for divergence validation) ---")

                # Log first candle (9:15-9:20) pivot low status
                bear_strat = self.bearish_divergence_strategy
                if bear_strat._first_candle_pivot:
                    fc_low = bear_strat._first_candle_pivot['price']
                    fc_already_pivot = any(pl['time'] == bear_strat._first_candle_pivot['time'] for pl in all_lows)
                    if bear_strat._first_candle_broken:
                        bear_log.info(
                            f"  [FIRST CANDLE] 9:15-9:20 low @ {fc_low:.2f} - BROKEN "
                            f"(at {bear_strat._first_candle_breakdown_time} @ {bear_strat._first_candle_breakdown_price:.2f})"
                        )
                    else:
                        fc_note = " (also a detected pivot)" if fc_already_pivot else " (first candle only)"
                        bear_log.info(f"  [FIRST CANDLE] 9:15-9:20 low @ {fc_low:.2f} - UNBROKEN{fc_note}")

                bear_log.info(f"Total: {len(all_lows)} | Broken: {len(broken_lows)} | Unbroken: {len(unbroken_lows)}")
                if unbroken_lows:
                    for pl in unbroken_lows:
                        pl_num = pl.get('pivot_number', 'N/A')
                        bear_log.info(f"  [UNBROKEN] Pivot Low {pl_num}: {pl['time']} @ {pl['price']:.2f}")
                else:
                    bear_log.info("  No unbroken pivot lows")
                if broken_lows:
                    for bl in broken_lows[-5:]:  # Show last 5 broken ones
                        bear_log.info(f"  [BROKEN]   Pivot Low {bl.get('pivot_number', 'N/A')}: {bl['pivot_time']} @ {bl['pivot_price']:.2f} -> broken @ {bl['breakdown_price']:.2f}")

                # Log entry setups status
                entry_summary = self.bearish_divergence_strategy.entry_manager.get_active_setups_summary()
                bear_log.info(f"\n{entry_summary}")
                bear_log.info("="*60 + "\n")

        except Exception as e:
            logger.error(f"Error in comprehensive divergence check: {e}")
