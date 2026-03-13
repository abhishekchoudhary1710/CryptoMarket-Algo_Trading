"""
SignalRouter
- Receives price updates tagged as "spot" or "fut"
- Runs BOTH bullish_strategy and bearish_strategy on spot
- Futures ticks are ignored for strategy but kept ready for later
- Optional auto-order execution if enabled in settings
"""

import time
from datetime import datetime
from utils.logger import logger
from utils.live_capture import log_entry
from config import settings


class SignalRouter:
    def __init__(self, bullish_strategy=None, bearish_strategy=None, divergence_strategy=None, bearish_divergence_strategy=None, fut_strategy=None, order_manager=None):
        self.bullish_strategy = bullish_strategy
        self.bearish_strategy = bearish_strategy
        self.divergence_strategy = divergence_strategy
        self.bearish_divergence_strategy = bearish_divergence_strategy
        self.fut_strategy = fut_strategy  # placeholder
        self.order_manager = order_manager

        # Keep old parameter for backward compatibility
        self.spot_strategy = bullish_strategy  # Legacy support

        # Safe toggle: defaults False if not present
        self.auto_execute = getattr(settings, "AUTO_ORDER_EXECUTION", False)

        # Track last order time for rate limiting
        self.last_order_time = None
        self.last_found_signals = []

        # Signal deduplication: prevent repeated order attempts for the same setup
        # Maps strategy type -> True if an order has already been attempted
        self._order_attempted = {}
        # Track D points per strategy to detect new setups and reset dedup flags
        self._last_d_points = {}

    def _check_and_reset_dedup(self, strategy, strategy_key):
        """Check if strategy's D point changed (new setup) and reset dedup flag."""
        if strategy is None:
            return
        current_d = getattr(strategy, 'D', None)
        last_d = self._last_d_points.get(strategy_key)
        if current_d != last_d:
            self._last_d_points[strategy_key] = current_d
            if strategy_key in self._order_attempted:
                del self._order_attempted[strategy_key]
                logger.info(f"[ROUTER] New setup detected for {strategy_key} (D: {last_d} -> {current_d}), reset order dedup flag")

    def _mark_order_attempted(self, strategy_key):
        """Mark that an order has been attempted for this strategy setup."""
        self._order_attempted[strategy_key] = True
        logger.info(f"[ROUTER] Marked {strategy_key} as order-attempted (will not retry until new setup)")

    def _is_order_attempted(self, strategy_key):
        """Check if an order was already attempted for this strategy setup."""
        return self._order_attempted.get(strategy_key, False)

    def route_price(self, instrument, price):
        """
        instrument: "spot" or "fut"
        Routes price to both bullish and bearish strategies
        """
        try:
            signals = []

            # ---------------- SPOT ROUTING ----------------
            if instrument == "spot":
                # Check bullish strategy
                if self.bullish_strategy:
                    self._check_and_reset_dedup(self.bullish_strategy, "bullish")
                    bullish_signal = self.bullish_strategy.check_live_tick(price)
                    if bullish_signal:
                        slog = getattr(self.bullish_strategy, 'logger', None)
                        logger.info(f"[ROUTER] Bullish signal detected at {price:.2f}")
                        if slog: slog.info(f"[ROUTER] Bullish signal detected at {price:.2f}")
                        signals.append({"type": "bullish", "signal": bullish_signal})
                        log_entry("bullish", "spot", price, bullish_signal)

                        if self.auto_execute and self.order_manager and not self._is_order_attempted("bullish"):
                            order_id = None
                            if 'selected_option' in bullish_signal and bullish_signal['selected_option'] is not None:
                                selected_option = bullish_signal['selected_option']
                                logger.info(f"[ROUTER] Placing bullish order:")
                                logger.info(f"  Strike: {selected_option['strike_float']} {selected_option['option_type']}")
                                logger.info(f"  Expiry: {selected_option['expiry_date']}")
                                logger.info(f"  Quantity: {bullish_signal.get('selected_quantity', 75)}")
                                logger.info(f"  Risk: ₹{bullish_signal.get('selected_risk', 'N/A')}")
                                logger.info(f"  SL Points: {bullish_signal.get('option_sl_points', 'N/A')}")
                                if slog:
                                    slog.info(f"[ROUTER] Placing bullish order:")
                                    slog.info(f"  Strike: {selected_option['strike_float']} {selected_option['option_type']}")
                                    slog.info(f"  Expiry: {selected_option['expiry_date']}")
                                    slog.info(f"  Quantity: {bullish_signal.get('selected_quantity', 75)}")
                                    slog.info(f"  Risk: ₹{bullish_signal.get('selected_risk', 'N/A')}")
                                    slog.info(f"  SL Points: {bullish_signal.get('option_sl_points', 'N/A')}")
                                order_id = self.order_manager.place_option_order_direct(
                                    bullish_signal,
                                    strategy_logger=slog
                                )
                            # Mark as attempted regardless of success/failure to prevent retry loop
                            self._mark_order_attempted("bullish")
                            if order_id:
                                logger.info(f"[ROUTER] Bullish order placed. Order ID: {order_id}")
                                if slog: slog.info(f"[ROUTER] Bullish order placed. Order ID: {order_id}")
                            else:
                                logger.error(f"[ROUTER] Failed to place bullish order")
                                if slog: slog.error(f"[ROUTER] Failed to place bullish order")

                # Check bearish strategy
                if self.bearish_strategy:
                    self._check_and_reset_dedup(self.bearish_strategy, "bearish")
                    bearish_signal = self.bearish_strategy.check_live_tick(price)
                    if bearish_signal:
                        slog = getattr(self.bearish_strategy, 'logger', None)
                        logger.info(f"[ROUTER] Bearish signal detected at {price:.2f}")
                        if slog: slog.info(f"[ROUTER] Bearish signal detected at {price:.2f}")
                        signals.append({"type": "bearish", "signal": bearish_signal})
                        log_entry("bearish", "spot", price, bearish_signal)

                        if self.auto_execute and self.order_manager and not self._is_order_attempted("bearish"):
                            order_id = None
                            if 'selected_option' in bearish_signal and bearish_signal['selected_option'] is not None:
                                selected_option = bearish_signal['selected_option']
                                logger.info(f"[ROUTER] Placing bearish order:")
                                logger.info(f"  Strike: {selected_option['strike_float']} {selected_option['option_type']}")
                                logger.info(f"  Expiry: {selected_option['expiry_date']}")
                                logger.info(f"  Quantity: {bearish_signal.get('selected_quantity', 75)}")
                                logger.info(f"  Risk: ₹{bearish_signal.get('selected_risk', 'N/A')}")
                                logger.info(f"  SL Points: {bearish_signal.get('option_sl_points', 'N/A')}")
                                if slog:
                                    slog.info(f"[ROUTER] Placing bearish order:")
                                    slog.info(f"  Strike: {selected_option['strike_float']} {selected_option['option_type']}")
                                    slog.info(f"  Expiry: {selected_option['expiry_date']}")
                                    slog.info(f"  Quantity: {bearish_signal.get('selected_quantity', 75)}")
                                    slog.info(f"  Risk: ₹{bearish_signal.get('selected_risk', 'N/A')}")
                                    slog.info(f"  SL Points: {bearish_signal.get('option_sl_points', 'N/A')}")
                                order_id = self.order_manager.place_option_order_direct(
                                    bearish_signal,
                                    strategy_logger=slog
                                )
                            # Mark as attempted regardless of success/failure to prevent retry loop
                            self._mark_order_attempted("bearish")
                            if order_id:
                                logger.info(f"[ROUTER] Bearish order placed. Order ID: {order_id}")
                                if slog: slog.info(f"[ROUTER] Bearish order placed. Order ID: {order_id}")
                            else:
                                logger.error(f"[ROUTER] Failed to place bearish order")
                                if slog: slog.error(f"[ROUTER] Failed to place bearish order")

                # Check divergence strategy (bullish)
                if self.divergence_strategy:
                    divergence_signal = self.divergence_strategy.check_live_tick(price)
                    if divergence_signal:
                        slog = getattr(self.divergence_strategy, 'logger', None)
                        # Use pivot_number to detect new divergence setups
                        pivot_num = divergence_signal.get('pivot_number')
                        dedup_key = f"bullish_divergence_p{pivot_num}"
                        logger.info(f"[ROUTER] Bullish Divergence signal detected at {price:.2f}")
                        if slog: slog.info(f"[ROUTER] Bullish Divergence signal detected at {price:.2f}")
                        signals.append({"type": "bullish_divergence", "signal": divergence_signal})
                        log_entry("bullish_divergence", "spot", price, divergence_signal)

                        if self.auto_execute and self.order_manager and not self._is_order_attempted(dedup_key):
                            order_id = None
                            if 'selected_option' in divergence_signal and divergence_signal['selected_option'] is not None:
                                selected_option = divergence_signal['selected_option']
                                logger.info(f"[ROUTER] Placing bullish divergence order:")
                                logger.info(f"  Strike: {selected_option.get('strike_float', 'N/A')} {selected_option.get('option_type', 'CE')}")
                                logger.info(f"  Quantity: {divergence_signal.get('selected_quantity', 75)}")
                                logger.info(f"  Risk: ₹{divergence_signal.get('selected_risk', 'N/A')}")
                                logger.info(f"  SL Points: {divergence_signal.get('option_sl_points', 'N/A')}")
                                if slog:
                                    slog.info(f"[ROUTER] Placing bullish divergence order:")
                                    slog.info(f"  Strike: {selected_option.get('strike_float', 'N/A')} {selected_option.get('option_type', 'CE')}")
                                    slog.info(f"  Quantity: {divergence_signal.get('selected_quantity', 75)}")
                                    slog.info(f"  Risk: ₹{divergence_signal.get('selected_risk', 'N/A')}")
                                    slog.info(f"  SL Points: {divergence_signal.get('option_sl_points', 'N/A')}")
                                order_id = self.order_manager.place_option_order_direct(
                                    divergence_signal,
                                    strategy_logger=slog
                                )
                            # Mark as attempted regardless of success/failure to prevent retry loop
                            self._mark_order_attempted(dedup_key)
                            if order_id:
                                logger.info(f"[ROUTER] Bullish Divergence order placed. Order ID: {order_id}")
                                if slog: slog.info(f"[ROUTER] Bullish Divergence order placed. Order ID: {order_id}")
                            else:
                                logger.error(f"[ROUTER] Failed to place bullish divergence order")
                                if slog: slog.error(f"[ROUTER] Failed to place bullish divergence order")

                # Check bearish divergence strategy
                if self.bearish_divergence_strategy:
                    bearish_div_signal = self.bearish_divergence_strategy.check_live_tick(price)
                    if bearish_div_signal:
                        slog = getattr(self.bearish_divergence_strategy, 'logger', None)
                        # Use pivot_number to detect new divergence setups
                        pivot_num = bearish_div_signal.get('pivot_number')
                        dedup_key = f"bearish_divergence_p{pivot_num}"
                        logger.info(f"[ROUTER] Bearish Divergence signal detected at {price:.2f}")
                        if slog: slog.info(f"[ROUTER] Bearish Divergence signal detected at {price:.2f}")
                        signals.append({"type": "bearish_divergence", "signal": bearish_div_signal})
                        log_entry("bearish_divergence", "spot", price, bearish_div_signal)

                        if self.auto_execute and self.order_manager and not self._is_order_attempted(dedup_key):
                            order_id = None
                            if 'selected_option' in bearish_div_signal and bearish_div_signal['selected_option'] is not None:
                                selected_option = bearish_div_signal['selected_option']
                                logger.info(f"[ROUTER] Placing bearish divergence order:")
                                logger.info(f"  Strike: {selected_option.get('strike_float', 'N/A')} {selected_option.get('option_type', 'PE')}")
                                logger.info(f"  Quantity: {bearish_div_signal.get('selected_quantity', 75)}")
                                logger.info(f"  Risk: ₹{bearish_div_signal.get('selected_risk', 'N/A')}")
                                logger.info(f"  SL Points: {bearish_div_signal.get('option_sl_points', 'N/A')}")
                                if slog:
                                    slog.info(f"[ROUTER] Placing bearish divergence order:")
                                    slog.info(f"  Strike: {selected_option.get('strike_float', 'N/A')} {selected_option.get('option_type', 'PE')}")
                                    slog.info(f"  Quantity: {bearish_div_signal.get('selected_quantity', 75)}")
                                    slog.info(f"  Risk: ₹{bearish_div_signal.get('selected_risk', 'N/A')}")
                                    slog.info(f"  SL Points: {bearish_div_signal.get('option_sl_points', 'N/A')}")
                                order_id = self.order_manager.place_option_order_direct(
                                    bearish_div_signal,
                                    strategy_logger=slog
                                )
                            # Mark as attempted regardless of success/failure to prevent retry loop
                            self._mark_order_attempted(dedup_key)
                            if order_id:
                                logger.info(f"[ROUTER] Bearish Divergence order placed. Order ID: {order_id}")
                                if slog: slog.info(f"[ROUTER] Bearish Divergence order placed. Order ID: {order_id}")
                            else:
                                logger.error(f"[ROUTER] Failed to place bearish divergence order")
                                if slog: slog.error(f"[ROUTER] Failed to place bearish divergence order")

            # ---------------- FUTURES ROUTING ----------------
            if instrument == "fut":
                # Check bullish divergence strategy futures entry managers
                if self.divergence_strategy:
                    bull_div_fut_signal = self.divergence_strategy.check_futures_tick(price)
                    if bull_div_fut_signal:
                        slog = getattr(self.divergence_strategy, 'logger', None)
                        pivot_num = bull_div_fut_signal.get('pivot_number')
                        data_source = bull_div_fut_signal.get('data_source', 'FUTURES')
                        dedup_key = f"bullish_divergence_fut_p{pivot_num}"
                        logger.info(f"[ROUTER] [{data_source}] Bullish Divergence signal detected at {price:.2f}")
                        if slog: slog.info(f"[ROUTER] [{data_source}] Bullish Divergence signal detected at {price:.2f}")
                        signals.append({"type": "bullish_divergence_futures", "signal": bull_div_fut_signal})
                        log_entry("bullish_divergence", "fut", price, bull_div_fut_signal)

                        if self.auto_execute and self.order_manager and not self._is_order_attempted(dedup_key):
                            order_id = None
                            if 'selected_option' in bull_div_fut_signal and bull_div_fut_signal['selected_option'] is not None:
                                order_id = self.order_manager.place_option_order_direct(
                                    bull_div_fut_signal,
                                    strategy_logger=slog
                                )
                            self._mark_order_attempted(dedup_key)
                            if order_id:
                                logger.info(f"[ROUTER] Bullish Divergence FUTURES order placed. Order ID: {order_id}")
                                if slog: slog.info(f"[ROUTER] Bullish Divergence FUTURES order placed. Order ID: {order_id}")

                # Check bearish divergence strategy futures entry managers
                if self.bearish_divergence_strategy:
                    bear_div_fut_signal = self.bearish_divergence_strategy.check_futures_tick(price)
                    if bear_div_fut_signal:
                        slog = getattr(self.bearish_divergence_strategy, 'logger', None)
                        pivot_num = bear_div_fut_signal.get('pivot_number')
                        data_source = bear_div_fut_signal.get('data_source', 'FUTURES')
                        dedup_key = f"bearish_divergence_fut_p{pivot_num}"
                        logger.info(f"[ROUTER] [{data_source}] Bearish Divergence signal detected at {price:.2f}")
                        if slog: slog.info(f"[ROUTER] [{data_source}] Bearish Divergence signal detected at {price:.2f}")
                        signals.append({"type": "bearish_divergence_futures", "signal": bear_div_fut_signal})
                        log_entry("bearish_divergence", "fut", price, bear_div_fut_signal)

                        if self.auto_execute and self.order_manager and not self._is_order_attempted(dedup_key):
                            order_id = None
                            if 'selected_option' in bear_div_fut_signal and bear_div_fut_signal['selected_option'] is not None:
                                order_id = self.order_manager.place_option_order_direct(
                                    bear_div_fut_signal,
                                    strategy_logger=slog
                                )
                            self._mark_order_attempted(dedup_key)
                            if order_id:
                                logger.info(f"[ROUTER] Bearish Divergence FUTURES order placed. Order ID: {order_id}")
                                if slog: slog.info(f"[ROUTER] Bearish Divergence FUTURES order placed. Order ID: {order_id}")

                return signals if signals else None

            return signals if signals else None

        except Exception as e:
            logger.error(f"Router error: {e}")
            return None

    def check_for_breakout_signals(self, instrument, price):
        """
        Check for breakout signals in live ticks and place orders if conditions are met.
        Now supports both bullish and bearish strategies.

        Features:
        - Quick check using signal_counter to avoid duplicates
        - 300-second (5-minute) minimum gap between orders
        - Updates Greeks before signal generation
        - Places order with selected option and quantity
        - Starts async order status check

        Args:
            instrument (str): "spot" or "fut"
            price (float): Current tick price
        """
        try:
            if instrument == 'spot':
                # Check bullish strategy
                self._check_strategy_breakout(self.bullish_strategy, price, "BULLISH")

                # Check bearish strategy
                self._check_strategy_breakout(self.bearish_strategy, price, "BEARISH")

                # NOTE: Divergence strategy is already called in route_price()
                # No need to call it again here to avoid duplicate processing

        except Exception as e:
            logger.error(f"Error in breakout signal check: {e}")

    def _check_strategy_breakout(self, strategy, price, strategy_type):
        """
        Helper method to check breakout for a specific strategy.

        Args:
            strategy: The strategy instance (bullish or bearish)
            price (float): Current tick price
            strategy_type (str): "BULLISH" or "BEARISH"
        """
        try:
            if strategy is not None:
                slog = getattr(strategy, 'logger', None)

                # Only check for breakout if we have a valid D point
                d_point = strategy.D
                if d_point is None:
                    return

                # Bullish: price > D, Bearish: price < D
                breakout_detected = (price > d_point) if strategy_type == "BULLISH" else (price < d_point)

                if breakout_detected:
                    # NOTE: Order placement is handled exclusively by route_price().
                    # Swing strategies rely on prefetched broker cache and do not
                    # trigger a breakout-time Greeks refresh here.
                    return

        except Exception as e:
            logger.error(f"Error in {strategy_type} breakout check: {e}")

    def _handle_divergence_signal(self, signal, price):
        """
        Handle divergence strategy signals.

        Args:
            signal: The divergence signal from the strategy
            price (float): Current tick price
        """
        try:
            current_time = datetime.now()

            logger.info(f"SIGNAL ALERT: Divergence signal at {price:.2f}")

            # Record signal
            self.last_found_signals.append({
                'time': current_time,
                'instrument': "spot",
                'strategy_type': "DIVERGENCE",
                'price': price,
                'signal': signal
            })

            # Update last order time
            self.last_order_time = current_time

            order_id = None

            # Check if signal has option data
            if 'selected_option' in signal and signal['selected_option'] is not None:
                selected_option = signal['selected_option']

                logger.info(f"Placing divergence order with selected option:")
                logger.info(f"  Strike: {selected_option.get('strike_float', 'N/A')} {selected_option.get('option_type', 'CE')}")
                logger.info(f"  Quantity: {signal.get('selected_quantity', 75)}")
                logger.info(f"  Risk: ₹{signal.get('selected_risk', 'N/A')}")
                logger.info(f"  SL Points: {signal.get('option_sl_points', 'N/A')}")

                # Place order — signal is the complete payload
                if self.order_manager:
                    order_id = self.order_manager.place_option_order_direct(signal)
                else:
                    logger.error("No order manager available")
            else:
                logger.warning("Divergence signal does not contain option data")

            # Process order result
            if order_id:
                logger.info(f"DIVERGENCE order placed successfully with ID: {order_id}")
                # Start background order status check
                if self.order_manager and hasattr(self.order_manager, '_async_check_order_status'):
                    self.order_manager._async_check_order_status(order_id)
            else:
                logger.error("Failed to place DIVERGENCE order")

        except Exception as e:
            logger.error(f"Error handling divergence signal: {e}")
