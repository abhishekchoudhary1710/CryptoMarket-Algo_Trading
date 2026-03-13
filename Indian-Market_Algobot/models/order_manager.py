import os
import time
import threading
import pandas as pd
from datetime import datetime
from utils.logger import logger, log_order
from config import settings

class OrderManager:
    def __init__(self, broker):
        """Initialize the OrderManager.

        Args:
            broker: Broker instance for placing orders
        """
        self.broker = broker
        self.last_order_id = None
        self.order_history = []
        self.order_counter = 0

        # Rate limiting tracking variables
        self._last_order_time = 0
        self._minute_order_count = 0
        self._minute_start_time = time.time()

        # Status check rate limiting variables
        self._last_status_check = 0
        self._minute_status_count = 0
        self._minute_status_start = time.time()

        # Ensure order history directory exists
        os.makedirs(settings.ORDER_HISTORY_DIR, exist_ok=True)

    def place_option_order_direct(self, signal, strategy_logger=None):
        """
        Place an option order from a strategy signal payload.

        Fetches live option LTP just before placement, then converts
        strategy-provided risk points into actual order prices.

        Args:
            signal: Dictionary with keys:
                selected_option: dict with symbol, token, strike_float, option_type, expiry
                selected_quantity: int
                transaction_type: 'BUY' (default)
                order_type: 'MARKET' (default)
                option_sl_points: Greeks-derived risk in option points
                target_multiple: risk-reward multiplier (default 2)

        Returns:
            order_id or None
        """
        try:
            slog = strategy_logger
            option_data = signal.get('selected_option', {})
            quantity = signal.get('selected_quantity', 75)
            order_type = signal.get('order_type', 'MARKET')
            transaction_type = signal.get('transaction_type', 'BUY')
            option_sl_points = signal.get('option_sl_points')
            target_multiple = signal.get('target_multiple', 2)

            logger.info(f"[ORDER] Quantity: {quantity}, SL points: {option_sl_points}, Target multiple: {target_multiple}")
            if slog: slog.info(f"[ORDER] Quantity: {quantity}, SL points: {option_sl_points}, Target multiple: {target_multiple}")

            if not self.broker or not hasattr(self.broker, 'api'):
                logger.error("Broker API not available for placing order")
                if slog: slog.error("[ORDER] Broker API not available for placing order")
                return None

            # Rate limit tracking
            current_time = time.time()

            # Check minute-based rate limit (500 per minute)
            if current_time - self._minute_start_time >= 60:
                self._minute_order_count = 0
                self._minute_start_time = current_time
            elif self._minute_order_count >= settings.MAX_REQUESTS_PER_MINUTE:
                sleep_time = 60 - (current_time - self._minute_start_time)
                if sleep_time > 0:
                    logger.info(f"Reached minute order rate limit, waiting {sleep_time:.2f} seconds")
                    if slog: slog.info(f"[ORDER] Reached minute order rate limit, waiting {sleep_time:.2f} seconds")
                    time.sleep(sleep_time)
                    self._minute_order_count = 0
                    self._minute_start_time = time.time()

            # Per-second rate limit (20 per second)
            time_since_last = current_time - self._last_order_time
            min_interval = settings.MIN_REQUEST_INTERVAL_MS / 1000
            if time_since_last < min_interval:
                time.sleep(min_interval - time_since_last)

            # Extract option details
            symbol = option_data.get('symbol')
            token = option_data.get('token')
            strike = option_data.get('strike_float')
            option_type = option_data.get('option_type')
            expiry = option_data.get('expiry')

            if not all([symbol, token, strike, option_type, expiry]):
                logger.error("Missing required option data for order")
                if slog: slog.error("[ORDER] Missing required option data for order")
                return None

            # Fetch live option LTP just before placement — this is the entry reference
            ltp_response = self.broker.get_ltp("NFO", symbol, str(token))
            option_price = None
            if ltp_response and ltp_response.get('data'):
                option_price = float(ltp_response['data'].get('ltp', 0))
                logger.info(f"[ORDER] Fetched live option LTP: {option_price:.2f}")
                if slog: slog.info(f"[ORDER] Fetched live option LTP: {option_price:.2f}")
            else:
                logger.error(f"[ORDER] Failed to fetch live LTP for {symbol}")
                if slog: slog.error(f"[ORDER] Failed to fetch live LTP for {symbol}")
                return None

            # Convert strategy risk points into actual order values
            sl_price = None
            target_price = None
            if option_sl_points and option_sl_points > 0:
                sl_price = option_price - option_sl_points  # Long option buy
                target_points = option_sl_points * target_multiple
                target_price = option_price + target_points
                logger.info(f"[ORDER] Entry: {option_price:.2f}, SL: {sl_price:.2f} (-{option_sl_points:.2f}), Target: {target_price:.2f} (+{target_points:.2f})")
                if slog: slog.info(f"[ORDER] Entry: {option_price:.2f}, SL: {sl_price:.2f} (-{option_sl_points:.2f}), Target: {target_price:.2f} (+{target_points:.2f})")
            else:
                logger.warning("[ORDER] No option_sl_points in signal, placing order without SL/target")
                if slog: slog.warning("[ORDER] No option_sl_points in signal, placing order without SL/target")

            # Prepare order parameters — use bracket order if SL and target are available
            if sl_price and target_price and order_type == 'MARKET':
                logger.info("[ORDER] Placing bracket order with SL and target")
                if slog: slog.info("[ORDER] Placing bracket order with SL and target")
                order_params = {
                    "variety": "BO",
                    "tradingsymbol": symbol,
                    "symboltoken": str(token),
                    "transactiontype": transaction_type,
                    "exchange": "NFO",
                    "ordertype": order_type,
                    "producttype": "INTRADAY",
                    "duration": "DAY",
                    "quantity": str(quantity),
                    "squareoff": str(round(option_sl_points * target_multiple, 2)),
                    "stoploss": str(round(option_sl_points, 2))
                }
            else:
                logger.info("[ORDER] Placing regular order without SL/target")
                if slog: slog.info("[ORDER] Placing regular order without SL/target")
                order_params = {
                    "variety": "NORMAL",
                    "tradingsymbol": symbol,
                    "symboltoken": str(token),
                    "transactiontype": transaction_type,
                    "exchange": "NFO",
                    "ordertype": order_type,
                    "producttype": "INTRADAY",
                    "duration": "DAY",
                    "quantity": str(quantity)
                }

            # Increment order counter right before attempting to place the order
            self.order_counter += 1
            logger.info(f"Incrementing order counter to {self.order_counter} for order attempt")
            if slog: slog.info(f"[ORDER] Incrementing order counter to {self.order_counter} for order attempt")

            # Place the order with minimal logging during execution
            try:
                order_response = self.broker.api.placeOrder(order_params)
                self._last_order_time = time.time()  # Update last order time
                self._minute_order_count += 1  # Increment minute counter

                # Quick order ID extraction
                order_id = None
                if isinstance(order_response, dict) and order_response.get('status'):
                    order_id = order_response.get('data', {}).get('orderid')
                elif isinstance(order_response, str) and order_response.isalnum():
                    order_id = order_response

                if order_id:
                    self.last_order_id = order_id

                    # Prepare order record
                    order_record = {
                        'order_id': order_id,
                        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'symbol': symbol,
                        'token': token,
                        'strike': float(strike),
                        'option_type': option_type,
                        'expiry': expiry,
                        'transaction_type': 'BUY',
                        'quantity': quantity,
                        'order_type': order_type,
                        'status': 'PLACED',
                        'variety': order_params.get('variety', 'NORMAL'),
                        'stop_loss': sl_price,
                        'target': target_price
                    }

                    # Save order details asynchronously
                    self._async_save_order(order_record)

                    logger.info(f"Order placed successfully with ID: {order_id}")
                    if slog: slog.info(f"[ORDER] Order placed successfully with ID: {order_id}")
                    return order_id
                else:
                    logger.error(f"Order placement failed: {order_response}")
                    if slog: slog.error(f"[ORDER] Order placement failed: {order_response}")

                    # Save failed order details
                    order_record = {
                        'order_id': 'FAILED',
                        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'symbol': symbol,
                        'token': token,
                        'strike': float(strike),
                        'option_type': option_type,
                        'expiry': expiry,
                        'transaction_type': 'BUY',
                        'quantity': quantity,
                        'order_type': order_type,
                        'status': 'FAILED',
                        'error': str(order_response),
                        'variety': order_params.get('variety', 'NORMAL'),
                        'stop_loss': sl_price,
                        'target': target_price
                    }
                    self._async_save_order(order_record)

                    return None
            except Exception as e:
                logger.error(f"Exception during order placement: {e}")
                if slog: slog.error(f"[ORDER] Exception during order placement: {e}")

                # Save exception details
                order_record = {
                    'order_id': 'ERROR',
                    'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'symbol': symbol,
                    'token': token,
                    'strike': float(strike),
                    'option_type': option_type,
                    'expiry': expiry,
                    'transaction_type': 'BUY',
                    'quantity': quantity,
                    'order_type': order_type,
                    'status': 'ERROR',
                    'error': str(e),
                    'variety': order_params.get('variety', 'NORMAL'),
                    'stop_loss': sl_price,
                    'target': target_price
                }
                self._async_save_order(order_record)

                return None

        except Exception as e:
            logger.error(f"Error placing option order: {e}")
            if slog: slog.error(f"[ORDER] Error placing option order: {e}")
            return None

    def place_order(self, signal_data):
        """Place an order based on signal data.

        Args:
            signal_data (dict): Signal data with order parameters

        Returns:
            str: Order ID if successful, None otherwise
        """
        if not self.broker:
            logger.error("Broker not available for placing order")
            return None

        if not signal_data:
            logger.error("Invalid signal data for order placement")
            return None

        # Prepare order parameters for bracket order
        order_params = {
            "symboltoken": signal_data.get('token'),
            "symbol": signal_data.get('symbol'),
            "quantity": signal_data.get('quantity', 1),
            "ordertype": "MARKET",
            "tradingsymbol": signal_data.get('trading_symbol'),
            "producttype": signal_data.get('product_type', 'INTRADAY'),
            "duration": signal_data.get('duration', 'DAY'),
            "price": signal_data.get('price', 0),
            "squareoff": signal_data.get('target', 0),
            "stoploss": signal_data.get('stop_loss', 0),
            "transactiontype": signal_data.get('transaction_type', 'BUY')
        }

        # Place bracket order
        order_id = self.broker.place_order(order_params, order_type="BO")

        if order_id:
            # Increment order counter
            self.order_counter += 1

            # Record order details
            order_record = {
                'order_id': order_id,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'symbol': signal_data.get('symbol'),
                'token': signal_data.get('token'),
                'quantity': signal_data.get('quantity'),
                'price': signal_data.get('price'),
                'stop_loss': signal_data.get('stop_loss'),
                'target': signal_data.get('target'),
                'transaction_type': signal_data.get('transaction_type'),
                'product_type': signal_data.get('product_type'),
                'order_type': 'BO',
                'status': 'PLACED'
            }

            # Save order record
            self._save_order_to_csv(order_record)
            self.order_history.append(order_record)
            self.last_order_id = order_id

            logger.info(f"Bracket order placed successfully with ID: {order_id}")
            return order_id

        logger.error("Failed to place order")
        return None

    def check_order_status(self, order_id=None, max_retries=3):
        """
        Check order status with retries and rate limiting.
        Enhanced version ported from bullish3t1.py lines 718-774.

        Features:
        - Rate limiting: 500/min, 10/sec
        - Retry logic with max 3 attempts
        - Short delay (100ms) between retries

        Args:
            order_id (str): Order ID to check (defaults to last_order_id)
            max_retries (int): Maximum retry attempts (default 3)

        Returns:
            dict: Order status information or None if failed
        """
        order_id = order_id or self.last_order_id
        if not order_id:
            return None

        for retry in range(max_retries):
            try:
                current_time = time.time()

                # Check minute-based rate limit (500 per minute)
                if current_time - self._minute_status_start >= 60:
                    self._minute_status_count = 0
                    self._minute_status_start = current_time
                elif self._minute_status_count >= settings.MAX_REQUESTS_PER_MINUTE:
                    sleep_time = 60 - (current_time - self._minute_status_start)
                    if sleep_time > 0:
                        logger.info(f"Reached minute status check rate limit, waiting {sleep_time:.2f} seconds")
                        time.sleep(sleep_time)
                        self._minute_status_count = 0
                        self._minute_status_start = time.time()

                # Per-second rate limit (10 per second)
                time_since_last = current_time - self._last_status_check
                if time_since_last < 0.1:  # Minimum 100ms between checks (10 per second)
                    time.sleep(0.1 - time_since_last)

                # Get order book from broker
                if not self.broker or not hasattr(self.broker, 'api'):
                    logger.error("Broker API not available for order status check")
                    return None

                order_book = self.broker.api.orderBook()
                self._last_status_check = time.time()
                self._minute_status_count += 1

                if order_book and 'data' in order_book:
                    for order in order_book['data']:
                        if order.get('orderid') == order_id:
                            return {
                                'order_id': order_id,
                                'status': order.get('status'),
                                'filled_quantity': order.get('filledqty'),
                                'average_price': order.get('averageprice')
                            }
                    break  # Exit loop if we got a response but order not found
                elif retry < max_retries - 1:
                    time.sleep(0.1)  # Short delay between retries
                    continue
            except Exception as e:
                if retry < max_retries - 1:
                    time.sleep(0.1)  # Short delay between retries
                    continue
                logger.error(f"Error checking order status: {e}")
                break
        return None

    def _save_order_to_csv(self, order_data):
        """Save order data to a CSV file.

        Args:
            order_data (dict): Order details
        """
        try:
            date_str = datetime.now().strftime('%Y%m%d')
            filename = os.path.join(settings.ORDER_HISTORY_DIR, f'order_history_{date_str}.csv')

            # Convert all values to string to avoid type conflicts
            for key, value in order_data.items():
                order_data[key] = str(value)

            # Create DataFrame and save to CSV
            df = pd.DataFrame([order_data])
            if os.path.exists(filename):
                df.to_csv(filename, mode='a', header=False, index=False)
            else:
                df.to_csv(filename, index=False)

            logger.info(f"Saved order details to {filename}")
        except Exception as e:
            logger.error(f"Error saving order to CSV: {e}")

    def _async_save_order(self, order_record):
        """
        Save order details asynchronously in background thread.
        Ported from bullish3t1.py lines 705-716.

        Args:
            order_record: Dictionary with order details
        """
        try:
            save_thread = threading.Thread(
                target=self._save_order_to_csv,
                args=(order_record,),
                daemon=True
            )
            save_thread.start()
        except Exception as e:
            logger.error(f"Error in async order save: {e}")

    def _async_check_order_status(self, order_id):
        """
        Check order status in background with optimized timing.
        Ported from bullish3t1.py lines 1738-1760.

        Checks order status twice:
        - Quick check after 100ms
        - Follow-up check after 1 second if not in final state

        Args:
            order_id: Order ID to check
        """
        def check_status():
            try:
                # Initial quick check after 100ms
                time.sleep(0.1)
                status = self.check_order_status(order_id)
                if status and status.get('status') in ['COMPLETE', 'REJECTED', 'CANCELLED']:
                    logger.info(f"Order {order_id} final status: {status['status']}")
                    return

                # If not in final state, check again after 1 second
                time.sleep(1)
                status = self.check_order_status(order_id)
                if status:
                    logger.info(f"Order {order_id} status: {status['status']}, Filled: {status.get('filled_quantity', 0)}")

            except Exception as e:
                logger.error(f"Error checking order status: {e}")

        status_thread = threading.Thread(target=check_status, daemon=True)
        status_thread.start()