# brokers/angelone.py
"""
Angel One broker integration.

Important:
- Broker handles auth + order APIs + websocket start.
- It does NOT contain strategy/tick logic.
"""
import threading
import time
import random
from datetime import datetime

import pyotp
import pytz
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

from utils.logger import logger, log_exception
from config import settings

class AngelOneBroker:
    def __init__(self):
        self.api = None
        self.feed_token = None
        self.refresh_token = None
        self.websocket = None
        self.is_connected = False

        # Per-expiry rate limiting (90 req/min per expiry) - matching bullish3t.py
        self.expiry_rate_limits = {}  # {expiry_str: {'count': int, 'start_time': float}}

        # Thread lock to prevent concurrent fetch_options_chain calls from multiple strategies
        self._fetch_lock = threading.Lock()

        # Shared options data cache (avoids redundant API calls across strategies)
        self._options_cache = None          # cached DataFrame
        self._options_cache_time = 0        # timestamp of last fetch
        self._options_cache_price = None    # spot price used for cache

        # ScripMaster cache (doesn't change during trading day)
        self._scripmaster_cache = None
        self._scripmaster_cache_time = 0

        # Global Greeks API rate limiting
        self._greeks_call_times = []  # timestamps of all optionGreek API calls

        # Single Greeks cache — populated ONLY by refresh_greeks(), consumed by fetch_options_chain()
        self._shared_greeks_data = {}    # greek_dict with multiple key formats
        self._shared_greeks_time = 0     # timestamp when last fetched

        # Lock for refresh_greeks() to prevent concurrent API calls
        self._greeks_refresh_lock = threading.Lock()
        self._greeks_service_lock = threading.Lock()
        self._greeks_refresh_stop = threading.Event()
        self._greeks_refresh_thread = None

    def _totp(self):
        try:
            return pyotp.TOTP(settings.TOTP_KEY).now()
        except Exception as e:
            log_exception(e)
            return None

    def connect(self):
        """
        Create or refresh session.
        """
        try:
            if self.api is None:
                logger.info("Initializing SmartAPI connection.")
                totp = self._totp()
                if not totp:
                    logger.error("TOTP generation failed.")
                    return False

                self.api = SmartConnect(api_key=settings.API_KEY)
                data = self.api.generateSession(settings.USERNAME, settings.PASSWORD, totp)

                if data and data.get("status"):
                    self.refresh_token = data["data"]["refreshToken"]
                    self.feed_token = self.api.getfeedToken()
                    self.is_connected = True
                    self.start_greeks_refresh_service()
                    logger.info("Successfully connected to broker.")
                    return True

                logger.error(f"Failed to connect: {data.get('message') if data else 'No response'}")
                self.api = None
                return False

            # verify session
            profile = self.api.getProfile(self.refresh_token)
            if profile and profile.get("status"):
                self.start_greeks_refresh_service()
                return True

            logger.warning("Session expired, reconnecting.")
            self.api = None
            return self.connect()

        except Exception as e:
            log_exception(e)
            self.api = None
            return False

    def _is_market_open(self, now=None):
        """Return True when current IST time is inside configured market hours."""
        ist = pytz.timezone("Asia/Kolkata")
        if now is None:
            now = datetime.now(ist)
        elif now.tzinfo is None:
            now = ist.localize(now)
        else:
            now = now.astimezone(ist)

        market_open = now.replace(
            hour=settings.MARKET_OPEN_HOUR,
            minute=settings.MARKET_OPEN_MINUTE,
            second=0,
            microsecond=0,
        )
        market_close = now.replace(
            hour=settings.MARKET_CLOSE_HOUR,
            minute=settings.MARKET_CLOSE_MINUTE,
            second=0,
            microsecond=0,
        )
        return market_open <= now <= market_close

    def start_greeks_refresh_service(self):
        """Keep the shared Greeks cache warm during market hours."""
        with self._greeks_service_lock:
            if self._greeks_refresh_thread and self._greeks_refresh_thread.is_alive():
                return

            self._greeks_refresh_stop.clear()
            self._greeks_refresh_thread = threading.Thread(
                target=self._greeks_refresh_loop,
                name="angelone-greeks-refresh",
                daemon=True,
            )
            self._greeks_refresh_thread.start()
            logger.info(
                "Started broker-owned Greeks refresh service "
                f"(interval={getattr(settings, 'GREEKS_REFRESH_INTERVAL', 10)}s)"
            )

    def _greeks_refresh_loop(self):
        """Refresh the shared Greeks cache on a fixed broker-owned cadence."""
        refresh_interval = max(1, int(getattr(settings, "GREEKS_REFRESH_INTERVAL", 10)))
        idle_interval = max(5, min(refresh_interval, 30))

        while not self._greeks_refresh_stop.is_set():
            wait_time = refresh_interval
            try:
                if self._is_market_open():
                    self.refresh_greeks(force=False)
                    wait_time = refresh_interval
                else:
                    wait_time = idle_interval
            except Exception as e:
                logger.error(f"Broker Greeks refresh service error: {e}")
                wait_time = max(1, min(refresh_interval, 5))

            self._greeks_refresh_stop.wait(wait_time)

    def stop_greeks_refresh_service(self):
        """Stop the background Greeks refresh loop."""
        with self._greeks_service_lock:
            self._greeks_refresh_stop.set()
            thread = self._greeks_refresh_thread
            self._greeks_refresh_thread = None

        if thread and thread.is_alive():
            thread.join(timeout=2)
            logger.info("Stopped broker-owned Greeks refresh service")

    def place_order(self, order_params, order_type="NORMAL"):
        """
        order_type:
          - NORMAL
          - BO (Bracket Order)
        """
        try:
            if not self.connect():
                return None

            resp = self.api.placeOrder(order_params)

            if isinstance(resp, dict) and resp.get("status"):
                return resp["data"]["orderid"]

            logger.error(f"Order rejected: {resp}")
            return None

        except Exception as e:
            log_exception(e)
            return None

    def order_book(self):
        try:
            if not self.connect():
                return None
            return self.api.orderBook()
        except Exception as e:
            log_exception(e)
            return None

    def get_ltp(self, exchange, tradingsymbol, symboltoken):
        try:
            if not self.connect():
                return None
            return self.api.ltpData(exchange, tradingsymbol, symboltoken)
        except Exception as e:
            log_exception(e)
            return None

    def historical_data(self, params):
        try:
            if not self.connect():
                return None
            return self.api.getCandleData(params)
        except Exception as e:
            log_exception(e)
            return None

    def _get_expiry_dates_from_scripmaster(self, scripmaster_data, num_expiries=2):
        """Extract nearest NIFTY option expiry dates from cached scripmaster data.

        Returns:
            list: Formatted expiry strings like ['10MAR2026', '13MAR2026']
        """
        from datetime import datetime as dt
        today = dt.now().date()
        expiry_dates = set()

        for item in scripmaster_data:
            if (item.get('exch_seg') == 'NFO' and
                item.get('instrumenttype') == 'OPTIDX' and
                item.get('name') == 'NIFTY'):

                expiry_raw = item.get('expiry', '')
                if not expiry_raw:
                    continue

                for fmt in ['%d-%b-%y', '%d%b%Y', '%d-%m-%Y']:
                    try:
                        expiry_date = dt.strptime(expiry_raw, fmt).date()
                        if expiry_date >= today:
                            expiry_dates.add(expiry_date)
                        break
                    except ValueError:
                        continue

        sorted_expiries = sorted(expiry_dates)[:num_expiries]
        result = [dt.combine(d, dt.min.time()).strftime('%d%b%Y').upper() for d in sorted_expiries]
        if result:
            logger.info(f"Expiry dates from scripmaster: {result}")
        return result

    def refresh_greeks(self, force=False):
        """Single robust Greeks fetch — the ONLY place that calls optionGreek API.

        Features:
        - Throttled: skips if fresh data available (< GREEKS_REFRESH_INTERVAL)
        - Rate limiting: 3 req/sec, 180 req/min
        - Retry with exponential backoff + jitter (3 attempts per expiry)
        - Uses cached scripmaster (no redundant 194K downloads)
        - Thread-safe: only one refresh runs at a time

        Returns:
            bool: True if Greeks data is available (fresh or cached)
        """
        now = time.time()

        # Throttle: skip if fresh data available
        if not force and self._shared_greeks_data:
            age = now - self._shared_greeks_time
            max_age = getattr(settings, 'GREEKS_REFRESH_INTERVAL', 60)
            if age < max_age:
                logger.debug(f"Greeks cache fresh ({age:.0f}s old), skipping refresh")
                return True

        # Thread safety — only one refresh at a time
        if not self._greeks_refresh_lock.acquire(blocking=False):
            logger.info("Greeks refresh already in progress, using existing cache")
            return bool(self._shared_greeks_data)

        try:
            if not self.connect():
                logger.error("Cannot refresh Greeks: not connected to broker")
                return bool(self._shared_greeks_data)

            # Get expiry dates from cached scripmaster (4h cache, no network hit)
            scripmaster = self._fetch_scripmaster()
            if not scripmaster:
                logger.error("Cannot refresh Greeks: scripmaster unavailable")
                return bool(self._shared_greeks_data)

            expiry_dates = self._get_expiry_dates_from_scripmaster(scripmaster)
            if not expiry_dates:
                logger.error("No valid expiry dates found in scripmaster")
                return bool(self._shared_greeks_data)

            # Fetch Greeks for each expiry with robust rate limiting + retry
            all_greeks = {}
            base_delay = 0.4   # 400ms minimum between requests (3/sec limit)
            max_retries = 3
            max_delay = 5.0
            fetch_start = time.time()

            for i, expiry_str in enumerate(expiry_dates):
                if i > 0:
                    time.sleep(base_delay)

                self._check_global_rate_limit()
                logger.info(f"Fetching Greeks for expiry: {expiry_str}")

                current_delay = base_delay
                success = False

                for attempt in range(max_retries):
                    try:
                        if attempt > 0:
                            jitter = random.uniform(0, 0.1 * current_delay)
                            sleep_time = min(current_delay + jitter, max_delay)
                            logger.info(f"  Retry {attempt+1}/{max_retries} for {expiry_str} after {sleep_time:.1f}s")
                            time.sleep(sleep_time)
                            current_delay *= 2

                        response = self.api.optionGreek({
                            "name": "NIFTY",
                            "expirydate": expiry_str
                        })
                        self._greeks_call_times.append(time.time())

                        if not response or not response.get('status'):
                            error_msg = response.get('message', 'Unknown') if response else 'No response'
                            if any(phrase in str(error_msg).lower() for phrase in ['rate limit', 'too many', 'exceeding access']):
                                logger.warning(f"  Rate limited on {expiry_str} (attempt {attempt+1})")
                                current_delay = min(current_delay * 2, max_delay)
                                continue
                            else:
                                logger.warning(f"  Greeks API error for {expiry_str}: {error_msg}")
                                break

                        # Process successful response — store in multiple key formats
                        items_count = 0
                        for item in response.get('data', []):
                            try:
                                strike = float(item.get('strikePrice', 0))
                                opt_type = item.get('optionType', '')
                                symbol = item.get('tradingsymbol') or item.get('symbol')

                                greek_vals = {
                                    'delta': item.get('delta'),
                                    'gamma': item.get('gamma'),
                                    'theta': item.get('theta'),
                                    'vega': item.get('vega'),
                                    'impliedVolatility': item.get('impliedVolatility') or item.get('iv'),
                                    'tradeVolume': item.get('tradeVolume'),
                                    'symbol': symbol,
                                    'strikePrice': strike,
                                    'optionType': opt_type,
                                    'expiry': expiry_str
                                }

                                # Multiple key formats for all consumers
                                all_greeks[f"NIFTY_{expiry_str}_{int(strike)}_{opt_type}"] = greek_vals
                                all_greeks[f"NIFTY_{expiry_str}_{strike:.2f}_{opt_type}"] = greek_vals
                                all_greeks[f"NIFTY_{expiry_str}_{strike*100:.2f}_{opt_type}"] = greek_vals
                                if symbol:
                                    all_greeks[symbol] = greek_vals
                                items_count += 1
                            except Exception:
                                continue

                        logger.info(f"  Fetched Greeks for {items_count} options (expiry {expiry_str})")
                        success = True
                        break

                    except Exception as e:
                        if 'exceeding access rate' in str(e).lower():
                            logger.warning(f"  Rate limited on {expiry_str} (attempt {attempt+1})")
                            current_delay = min(current_delay * 2, max_delay)
                        else:
                            logger.error(f"  Greeks API exception for {expiry_str}: {e}")
                            break

                if not success:
                    logger.warning(f"Failed to fetch Greeks for {expiry_str} after {max_retries} attempts")

            fetch_duration = time.time() - fetch_start

            if all_greeks:
                self._shared_greeks_data = all_greeks
                self._shared_greeks_time = time.time()
                # Invalidate options cache so next fetch_options_chain re-merges with new Greeks
                self._options_cache_time = 0
                logger.info(f"Greeks refreshed: {len(all_greeks)} entries for {len(expiry_dates)} expiries in {fetch_duration:.1f}s")
                return True
            else:
                logger.warning("No Greeks data fetched from API")
                return bool(self._shared_greeks_data)  # True if old cache exists

        finally:
            self._greeks_refresh_lock.release()

    def _check_global_rate_limit(self):
        """Enforce global Greeks API rate limits (per-second and per-minute)."""
        now = time.time()

        # Clean up old timestamps (older than 60s)
        self._greeks_call_times = [t for t in self._greeks_call_times if now - t < 60]

        # Per-second limit
        calls_last_second = sum(1 for t in self._greeks_call_times if now - t < 1)
        if calls_last_second >= settings.GREEKS_PER_SECOND_LIMIT:
            sleep_time = 1.0 - (now - min(t for t in self._greeks_call_times if now - t < 1))
            if sleep_time > 0:
                logger.info(f"Global per-second rate limit ({settings.GREEKS_PER_SECOND_LIMIT}/s), waiting {sleep_time:.2f}s")
                time.sleep(sleep_time)

        # Per-minute limit
        if len(self._greeks_call_times) >= settings.GREEKS_MINUTE_LIMIT:
            oldest_in_window = min(self._greeks_call_times)
            sleep_time = 60.0 - (now - oldest_in_window)
            if sleep_time > 0:
                logger.info(f"Global per-minute rate limit ({settings.GREEKS_MINUTE_LIMIT}/min), waiting {sleep_time:.2f}s")
                time.sleep(sleep_time)

    def _fetch_scripmaster(self):
        """Fetch and cache ScripMaster data (cached for 4 hours since it doesn't change during trading day)."""
        import requests

        now = time.time()
        if self._scripmaster_cache is not None and (now - self._scripmaster_cache_time) < 14400:
            logger.debug("Using cached ScripMaster data")
            return self._scripmaster_cache

        scripmaster_url = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
        response = requests.get(scripmaster_url, timeout=30)
        if response.status_code != 200:
            logger.error(f"Failed to fetch ScripMaster: HTTP {response.status_code}")
            return self._scripmaster_cache  # return stale cache if available

        self._scripmaster_cache = response.json()
        self._scripmaster_cache_time = now
        logger.info("ScripMaster data fetched and cached")
        return self._scripmaster_cache

    def fetch_options_chain(self, current_price, fetch_greeks=True):
        """
        Fetch NIFTY options chain merged with the shared Greeks cache.
        Thread-safe with shared caching across all strategies.

        This method does NOT trigger a Greeks refresh — the background refresh
        service (start_greeks_refresh_service) keeps _shared_greeks_data warm.
        Strategies call this to read the latest cached snapshot.

        Args:
            current_price: Current NIFTY spot price
            fetch_greeks: Whether to merge Greeks from shared cache (default True)

        Returns:
            pandas.DataFrame with columns matching bullish3t.py format
        """
        with self._fetch_lock:
            return self._fetch_options_chain_locked(current_price, fetch_greeks)

    def _fetch_options_chain_locked(self, current_price, fetch_greeks=True):
        """Internal method that does the actual fetching (must be called under _fetch_lock)."""
        try:
            import pandas as pd
            from datetime import datetime
            import re

            # Check cache: return cached data if fresh and price hasn't moved significantly
            now = time.time()
            cache_ttl = getattr(settings, 'GREEKS_CACHE_DURATION', 60)
            if (self._options_cache is not None and
                (now - self._options_cache_time) < cache_ttl and
                self._options_cache_price is not None and
                abs(current_price - self._options_cache_price) < 200):
                logger.info(f"Using cached options data (age: {now - self._options_cache_time:.0f}s, "
                           f"price drift: {abs(current_price - self._options_cache_price):.1f} pts)")
                return self._options_cache.copy()

            if not self.connect():
                logger.error("Cannot fetch options chain: Not connected to broker")
                return None

            logger.info(f"Fetching NIFTY options chain for current price: {current_price:.2f}")

            # Fetch ScripMaster (cached for 4 hours)
            scripmaster_data = self._fetch_scripmaster()
            if scripmaster_data is None:
                return None

            # Filter for NIFTY options
            nifty_options = []
            for instrument in scripmaster_data:
                if (instrument.get('name') == 'NIFTY' and
                    instrument.get('instrumenttype') in ['OPTIDX'] and
                    instrument.get('exch_seg') == 'NFO'):

                    try:
                        # Extract and normalize strike (handle paise format)
                        strike_val = float(instrument.get('strike', 0))
                        strike_float = strike_val / 100 if strike_val > 100000 else strike_val

                        # Extract option type from symbol
                        symbol = instrument.get('symbol', '')
                        match = re.search(r'(CE|PE)$', symbol)
                        option_type = match.group(1) if match else None

                        if not option_type:
                            continue

                        # Parse expiry date
                        expiry_raw = instrument.get('expiry', '')
                        try:
                            for fmt in ["%d-%b-%y", "%d%b%Y", "%d-%m-%Y"]:
                                try:
                                    expiry_date = datetime.strptime(expiry_raw, fmt)
                                    break
                                except:
                                    continue
                            else:
                                continue
                        except:
                            continue

                        # Format expiry for API (matching bullish3t.py)
                        expiry_formatted = expiry_date.strftime("%d%b%Y").upper()

                        # Create match_key for Greek merging (matching bullish3t.py)
                        match_key = f"NIFTY_{expiry_formatted}_{int(strike_float)}_{option_type}"

                        nifty_options.append({
                            'token': instrument.get('token'),
                            'symbol': symbol,
                            'name': 'NIFTY',
                            'expiry': expiry_raw,
                            'strike': instrument.get('strike'),
                            'lotsize': instrument.get('lotsize', 75),
                            'instrumenttype': 'OPTIDX',
                            'exch_seg': 'NFO',
                            'tick_size': instrument.get('tick_size', 0.05),
                            'option_type': option_type,
                            'expiry_date': expiry_date,
                            'expiry_formatted': expiry_formatted,
                            'expiry_smartapi': expiry_formatted,
                            'strike_float': strike_float,
                            'match_key': match_key,
                            # Greeks placeholder
                            'delta': None,
                            'gamma': None,
                            'theta': None,
                            'vega': None,
                            'impliedVolatility': None,
                            'tradeVolume': None
                        })
                    except (ValueError, TypeError) as e:
                        continue

            if not nifty_options:
                logger.warning("No NIFTY options found in ScripMaster")
                return None

            options_df = pd.DataFrame(nifty_options)

            # Sort by expiry and strike
            options_df = options_df.sort_values(['expiry_date', 'strike_float'])

            logger.info(f"Fetched {len(options_df)} NIFTY options from ScripMaster")

            # Merge Greeks from single cache (populated by refresh_greeks())
            if fetch_greeks:
                if self._shared_greeks_data:
                    greeks_age = time.time() - self._shared_greeks_time
                    logger.info(f"Merging Greeks from cache ({len(self._shared_greeks_data)} entries, {greeks_age:.0f}s old)")
                    merged_count = 0
                    for idx, row in options_df.iterrows():
                        match_key = row['match_key']  # e.g. NIFTY_10MAR2026_22750_PE
                        strike = row['strike_float']
                        expiry_fmt = row['expiry_formatted']
                        opt_type = row['option_type']

                        # Try multiple key formats
                        keys_to_try = [
                            match_key,                                                          # NIFTY_10MAR2026_22750_PE
                            f"NIFTY_{expiry_fmt}_{strike:.2f}_{opt_type}",                     # NIFTY_10MAR2026_22750.00_PE
                            f"NIFTY_{expiry_fmt}_{strike*100:.2f}_{opt_type}",                 # paise key
                        ]

                        for key in keys_to_try:
                            if key in self._shared_greeks_data:
                                greek_vals = self._shared_greeks_data[key]
                                for field in ['delta', 'gamma', 'theta', 'vega', 'impliedVolatility', 'tradeVolume']:
                                    options_df.at[idx, field] = greek_vals.get(field)
                                merged_count += 1
                                break

                    greeks_found = options_df['delta'].notna().sum()
                    logger.info(f"Merged {greeks_found} Greeks (matched {merged_count} keys)")
                else:
                    logger.warning("No Greeks available - call refresh_greeks() to fetch")

            logger.info(f"   Strikes range: {options_df['strike_float'].min():.0f} - {options_df['strike_float'].max():.0f}")
            logger.info(f"   Expiries: {options_df['expiry_formatted'].unique()[:3].tolist()}")

            # Update cache - use short TTL if Greeks are missing so we retry sooner
            greeks_present = options_df['delta'].notna().any()
            self._options_cache = options_df.copy()
            self._options_cache_time = time.time() if greeks_present else (time.time() - max(0, getattr(settings, 'GREEKS_CACHE_DURATION', 60) - 5))
            self._options_cache_price = current_price
            if not greeks_present:
                logger.warning("Caching options data WITHOUT Greeks (cache will expire in ~5s to allow retry)")

            return options_df

        except Exception as e:
            logger.error(f"Error fetching options chain: {e}")
            log_exception(e)
            return None

    def shutdown(self):
        """Release background resources owned by the broker."""
        self.stop_greeks_refresh_service()

    def start_websocket(self, token_list, on_data_callback):
        """
        token_list example:
          [{"exchangeType":1,"tokens":["26000"]}, {"exchangeType":2,"tokens":["37054"]}]
        """
        if not self.connect():
            return None

        try:
            ws = SmartWebSocketV2(
                self.refresh_token,
                settings.API_KEY,
                settings.USERNAME,
                self.feed_token
            )

            def on_open(wsapp):
                logger.info("WebSocket connection opened")
                ws.subscribe("nifty_data_tracker", 1, token_list)

            def on_data(wsapp, message):
                on_data_callback(message)

            def on_error(wsapp, error):
                logger.error(f"WebSocket error: {error}")

            def on_close(wsapp, code=None, reason=None):
                logger.info(f"WebSocket closed: {code} - {reason}")

            ws.on_open = on_open
            ws.on_data = on_data
            ws.on_error = on_error
            ws.on_close = on_close

            threading.Thread(target=ws.connect, daemon=True).start()
            self.websocket = ws
            return ws

        except Exception as e:
            log_exception(e)
            return None
