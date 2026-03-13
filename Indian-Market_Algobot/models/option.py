"""
Option model module.
Provides classes and functions for working with option data.
"""
import pandas as pd
import numpy as np
import time
import random
from datetime import datetime
from utils.logger import logger
from config import settings

class OptionData:
    """Class representing option data with Greeks and calculations."""
    
    def __init__(self, data=None):
        """
        Initialize option data.
        
        Args:
            data (dict, optional): Option data dictionary
        """
        self.symbol = data.get('symbol', '')
        self.token = data.get('token', '')
        self.strike = float(data.get('strike_float', 0))
        self.option_type = data.get('option_type', '')  
        self.expiry = data.get('expiry_date') if data.get('expiry_date') else data.get('expiry', '')
        self.lot_size = int(data.get('lotsize', settings.DEFAULT_LOT_SIZE))
        
        self.delta = float(data.get('delta', 0) or 0)
        self.gamma = float(data.get('gamma', 0) or 0)
        self.theta = float(data.get('theta', 0) or 0)
        self.vega = float(data.get('vega', 0) or 0)
        self.implied_volatility = float(data.get('impliedVolatility', 0) or 0)
        
        self.last_price = float(data.get('last_price', 0) or 0)
        self.bid_price = float(data.get('bid_price', 0) or 0)
        self.ask_price = float(data.get('ask_price', 0) or 0)
        self.volume = int(data.get('volume', 0) or 0)
        self.open_interest = int(data.get('open_interest', 0) or 0)
        
        self.stop_loss = None
        self.target = None
        self.risk_per_lot = None
        
    def __str__(self):
        """String representation of the option."""
        return f"{self.symbol} {self.strike} {self.option_type} {self.expiry}"
        
    def to_dict(self):
        """Convert to dictionary representation."""
        return {
            'symbol': self.symbol,
            'token': self.token,
            'strike': self.strike,
            'option_type': self.option_type,
            'expiry': self.expiry,
            'lot_size': self.lot_size,
            'delta': self.delta,
            'gamma': self.gamma,
            'theta': self.theta,
            'vega': self.vega,
            'implied_volatility': self.implied_volatility,
            'last_price': self.last_price,
            'bid_price': self.bid_price,
            'ask_price': self.ask_price,
            'volume': self.volume,
            'open_interest': self.open_interest,
            'stop_loss': self.stop_loss,
            'target': self.target,
            'risk_per_lot': self.risk_per_lot
        }
        
    def calculate_stop_loss(self, underlying_entry, underlying_sl):
        """
        Calculate comprehensive option-specific stop loss using Greeks and underlying movement.

        Uses Greeks to calculate expected option price change:
        - Delta Impact: delta × underlying_move (first-order price change)
        - Gamma Impact: 0.5 × gamma × underlying_move² (second-order price change)
        - Theta Impact: Daily theta decay adjusted for 10 minutes

        Args:
            underlying_entry (float): Entry price of the underlying
            underlying_sl (float): Stop loss price of the underlying

        Returns:
            dict: Stop loss calculation details with price_change, total_sl, and components
        """
        try:
            # Ensure numeric values
            underlying_entry = float(underlying_entry)
            underlying_sl = float(underlying_sl)

            # Calculate underlying move to SL
            underlying_move = abs(underlying_entry - underlying_sl)

            # Calculate expected option price change components
            delta_impact = self.delta * underlying_move  # First-order price change
            gamma_impact = 0.5 * self.gamma * (underlying_move ** 2)  # Second-order price change

            # Theta is typically expressed as daily decay, convert to 10-minute impact
            # Daily theta / (24 hours × 6 ten-minute periods per hour)
            theta_impact = (abs(self.theta) / (24 * 6)) * (10/60)  # Theta impact for exactly 10 minutes

            # Calculate total expected price change
            total_price_change = delta_impact + gamma_impact + theta_impact

            # Store risk per lot for position sizing
            self.risk_per_lot = total_price_change

            # Calculate stop loss and target prices if last_price is available
            if self.last_price > 0:
                self.stop_loss = max(self.last_price - total_price_change, 0.1)

                # Calculate target with 1:2 risk-reward ratio
                risk = self.last_price - self.stop_loss
                self.target = self.last_price + (2 * risk)

            return {
                'price_change': total_price_change,
                'total_sl': total_price_change,
                'components': {
                    'delta_impact': delta_impact,
                    'gamma_impact': gamma_impact,
                    'theta_impact': theta_impact
                }
            }
        except Exception as e:
            logger.error(f"Error calculating option stop loss: {e}")
            return None
            
    @staticmethod
    def select_optimal_strike(options_data, current_price, target_risk_range=(800, 900), lot_size=75):
        """
        Select the optimal strike based on risk parameters.
        
        Args:
            options_data (pd.DataFrame): DataFrame containing options data
            current_price (float): Current price of the underlying
            target_risk_range (tuple): Target risk range in rupees (min, max)
            lot_size (int): Standard lot size
            
        Returns:
            tuple: (selected_option, quantity, total_risk)
        """
        if options_data is None or options_data.empty:
            logger.error("No options data available for strike selection")
            return None, None, None
            
        target_min, target_max = target_risk_range
        target_mid = (target_min + target_max) / 2
        
       
        options = []
        for _, row in options_data.iterrows():
            option = OptionData(row.to_dict())
            options.append(option)
            
        
        valid_options = []
        for option in options:
            if option.calculate_stop_loss(current_price, current_price * 0.995): 
                valid_options.append(option)
                
        if not valid_options:
            logger.warning("No valid options with stop loss calculation")
            return None, None, None
            
       
        options_in_range = []
        for option in valid_options:
          
            for lots in range(1, 51): 
                quantity = lots * lot_size
                total_risk = option.risk_per_lot * quantity
                
                if target_min <= total_risk <= target_max:
                    options_in_range.append((option, quantity, total_risk, abs(total_risk - target_mid)))
                    
               
                if total_risk > target_max:
                    break
                    
        if options_in_range:

            options_in_range.sort(key=lambda x: x[3])
            return options_in_range[0][:3]
        else:

            valid_options.sort(key=lambda opt: abs(opt.risk_per_lot * lot_size - target_mid))
            closest_option = valid_options[0]
            return closest_option, lot_size, closest_option.risk_per_lot * lot_size


def get_option_greeks(smart_api, expiry_dates):
    """
    Get option Greek values for NIFTY with multiple expiry dates.

    Implements strict rate limiting:
    - Daily: 3000 requests max
    - Per minute: 180 requests
    - Per second: 3 requests (0.334s minimum interval)
    - Exponential backoff with jitter on errors

    Args:
        smart_api: SmartAPI connection instance
        expiry_dates (list): List of expiry date dictionaries with 'formatted' key

    Returns:
        dict: Dictionary with Greek values keyed by multiple formats
              (match_key, paise_key, symbol)
    """
    all_greek_data = {}

    # Rate limit parameters
    base_delay = 0.4  # Minimum 400ms between requests (to stay under 3 requests/sec)
    max_retries = 3  # Maximum number of retries per expiry
    max_delay = 5  # Maximum delay in seconds
    minute_request_count = 0
    minute_start_time = time.time()

    for i, expiry_info in enumerate(expiry_dates):
        expiry_date = expiry_info['formatted']
        logger.info(f"\nFetching option Greeks for expiry date: {expiry_date}")
        greek_param = {
            "name": "NIFTY",
            "expirydate": expiry_date
        }

        # Check minute-based rate limit (180 requests per minute)
        current_time = time.time()
        if current_time - minute_start_time >= 60:
            # Reset minute counter if a minute has passed
            minute_request_count = 0
            minute_start_time = current_time
        elif minute_request_count >= 180:
            # Wait for the remainder of the minute if we've hit the limit
            sleep_time = 60 - (current_time - minute_start_time)
            if sleep_time > 0:
                logger.info(f"Reached minute rate limit, waiting {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
                minute_request_count = 0
                minute_start_time = time.time()

        # Add initial delay between API calls to respect per-second rate limit
        if i > 0:
            time.sleep(base_delay)

        success = False
        current_delay = base_delay

        for retry in range(max_retries):
            try:
                if retry > 0:
                    # Exponential backoff with jitter
                    jitter = random.uniform(0, 0.1 * current_delay)
                    sleep_time = min(current_delay + jitter, max_delay)
                    logger.info(f"Retry {retry + 1}/{max_retries} after {sleep_time:.2f}s delay...")
                    time.sleep(sleep_time)
                    current_delay *= 2  # Exponential backoff

                greek_res = smart_api.optionGreek(greek_param)
                minute_request_count += 1  # Increment request counter

                if not greek_res.get('status'):
                    error_msg = greek_res.get('message', 'Unknown error')

                    # Check for specific rate limit errors
                    if any(phrase in str(error_msg).lower() for phrase in ['rate limit', 'too many requests', 'exceeding access']):
                        if retry < max_retries - 1:
                            logger.warning(f"Rate limit hit. Backing off...")
                            # Double the delay on rate limit errors
                            current_delay = min(current_delay * 2, max_delay)
                            continue
                        else:
                            logger.error(f"Max retries reached for rate limit. Skipping expiry {expiry_date}")
                            break
                    else:
                        logger.error(f"Non-rate-limit error: {error_msg}")
                        break

                # Process successful response
                if 'data' in greek_res:
                    greek_data = greek_res['data']
                    greek_dict = {}

                    for item in greek_data:
                        try:
                            name = item.get('name')
                            expiry = item.get('expiry')
                            strike_price = item.get('strikePrice')
                            option_type = item.get('optionType')

                            if not all([name, expiry, strike_price, option_type]):
                                continue

                            try:
                                strike_float = float(strike_price)
                                match_key = f"{name}_{expiry}_{strike_float:.2f}_{option_type}"
                                paise_key = f"{name}_{expiry}_{strike_float*100:.2f}_{option_type}"
                            except (ValueError, TypeError):
                                continue

                            symbol = item.get('tradingsymbol') or item.get('symbol')

                            greek_values = {
                                'delta': item.get('delta'),
                                'gamma': item.get('gamma'),
                                'theta': item.get('theta'),
                                'vega': item.get('vega'),
                                'impliedVolatility': item.get('impliedVolatility'),
                                'tradeVolume': item.get('tradeVolume'),
                                'symbol': symbol,
                                'strikePrice': strike_float,
                                'optionType': option_type,
                                'expiry': expiry
                            }

                            greek_dict[match_key] = greek_values
                            greek_dict[paise_key] = greek_values.copy()
                            if symbol:
                                greek_dict[symbol] = greek_values.copy()

                        except Exception as e:
                            logger.error(f"Error processing Greek data item: {e}")
                            continue

                    all_greek_data.update(greek_dict)
                    success = True
                    logger.info(f"Successfully fetched Greeks for expiry {expiry_date}")
                    break  # Exit retry loop on success

            except Exception as e:
                logger.error(f"Exception when fetching Greek data for {expiry_date}: {e}")
                if retry < max_retries - 1:
                    continue
                break

        if not success:
            logger.warning(f"Failed to fetch Greeks for expiry {expiry_date} after {max_retries} attempts")

    # Print summary of all Greek data
    logger.info(f"\nTotal Greek records across all expiries: {len(all_greek_data)}")
    return all_greek_data


def merge_options_with_greeks(options_df, greek_dict):
    """
    Merge options DataFrame with Greek values using multiple matching strategies.

    Matching strategies (in order):
    1. Direct symbol match
    2. Precomputed match_key
    3. Constructed key on-the-fly
    4. Fuzzy strike matching (±0.05, ±0.1, ±1, ±5)

    Args:
        options_df (pd.DataFrame): DataFrame containing options data
        greek_dict (dict): Dictionary of Greek values from get_option_greeks()

    Returns:
        pd.DataFrame: DataFrame with merged Greeks columns
    """
    logger.info("\nMerging option data with Greek values...")

    # Initialize Greek columns with None
    greek_columns = ['delta', 'gamma', 'theta', 'vega', 'impliedVolatility', 'tradeVolume']
    for col in greek_columns:
        options_df[col] = None

    # If we have no Greek data (possibly due to rate limiting), return the dataframe with empty Greek columns
    if not greek_dict:
        logger.warning("No Greek data available for merging due to rate limiting or other errors.")
        logger.warning("Continuing with options data that has empty Greek columns.")
        return options_df

    # Track matches for diagnostics
    match_count = 0
    match_methods = {
        'direct_symbol': 0,
        'match_key': 0,
        'constructed_key': 0,
        'strike_option_type': 0
    }

    # Create a mapping of (expiry, strike, option_type) for fuzzy matching
    strike_option_map = {}
    for key, data in greek_dict.items():
        if isinstance(data, dict) and 'strikePrice' in data and 'optionType' in data and 'expiry' in data:
            expiry = data['expiry']
            strike = data['strikePrice']
            opt_type = data['optionType']
            strike_option_map[(expiry, strike, opt_type)] = data

    # Apply matching strategies for each option
    for idx, row in options_df.iterrows():
        match_found = False

        # Strategy 1: Direct symbol match
        symbol = row.get('symbol')
        if symbol in greek_dict:
            for col in greek_columns:
                options_df.at[idx, col] = greek_dict[symbol].get(col)
            match_count += 1
            match_methods['direct_symbol'] += 1
            match_found = True
            continue

        # Strategy 2: Match using precomputed match_key
        match_key = row.get('match_key')
        if match_key and match_key in greek_dict:
            for col in greek_columns:
                options_df.at[idx, col] = greek_dict[match_key].get(col)
            match_count += 1
            match_methods['match_key'] += 1
            match_found = True
            continue

        # Strategy 3: Construct a key on the fly
        try:
            if all([row.get('name'), row.get('expiry_smartapi'),
                   row.get('strike_float'), row.get('option_type')]):

                constructed_key = f"{row['name']}_{row['expiry_smartapi']}_{float(row['strike_float']):.2f}_{row['option_type']}"

                if constructed_key in greek_dict:
                    for col in greek_columns:
                        options_df.at[idx, col] = greek_dict[constructed_key].get(col)
                    match_count += 1
                    match_methods['constructed_key'] += 1
                    match_found = True
                    continue
        except Exception as e:
            pass  # Quietly continue to next strategy

        # Strategy 4: Match by expiry, strike price and option type directly
        try:
            expiry = row.get('expiry_smartapi')
            strike_float = row.get('strike_float')  # Use normalized strike_float
            if not strike_float and 'strike' in row:
                # Try to normalize strike on the fly if needed
                raw_strike = float(row.get('strike'))
                if raw_strike > 100000:  # Threshold to detect paise format
                    strike_float = raw_strike / 100
                else:
                    strike_float = raw_strike

            opt_type = row.get('option_type')

            if expiry and strike_float and opt_type and (expiry, strike_float, opt_type) in strike_option_map:
                greek_data = strike_option_map[(expiry, strike_float, opt_type)]
                for col in greek_columns:
                    options_df.at[idx, col] = greek_data.get(col)
                match_count += 1
                match_methods['strike_option_type'] += 1
                match_found = True
                continue

            # Try with adjusted strike value - checking nearby values
            # This handles potential rounding issues between data sources
            if expiry and strike_float and opt_type:
                for adj in [-0.05, 0.05, -0.1, 0.1, -1, 1, -5, 5]:
                    adj_strike = round(strike_float + adj, 2)
                    if (expiry, adj_strike, opt_type) in strike_option_map:
                        greek_data = strike_option_map[(expiry, adj_strike, opt_type)]
                        for col in greek_columns:
                            options_df.at[idx, col] = greek_data.get(col)
                        match_count += 1
                        match_methods['strike_option_type'] += 1
                        match_found = True
                        break
        except Exception as e:
            pass  # Quietly continue

    # Print match statistics
    logger.info(f"Total matches: {match_count} out of {len(options_df)} options")
    logger.info(f"Match methods used: {match_methods}")

    # Match rate by expiry
    expiry_stats = options_df.groupby('expiry_date').agg(
        total=('symbol', 'count'),
        matched=('delta', lambda x: x.notnull().sum())
    )
    expiry_stats['match_rate'] = (expiry_stats['matched'] / expiry_stats['total'] * 100).round(2)

    logger.info("\nMatch rates by expiry date:")
    logger.info(f"{expiry_stats}")

    return options_df
