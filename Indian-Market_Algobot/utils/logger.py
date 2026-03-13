"""
Logging utility for the AlgoTrading application.
Provides centralized logging configuration and functions.
"""
import os
import logging
from logzero import setup_logger, LogFormatter
from datetime import datetime
from config import settings

# Create custom formatter - matching bullish3t1.py format (YYMMDD HH:MM:SS)
formatter = LogFormatter(
    fmt='%(color)s[%(levelname)1.1s %(asctime)s %(module)s:%(lineno)d]%(end_color)s %(message)s',
    datefmt='%y%m%d %H:%M:%S'
)

# Setup root logger
def get_logger(name=None, logfile=None):
    """
    Get a configured logger instance.

    Args:
        name (str, optional): Name of the logger. Defaults to None for root logger.
        logfile (str, optional): Path to log file. Defaults to settings.LOG_FILE.

    Returns:
        Logger: Configured logger instance
    """
    log_level = getattr(logging, settings.LOG_LEVEL)

    # Use provided logfile or default
    log_path = logfile if logfile else settings.LOG_FILE

    logger = setup_logger(
        name=name,
        level=log_level,
        formatter=formatter,
        logfile=log_path,
        maxBytes=10e6,  # 10MB
        backupCount=5,
        disableStderrLogger=False
    )
    return logger

def get_strategy_logger(strategy_name):
    """
    Get a strategy-specific logger with its own log file.

    Args:
        strategy_name (str): Name of the strategy (e.g., 'bullish', 'bearish')

    Returns:
        Logger: Configured logger instance for the strategy
    """
    log_dir = settings.LOGS_DIR
    logfile = str(log_dir / f"{strategy_name}_strategy.log")
    return get_logger(f"strategy.{strategy_name}", logfile=logfile)

# Create root logger
logger = get_logger("algotrading")

def log_exception(exc):
    """
    Log an exception with traceback and additional details.
    
    Args:
        exc (Exception): The exception to log
    """
    logger.exception(f"Exception occurred: {str(exc)}")
    
def log_order(order_data, status="INFO"):
    """
    Log order information in a standardized format.
    
    Args:
        order_data (dict): Order details
        status (str): Status level (INFO, WARNING, ERROR)
    """
    log_method = getattr(logger, status.lower())
    
    order_id = order_data.get('order_id', 'NA')
    symbol = order_data.get('symbol', 'NA')
    quantity = order_data.get('quantity', 0)
    order_type = order_data.get('order_type', 'NA')
    
    message = f"ORDER [{order_id}]: {symbol} x{quantity} {order_type}"
    
    if 'stop_loss' in order_data and 'target' in order_data:
        sl = order_data.get('stop_loss', 'NA')
        target = order_data.get('target', 'NA')
        message += f" | SL: {sl} | Target: {target}"
        
    if 'status' in order_data:
        message += f" | Status: {order_data['status']}"
        
    log_method(message)