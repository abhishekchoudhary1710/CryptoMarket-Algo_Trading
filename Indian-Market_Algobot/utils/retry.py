# utils/retry.py
"""
Lightweight retry utility to keep retries out of business logic.
"""
import time
from utils.logger import logger

def retry(fn, retries=3, delay=2, backoff=2, exceptions=(Exception,), name="operation"):
    last_exc = None
    cur_delay = delay
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except exceptions as e:
            last_exc = e
            logger.warning(f"{name} failed (attempt {attempt}/{retries}): {e}")
            if attempt < retries:
                time.sleep(cur_delay)
                cur_delay *= backoff
    logger.error(f"{name} failed after {retries} attempts: {last_exc}")
    return None
