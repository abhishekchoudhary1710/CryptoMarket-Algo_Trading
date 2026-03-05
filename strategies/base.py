"""
Abstract base class for all trading strategies.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, Optional

import pandas as pd


class BaseStrategy(ABC):
    """
    Every strategy must implement ``next_signal()``.

    A signal is a dict with at least::

        {"side": "buy"|"sell", "entry": float, "sl": float, "tp": float, "reason": str}

    Return *None* when there is no actionable signal.
    """

    @abstractmethod
    def next_signal(self, df: pd.DataFrame) -> Optional[Dict[str, float]]:
        ...
