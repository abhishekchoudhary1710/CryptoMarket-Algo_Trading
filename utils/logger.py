"""
Centralised logging configuration.

Call ``setup_logging()`` once at startup.  Every module should then use::

    import logging
    log = logging.getLogger(__name__)
"""

from __future__ import annotations

import logging
from pathlib import Path

_CONFIGURED = False


def setup_logging(log_dir: str = "logs", level: int = logging.INFO) -> None:
    """Configure root logger with file + console handlers (idempotent)."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    Path(log_dir).mkdir(exist_ok=True)

    fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")

    file_handler = logging.FileHandler(
        f"{log_dir}/trading_bot.log", encoding="utf-8"
    )
    file_handler.setFormatter(fmt)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)

    root = logging.getLogger()
    root.setLevel(level)
    root.addHandler(file_handler)
    root.addHandler(console_handler)

    _CONFIGURED = True
