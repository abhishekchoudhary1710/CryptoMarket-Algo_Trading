"""
Centralised logging helpers used by both crypto and Indian-market adapters.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

_CONFIGURED = False
_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
logger = logging.getLogger("trading_bot")


def _resolve_level(level: int | str) -> int:
    if isinstance(level, str):
        return getattr(logging, level.upper(), logging.INFO)
    return level


def setup_logging(log_dir: str = "logs", level: int | str = logging.INFO) -> None:
    """Configure root logger with file + console handlers (idempotent)."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    resolved_level = _resolve_level(level)
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter(_FORMAT)

    file_handler = logging.FileHandler(f"{log_dir}/trading_bot.log", encoding="utf-8")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(resolved_level)
    root.addHandler(file_handler)
    root.addHandler(console_handler)

    _CONFIGURED = True


def get_logger(name: str | None = None) -> logging.Logger:
    if not _CONFIGURED:
        setup_logging()
    return logging.getLogger(name) if name else logger


def get_strategy_logger(strategy_name: str) -> logging.Logger:
    """Return a dedicated logger for a strategy with its own file."""
    if not _CONFIGURED:
        setup_logging()

    strategy_logger = logging.getLogger(f"strategy.{strategy_name}")
    strategy_logger.setLevel(logging.getLogger().level)
    strategy_logger.propagate = True

    target_filename = f"{strategy_name}_strategy.log"
    for handler in strategy_logger.handlers:
        if isinstance(handler, logging.FileHandler) and handler.baseFilename.endswith(target_filename):
            return strategy_logger

    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(log_dir / target_filename, encoding="utf-8")
    handler.setFormatter(logging.Formatter(_FORMAT))
    strategy_logger.addHandler(handler)
    return strategy_logger


def log_exception(exc: Exception) -> None:
    get_logger().exception("Exception occurred: %s", exc)


def log_order(order_data: dict[str, Any], status: str = "INFO") -> None:
    log_method = getattr(get_logger(), status.lower(), get_logger().info)
    order_id = order_data.get("order_id", "NA")
    symbol = order_data.get("symbol", "NA")
    qty = order_data.get("quantity", 0)
    order_type = order_data.get("order_type", "NA")
    log_method("ORDER [%s]: %s x%s %s", order_id, symbol, qty, order_type)
