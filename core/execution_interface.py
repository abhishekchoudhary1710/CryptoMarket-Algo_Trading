"""Execution interfaces for live or paper routing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from utils.logger import logger


@dataclass
class OrderResult:
    status: str
    order_id: str | None
    message: str
    timestamp: datetime


class ExecutionClient(Protocol):
    def place_order(self, side: str, quantity: float, sl: float | None, tp: float | None) -> OrderResult:
        ...

    def cancel_order(self, order_id: str) -> OrderResult:
        ...

    def get_order_status(self, order_id: str) -> OrderResult:
        ...


class NoOpExecutionClient:
    """Phase-1 execution stub; logs calls but does not place orders."""

    def place_order(self, side: str, quantity: float, sl: float | None, tp: float | None) -> OrderResult:
        logger.info(
            "[NO-OP EXECUTION] place_order side=%s qty=%s sl=%s tp=%s",
            side,
            quantity,
            sl,
            tp,
        )
        return OrderResult(
            status="noop",
            order_id=None,
            message="Execution disabled in phase-1 (signals only)",
            timestamp=datetime.now(),
        )

    def cancel_order(self, order_id: str) -> OrderResult:
        logger.info("[NO-OP EXECUTION] cancel_order id=%s", order_id)
        return OrderResult(
            status="noop",
            order_id=order_id,
            message="Execution disabled in phase-1 (signals only)",
            timestamp=datetime.now(),
        )

    def get_order_status(self, order_id: str) -> OrderResult:
        logger.info("[NO-OP EXECUTION] get_order_status id=%s", order_id)
        return OrderResult(
            status="noop",
            order_id=order_id,
            message="Execution disabled in phase-1 (signals only)",
            timestamp=datetime.now(),
        )
