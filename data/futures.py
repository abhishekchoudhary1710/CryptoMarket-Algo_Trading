"""
Futures token resolvers for Angel ScripMaster instruments.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Optional

from config import settings
from utils.logger import logger


def _parse_expiry(expiry_raw: str | None) -> Optional[datetime]:
    if not expiry_raw:
        return None
    for fmt in ("%d-%b-%y", "%d%b%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(expiry_raw, fmt)
        except ValueError:
            continue
    return None


def resolve_gold_mcx_futures_token(
    scripmaster_data: Iterable[dict[str, Any]],
    exchange_segment: str | None = None,
    instrument_type: str | None = None,
    name_filter: str | None = None,
    symbol_contains: str | None = None,
    exclude_keywords: Iterable[str] | None = None,
) -> Optional[dict[str, Any]]:
    """
    Resolve nearest-expiry GOLD MCX futures contract.

    Returns:
        dict with token + symbol metadata, or None.
    """
    exch_seg = (exchange_segment or settings.MCX_EXCHANGE_SEGMENT).upper()
    inst_type = (instrument_type or settings.MCX_INSTRUMENT_TYPE).upper()
    name = (name_filter or settings.MCX_NAME).upper()
    symbol_hint = (symbol_contains or settings.MCX_TRADINGSYMBOL_CONTAINS).upper()
    exclude = {word.upper() for word in (exclude_keywords or settings.MCX_EXCLUDE_KEYWORDS)}
    today = datetime.now().date()
    candidates: list[dict[str, Any]] = []

    for item in scripmaster_data:
        try:
            item_exch = str(item.get("exch_seg", "")).upper()
            item_inst = str(item.get("instrumenttype", "")).upper()
            item_name = str(item.get("name", "")).upper()
            symbol = str(item.get("symbol", "")).upper()

            if item_exch != exch_seg:
                continue
            if item_inst != inst_type:
                continue
            if name and name not in item_name:
                continue
            if symbol_hint and symbol_hint not in symbol:
                continue
            if exclude and any(word in symbol for word in exclude):
                continue

            expiry_dt = _parse_expiry(item.get("expiry"))
            if expiry_dt is None:
                continue
            if expiry_dt.date() < today:
                continue

            token = str(item.get("token", "")).strip()
            if not token:
                continue

            candidates.append(
                {
                    "token": token,
                    "symbol": item.get("symbol"),
                    "name": item.get("name"),
                    "exchange_segment": item.get("exch_seg"),
                    "instrument_type": item.get("instrumenttype"),
                    "expiry": expiry_dt,
                    "raw": item,
                }
            )
        except Exception:
            continue

    if not candidates:
        logger.error(
            "No GOLD MCX futures token found for exch=%s instrument=%s name~%s symbol~%s",
            exch_seg,
            inst_type,
            name,
            symbol_hint,
        )
        return None

    candidates.sort(key=lambda row: row["expiry"])

    # If today is expiry day, prefer next contract when available.
    selected = candidates[0]
    if selected["expiry"].date() == today and len(candidates) > 1:
        selected = candidates[1]

    logger.info(
        "Selected GOLD MCX futures token=%s symbol=%s expiry=%s",
        selected["token"],
        selected["symbol"],
        selected["expiry"].date(),
    )
    return selected
