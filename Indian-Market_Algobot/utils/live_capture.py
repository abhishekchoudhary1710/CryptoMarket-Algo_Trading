"""
Live tick and entry capture utilities.
Writes CSV files under outputs/data/raw_ticks for easy inspection.
"""

import csv
from datetime import datetime
from config import settings


def _append_csv_row(path, fieldnames, row):
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()
    with path.open("a", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def _format_number(value):
    if value is None:
        return ""
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return str(value)


def log_tick(instrument, token, price, ts=None):
    ts = ts or datetime.now()
    date_tag = ts.strftime("%Y%m%d")
    filename = f"{instrument}_ticks_{date_tag}.csv"
    path = settings.RAW_TICKS_DIR / filename
    row = {
        "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "instrument": instrument,
        "token": token,
        "price": _format_number(price),
    }
    _append_csv_row(path, ["timestamp", "instrument", "token", "price"], row)


def log_entry(strategy_type, instrument, price, signal, ts=None):
    ts = ts or datetime.now()
    date_tag = ts.strftime("%Y%m%d")
    filename = f"entries_{date_tag}.csv"
    path = settings.RAW_TICKS_DIR / filename

    selected_option = signal.get("selected_option") if isinstance(signal, dict) else None
    if not isinstance(selected_option, dict):
        selected_option = {}

    row = {
        "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "strategy": strategy_type,
        "instrument": instrument,
        "tick_price": _format_number(price),
        "entry_price": _format_number(signal.get("entry_price") if isinstance(signal, dict) else None),
        "stop_loss": _format_number(signal.get("stop_loss") if isinstance(signal, dict) else None),
        "target": _format_number(signal.get("target") if isinstance(signal, dict) else None),
        "option_type": selected_option.get("option_type") or selected_option.get("optionType") or "",
        "strike": _format_number(selected_option.get("strike_float") or selected_option.get("strike")),
        "expiry": selected_option.get("expiry_date") or selected_option.get("expiry") or "",
        "quantity": selected_option.get("quantity") or signal.get("selected_quantity", "") if isinstance(signal, dict) else "",
        "risk": _format_number(signal.get("selected_risk") if isinstance(signal, dict) else None),
    }
    _append_csv_row(
        path,
        [
            "timestamp",
            "strategy",
            "instrument",
            "tick_price",
            "entry_price",
            "stop_loss",
            "target",
            "option_type",
            "strike",
            "expiry",
            "quantity",
            "risk",
        ],
        row,
    )
