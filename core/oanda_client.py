"""OANDA v20 pricing client for XAUUSD practice/live feed."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests


@dataclass
class OandaPriceTick:
    instrument: str
    bid: float
    ask: float
    mid: float
    timestamp: datetime
    raw: dict[str, Any]


class OandaPricingClient:
    def __init__(
        self,
        api_token: str,
        account_id: str,
        instrument: str = "XAU_USD",
        env: str = "practice",
        timeout_seconds: int = 10,
    ) -> None:
        self.api_token = api_token
        self.account_id = account_id
        self.instrument = instrument
        self.env = env.lower().strip()
        self.timeout_seconds = timeout_seconds
        if self.env not in {"practice", "live"}:
            raise ValueError("OANDA env must be 'practice' or 'live'")

        if self.env == "practice":
            self.base_url = "https://api-fxpractice.oanda.com"
        else:
            self.base_url = "https://api-fxtrade.oanda.com"

    @property
    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        }

    def connect(self) -> None:
        if not self.api_token or not self.account_id:
            raise RuntimeError("OANDA_API_TOKEN and OANDA_ACCOUNT_ID must be configured")
        _ = self.fetch_latest_price()

    @staticmethod
    def _parse_time(raw_time: str | None) -> datetime:
        if not raw_time:
            return datetime.now(tz=timezone.utc)
        normalized = raw_time.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)

    @staticmethod
    def normalize_price_response(payload: dict[str, Any], instrument: str) -> OandaPriceTick | None:
        prices = payload.get("prices")
        if not isinstance(prices, list) or not prices:
            return None

        selected: dict[str, Any] | None = None
        for item in prices:
            if str(item.get("instrument", "")).upper() == instrument.upper():
                selected = item
                break
        if selected is None:
            selected = prices[0]

        bids = selected.get("bids") or []
        asks = selected.get("asks") or []
        if not bids or not asks:
            return None

        bid = float(bids[0]["price"])
        ask = float(asks[0]["price"])
        mid = (bid + ask) / 2.0
        tick_time = OandaPricingClient._parse_time(selected.get("time"))

        return OandaPriceTick(
            instrument=str(selected.get("instrument", instrument)),
            bid=bid,
            ask=ask,
            mid=mid,
            timestamp=tick_time,
            raw=selected,
        )

    def fetch_latest_price(self) -> OandaPriceTick | None:
        endpoint = f"{self.base_url}/v3/accounts/{self.account_id}/pricing"
        response = requests.get(
            endpoint,
            headers=self._headers,
            params={"instruments": self.instrument},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        return self.normalize_price_response(payload, self.instrument)
