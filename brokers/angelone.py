"""
Angel One broker bridge focused on authentication + live tick websocket usage.
"""

from __future__ import annotations

import json
import threading
from datetime import datetime
from typing import Any, Callable, Optional

import requests

from config import settings
from utils.logger import log_exception, logger


class AngelOneBroker:
    """Minimal SmartAPI wrapper for login, scripmaster fetch, and live ticks."""

    def __init__(self) -> None:
        self.api: Optional[Any] = None
        self.feed_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.websocket: Optional[Any] = None
        self.is_connected = False

    def _load_smartapi(self):
        try:
            from SmartApi import SmartConnect
            from SmartApi.smartWebSocketV2 import SmartWebSocketV2
            import pyotp
        except ImportError as exc:
            raise RuntimeError(
                "Missing SmartAPI dependencies. Install: smartapi-python pyotp websocket-client"
            ) from exc
        return SmartConnect, SmartWebSocketV2, pyotp

    def _totp(self) -> str:
        _, _, pyotp = self._load_smartapi()
        if not settings.ANGEL_TOTP_KEY:
            raise RuntimeError("ANGEL_TOTP_KEY is not configured.")
        return pyotp.TOTP(settings.ANGEL_TOTP_KEY).now()

    def connect(self) -> bool:
        """Authenticate and open session."""
        try:
            if self.is_connected and self.api is not None:
                return True

            if not settings.ANGEL_API_KEY or not settings.ANGEL_USERNAME or not settings.ANGEL_PASSWORD:
                raise RuntimeError("ANGEL_API_KEY/ANGEL_USERNAME/ANGEL_PASSWORD must be set.")

            SmartConnect, _, _ = self._load_smartapi()
            self.api = SmartConnect(api_key=settings.ANGEL_API_KEY)
            session = self.api.generateSession(
                settings.ANGEL_USERNAME,
                settings.ANGEL_PASSWORD,
                self._totp(),
            )
            if not session or not session.get("status"):
                logger.error("Angel session failed: %s", session)
                self.api = None
                return False

            self.refresh_token = session["data"]["refreshToken"]
            self.feed_token = self.api.getfeedToken()
            self.is_connected = True
            logger.info("Angel One session established.")
            return True
        except Exception as exc:
            log_exception(exc)
            self.api = None
            self.is_connected = False
            return False

    def fetch_scripmaster_data(self, timeout: int = 30) -> list[dict[str, Any]]:
        """Download and parse Angel OpenAPI ScripMaster instrument list."""
        try:
            response = requests.get(settings.ANGEL_SCRIPMASTER_URL, timeout=timeout)
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, list):
                logger.info("Fetched %s rows from ScripMaster.", len(payload))
                return payload
            logger.error("Unexpected ScripMaster payload type: %s", type(payload).__name__)
            return []
        except Exception as exc:
            log_exception(exc)
            return []

    def start_websocket(
        self,
        token_list: list[dict[str, Any]],
        on_data_callback: Callable[[dict[str, Any] | str], None],
        correlation_id: str = "mcx_gold_feed",
        mode: int = 1,
    ):
        """
        Start websocket and subscribe to tokens.

        token_list example:
        [
            {"exchangeType": 5, "tokens": ["12345"]},
        ]
        """
        if not self.connect():
            return None

        try:
            _, SmartWebSocketV2, _ = self._load_smartapi()
            ws = SmartWebSocketV2(
                self.refresh_token,
                settings.ANGEL_API_KEY,
                settings.ANGEL_USERNAME,
                self.feed_token,
            )

            def on_open(_wsapp):
                logger.info("Angel websocket opened. Subscribing to %s", token_list)
                ws.subscribe(correlation_id, mode, token_list)

            def on_data(_wsapp, message):
                on_data_callback(message)

            def on_error(_wsapp, error):
                logger.error("Angel websocket error: %s", error)

            def on_close(_wsapp, code=None, reason=None):
                logger.info("Angel websocket closed: %s - %s", code, reason)

            ws.on_open = on_open
            ws.on_data = on_data
            ws.on_error = on_error
            ws.on_close = on_close

            thread = threading.Thread(target=ws.connect, daemon=True)
            thread.start()
            self.websocket = ws
            return ws
        except Exception as exc:
            log_exception(exc)
            return None

    def close(self) -> None:
        if self.websocket and hasattr(self.websocket, "close_connection"):
            try:
                self.websocket.close_connection()
            except Exception as exc:
                log_exception(exc)
        self.websocket = None
        self.is_connected = False

    @staticmethod
    def extract_ltp(message: dict[str, Any] | str, expected_token: str | None = None) -> Optional[dict[str, Any]]:
        """
        Normalize websocket tick payload and return LTP in rupees.
        """
        try:
            payload = message
            if isinstance(payload, str):
                payload = json.loads(payload)

            if isinstance(payload, dict) and isinstance(payload.get("data"), list):
                ticks = payload["data"]
            elif isinstance(payload, list):
                ticks = payload
            elif isinstance(payload, dict):
                ticks = [payload]
            else:
                return None

            for tick in ticks:
                token = str(tick.get("token") or tick.get("symbolToken") or "")
                if expected_token and token != str(expected_token):
                    continue

                ltp_raw = (
                    tick.get("last_traded_price")
                    or tick.get("ltp")
                    or tick.get("lastTradedPrice")
                )
                if ltp_raw is None:
                    continue

                ltp = float(ltp_raw) * 0.01
                return {
                    "token": token,
                    "ltp": ltp,
                    "timestamp": datetime.now(),
                    "raw": tick,
                }
            return None
        except Exception:
            return None
