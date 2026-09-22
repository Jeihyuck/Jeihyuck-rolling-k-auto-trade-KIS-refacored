# -*- coding: utf-8 -*-
"""KIS WebSocket-first realtime price cache shared by KR and US.

Price streams only. Account fill notices remain on the existing broker/reconcile path.
The service lazily obtains the KIS WebSocket approval key from the existing App Key/
Secret, subscribes symbols on demand, reconnects/re-subscribes after disconnects, and
keeps a process-local freshness-tagged cache. REST callers remain authoritative
fallbacks when no fresh WebSocket quote is available.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Any

import requests

logger = logging.getLogger(__name__)


def _env_flag(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except (TypeError, ValueError):
        return float(default)


@dataclass(frozen=True)
class RealtimeQuote:
    market: str
    symbol: str
    last: float
    bid: float | None
    ask: float | None
    received_at: float
    source: str = "KIS_WEBSOCKET"
    exchange: str | None = None

    def as_dict(self) -> dict[str, Any]:
        age = max(0.0, time.time() - self.received_at)
        return {
            "market": self.market,
            "symbol": self.symbol,
            "last": self.last,
            "prpr": self.last,
            "stck_prpr": self.last if self.market == "KR" else None,
            "bid": self.bid,
            "ask": self.ask,
            "received_at": self.received_at,
            "age_sec": age,
            "stale": False,
            "quality": "fresh",
            "source": self.source,
            "exchange": self.exchange,
            "raw": {
                "source": self.source,
                "received_at": self.received_at,
                "age_sec": age,
            },
        }


class KisWebSocketPriceService:
    KR_TR_ID = "H0STCNT0"
    US_TR_ID = "HDFSCNT0"
    _US_PREFIX = {
        "NASD": "DNAS",
        "NASDAQ": "DNAS",
        "NAS": "DNAS",
        "NYSE": "DNYS",
        "NYS": "DNYS",
        "AMEX": "DAMS",
        "AMS": "DAMS",
    }

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._quotes: dict[tuple[str, str], RealtimeQuote] = {}
        self._desired: dict[tuple[str, str], tuple[str, str]] = {}
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._connected = threading.Event()
        self._last_error: str | None = None
        self._connection_count = 0
        self._reconnect_count = 0

    def enabled(self) -> bool:
        if os.getenv("PYTEST_CURRENT_TEST") and not _env_flag("KIS_WS_PRICE_TEST_ENABLE", False):
            return False
        return _env_flag("KIS_WS_PRICE_ENABLED", True)

    @staticmethod
    def _normalize_symbol(market: str, symbol: str) -> str:
        market = str(market or "").upper()
        raw = str(symbol or "").strip().upper()
        if market == "KR":
            return raw.lstrip("A").zfill(6)
        for prefix in ("DNAS", "DNYS", "DAMS"):
            if raw.startswith(prefix) and len(raw) > len(prefix):
                return raw[len(prefix):]
        return raw

    @classmethod
    def us_subscription_key(cls, symbol: str, exchange: str) -> str:
        prefix = cls._US_PREFIX.get(str(exchange or "").strip().upper(), "DNAS")
        return f"{prefix}{cls._normalize_symbol('US', symbol)}"

    def subscribe_kr(self, symbol: str) -> None:
        symbol = self._normalize_symbol("KR", symbol)
        if not symbol:
            return
        with self._lock:
            self._desired[("KR", symbol)] = (self.KR_TR_ID, symbol)
        self._ensure_started()

    def subscribe_us(self, symbol: str, exchange: str) -> None:
        symbol = self._normalize_symbol("US", symbol)
        if not symbol:
            return
        key = self.us_subscription_key(symbol, exchange)
        with self._lock:
            self._desired[("US", symbol)] = (self.US_TR_ID, key)
        self._ensure_started()

    def get_fresh_quote(self, market: str, symbol: str, *, max_age_sec: float) -> dict[str, Any] | None:
        key = (str(market or "").upper(), self._normalize_symbol(market, symbol))
        with self._lock:
            quote = self._quotes.get(key)
        if not quote:
            return None
        age = max(0.0, time.time() - quote.received_at)
        if age > max(0.05, float(max_age_sec)):
            return None
        out = quote.as_dict()
        out["age_sec"] = age
        return out

    def wait_for_fresh_quote(
        self,
        market: str,
        symbol: str,
        *,
        max_age_sec: float,
        wait_sec: float,
    ) -> dict[str, Any] | None:
        if not self.enabled():
            return None
        deadline = time.monotonic() + max(0.0, float(wait_sec))
        while True:
            quote = self.get_fresh_quote(market, symbol, max_age_sec=max_age_sec)
            if quote:
                return quote
            if time.monotonic() >= deadline:
                return None
            time.sleep(0.02)

    def put_quote(
        self,
        *,
        market: str,
        symbol: str,
        last: float,
        bid: float | None = None,
        ask: float | None = None,
        exchange: str | None = None,
        received_at: float | None = None,
    ) -> None:
        if not last or float(last) <= 0:
            return
        normalized = self._normalize_symbol(market, symbol)
        quote = RealtimeQuote(
            market=str(market).upper(),
            symbol=normalized,
            last=float(last),
            bid=float(bid) if bid not in (None, "") and float(bid) > 0 else None,
            ask=float(ask) if ask not in (None, "") and float(ask) > 0 else None,
            received_at=float(received_at or time.time()),
            exchange=exchange,
        )
        with self._lock:
            self._quotes[(quote.market, quote.symbol)] = quote

    def stats(self) -> dict[str, Any]:
        with self._lock:
            desired = len(self._desired)
            quotes = len(self._quotes)
        return {
            "enabled": self.enabled(),
            "connected": self._connected.is_set(),
            "subscriptions": desired,
            "quotes": quotes,
            "connections": self._connection_count,
            "reconnects": self._reconnect_count,
            "last_error": self._last_error,
        }

    def _ensure_started(self) -> None:
        if not self.enabled():
            return
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._thread_main,
                name="kis-ws-price",
                daemon=True,
            )
            self._thread.start()

    def _thread_main(self) -> None:
        try:
            asyncio.run(self._run_forever())
        except Exception as exc:  # pragma: no cover - defensive outer boundary
            self._last_error = f"{type(exc).__name__}: {exc}"
            logger.exception("[KIS_WS][FATAL] err=%s", exc)

    @staticmethod
    def _us_process() -> bool:
        scope = str(
            os.getenv("MARKET_SCOPE")
            or os.getenv("TRADING_MARKET")
            or os.getenv("TRADING_REGION")
            or ""
        ).strip().upper()
        return scope == "US"

    def _credentials(self) -> tuple[str, str]:
        if self._us_process():
            key = str(os.getenv("KIS_US_APP_KEY") or os.getenv("KIS_APP_KEY") or "").strip()
            secret = str(os.getenv("KIS_US_APP_SECRET") or os.getenv("KIS_APP_SECRET") or "").strip()
        else:
            key = str(os.getenv("KIS_APP_KEY") or os.getenv("KIS_US_APP_KEY") or "").strip()
            secret = str(os.getenv("KIS_APP_SECRET") or os.getenv("KIS_US_APP_SECRET") or "").strip()
        return key, secret

    def _rest_base(self) -> str:
        if self._us_process():
            explicit = str(
                os.getenv("KIS_US_REST_URL")
                or os.getenv("KIS_REST_URL")
                or os.getenv("API_BASE_URL")
                or ""
            ).strip().rstrip("/")
        else:
            explicit = str(
                os.getenv("KIS_REST_URL")
                or os.getenv("API_BASE_URL")
                or ""
            ).strip().rstrip("/")
        if explicit:
            return explicit
        env = str(os.getenv("KIS_ENV") or "practice").strip().lower()
        return "https://openapi.koreainvestment.com:9443" if env == "real" else "https://openapivts.koreainvestment.com:29443"

    def _ws_url(self) -> str:
        explicit = str(os.getenv("KIS_WS_URL") or "").strip()
        if explicit:
            return explicit
        env = str(os.getenv("KIS_ENV") or "practice").strip().lower()
        base = "ws://ops.koreainvestment.com:21000" if env == "real" else "ws://ops.koreainvestment.com:31000"
        return f"{base}/tryitout"

    def _approval_key(self) -> str:
        app_key, app_secret = self._credentials()
        if not app_key or not app_secret:
            raise RuntimeError("KIS WebSocket credentials missing")
        url = f"{self._rest_base()}/oauth2/Approval"
        response = requests.post(
            url,
            headers={"content-type": "application/json"},
            json={
                "grant_type": "client_credentials",
                "appkey": app_key,
                "secretkey": app_secret,
            },
            timeout=(3.0, 7.0),
        )
        response.raise_for_status()
        data = response.json()
        approval = str(data.get("approval_key") or "").strip()
        if not approval:
            raise RuntimeError(f"KIS approval_key missing rt={data.get('rt_cd')} msg={data.get('msg1')}")
        return approval

    async def _run_forever(self) -> None:
        try:
            import websockets
        except ImportError as exc:  # pragma: no cover
            self._last_error = "websockets dependency missing"
            logger.error("[KIS_WS][DISABLED] reason=dependency_missing")
            return

        backoff = 1.0
        while not self._stop.is_set():
            try:
                approval = await asyncio.to_thread(self._approval_key)
                async with websockets.connect(
                    self._ws_url(),
                    ping_interval=None,
                    close_timeout=2,
                    open_timeout=_env_float("KIS_WS_CONNECT_TIMEOUT_SEC", 7.0),
                    max_size=2**20,
                ) as ws:
                    self._connection_count += 1
                    if self._connection_count > 1:
                        self._reconnect_count += 1
                    self._connected.set()
                    self._last_error = None
                    logger.info("[KIS_WS][CONNECTED] url=%s connection=%d", self._ws_url(), self._connection_count)
                    sent: set[tuple[str, str]] = set()
                    backoff = 1.0
                    while not self._stop.is_set():
                        with self._lock:
                            desired = dict(self._desired)
                        max_subscriptions = int(float(os.getenv("KIS_WS_MAX_SUBSCRIPTIONS", "40") or 40))
                        if len(desired) > max_subscriptions:
                            logger.error(
                                "[KIS_WS][SUBSCRIPTION_LIMIT] desired=%d max=%d action=REST_FALLBACK_FOR_EXCESS",
                                len(desired), max_subscriptions,
                            )
                        for logical_key, subscription in list(desired.items())[:max_subscriptions]:
                            if logical_key in sent:
                                continue
                            tr_id, tr_key = subscription
                            payload = {
                                "header": {
                                    "approval_key": approval,
                                    "custtype": "P",
                                    "tr_type": "1",
                                    "content-type": "utf-8",
                                },
                                "body": {"input": {"tr_id": tr_id, "tr_key": tr_key}},
                            }
                            await ws.send(json.dumps(payload, ensure_ascii=False))
                            sent.add(logical_key)
                            logger.info("[KIS_WS][SUBSCRIBE] market=%s symbol=%s tr_id=%s tr_key=%s", logical_key[0], logical_key[1], tr_id, tr_key)
                            await asyncio.sleep(_env_float("KIS_WS_SUBSCRIBE_INTERVAL_SEC", 0.5))
                        try:
                            message = await asyncio.wait_for(ws.recv(), timeout=0.25)
                        except asyncio.TimeoutError:
                            continue
                        await self._handle_message(ws, message)
            except Exception as exc:
                self._connected.clear()
                self._last_error = f"{type(exc).__name__}: {exc}"
                logger.warning("[KIS_WS][DISCONNECTED] err=%s retry_sec=%.1f", self._last_error, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, _env_float("KIS_WS_RECONNECT_MAX_SEC", 15.0))

    async def _handle_message(self, ws: Any, message: Any) -> None:
        if isinstance(message, bytes):
            message = message.decode("utf-8", errors="ignore")
        text = str(message or "")
        if not text:
            return
        if text.startswith("{"):
            try:
                payload = json.loads(text)
            except Exception:
                return
            tr_id = str((payload.get("header") or {}).get("tr_id") or "")
            if tr_id == "PINGPONG":
                try:
                    await ws.pong(text.encode("utf-8"))
                except Exception:
                    pass
            elif str((payload.get("body") or {}).get("rt_cd") or "0") != "0":
                logger.warning("[KIS_WS][SUBSCRIBE_FAIL] tr_id=%s msg=%s", tr_id, (payload.get("body") or {}).get("msg1"))
            return

        parts = text.split("|", 3)
        if len(parts) < 4:
            return
        tr_id = parts[1]
        try:
            record_count = max(1, int(parts[2] or 1))
        except (TypeError, ValueError):
            record_count = 1
        raw = parts[3]
        if tr_id == self.KR_TR_ID:
            self._handle_trade_records(raw, record_count, fields_per_record=46, parser=self.parse_kr_trade)
        elif tr_id == self.US_TR_ID:
            total_fields = len(str(raw or "").split("^"))
            fields_per_record = 25
            if record_count > 0 and total_fields % record_count == 0:
                candidate_width = total_fields // record_count
                if candidate_width in {25, 26}:
                    fields_per_record = candidate_width
            self._handle_trade_records(
                raw,
                record_count,
                fields_per_record=fields_per_record,
                parser=self.parse_us_trade,
            )

    def _handle_trade_records(
        self,
        raw: str,
        record_count: int,
        *,
        fields_per_record: int,
        parser: Any,
    ) -> None:
        fields = str(raw or "").split("^")
        if record_count <= 1:
            parsed = parser(raw)
            if parsed:
                self.put_quote(**parsed)
            return
        for index in range(record_count):
            start = index * fields_per_record
            chunk = fields[start:start + fields_per_record]
            if len(chunk) < fields_per_record:
                break
            parsed = parser("^".join(chunk))
            if parsed:
                self.put_quote(**parsed)

    @staticmethod
    def _number(value: Any) -> float | None:
        try:
            val = float(str(value).replace(",", "").strip())
            return val if val > 0 else None
        except Exception:
            return None

    @classmethod
    def parse_kr_trade(cls, raw: str) -> dict[str, Any] | None:
        fields = str(raw or "").split("^")
        if len(fields) < 12:
            return None
        last = cls._number(fields[2])
        if not last:
            return None
        return {
            "market": "KR",
            "symbol": fields[0],
            "last": last,
            "ask": cls._number(fields[10]),
            "bid": cls._number(fields[11]),
        }

    @classmethod
    def parse_us_trade(cls, raw: str) -> dict[str, Any] | None:
        fields = str(raw or "").split("^")
        # KIS currently exposes a 25-field helper layout, while legacy/raw
        # samples include one extra leading realtime-code field. Accept both
        # explicitly so LAST/PBID/PASK cannot shift silently.
        if len(fields) >= 26:
            symbol_field = fields[0] if fields[0].startswith(("DNAS", "DNYS", "DAMS")) else fields[1]
            last_idx, bid_idx, ask_idx = 11, 15, 16
        elif len(fields) >= 16:
            symbol_field = fields[0]
            last_idx, bid_idx, ask_idx = 10, 14, 15
        else:
            return None
        last = cls._number(fields[last_idx])
        if not last:
            return None
        exchange_probe = fields[0] if fields[0].startswith(("DNAS", "DNYS", "DAMS")) else symbol_field
        exchange = None
        if exchange_probe.startswith("DNAS"):
            exchange = "NASDAQ"
        elif exchange_probe.startswith("DNYS"):
            exchange = "NYSE"
        elif exchange_probe.startswith("DAMS"):
            exchange = "AMEX"
        return {
            "market": "US",
            "symbol": cls._normalize_symbol("US", symbol_field),
            "last": last,
            "bid": cls._number(fields[bid_idx]),
            "ask": cls._number(fields[ask_idx]),
            "exchange": exchange,
        }


_service = KisWebSocketPriceService()


def get_kis_ws_price_service() -> KisWebSocketPriceService:
    return _service
