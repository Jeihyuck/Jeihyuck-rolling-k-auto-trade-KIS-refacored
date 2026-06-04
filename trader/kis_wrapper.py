# -*- coding: utf-8 -*-
# kis_wrapper.py — KIS OpenAPI wrapper (practice/real 공용)
# - 세션/리트라이/레이트리밋
# - 토큰 캐시
# - 시세/일봉/ATR
# - 잔고/주문
# - ✅ 예수금: output2.ord_psbl_cash 우선 사용 (fallback: nrcvb_buy_amt → dnca_tot_amt, 최후: 최근 캐시)
# - ✅ SSL EOF/JSON Decode 등 일시 오류 내성 강화
# - ✅ 시세 0원 방지(J↔U, A접두/무접두 교차, 지수 백오프 재시도)
# - ✅ 잔고 페이징(ctx_area_*) , empty 순간응답 디바운스
# - ✅ [NEW] 세션 리셋/지수형 백오프를 포함한 안전요청(_safe_request), 체결 후 잔고 동기화(refresh_after_order)
# - ✅ [PATCH] 가격조회 TTL 캐시 + inflight 공유 + 레이트리미터 + 서킷 브레이커

import os
import json
import time
import random
import logging
import threading
import copy
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import requests
import pytz
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urlparse
import sqlalchemy as sa

from settings import APP_KEY, APP_SECRET, API_BASE_URL, CANO, ACNT_PRDT_CD, KIS_ENV
from trader.kis_rate_limiter import get_kis_limiter
from trader.runtime_paths import runtime_path
from trader.time_utils import is_trading_day, is_trading_window, now_kst
from trader.config import (
    DAILY_CAPITAL as DEFAULT_DAILY_CAPITAL,
    MARKET_MAP,
    SUBJECT_FLOW_TIMEOUT_SEC,
    SUBJECT_FLOW_RETRY,
)
from trader.fills import append_fill
from trader.db.engine import make_engine
from trader.db.schema import PRICE_DAILY
from trader.rate_limit import get_kis_gate
from trader.cache_ttl import price_cache, PRICE_SNAPSHOT_TTL_SEC
from trader.eventlog import emit_event

logger = logging.getLogger(__name__)


# ===== [NEW] KIS HTTP 차단 함수 =====
def kis_http_enabled() -> bool:
    """
    KIS API HTTP 호출 허용 여부 판단.
    - FORCE_HTTP=1 → True (DIAG 포함, 최우선 허용)
    - MINERVINI_ONLY=1 → False (FORCE_HTTP=1이면 예외)
    - KIS_HTTP_ENABLED=0/FALSE/NO/OFF → False (차단)
    - KIS_HTTP_ENABLED=AUTO → STRATEGY_MODE=LIVE일 때만 True
    - 그 외 → True
    """
    force_http = os.getenv("FORCE_HTTP", "0").strip() == "1"

    # [CRITICAL] MINERVINI_ONLY가 1이면 무조건 HTTP 차단
    from trader.config import MINERVINI_ONLY
    if MINERVINI_ONLY and not force_http:
        return False
    
    v = os.getenv("KIS_HTTP_ENABLED", "AUTO").strip().upper()
    if v in ("0", "FALSE", "NO", "OFF"):
        return force_http
    if v == "AUTO":
        http_enabled = os.getenv("STRATEGY_MODE", "").strip().upper() == "LIVE"
        return http_enabled or force_http
    return True


def _endpoint_path(endpoint: str) -> str:
    parsed = urlparse(str(endpoint or ""))
    return (parsed.path or str(endpoint or "")).lower()


def _endpoint_name(endpoint: str) -> str:
    path = _endpoint_path(endpoint).rstrip("/")
    if not path:
        return "unknown"
    return path.split("/")[-1] or "unknown"


def is_order_endpoint(endpoint: str) -> bool:
    path = _endpoint_path(endpoint)
    return any(
        token in path
        for token in (
            "/trading/order-cash",
            "/trading/order-rvsecncl",
            "/trading/order-resv",
            "/order-cash",
            "/order-rvsecncl",
            "/order/",
        )
    )


def is_data_endpoint(endpoint: str) -> bool:
    path = _endpoint_path(endpoint)
    if "/oauth2/token" in path:
        return True
    return any(
        token in path
        for token in (
            "/quotations/",
            "inquire-price",
            "inquire-daily-itemchartprice",
            "inquire-asking-price-exp-ccn",
            "inquire-investor",
            "program-trade",
            "market-cap",
            "search-stock-info",
            "inquire-daily-ccld",
            "inquire-balance",
            "inquire-psbl-order",
        )
    )


def _resolve_kis_http_caller_route(default: str = "live") -> str:
    raw = (os.getenv("KIS_HTTP_CALLER_ROUTE") or "").strip().lower()
    if raw:
        return raw
    mode = (os.getenv("MODE") or "").strip().lower()
    strategy_mode = (os.getenv("STRATEGY_MODE") or "").strip().upper()
    if mode == "prep":
        return "prep"
    if strategy_mode == "DIAG" and (
        os.getenv("PB1_DIAG_FULL_EXEC", "0").strip() == "1"
        or os.getenv("FORCE_RUN", "0").strip() == "1"
        or os.getenv("WATCHLIST_MODE", "0").strip() == "1"
    ):
        return "manual_test"
    return default


def kis_http_allowed(endpoint: str, strategy_mode: str, allow_data_http_in_diag: bool, caller_route: str | None = None) -> bool:
    normalized_mode = str(strategy_mode or "").strip().upper()
    route = (caller_route or _resolve_kis_http_caller_route(default="live")).strip().lower() or "live"
    if normalized_mode == "DIAG":
        if is_order_endpoint(endpoint):
            return False
        if is_data_endpoint(endpoint):
            if route == "smoke":
                return False
            return bool(allow_data_http_in_diag)
    return True


def is_trading_endpoint(url: str) -> bool:
    return is_order_endpoint(url)


def kis_explicit_offline_mode() -> bool:
    if os.getenv("KIS_EXPLICIT_OFFLINE", "0").strip() == "1":
        return True
    if (os.getenv("DIAG_KIS_CALLS_ENABLED") or "").strip() == "0":
        return True
    return False


def kis_data_http_allowed_in_diag() -> bool:
    """
    DIAG 모드에서 KIS 데이터 HTTP 허용 여부.
    ALLOW_KIS_DATA_HTTP_IN_DIAG=1이면 데이터 조회 허용.
    """
    return os.getenv("ALLOW_KIS_DATA_HTTP_IN_DIAG", "0").strip() == "1"


# ✅ Export public exceptions
__all__ = [
    "KisAPI",
    "KisTemporaryError",
    "KisAuthError",
    "KisPermanentError",
    "KisBalanceUnavailable",
    "NetTemporaryError",
    "DataEmptyError",
    "DataShortError",
    "OrderBlockedError",
    "KISBlockedError",
]

_ORDER_BLOCK_STATE: Dict[str, Any] = {"date": None, "reason": None}
_DAILY_CAP_WARNED = False
_BALANCE_CACHE_INVALID_LOGGED = False
_KIS_BREAKER_LOCK = threading.Lock()
_KIS_BREAKER_STATE: dict | None = None
_KIS_BREAKER_WINDOW_SEC = int(os.getenv("KIS_BREAKER_WINDOW_SEC", "300") or "300")
_KIS_BREAKER_THRESHOLD_ORDER = int(os.getenv("KIS_BREAKER_THRESHOLD_ORDER", "2") or "2")
_KIS_BREAKER_THRESHOLD_AUTH = int(os.getenv("KIS_BREAKER_THRESHOLD_AUTH", "2") or "2")
_KIS_BREAKER_THRESHOLD_DATA = int(os.getenv("KIS_BREAKER_THRESHOLD_DATA", "10") or "10")
_KIS_BREAKER_OPEN_SEC = int(os.getenv("KIS_BREAKER_OPEN_SEC", "60") or "60")
_KIS_TEMP_ERROR_CODES: set[str] = {
    code.strip()
    for code in (os.getenv("KIS_TEMP_ERROR_CODES") or "EGW00201").split(",")
    if code.strip()
}


class KisTemporaryError(Exception):
    """429/5xx/timeout 등 재시도 가능한 오류."""


class KisAuthError(Exception):
    """401/403 인증 오류."""


class KisPermanentError(Exception):
    """기타 4xx 등 영구 오류."""


class KisBalanceUnavailable(KisTemporaryError):
    """잔고 조회 실패."""


class NetTemporaryError(KisTemporaryError):
    """네트워크/SSL 등 일시적 오류를 의미 (제외 금지, 루프 스킵)."""


class DataEmptyError(Exception):
    """정상응답이나 캔들이 0개 (실제 데이터 없음)."""
    pass


class DataShortError(Exception):
    """정상응답이나 캔들이 need_n 미만."""
    pass


class OrderBlockedError(RuntimeError):
    """주문 하드 가드에 의해 차단된 경우."""


class KISBlockedError(RuntimeError):
    """DIAG 모드에서 KIS API 차단."""
    pass


def botstate_path(*parts: str) -> Path:
    return runtime_path(*parts)


def normalize_base_url(url: str) -> str:
    """API_BASE_URL을 정규화하여 /uapi 중복을 방지."""
    url = url.rstrip("/")
    if url.endswith("/uapi"):
        url = url[:-5]
    return url


def _build_session():
    s = requests.Session()
    retry = Retry(
        total=0, connect=0, read=0, status=0,
        backoff_factor=0,
        status_forcelist=[],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "RKMax/1.0", "Connection": "keep-alive"})
    return s


SESSION = _build_session()


def _get_json(url, params=None, timeout=(3.0, 7.0)):
    try:
        r = SESSION.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.SSLError as e:
        logger.warning("[NET:SSL_ERROR] %s %s", url, e)
        raise NetTemporaryError()
    except requests.exceptions.RequestException as e:
        logger.warning("[NET:REQ_ERROR] %s %s", url, e)
        raise NetTemporaryError()


def safe_strip(val):
    if val is None:
        return ""
    if isinstance(val, str):
        return val.replace("\n", "").replace("\r", "").strip()
    return str(val).strip()


def _digits_only(value: str) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def extract_order_no(response: dict | None) -> str | None:
    if not isinstance(response, dict):
        return None
    output = response.get("output") or response.get("output1") or {}
    if isinstance(output, list) and output:
        output = output[0] if isinstance(output[0], dict) else {}
    if not isinstance(output, dict):
        output = {}
    odno = output.get("ODNO") or output.get("odno") or output.get("ord_no") or response.get("ODNO")
    odno_s = str(odno or "").strip()
    return odno_s or None


def mask_order_response(response: Any) -> dict[str, Any]:
    if not isinstance(response, dict):
        return {"type": type(response).__name__}
    output = response.get("output") or response.get("output1") or {}
    if isinstance(output, list) and output:
        output = output[0] if isinstance(output[0], dict) else {}
    if not isinstance(output, dict):
        output = {}
    return {
        "rt_cd": str(response.get("rt_cd") or ""),
        "msg_cd": str(response.get("msg_cd") or ""),
        "msg1": str(response.get("msg1") or "")[:200],
        "odno": extract_order_no(response),
        "blocked": bool(response.get("blocked")),
        "kis_disabled": bool(response.get("_kis_disabled")),
        "output_keys": sorted(output.keys()),
    }


def is_order_accepted(response: dict | None, *, kis_env: str | None = None) -> bool:
    if not isinstance(response, dict):
        return False
    if response.get("blocked") or response.get("_kis_disabled"):
        return False
    rt_cd = str(response.get("rt_cd") or "").strip()
    msg_cd = str(response.get("msg_cd") or "").strip().upper()
    msg1 = str(response.get("msg1") or "").strip().lower()
    odno = extract_order_no(response)
    env_name = str(kis_env or os.getenv("KIS_ENV") or "practice").strip().lower()
    if rt_cd != "0":
        return False
    if odno:
        return True
    if env_name == "practice":
        return msg_cd not in {"NO_TRADE", "LIVE_GATE_BLOCKED", "ORDER_BLOCK"} and "blocked" not in msg1
    return False


def _json_dumps(body: dict) -> str:
    return json.dumps(body, ensure_ascii=False, separators=(",", ":"), sort_keys=False)


def _deepcopy_json(value: Any) -> Any:
    try:
        return copy.deepcopy(value)
    except Exception:
        return value


def _env_int(name: str, default: int) -> int:
    try:
        raw = os.getenv(name, "").strip()
        return int(raw) if raw else default
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        raw = os.getenv(name, "").strip()
        return float(raw) if raw else default
    except Exception:
        return default


def _env_first_int(names: tuple[str, ...], default: int) -> int:
    for name in names:
        raw = str(os.getenv(name) or "").strip()
        if not raw:
            continue
        try:
            return int(raw)
        except Exception:
            continue
    return default


def _env_first_float(names: tuple[str, ...], default: float) -> float:
    for name in names:
        raw = str(os.getenv(name) or "").strip()
        if not raw:
            continue
        try:
            return float(raw)
        except Exception:
            continue
    return default


def _is_sensitive_log_key(key: str) -> bool:
    lowered = str(key or "").strip().lower()
    return any(token in lowered for token in ("authorization", "token", "secret", "appkey", "appsecret", "hashkey", "cano"))


def sanitize_headers(headers: dict | None) -> dict[str, Any]:
    masked: dict[str, Any] = {}
    for key, value in (headers or {}).items():
        masked[str(key)] = "***" if _is_sensitive_log_key(str(key)) else value
    return masked


def sanitize_log_mapping(payload: dict | None) -> dict[str, Any]:
    masked: dict[str, Any] = {}
    for key, value in (payload or {}).items():
        lowered = str(key or "").strip().lower()
        if _is_sensitive_log_key(lowered) or lowered in {"access_token", "refresh_token", "appsecret", "appkey"}:
            masked[str(key)] = "***"
        else:
            masked[str(key)] = value
    return masked


def _load_price_policy_config() -> dict[str, float]:
    min_interval_ms = _env_first_float(("KIS_PRICE_MIN_INTERVAL_MS",), 0.0)
    min_interval_sec = _env_first_float(
        ("KIS_PRICE_MIN_INTERVAL_SEC", "KIS_INQUIRE_PRICE_MIN_INTERVAL_SEC"),
        0.0,
    )

    if min_interval_ms > 0:
        price_qps = 1000.0 / min_interval_ms
    elif min_interval_sec > 0:
        price_qps = 1.0 / min_interval_sec
    else:
        price_qps = _env_first_float(("KIS_PRICE_QPS", "PRICE_QPS"), 3.0)
    return {
        "ttl_sec": _env_first_float(("KIS_PRICE_CACHE_TTL_SEC", "PRICE_TTL_SEC", "PRICE_SNAPSHOT_TTL_SEC"), 15.0),
        "qps": price_qps,
        "burst": float(_env_first_int(("KIS_PRICE_BURST", "PRICE_BURST"), 3)),
        "circuit_sec": _env_first_float(("KIS_RATE_LIMIT_COOLDOWN_SEC", "PRICE_CIRCUIT_SEC"), 15.0),
        "jitter_sec": _env_first_float(("KIS_PRICE_JITTER_MAX_SEC", "PRICE_JITTER_MAX_SEC"), 0.12),
    }


def _load_breaker_state() -> dict:
    global _KIS_BREAKER_STATE
    if _KIS_BREAKER_STATE is not None:
        return _KIS_BREAKER_STATE
    if not isinstance(_KIS_BREAKER_STATE, dict):
        _KIS_BREAKER_STATE = {"endpoints": {}}
    return _KIS_BREAKER_STATE


def _save_breaker_state(state: dict) -> None:
    global _KIS_BREAKER_STATE
    _KIS_BREAKER_STATE = state


def _breaker_key(method: str, url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path or url
    return f"{method.upper()} {path}"


def _breaker_policy(method: str, url: str) -> dict[str, Any]:
    _ = method
    endpoint_name = _endpoint_name(url)

    if "/oauth2/token" in _endpoint_path(url):
        category = "auth"
        threshold = _KIS_BREAKER_THRESHOLD_AUTH
        applicable = True

    elif is_order_endpoint(url):
        category = "order"
        threshold = _KIS_BREAKER_THRESHOLD_ORDER
        applicable = True

    else:
        category = "data"
        kr_data_breaker_enabled = os.getenv("KR_KIS_DATA_BREAKER_ENABLED", "1").strip() == "1"

        if kr_data_breaker_enabled and endpoint_name in {
            "inquire-price",
            "inquire-balance",
            "inquire-psbl-order",
            "inquire-daily-ccld",
        }:
            applicable = True
            if endpoint_name == "inquire-price":
                threshold = int(os.getenv("KIS_BREAKER_THRESHOLD_PRICE", "3") or "3")
            elif endpoint_name in {"inquire-balance", "inquire-psbl-order"}:
                threshold = int(os.getenv("KIS_BREAKER_THRESHOLD_BALANCE", "2") or "2")
            else:
                threshold = int(os.getenv("KIS_BREAKER_THRESHOLD_DATA", "5") or "5")
        else:
            applicable = False
            threshold = _KIS_BREAKER_THRESHOLD_DATA

    logger.info(
        "[KIS][BREAKER_POLICY] endpoint=%s category=%s global_breaker_applicable=%s threshold=%s",
        endpoint_name,
        category,
        int(applicable),
        threshold,
    )
    return {
        "endpoint": endpoint_name,
        "category": category,
        "global_breaker_applicable": applicable,
        "threshold": threshold,
    }


def _breaker_check(method: str, url: str) -> tuple[bool, float | None]:
    policy = _breaker_policy(method, url)
    if not bool(policy.get("global_breaker_applicable")):
        return False, None
    key = _breaker_key(method, url)
    now_ts = time.time()
    with _KIS_BREAKER_LOCK:
        state = _load_breaker_state()
        entry = state.get("endpoints", {}).get(key, {})
        open_until = entry.get("open_until")
        if isinstance(open_until, (int, float)) and now_ts < open_until:
            return True, float(open_until)
    return False, None


def _breaker_record_temp_failure(method: str, url: str) -> None:
    policy = _breaker_policy(method, url)
    if not bool(policy.get("global_breaker_applicable")):
        return
    key = _breaker_key(method, url)
    now_ts = time.time()
    with _KIS_BREAKER_LOCK:
        state = _load_breaker_state()
        entry = state.setdefault("endpoints", {}).setdefault(key, {})
        failures = entry.get("failures") or []
        if not isinstance(failures, list):
            failures = []
        window_start = now_ts - _KIS_BREAKER_WINDOW_SEC
        failures = [ts for ts in failures if isinstance(ts, (int, float)) and ts >= window_start]
        failures.append(now_ts)
        entry["failures"] = failures
        if len(failures) >= int(policy.get("threshold") or _KIS_BREAKER_THRESHOLD_DATA):
            entry["open_until"] = now_ts + _KIS_BREAKER_OPEN_SEC
        state["endpoints"][key] = entry
        _save_breaker_state(state)


def _breaker_record_success(method: str, url: str) -> None:
    policy = _breaker_policy(method, url)
    if not bool(policy.get("global_breaker_applicable")):
        return
    key = _breaker_key(method, url)
    with _KIS_BREAKER_LOCK:
        state = _load_breaker_state()
        entry = state.get("endpoints", {}).get(key)
        if not entry:
            return
        entry["failures"] = []
        entry.pop("open_until", None)
        state["endpoints"][key] = entry
        _save_breaker_state(state)


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, str):
            cleaned = value.replace(",", "").strip()
            if cleaned == "":
                return default
            return float(cleaned)
        return float(value)
    except Exception:
        return default


# ========================================================================
# 가격조회 TTL 캐시 + 레이트리미터 + 서킷 브레이커
# ========================================================================

class _TokenBucket:
    """thread-safe token bucket rate limiter"""
    def __init__(self, rate: float, burst: int):
        self.rate = float(rate)
        self.capacity = int(burst)
        self.tokens = float(burst)
        self.updated = time.time()
        self.lock = threading.Lock()

    def acquire(self) -> float:
        """returns sleep seconds needed (0 if ok)"""
        now = time.time()
        with self.lock:
            elapsed = now - self.updated
            self.updated = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return 0.0
            need = 1.0 - self.tokens
            wait = need / self.rate if self.rate > 0 else 1.0
            self.tokens = 0.0
            return max(0.0, wait)


@dataclass
class _PriceRow:
    ts: float
    data: dict


class _PriceCache:
    """
    - TTL cache: same code within TTL won't re-call API
    - inflight: concurrent callers share the same result
    - circuit breaker: after EGW002, pause calls for a while
    """
    def __init__(self, ttl_sec: float, circuit_sec: float):
        self.ttl = float(ttl_sec)
        self.circuit_sec = float(circuit_sec)
        self.cache: Dict[Tuple[str, str], _PriceRow] = {}
        self.inflight: Dict[Tuple[str, str], threading.Event] = {}
        self.inflight_result: Dict[Tuple[str, str], dict] = {}
        self.lock = threading.Lock()
        self.circuit_until = 0.0
        self.cache_hits = 0
        self.cache_misses = 0
        self.rate_limit_hits = 0
        self.retry_waits = 0

    def is_circuit_open(self) -> bool:
        return time.time() < self.circuit_until

    def open_circuit(self, *, rate_limited: bool = False):
        self.circuit_until = max(self.circuit_until, time.time() + self.circuit_sec)
        if rate_limited:
            self.rate_limit_hits += 1

    def get_cached(self, key: Tuple[str, str]) -> Optional[dict]:
        row = self.cache.get(key)
        if not row:
            return None
        if time.time() - row.ts <= self.ttl:
            return row.data
        return None

    def set_cached(self, key: Tuple[str, str], data: dict):
        self.cache[key] = _PriceRow(ts=time.time(), data=data)

    def record_cache_hit(self) -> None:
        self.cache_hits += 1

    def record_cache_miss(self) -> None:
        self.cache_misses += 1

    def record_retry_wait(self) -> None:
        self.retry_waits += 1

    def snapshot_stats(self, *, reset: bool = False) -> dict[str, int]:
        stats = {
            "cache_hit": int(self.cache_hits),
            "cache_miss": int(self.cache_misses),
            "price_http_fail_count": int(self.rate_limit_hits),
            "retry_count": int(self.retry_waits),
        }
        if reset:
            self.cache_hits = 0
            self.cache_misses = 0
            self.rate_limit_hits = 0
            self.retry_waits = 0
        return stats

    def begin_inflight(self, key: Tuple[str, str]) -> Optional[threading.Event]:
        """
        If already inflight, returns its event (caller should wait).
        Else creates inflight and returns None (caller becomes leader).
        """
        with self.lock:
            ev = self.inflight.get(key)
            if ev:
                return ev
            ev = threading.Event()
            self.inflight[key] = ev
            return None

    def finish_inflight(self, key: Tuple[str, str], data: dict):
        with self.lock:
            self.inflight_result[key] = data
            ev = self.inflight.pop(key, None)
            if ev:
                ev.set()

    def wait_inflight(self, key: Tuple[str, str], ev: threading.Event, timeout: float = 5.0) -> dict:
        ok = ev.wait(timeout=timeout)
        with self.lock:
            return self.inflight_result.pop(key, {}) if ok else {}


# 환경변수에서 설정 로드
_PRICE_POLICY = _load_price_policy_config()
_PRICE_TTL_SEC = float(_PRICE_POLICY["ttl_sec"])
_PRICE_QPS = float(_PRICE_POLICY["qps"])
_PRICE_BURST = int(_PRICE_POLICY["burst"])
_PRICE_CIRCUIT_SEC = float(_PRICE_POLICY["circuit_sec"])
_PRICE_JITTER_MAX_SEC = float(_PRICE_POLICY["jitter_sec"])

# 전역 인스턴스 생성
_price_rl = _TokenBucket(rate=_PRICE_QPS, burst=_PRICE_BURST)
_price_cache = _PriceCache(ttl_sec=_PRICE_TTL_SEC, circuit_sec=_PRICE_CIRCUIT_SEC)


def get_price_runtime_stats(*, reset: bool = False) -> dict[str, int]:
    return _price_cache.snapshot_stats(reset=reset)


def _mark_price_rate_limited(endpoint: str, code: str | None, msg_cd: str, msg1: str | None) -> None:
    logger.warning(
        "[KIS][PRICE_RATE_LIMIT][COOLDOWN] endpoint=%s code=%s cooldown=%.2fs msg_cd=%s msg1=%s",
        endpoint,
        code,
        _PRICE_CIRCUIT_SEC,
        msg_cd,
        msg1,
    )
    _price_cache.open_circuit(rate_limited=True)


def safe_int(value: Any, default: int = 0) -> int:
    return int(safe_float(value, default=float(default)))


def _normalize_balance_snapshot(snapshot: Any) -> dict | None:
    if not isinstance(snapshot, dict):
        return None
    rt_cd = snapshot.get("rt_cd")
    if rt_cd is not None and str(rt_cd) != "0":
        return None
    output1 = snapshot.get("output1")
    if output1 is None:
        output1 = []
    elif isinstance(output1, dict):
        output1 = [output1]
    elif not isinstance(output1, list):
        output1 = []
    output2 = snapshot.get("output2")
    if output2 is None:
        output2 = {}
    elif isinstance(output2, list):
        output2 = output2[0] if output2 and isinstance(output2[0], dict) else {}
    elif not isinstance(output2, dict):
        output2 = {}
    normalized = dict(snapshot)
    normalized["output1"] = output1
    normalized["output2"] = output2
    return normalized


def _is_raw_balance_snapshot(snapshot: Any) -> bool:
    if not isinstance(snapshot, dict):
        return False
    output2 = snapshot.get("output2")
    if isinstance(output2, list) and output2:
        first = output2[0]
        return isinstance(first, dict) and bool(first)
    if isinstance(output2, dict) and output2:
        return True
    return False


def _order_block_reason(now: datetime | None = None) -> Optional[str]:
    now = now or now_kst()
    state_date = _ORDER_BLOCK_STATE.get("date")
    state_reason = _ORDER_BLOCK_STATE.get("reason")
    if state_date and state_date != now.date():
        _ORDER_BLOCK_STATE.update({"date": None, "reason": None})
        state_date, state_reason = None, None
    if state_date == now.date() and state_reason:
        return str(state_reason)
    if not is_trading_day(now):
        _ORDER_BLOCK_STATE.update({"date": now.date(), "reason": "NON_TRADING_DAY"})
        return "NON_TRADING_DAY"
    if not is_trading_window(now):
        return "OUTSIDE_TRADING_WINDOW"
    return None


def _assert_orders_allowed(action: str) -> None:
    block = (os.getenv("PB1_BLOCK_ORDERS") or "").strip().lower() in {"1", "true", "yes", "y"}
    diag_rehearsal = (os.getenv("PB1_DIAG_REHEARSAL") or "").strip().lower() in {"1", "true", "yes", "y"}
    mode = (os.getenv("EFFECTIVE_STRATEGY_MODE") or os.getenv("STRATEGY_MODE") or "").strip().upper()
    live = (os.getenv("LIVE_TRADING_ENABLED") or "").strip().lower() in {"1", "true", "yes", "y"}

    if block or diag_rehearsal or mode == "DIAG" or not live:
        raise OrderBlockedError(
            "Orders are blocked. "
            f"action={action} PB1_BLOCK_ORDERS={os.getenv('PB1_BLOCK_ORDERS')} "
            f"mode={mode} LIVE_TRADING_ENABLED={os.getenv('LIVE_TRADING_ENABLED')}"
        )


def _mark_order_blocked(reason: str, now: datetime | None = None) -> None:
    now = now or now_kst()
    _ORDER_BLOCK_STATE.update({"date": now.date(), "reason": reason})


def _is_order_disallowed(resp: Any) -> Optional[str]:
    if not isinstance(resp, dict):
        return None
    msg1 = str(resp.get("msg1") or "")
    msg_cd = str(resp.get("msg_cd") or "")
    msg = f"{msg1} {msg_cd}".strip()
    primary_phrases = ("영업일이 아닙니다", "주문 가능 시간이 아닙니다", "주문가능시간이 아닙니다")
    if any(p in msg1 for p in primary_phrases):
        return msg or "ORDER_NOT_ALLOWED"

    low = msg.lower()
    keywords = ("휴장", "가능시간", "closed")
    if any(k in low for k in keywords):
        return msg or "ORDER_NOT_ALLOWED"
    status = resp.get("_status")
    if isinstance(status, int) and status in (401, 403):
        return f"HTTP_{status}"
    return None


class _RateLimiter:
    def __init__(self, min_interval_sec: float = 0.20):
        self.min_interval = float(min_interval_sec)
        self.last_at: Dict[str, float] = {}
        self._lock = threading.Lock()

    def wait(self, key: str):
        with self._lock:
            now = time.time()
            last = self.last_at.get(key, 0.0)
            delta = now - last
            if delta < self.min_interval:
                sleep_sec = self.min_interval - delta + random.uniform(0, 0.03)
                logger.debug(
                    "[KIS][RATE_LIMIT][SLEEP] endpoint=%s sleep=%.3f reason=global_qps_guard",
                    key, sleep_sec,
                )
                time.sleep(sleep_sec)
            self.last_at[key] = time.time()


def _is_egw002_error(response_data: dict | None, msg_cd: str | None = None) -> bool:
    """EGW002 (초당 거래건수 초과) 에러 판별."""
    if msg_cd and str(msg_cd).startswith("EGW002"):
        return True
    if response_data:
        mc = str(response_data.get("msg_cd") or "")
        if mc.startswith("EGW002"):
            return True
        if "초당 거래건수" in str(response_data.get("msg1") or ""):
            return True
    return False


def _egw002_backoff_sleep(attempt: int = 1) -> float:
    """EGW002 발생 시 backoff + jitter 계산 후 sleep. 실제 sleep 시간 반환."""
    base = float(os.getenv("KIS_EGW002_BACKOFF_BASE_SEC", "2.0"))
    cap = float(os.getenv("KIS_EGW002_BACKOFF_MAX_SEC", "10.0"))
    delay = min(cap, base * (2 ** (attempt - 1))) + random.uniform(0, 0.5)
    logger.warning(
        "[KIS][EGW002][BACKOFF] sleep=%.2f attempt=%s base=%.1f cap=%.1f",
        delay, attempt, base, cap,
    )
    time.sleep(delay)
    return delay


TR_MAP = {
    "practice": {
        "ORDER_BUY": [os.getenv("KIS_TR_ID_ORDER_BUY", "VTTC0012U"), "VTTC0802U"],
        "ORDER_SELL": [os.getenv("KIS_TR_ID_ORDER_SELL", "VTTC0011U"), "VTTC0801U"],
        "BALANCE": [os.getenv("KIS_TR_ID_BALANCE", "VTTC8434R")],
        "PRICE": [os.getenv("KIS_TR_ID_PRICE", "FHKST01010100")],
        "ORDERBOOK": [os.getenv("KIS_TR_ID_ORDERBOOK", "VHKST01010200")],
        "DAILY_CHART": [os.getenv("KIS_TR_ID_DAILY_CHART", "FHKST03010100")],
        "INTRADAY_CHART": [os.getenv("KIS_TR_ID_INTRADAY_CHART", "FHKST03010200")],
        "PSBL_ORDER": [os.getenv("KIS_TR_ID_PSBL_ORDER", "VTTC8908R")],
        "DAILY_CCLD": [os.getenv("KIS_TR_ID_DAILY_CCLD", "VTTC8001R")],
        "TOKEN": "/oauth2/tokenP",
    },
    "real": {
        "ORDER_BUY": [os.getenv("KIS_TR_ID_ORDER_BUY_REAL", "TTTC0012U")],
        "ORDER_SELL": [os.getenv("KIS_TR_ID_ORDER_SELL_REAL", "TTTC0011U")],
        "BALANCE": [os.getenv("KIS_TR_ID_BALANCE_REAL", "TTTC8434R")],
        "PRICE": [os.getenv("KIS_TR_ID_PRICE_REAL", "FHKST01010100")],
        "ORDERBOOK": [os.getenv("KIS_TR_ID_ORDERBOOK_REAL", "FHKST01010200")],
        "DAILY_CHART": [os.getenv("KIS_TR_ID_DAILY_CHART_REAL", "FHKST03010100")],
        "INTRADAY_CHART": [os.getenv("KIS_TR_ID_INTRADAY_CHART_REAL", "FHKST03010200")],
        "PSBL_ORDER": [os.getenv("KIS_TR_ID_PSBL_ORDER_REAL", "TTTC8908R")],
        "DAILY_CCLD": [os.getenv("KIS_TR_ID_DAILY_CCLD_REAL", "TTTC8001R")],
        "TOKEN": "/oauth2/token",
    },
}


def _pick_tr(env: str, key: str) -> List[str]:
    try:
        return TR_MAP[env][key]
    except Exception:
        return []


# --- KisAPI 이하 실전 전체 로직 ---
class KisAPI:
    _token_cache = {"token": None, "expires_at": 0, "issued_at": 0}
    _cache_path: Path | None = None
    _token_lock = threading.Lock()

    def should_cooldown(self, now_kst: datetime | None = None) -> bool:
        """
        VWAP / 롤링K 메인 루프에서 '잠깐 쉬어야 하는 구간'을 체크하는 헬퍼.

        지금은 최소 구현 버전:
        - 항상 False를 리턴해서 쿨다운을 사용하지 않는다.
        - 나중에 점심시간 / 장 마감 직전 / 과열 구간 등 세부 로직을 여기로 옮기면 된다.
        """
        return False

    def __init__(self, kis_env: str | None = None, **kwargs):
        if kis_env is None and "env" in kwargs:
            kis_env = kwargs.pop("env")
        self.CANO = _digits_only(safe_strip(CANO))
        self.ACNT_PRDT_CD = _digits_only(safe_strip(ACNT_PRDT_CD))
        self.env = safe_strip(kis_env or KIS_ENV or "practice").lower()
        if self.env not in ("practice", "real"):
            self.env = "practice"

        # [CHG] 세션 생성 → 멤버로 보관
        self.session = _build_session()

        # [NEW] 네트워크 안전 요청 백오프/세션리셋 파라미터
        self._safe_attempts = max(1, _env_int("KIS_HTTP_MAX_RETRIES", 5))
        self._safe_backoff_base = _env_float("KIS_HTTP_BACKOFF_BASE_SEC", 1.2)
        self._safe_backoff_cap = _env_float("KIS_HTTP_BACKOFF_CAP_SEC", 8.0)
        self._safe_max_seconds = _env_float("KIS_HTTP_MAX_SECONDS", 18.0)

        qps = _env_float("KIS_QPS", 5.0)
        min_interval_sec = 1.0 / qps if qps > 0 else 0.20
        self._limiter = _RateLimiter(min_interval_sec=min_interval_sec)
        # [2026-04-30] 데이터/가격/주문별 세분화된 rate limiter
        self._data_limiter = _RateLimiter(
            min_interval_sec=float(os.getenv("KIS_DATA_MIN_INTERVAL_SEC", "0.35"))
        )
        self._price_limiter = _RateLimiter(
            min_interval_sec=float(os.getenv("KIS_PRICE_MIN_INTERVAL_SEC", "0.35"))
        )
        self._order_limiter = _RateLimiter(
            min_interval_sec=float(os.getenv("KIS_ORDER_MIN_INTERVAL_SEC", "0.25"))
        )
        self._last_hashkey_at = 0.0
        self._order_hashkey_gap_sec = float(os.getenv("KIS_ORDER_HASHKEY_GAP_SEC", "0.35"))
        self._concurrency_sem = threading.Semaphore(_env_int("KIS_CONCURRENCY", 2))
        self._recent_sells: Dict[str, float] = {}
        self._recent_sells_lock = threading.Lock()
        self._recent_sells_cooldown = 60.0

        self._last_cash: Optional[int] = None  # ✅ 예수금 캐시(네트워크 실패/0원 응답 대응)
        self._balance_cache: Optional[dict] = None
        self._balance_cache_at: Optional[datetime] = None
        self._orderable_cash_cache: Optional[int] = None
        self._orderable_cash_cache_at: Optional[datetime] = None
        self._orderable_cash_cache_ttl_sec = 5.0
        self.safe_mode = False
        self._load_safe_mode_state()

        self.token = self.get_valid_token()
        meta = self._account_param_meta()
        logger.info(
            "[생성자 체크] CANO=%s ACNT_PRDT_CD=%s ENV=%s",
            meta.get("cano_masked"),
            meta.get("acnt_prdt_cd_masked"),
            self.env,
        )

        self._today_open_cache: Dict[str, Tuple[float, float]] = {}  # code -> (open_price, ts)
        self._today_open_ttl = 60 * 60 * 9  # 9시간 TTL (당일만 유효)

        # Daily chart cache: key -> (data, ts)
        self._daily_chart_cache: Dict[Tuple[str, str, str, str, str], Tuple[List[Dict[str, Any]], float]] = {}
        self._daily_chart_ttl = 10 * 60  # 10분 TTL

        # [NEW] 호가 조회 404 쿨다운 캐시
        self.askbid_unavailable_cache: Dict[str, float] = {}  # code -> unavailable_until_timestamp
        self.askbid_cooldown_sec = int(os.getenv("KIS_ASKBID_COOLDOWN_SEC", "3600"))  # 기본 1시간

    def _rate_limit_safe_enabled(self) -> bool:
        return str(os.getenv("PB1_KIS_RATE_LIMIT_SAFE") or "0") == "1"

    def _wait_before_hashkey(self) -> None:
        if self._rate_limit_safe_enabled():
            self._order_limiter.wait("order-hashkey")

    def _wait_before_order_submit(self) -> None:
        if self._rate_limit_safe_enabled():
            self._order_limiter.wait("orders-safe")
            now = time.time()
            delta = now - float(self._last_hashkey_at or 0.0)
            if delta < float(self._order_hashkey_gap_sec or 0.0):
                sleep_sec = float(self._order_hashkey_gap_sec) - delta + random.uniform(0, 0.03)
                logger.warning(
                    "[KIS][RATE_LIMIT][ORDER_GAP] sleep=%.3f gap=%.3f",
                    sleep_sec,
                    delta,
                )
                time.sleep(sleep_sec)
        self._limiter.wait("orders")

    def _account_param_meta(self) -> dict:
        cano = _digits_only(self.CANO)
        acnt = _digits_only(self.ACNT_PRDT_CD)
        return {
            "env": self.env,
            "cano_len": len(cano),
            "acnt_prdt_cd_len": len(acnt),
            "cano_masked": f"***{cano[-4:]}" if len(cano) >= 4 else "***",
            "acnt_prdt_cd_masked": f"**{acnt[-1:]}" if len(acnt) >= 1 else "**",
        }

    def _validate_account_params(self) -> tuple[bool, str]:
        meta = self._account_param_meta()
        cano_len = int(meta.get("cano_len") or 0)
        acnt_len = int(meta.get("acnt_prdt_cd_len") or 0)
        if cano_len != 8:
            return False, f"invalid_cano_len:{cano_len}"
        if acnt_len != 2:
            return False, f"invalid_acnt_prdt_cd_len:{acnt_len}"
        return True, "ok"

    def _safe_mode_path(self) -> Path:
        path = botstate_path("runtime", "status", "kis_safe_mode.json")
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def _load_safe_mode_state(self) -> None:
        ttl_sec = int(os.getenv("KIS_SAFE_MODE_TTL_SEC", "300") or "300")
        path = self._safe_mode_path()
        if not path.exists():
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            ts_raw = payload.get("ts")
            if not ts_raw:
                return
            ts = datetime.fromisoformat(ts_raw)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=pytz.timezone("Asia/Seoul"))
            age = (now_kst() - ts).total_seconds()
            if age <= ttl_sec:
                self.safe_mode = True
        except Exception:
            logger.warning("[SAFE_MODE][LOAD_FAIL]", exc_info=True)

    def _set_safe_mode(self, *, reason: str, err: Exception | None = None) -> None:
        self.safe_mode = True
        try:
            payload = {
                "ts": now_kst().isoformat(),
                "reason": reason,
                "err": str(err) if err else None,
            }
            path = self._safe_mode_path()
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            logger.warning("[SAFE_MODE][WRITE_FAIL]", exc_info=True)

    def _get_tick_size(self, price: float) -> int:
        """가격에 따른 호가단위(틱사이즈) 반환."""
        tick_table = [
            (1_000_000, 1_000),
            (500_000, 500),
            (100_000, 100),
            (10_000, 50),
            (1_000, 10),
            (100, 1),
            (0, 1),
        ]
        for base, tick in tick_table:
            if price >= base:
                return tick
        return 1

    def _log_kis_resp(self, operation: str, code: str, params: dict, data: dict | None, elapsed_ms: float):
        """Log KIS response for debugging, masking sensitive info."""
        if not data:
            logger.debug("[KIS][%s][FAIL] code=%s elapsed_ms=%.0f", operation, code, elapsed_ms)
            return
        rt_cd = data.get("rt_cd")
        msg1 = data.get("msg1", "")
        # Mask sensitive info if any
        safe_data = {"rt_cd": rt_cd, "msg1": msg1[:100]}  # Truncate long messages
        logger.debug("[KIS][%s][RESP] code=%s rt_cd=%s msg1=%s elapsed_ms=%.0f", operation, code, rt_cd, msg1, elapsed_ms)

    # ===== [NEW] 안전요청 & 세션리셋 =====
    def _reset_session(self):
        try:
            old = self.session
            self.session = _build_session()
            try:
                old.close()
            except Exception:
                pass
            logger.warning("[NET] session reset")
        except Exception as e:
            logger.warning("[NET] session reset failed: %s", e)

    def _safe_request(self, method: str, url: str, *, reset_on_error: bool = True, endpoint: str | None = None, **kwargs) -> requests.Response:
        """
        공통 안전요청 래퍼:
        - DIAG 모드: 주문 차단, 데이터는 ALLOW_KIS_DATA_HTTP_IN_DIAG=1이면 허용
        - MINERVINI_ONLY: FORCE_HTTP=1이 아니면 전체 차단
        - SSLError/일시 오류 시 지수형 백오프 + 세션 리셋 후 재시도
        - 기본 시도 self._safe_attempts
        - [2026-05-01] endpoint별 proactive throttle (inquire_daily_chart 0.7s, order_cash 0.5s, inquire_price 0.4s)
        - [2026-05-01] EGW002 exponential backoff + circuit breaker
        """
        strategy_mode = os.getenv("STRATEGY_MODE", "").upper()
        force_http = os.getenv("FORCE_HTTP", "0").strip() == "1"
        allow_data_http_in_diag_raw = os.getenv("ALLOW_KIS_DATA_HTTP_IN_DIAG", "0").strip()
        allow_data_http_in_diag = kis_data_http_allowed_in_diag()
        caller_route = _resolve_kis_http_caller_route(default="live")
        logger.info(
            "[KIS][HTTP_POLICY][ENV] strategy_mode=%s allow_data_http_in_diag_raw=%s parsed=%s caller=%s endpoint=%s",
            strategy_mode,
            allow_data_http_in_diag_raw or "0",
            int(allow_data_http_in_diag),
            caller_route,
            endpoint or _endpoint_name(url),
        )
        
        # ✅ Proactive endpoint-specific throttling
        if endpoint and self._rate_limit_safe_enabled():
            endpoint_intervals = {
                "inquire_daily_chart": 0.7,
                "order_cash": 0.5,
                "inquire_price": 0.4,
            }
            min_interval = endpoint_intervals.get(endpoint)
            if min_interval:
                with self._limiter._lock:
                    now = time.time()
                    last = self._limiter.last_at.get(endpoint, 0.0)
                    delta = now - last
                    if delta < min_interval:
                        sleep_sec = min_interval - delta + random.uniform(0, 0.05)
                        logger.debug(
                            "[KIS][ENDPOINT_THROTTLE] endpoint=%s sleep=%.3f min_interval=%.3f",
                            endpoint, sleep_sec, min_interval,
                        )
                        time.sleep(sleep_sec)
                    self._limiter.last_at[endpoint] = time.time()
        
        # ✅ MINERVINI_ONLY: FORCE_HTTP 없으면 전체 차단
        from trader.config import MINERVINI_ONLY
        if MINERVINI_ONLY and not force_http:
            logger.warning("[KIS][HTTP_DISABLED] MINERVINI_ONLY mode, endpoint=%s", url)
            
            class _DiagDummyResponse:
                status_code = 200
                text = ""

                @staticmethod
                def json() -> dict:
                    return {"_kis_disabled": True, "rt_cd": "0", "msg1": "KIS_HTTP_DISABLED"}

            return _DiagDummyResponse()
        if not force_http and not kis_http_allowed(url, strategy_mode, allow_data_http_in_diag, caller_route):
            if is_order_endpoint(url):
                raise KISBlockedError(f"KIS API order endpoint blocked in DIAG mode: {method} {url}")
            if kis_explicit_offline_mode():
                logger.warning("[KIS][HTTP_DISABLED] mode=%s endpoint=%s explicit_offline=1", strategy_mode, _endpoint_name(url))

                class _DiagDummyResponse:
                    status_code = 200
                    text = ""

                    @staticmethod
                    def json() -> dict:
                        return {"_kis_disabled": True, "rt_cd": "0", "msg1": "KIS_HTTP_DISABLED"}

                return _DiagDummyResponse()
            raise KISBlockedError(f"KIS API data endpoint blocked in DIAG mode: {method} {url}")
        
        if (os.getenv("DIAG_KIS_CALLS_ENABLED") or "").strip() == "0":
            logger.warning("[NET][DIAG] KIS calls disabled; skipping request method=%s url=%s", method, url)

            class _DiagDummyResponse:
                status_code = 200
                text = ""

                @staticmethod
                def json() -> dict:
                    return {}

            return _DiagDummyResponse()

        attempts = max(self._safe_attempts, 1)
        start_ts = time.monotonic()
        auth_refreshed = False
        reset_done = False
        consecutive_temp_failures = 0

        # Apply rate limiter based on endpoint
        parsed = urlparse(url)
        path = parsed.path
        limiter_key = None
        if "inquire-price" in path:
            limiter_key = "PRICE"
        elif "inquire-daily-itemchartprice" in path:
            limiter_key = "DAILY_CHART"
        elif "inquire-asking-price-exp-ccn" in path:
            limiter_key = "HOGA"
        if limiter_key:
            get_kis_limiter().acquire(limiter_key)

        breaker_open, breaker_until = _breaker_check(method, url)
        if breaker_open:
            logger.warning(
                "[NET:FAST_FAIL] breaker=open method=%s url=%s until=%.0f",
                method,
                url,
                breaker_until or 0,
            )
            raise KisTemporaryError(f"FAST_FAIL breaker open for {url}")
        last_err: Exception | None = None
        for i in range(1, attempts + 1):
            try:
                logger.info("[KIS][FINAL_URL] %s %s", method, url)
                resp = self.session.request(
                    method,
                    url,
                    timeout=kwargs.pop("timeout", (3.0, 7.0)),
                    **kwargs,
                )
                status = resp.status_code
                elapsed_ms = (time.monotonic() - start_ts) * 1000
                params = kwargs.get("params", {}) or {}
                json_data = kwargs.get("json", {}) or {}
                params_masked = sanitize_log_mapping(params)
                json_masked = sanitize_log_mapping(json_data)
                headers_masked = sanitize_headers(kwargs.get("headers", {}) or {})
                if status in (401, 403):
                    logger.warning("[KIS][HTTP_FAIL] method=%s url=%s params=%s json=%s headers=%s status=%s elapsed_ms=%.0f resp_text=%s",
                                   method, url, params_masked, json_masked, headers_masked, status, elapsed_ms, resp.text[:500])
                    if not auth_refreshed and "/oauth2/token" not in url:
                        auth_refreshed = True
                        logger.warning("[NET:AUTH] status=%s url=%s -> refresh token", status, url)
                        self.refresh_token()
                        continue
                    raise KisAuthError(f"HTTP {status} for {url}")
                if status in (429, 500, 502, 503, 504):
                    if status == 429 and "inquire-price" in _endpoint_path(url):
                        _mark_price_rate_limited(
                            _endpoint_name(url),
                            str((kwargs.get("params") or {}).get("fid_input_iscd") or "") or None,
                            "HTTP_429",
                            "too_many_requests",
                        )
                    logger.warning("[KIS][HTTP_FAIL] method=%s url=%s params=%s json=%s headers=%s status=%s elapsed_ms=%.0f resp_text=%s",
                                   method, url, params_masked, json_masked, headers_masked, status, elapsed_ms, resp.text[:500])
                    raise KisTemporaryError(f"HTTP {status} for {url}")
                try:
                    body = resp.json()
                except Exception as json_err:
                    logger.warning("[KIS][HTTP_FAIL] method=%s url=%s params=%s json=%s headers=%s status=%s elapsed_ms=%.0f resp_text=%s json_parse_err=%s",
                                   method, url, params_masked, json_masked, headers_masked, status, elapsed_ms, resp.text[:500], json_err)
                    body = None
                if isinstance(body, dict):
                    msg_cd = str(body.get("msg_cd") or "").strip()
                    msg_text = str(body.get("msg1") or "").lower()
                    rt_cd = str(body.get("rt_cd") or "").strip()
                    
                    # ✅ EGW002 (초당 거래건수 초과) 전용 처리: exponential backoff + circuit breaker
                    if _is_egw002_error(body, msg_cd):
                        _breaker_record_temp_failure(method, url)

                        if "inquire-price" in _endpoint_path(url):
                            _mark_price_rate_limited(
                                _endpoint_name(url),
                                str((kwargs.get("params") or {}).get("fid_input_iscd") or "") or None,
                                msg_cd,
                                str(body.get("msg1") or ""),
                            )
                        logger.error(
                            "[KIS][EGW002][DETECTED] endpoint=%s attempt=%s msg_cd=%s msg1=%s",
                            endpoint or _endpoint_name(url),
                            i,
                            msg_cd,
                            body.get("msg1"),
                        )
                        # Circuit breaker pause (15s)
                        _price_cache.open_circuit(rate_limited=True)
                        # Exponential backoff
                        _egw002_backoff_sleep(attempt=i)
                        if i < attempts:
                            continue
                        raise KisTemporaryError(f"EGW002 msg_cd={msg_cd}")
                    
                    if msg_cd and msg_cd in _KIS_TEMP_ERROR_CODES:
                        if "inquire-price" in _endpoint_path(url):
                            _mark_price_rate_limited(
                                _endpoint_name(url),
                                str((kwargs.get("params") or {}).get("fid_input_iscd") or "") or None,
                                msg_cd,
                                str(body.get("msg1") or ""),
                            )
                            logger.warning(
                                "[KIS][RATE_LIMIT][EGW00201] endpoint=%s code=%s attempt=%s msg_cd=%s msg1=%s",
                                _endpoint_name(url),
                                (kwargs.get("params") or {}).get("fid_input_iscd"),
                                i,
                                msg_cd,
                                body.get("msg1"),
                            )
                        logger.warning("[KIS][HTTP_FAIL] method=%s url=%s params=%s json=%s headers=%s status=%s elapsed_ms=%.0f resp_text=%s rt_cd=%s msg_cd=%s msg1=%s",
                                       method, url, params_masked, json_masked, headers_masked, status, elapsed_ms, resp.text[:500], rt_cd, msg_cd, body.get("msg1"))
                        raise KisTemporaryError(f"BODY_TEMP_ERROR msg_cd={msg_cd}")
                    if any(token in msg_text for token in ("timeout", "tempor", "일시", "오류", "지연", "초당")):
                        if "inquire-price" in _endpoint_path(url):
                            _mark_price_rate_limited(
                                _endpoint_name(url),
                                str((kwargs.get("params") or {}).get("fid_input_iscd") or "") or None,
                                msg_cd,
                                str(body.get("msg1") or ""),
                            )
                            logger.warning(
                                "[KIS][RATE_LIMIT][EGW00201] endpoint=%s code=%s attempt=%s msg_cd=%s msg1=%s",
                                _endpoint_name(url),
                                (kwargs.get("params") or {}).get("fid_input_iscd"),
                                i,
                                msg_cd,
                                body.get("msg1"),
                            )
                        logger.warning("[KIS][HTTP_FAIL] method=%s url=%s params=%s json=%s headers=%s status=%s elapsed_ms=%.0f resp_text=%s rt_cd=%s msg_cd=%s msg1=%s",
                                       method, url, params_masked, json_masked, headers_masked, status, elapsed_ms, resp.text[:500], rt_cd, msg_cd, body.get("msg1"))
                        raise KisTemporaryError("BODY_TEMP_ERROR msg1")
                if 400 <= status < 500:
                    logger.warning("[KIS][HTTP_FAIL] method=%s url=%s params=%s json=%s headers=%s status=%s elapsed_ms=%.0f resp_text=%s",
                                   method, url, params_masked, json_masked, headers_masked, status, elapsed_ms, resp.text[:500])
                    raise KisPermanentError(f"HTTP {status} for {url}")
                _breaker_record_success(method, url)
                consecutive_temp_failures = 0  # Reset on success
                return resp
            except requests.exceptions.SSLError as e:
                logger.warning("[NET:SSL_ERROR] attempt=%s url=%s err=%s", i, url, e)
                _breaker_record_temp_failure(method, url)
                last_err = e
                consecutive_temp_failures += 1
                if reset_on_error and not reset_done and consecutive_temp_failures >= 2:
                    self._reset_session()
                    reset_done = True
            except requests.exceptions.RequestException as e:
                logger.warning("[NET:REQ_ERROR] attempt=%s url=%s err=%s", i, url, e)
                _breaker_record_temp_failure(method, url)
                last_err = e
                consecutive_temp_failures += 1
                if reset_on_error and not reset_done and consecutive_temp_failures >= 2:
                    self._reset_session()
                    reset_done = True
            except KisTemporaryError as e:
                logger.warning("[NET:TEMP_ERROR] attempt=%s url=%s err=%s", i, url, e)
                _breaker_record_temp_failure(method, url)
                last_err = e
                consecutive_temp_failures += 1
                if reset_on_error and not reset_done and consecutive_temp_failures >= 2:
                    self._reset_session()
                    reset_done = True
            except KisAuthError:
                raise
            except KisPermanentError:
                raise
            if time.monotonic() - start_ts >= self._safe_max_seconds:
                break
            if i >= attempts:
                break
            delay = min(self._safe_backoff_cap, self._safe_backoff_base * (2 ** (i - 1)))
            # 500 에러(초당 거래건수 초과) 시 더 긴 백오프
            if "초당" in str(last_err).lower() or isinstance(last_err, KisTemporaryError):
                delay = random.uniform(1.5, 3.0)  # 강력한 백오프 (rate limit 보호)
            jitter = random.uniform(0.0, delay * 0.25)
            sleep_s = delay + jitter
            logger.warning(
                "[KIS][HTTP][RETRY] attempt=%s/%s sleep=%.2f err=%s",
                i,
                attempts,
                sleep_s,
                last_err,
            )
            time.sleep(sleep_s)
        raise KisTemporaryError(f"request failed after retries: {url} err={last_err}")

    @classmethod
    def _resolve_cache_path(cls) -> Path:
        if cls._cache_path is None:
            cls._cache_path = botstate_path("runtime", "kis_token.json")
            cls._cache_path.parent.mkdir(parents=True, exist_ok=True)
        return cls._cache_path

    # ===== 토큰 처리 =====
    def get_valid_token(self):
        with KisAPI._token_lock:
            now = time.time()
            if self._token_cache["token"] and now < self._token_cache["expires_at"] - 300:
                return self._token_cache["token"]

            cache_path = self._resolve_cache_path()
            if cache_path.exists():
                try:
                    with open(cache_path, "r", encoding="utf-8") as f:
                        cache = json.load(f)
                    if "access_token" in cache and now < cache["expires_at"] - 300:
                        issued_at = cache.get("issued_at", cache.get("last_issued", 0))
                        self._token_cache.update({
                            "token": cache["access_token"],
                            "expires_at": cache["expires_at"],
                            "issued_at": issued_at,
                        })
                        logger.info("[토큰캐시] 파일캐시 사용 expires_at=%s", cache["expires_at"])
                        return cache["access_token"]
                except Exception as e:
                    logger.warning(f"[토큰캐시 읽기 실패] {e}")

            if now - self._token_cache["issued_at"] < 61:
                logger.warning("[토큰] 1분 이내 재발급 시도 차단, 기존 토큰 재사용")
                if self._token_cache["token"]:
                    return self._token_cache["token"]
                raise Exception("토큰 발급 제한(1분 1회), 잠시 후 재시도 필요")

            try:
                token, expires_in = self._issue_token_and_expire()
            except Exception as exc:
                msg = str(exc)
                if "1분당 1회" in msg or "1분 1회" in msg or "1 minute" in msg:
                    logger.warning("[토큰] rate limit 감지 -> 65초 대기 후 재시도")
                    time.sleep(65)
                    token, expires_in = self._issue_token_and_expire()
                else:
                    raise
            issued_at = time.time()
            expires_at = issued_at + int(expires_in)
            self._token_cache.update({"token": token, "expires_at": expires_at, "issued_at": issued_at})
            try:
                with open(cache_path, "w", encoding="utf-8") as f:
                    json.dump(
                        {"access_token": token, "expires_at": expires_at, "issued_at": issued_at},
                        f,
                        ensure_ascii=False,
                    )
            except Exception as e:
                logger.warning(f"[토큰캐시 쓰기 실패] {e}")
            logger.info("[토큰캐시] 새 토큰 발급 및 캐시")
            return token

    def _issue_token_and_expire(self):
        strategy_mode = os.getenv("STRATEGY_MODE", "").upper()
        intended_live = os.getenv("INTENDED_LIVE", "0").strip()
        dry_run = os.getenv("DRY_RUN", "")
        token_path = TR_MAP[self.env]["TOKEN"]
        url = f"{API_BASE_URL}{token_path}"
        allow_data_http_in_diag_raw = os.getenv("ALLOW_KIS_DATA_HTTP_IN_DIAG", "0").strip()
        allow_data_http_in_diag = kis_data_http_allowed_in_diag()
        caller_route = _resolve_kis_http_caller_route(default="live")
        logger.info(
            "[KIS][TOKEN_POLICY][ENV] STRATEGY_MODE=%s intended_live=%s dry_run=%s ALLOW_KIS_DATA_HTTP_IN_DIAG_RAW=%s parsed=%s caller=%s",
            strategy_mode,
            intended_live,
            dry_run or "",
            allow_data_http_in_diag_raw or "0",
            int(allow_data_http_in_diag),
            caller_route,
        )
        http_allowed = kis_http_allowed(url, strategy_mode, allow_data_http_in_diag, caller_route)
        if not http_allowed:
            explicit_offline = kis_explicit_offline_mode()
            block_reason = "explicit_offline" if explicit_offline else "http_policy_blocked"
            logger.info(
                "[KIS][TOKEN_POLICY] strategy_mode=%s intended_live=%s dry_run=%s allow_data_http_in_diag=%s http_allowed=%s source=%s explicit_offline=%s caller=%s block_reason=%s",
                strategy_mode,
                intended_live,
                dry_run or "",
                int(allow_data_http_in_diag),
                int(http_allowed),
                "dummy_token" if explicit_offline else "blocked",
                int(explicit_offline),
                caller_route,
                block_reason,
            )
            if explicit_offline:
                return "DIAG_DUMMY_TOKEN", 21600
            raise KISBlockedError("token endpoint blocked by HTTP policy")
        logger.info(
            "[KIS][TOKEN_POLICY] strategy_mode=%s intended_live=%s dry_run=%s allow_data_http_in_diag=%s http_allowed=%s source=real_http caller=%s block_reason=none",
            strategy_mode,
            intended_live,
            dry_run or "",
            int(allow_data_http_in_diag),
            int(http_allowed),
            caller_route,
        )
        headers = {"content-type": "application/json"}
        data = {"grant_type": "client_credentials", "appkey": APP_KEY, "appsecret": APP_SECRET}
        try:
            # [CHG] 안전요청 사용
            resp = self._safe_request("POST", url, json=data, headers=headers)
            j = resp.json()
        except Exception as e:
            logger.error(f"[🔑 토큰발급 예외] {e}")
            raise
        if "access_token" in j:
            logger.info(
                "[🔑 토큰발급] 성공 expires_in=%s body=%s",
                j.get("expires_in", 86400),
                sanitize_log_mapping(j),
            )
            return j["access_token"], j.get("expires_in", 86400)
        logger.error("[🔑 토큰발급 실패] %s", sanitize_log_mapping(j if isinstance(j, dict) else {"error": str(j)}))
        raise Exception(f"토큰 발급 실패: {j.get('error_description', j)}")

    def _headers(self, tr_id: str, hashkey: Optional[str] = None):
        h = {
            "authorization": f"Bearer {self.get_valid_token()}",
            "appkey": APP_KEY,
            "appsecret": APP_SECRET,
            "tr_id": tr_id,
            "custtype": "P",
            "content-type": "application/json; charset=utf-8",
        }
        if hashkey:
            h["hashkey"] = hashkey
        return h

    def refresh_token(self):
        """강제 토큰 재발급: 주문 실패 등에서 재시도 전에 호출."""
        try:
            with KisAPI._token_lock:
                KisAPI._token_cache = {"token": None, "expires_at": 0, "issued_at": 0}
                cache_path = self._resolve_cache_path()
                if cache_path.exists():
                    try:
                        os.remove(cache_path)
                    except Exception:
                        pass
            self.get_valid_token()
            logger.info("[토큰] 강제 재발급 완료")
        except Exception as e:
            logger.error(f"[토큰 재발급 실패] {e}")

    # HashKey
    def _create_hashkey(self, body_dict: dict) -> str:
        url = f"{API_BASE_URL}/uapi/hashkey"
        headers = {
            "content-type": "application/json; charset=utf-8",
            "appkey": APP_KEY,
            "appsecret": APP_SECRET,
        }
        body_str = _json_dumps(body_dict)
        try:
            self._wait_before_hashkey()
            # [CHG] 안전요청 사용
            r = self._safe_request("POST", url, headers=headers, data=body_str.encode("utf-8"))
            j = r.json()
        except Exception as e:
            logger.error(f"[HASHKEY 예외] {e}")
            raise
        hk = j.get("HASH") or j.get("hash") or j.get("hashkey")
        if not hk:
            logger.error(f"[HASHKEY 실패] resp={j}")
            raise Exception(f"HashKey 생성 실패: {j}")
        self._last_hashkey_at = time.time()
        return hk

    # ===== 신규: 예수금/과매수 방지 유틸 =====
    def get_orderable_cash(self, code_hint: str | None = None, price_hint: float | None = None) -> tuple[int, dict]:
        code = safe_strip(code_hint) or "005930"
        cash_meta: dict = {"source": "psbl_order", "raw_fields": {}, "selected_key": None, "clamp_applied": False}
        cash = 0
        cache_stale = False
        try:
            resp = self._inquire_psbl_order(code, price_hint)
            cash, meta = self._parse_cash_from_psbl_order(resp)
            cash_meta.update(meta)
            cash_meta["source"] = "psbl_order"
            raw_fields = cash_meta.get("raw_fields") or {}
            logger.info(
                "[CASH][PSBL] ord_psbl_cash=%s ord_psbl_amt=%s",
                raw_fields.get("ord_psbl_cash"),
                raw_fields.get("ord_psbl_amt"),
            )
        except Exception as e:
            cache_stale = True
            logger.warning("[CASH][PSBL][FAIL] code=%s err=%s", code, e)
            if self._orderable_cash_cache:
                cash = int(self._orderable_cash_cache)
                cash_meta["source"] = "psbl_order_cache"
                logger.warning(
                    "[CASH][PSBL][FALLBACK_CACHE] value=%s age=%.1fs",
                    cash,
                    (now_kst() - (self._orderable_cash_cache_at or now_kst())).total_seconds()
                    if self._orderable_cash_cache_at
                    else -1.0,
                )
        if cash <= 0:
            try:
                j = self.inquire_balance_all()
                out2 = j.get("output2")
                bal_cash, bal_meta = self._parse_cash_from_output2(out2)
                cash = bal_cash
                cash_meta = {"source": "balance_out2", **bal_meta}
                raw_fields = cash_meta.get("raw_fields") or {}
                logger.info(
                    "[CASH][FALLBACK_BALANCE] ord_psbl_cash=%s ord_psbl_amt=%s nrcvb_buy_amt=%s dnca_tot_amt=%s",
                    raw_fields.get("ord_psbl_cash"),
                    raw_fields.get("ord_psbl_amt"),
                    raw_fields.get("nrcvb_buy_amt"),
                    raw_fields.get("dnca_tot_amt"),
                )
            except Exception as e:
                logger.warning("[CASH][FALLBACK_BALANCE][FAIL] %s", e)
        clamp_applied = bool(cash_meta.get("clamp_applied"))
        if cash < 0:
            cash = 0
            clamp_applied = True
        if cash <= 0 and self._last_cash:
            logger.warning("[CASH][ORDERABLE][FALLBACK_LAST] live=%s → use last=%s", cash, self._last_cash)
            cash = self._last_cash
            cash_meta["source"] = f"{cash_meta.get('source', 'unknown')}_cache"
            cache_stale = True
        cash_meta["clamp_applied"] = clamp_applied
        cash_meta.setdefault("raw_fields", {})
        if cash > 0:
            self._last_cash = cash
            if not cache_stale:
                self._orderable_cash_cache = cash
                self._orderable_cash_cache_at = now_kst()
        self._write_orderable_cash_status(
            cache_stale=cache_stale,
            source=str(cash_meta.get("source") or "unknown"),
            value=int(cash),
        )
        logger.info(
            "[CASH][ORDERABLE] value=%s source=%s clamp=%s",
            cash,
            cash_meta.get("source", "unknown"),
            cash_meta.get("clamp_applied"),
        )
        return cash, cash_meta

    def get_orderable_cash_krw(self, force: bool = False) -> int:
        """
        주문가능현금(원) 단일 조회.
        - psbl order endpoint만 사용
        - 짧은 TTL 캐시 적용
        """
        if not force and self._orderable_cash_cache is not None and self._orderable_cash_cache_at is not None:
            age_s = (now_kst() - self._orderable_cash_cache_at).total_seconds()
            if age_s <= self._orderable_cash_cache_ttl_sec:
                logger.info("[CASH][PSBL][CACHE] hit=True age_s=%.2f", age_s)
                return int(self._orderable_cash_cache)
        logger.info("[CASH][PSBL][CACHE] hit=False force=%s", force)
        cash = 0
        cache_stale = False
        try:
            resp = self._inquire_psbl_order("005930", 1000)
            cash, meta = self._parse_cash_from_psbl_order(resp)
            raw_fields = meta.get("raw_fields") or {}
            logger.info(
                "[CASH][PSBL][KRW] ord_psbl_cash=%s ord_psbl_amt=%s",
                raw_fields.get("ord_psbl_cash"),
                raw_fields.get("ord_psbl_amt"),
            )
        except Exception as exc:
            cache_stale = True
            logger.warning("[CASH][PSBL][KRW][FAIL] err=%s", exc)
            if self._orderable_cash_cache is not None:
                cached = int(self._orderable_cash_cache)
                self._write_orderable_cash_status(cache_stale=True, source="psbl_order_cache", value=cached)
                return cached
            self._write_orderable_cash_status(cache_stale=True, source="psbl_order_fail", value=0)
            return 0
        if cash > 0:
            self._orderable_cash_cache = cash
            self._orderable_cash_cache_at = now_kst()
        self._write_orderable_cash_status(
            cache_stale=cache_stale,
            source="psbl_order",
            value=int(cash),
        )
        return int(max(cash, 0))

    def _parse_total_cash_from_output2(self, out2: Any) -> tuple[int, dict]:
        row = None
        if isinstance(out2, list) and out2:
            row = out2[0]
        elif isinstance(out2, dict):
            row = out2
        else:
            return 0, {"raw_fields": {}, "selected_key": None, "clamp_applied": False}

        raw_fields = {
            "dnca_tot_amt": row.get("dnca_tot_amt"),
            "ord_psbl_cash": row.get("ord_psbl_cash"),
            "nrcvb_buy_amt": row.get("nrcvb_buy_amt"),
        }
        total_cash = self._cash_to_int(row.get("dnca_tot_amt"))
        clamp_applied = False
        if total_cash < 0:
            total_cash = 0
            clamp_applied = True
        return total_cash, {"raw_fields": raw_fields, "selected_key": "dnca_tot_amt", "clamp_applied": clamp_applied}

    def get_cash_summary(self) -> dict:
        total_cash = 0
        order_possible = 0
        source_total = "balance_api"
        source_order_possible = "cash_psbl_api"
        try:
            balance = self.get_balance_cached(force=False)
            out2 = balance.get("output2") if isinstance(balance, dict) else None
            total_cash, total_meta = self._parse_total_cash_from_output2(out2)
            logger.info("[CASH][TOTAL] dnca_tot_amt=%s", (total_meta.get("raw_fields") or {}).get("dnca_tot_amt"))
        except Exception as exc:
            logger.warning("[CASH][TOTAL][FAIL] err=%s", exc)
        try:
            order_possible = self.get_orderable_cash_krw(force=False)
        except Exception as exc:
            logger.warning("[CASH][PSBL][SUMMARY_FAIL] err=%s", exc)
        usable = min(int(total_cash), int(order_possible))
        logger.info(
            "[CASH][SUMMARY] TOTAL_CASH_KRW=%s ORDER_POSSIBLE_CASH_KRW=%s usable=%s",
            total_cash,
            order_possible,
            usable,
        )
        return {
            "total_cash_krw": int(total_cash),
            "order_possible_cash_krw": int(order_possible),
            "source_total": source_total,
            "source_order_possible": source_order_possible,
        }

    def get_cash_available_today(self) -> int:
        """
        당일 매수 가능 예수금(가용현금) 반환.
        ✅ 주문가능조회 → 잔고조회 순으로 파싱.
        실패/0원 시 최근 조회값 캐시 사용.
        """
        try:
            cash, _meta = self.get_orderable_cash()
            if cash < 0:
                logger.warning("[CASH_GUARD] 예수금 음수 감지(%s) → 0으로 처리", cash)
                return 0
            return cash
        except Exception as e:
            logger.error(f"[CASH_QUERY_FAIL] 예수금 조회 실패: {e}")
            return int(self._last_cash or 0)

    def _estimate_buy_cost(self, price: float, qty: int,
                           fee_pct: float = 0.00015, tax_pct: float = 0.0) -> int:
        """매수 예상금액(수수료/세금 포함, 반올림)."""
        try:
            price = float(price)
        except Exception:
            price = 0.0
        try:
            qty = int(qty)
        except Exception:
            qty = 0
        gross = price * qty
        fee = gross * max(0.0, float(fee_pct))
        tax = gross * max(0.0, float(tax_pct))
        return int(round(gross + fee + tax))

    def affordable_qty(self, code: str, price: float, req_qty: int,
                       fee_pct: float = 0.00015, tax_pct: float = 0.0) -> int:
        """
        현재 예수금으로 매수 가능한 수량(요청수량 상한).
        price<=0 또는 예수금 0이면 0.
        """
        try:
            price = float(price)
        except Exception:
            price = 0.0
        if price <= 0:
            return 0
        cash = self.get_cash_available_today()
        if cash <= 0:
            return 0

        try:
            max_qty = int(cash // price)
        except Exception:
            max_qty = 0
        max_qty = min(max_qty, int(req_qty) if req_qty else 0)
        if max_qty <= 0:
            return 0

        lo, hi = 0, max_qty
        while lo < hi:
            mid = (lo + hi + 1) // 2
            cost = self._estimate_buy_cost(price, mid, fee_pct, tax_pct)
            if cost <= cash:
                lo = mid
            else:
                hi = mid - 1
        return lo

    # === 시세 ===
    def _inquire_price_once(self, tr_id: str, market_div: str, code_fmt: str) -> Tuple[Optional[float], str]:
        """
        단일 TR/마켓/코드 조합으로 현재가 1회 조회.
        반환: (가격, 오류코드)
        - 성공: (price, "OK")
        - KIS 초당 제한: (None, "RATE_LIMIT")
        - HTTP 오류: (None, "HTTP_FAIL")
        - 파싱 실패: (None, "PARSE_FAIL")
        """
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
        headers = self._headers(tr_id)
        params = {"fid_cond_mrkt_div_code": market_div, "fid_input_iscd": code_fmt}
        try:
            # [CHG] 안전요청 사용
            resp = self._safe_request("GET", url, headers=headers, params=params, timeout=(3.0, 5.0))
            data = resp.json()
        except Exception as e:
            logger.debug("[PRICE_ONCE_EX] %s/%s %s → %s", market_div, code_fmt, tr_id, e)
            return (None, "HTTP_FAIL")

        if resp.status_code != 200 or data.get("rt_cd") != "0":
            msg_cd = data.get("msg_cd", "")
            logger.warning("[PRICE][FAIL] status=%s rt_cd=%s msg_cd=%s msg1=%s resp_text=%s",
                           resp.status_code, data.get("rt_cd"), msg_cd, data.get("msg1"), resp.text[:300])
            # EGW002 또는 초당 거래건수 메시지 체크
            if msg_cd.startswith("EGW002") or "초당 거래건수" in (data.get("msg1") or ""):
                return (None, "RATE_LIMIT")
            return (None, "HTTP_FAIL")
        if "초당 거래건수" in (data.get("msg1") or ""):
            return (None, "RATE_LIMIT")
        if data.get("output"):
            try:
                px = float(data["output"].get("stck_prpr") or 0)
                if px > 0:
                    return (px, "OK")
                return (None, "PARSE_FAIL")
            except Exception:
                return (None, "PARSE_FAIL")
        return (None, "PARSE_FAIL")

    def get_last_price(self, code: str, *, attempts: int = 2) -> float:
        """
        견고한 현재가 조회 - get_price_snapshot()을 사용하여 중복 호출 방지.
        """
        # ✅ DIAG 모드에서 KIS HTTP 차단 시 DB에서 마지막 가격 조회
        if not kis_http_enabled():
            logger.warning("[PRICE][HTTP_DISABLED] mode=%s code=%s → using DB last price", os.getenv("STRATEGY_MODE"), code)
            try:
                c = safe_strip(code).lstrip("A")
                engine = make_engine()
                with engine.connect() as conn:
                    result = conn.execute(
                        sa.select(PRICE_DAILY.c.close)
                        .where(PRICE_DAILY.c.code == c)
                        .order_by(PRICE_DAILY.c.date.desc())
                        .limit(1)
                    )
                    row = result.fetchone()
                    if row and row.close:
                        price = float(row.close)
                        logger.info("[PRICE][DB_FALLBACK] code=%s price=%s", code, price)
                        return price
            except Exception as e:
                logger.warning("[PRICE][DB_FALLBACK_FAIL] code=%s err=%s", code, e)
            # DB 조회 실패 시 기본값 반환 (100000원 - 임의)
            logger.warning("[PRICE][STUB] code=%s → returning stub price 100000", code)
            return 100000.0
        
        c = safe_strip(code)
        if not c:
            raise ValueError(f"Invalid code: {code}")
        
        # get_price_snapshot()을 통해 캐시된 가격 조회
        quote = self.get_price_snapshot(c, market="J")
        
        # prpr 또는 last 값 추출
        price = quote.get("prpr") or quote.get("last")
        if price and price > 0:
            return float(price)
        
        # U 마켓 시도
        quote = self.get_price_snapshot(c, market="U")
        price = quote.get("prpr") or quote.get("last")
        if price and price > 0:
            return float(price)
        
        raise RuntimeError(f"invalid last price 0 for {code}")

    def get_price_quote(self, code: str, *, diag_mode: bool = False, attempts: int = 2) -> dict:
        """
        inquire-price 래퍼: 현재가와 최우선 호가를 dict로 반환.

        반환 예: {"last": 12345.0, "bid": 12340.0, "ask": 12350.0, "raw": {...}, ...}
        diag_mode=True 이면 실패 시 경고만 남기고 빈 dict 반환.
        - 캐시 우선, 게이트 + 스로틀링 적용
        """
        # 캐시 확인
        cache_key = ("inquire-price", code)
        cached = price_cache.get(cache_key)
        if cached:
            return cached

        # 게이트 + 스로틀링
        gate = get_kis_gate()
        if not gate.allow("inquire-price"):
            logger.warning("[PRICE_GATE_BLOCKED] %s", code)
            return {}
        sleep_time = gate.wait_if_needed("inquire-price")
        if sleep_time > 0:
            time.sleep(sleep_time)

        start_time = time.time()
        c = safe_strip(code)
        if not c:
            return {}
        code_variants = [c, f"A{c}"] if not c.startswith("A") else [c, c[1:]]
        markets = ("J", "U")
        base = normalize_base_url(API_BASE_URL)
        url = f"{base}/uapi/domestic-stock/v1/quotations/inquire-price"
        last_error: Exception | None = None
        raw_output: dict | None = None

        def _to_float(val: Any) -> float | None:
            try:
                if val is None:
                    return None
                f = float(val)
                return f if f == f else None  # NaN guard
            except Exception:
                return None

        retry_max = _env_int("KIS_RETRY_MAX", 5)
        base_sleep = _env_float("KIS_RETRY_BASE_SLEEP", 1.2)
        jitter = _env_float("KIS_RETRY_JITTER", 0.5)
        attempts = max(attempts, retry_max)
        for attempt in range(1, attempts + 1):
            with self._concurrency_sem:
                self._limiter.wait("price-quote")
                for tr in _pick_tr(self.env, "PRICE"):
                    headers = self._headers(tr)
                    for market_div in markets:
                        for code_fmt in code_variants:
                            params = {"fid_cond_mrkt_div_code": market_div, "fid_input_iscd": code_fmt}
                            try:
                                resp = self._safe_request("GET", url, headers=headers, params=params, timeout=(3.0, 5.0), endpoint="inquire_price")
                                data = resp.json()
                            except KisTemporaryError as exc:
                                last_error = exc
                                if diag_mode:
                                    logger.warning("[KIS][QUOTE][TEMP_ERROR] diag mode code=%s attempt=%s err=%s", code, attempt, repr(exc))
                                    return {}
                                sleep_time = min(base_sleep ** attempt, 10.0) * (1 + random.uniform(0, jitter))
                                time.sleep(sleep_time)
                                continue
                            except Exception as exc:
                                last_error = exc
                                if diag_mode:
                                    logger.warning("[KIS][QUOTE][WARN] diag mode code=%s attempt=%s err=%s", code, attempt, repr(exc))
                                    return {}
                                continue
                            if "초당 거래건수" in (data.get("msg1") or ""):
                                # Rate limit 초과 시 TEMP_ERROR로 처리
                                last_error = KisTemporaryError("Rate limit exceeded")
                                if diag_mode:
                                    logger.warning("[KIS][QUOTE][RATE_LIMIT] diag mode code=%s attempt=%s", code, attempt)
                                    return {}
                                sleep_time = min(base_sleep ** attempt, 10.0) * (1 + random.uniform(0, jitter))
                                time.sleep(sleep_time)
                                continue
                            if resp.status_code == 200 and data.get("rt_cd") == "0" and data.get("output"):
                                raw_output = dict(data["output"])
                                break
                            else:
                                # Log failure
                                logger.warning("[KIS][QUOTE][FAIL] code=%s status=%s rt_cd=%s msg_cd=%s msg1=%s resp_text=%s",
                                               code, resp.status_code, data.get("rt_cd"), data.get("msg_cd"), data.get("msg1"), resp.text[:300])
                                continue
                        if raw_output is not None:
                            break
                    if raw_output is not None:
                        break
            if raw_output is not None:
                break
            if diag_mode:
                break
            sleep_time = min(base_sleep ** attempt, 10.0) * (1 + random.uniform(0, jitter))
            time.sleep(sleep_time)

        elapsed_ms = (time.time() - start_time) * 1000
        self._log_kis_resp("QUOTE", code, {"attempts": attempts}, raw_output and {"rt_cd": "0", "output": raw_output} or None, elapsed_ms)

        elapsed_ms = (time.time() - start_time) * 1000
        self._log_kis_resp("QUOTE", code, {"attempts": attempts}, raw_output and {"rt_cd": "0", "output": raw_output} or None, elapsed_ms)

        if raw_output is None:
            if diag_mode:
                return {}
            if last_error:
                raise RuntimeError(f"get_price_quote failed for {code}: {last_error}")
            raise RuntimeError(f"get_price_quote failed for {code}")

        last_price = _to_float(raw_output.get("stck_prpr") or raw_output.get("prpr"))
        bid_price = _to_float(raw_output.get("bidp1") or raw_output.get("bidp") or raw_output.get("bid"))
        ask_price = _to_float(raw_output.get("askp1") or raw_output.get("askp") or raw_output.get("ask"))

        # [REMOVED] 호가가 없으면 별도 조회로 보강 - fallback 시 재호출 방지

        quote: dict = {**raw_output}
        quote.setdefault("stck_prpr", last_price)
        quote.setdefault("prpr", last_price)
        quote.update({"last": last_price, "bid": bid_price, "ask": ask_price, "raw": raw_output})
        # 캐시 set
        price_cache.set(cache_key, quote, PRICE_SNAPSHOT_TTL_SEC)
        return quote

    def get_price_only(self, code: str, *, attempts: int = 2) -> dict:
        try:
            price = self.get_last_price(code, attempts=attempts)
        except Exception:
            return {"ask": None, "bid": None, "prpr": None, "fallback_used": "none"}
        return {"ask": None, "bid": None, "prpr": price, "last": price, "fallback_used": "price_only"}

    def get_quote_safe(self, code: str, *, diag_mode: bool = False, attempts: int = 2) -> dict:
        quote: dict | None = None
        reasons = []
        try:
            quote = self.get_price_quote(code, diag_mode=diag_mode, attempts=attempts)
        except Exception as exc:
            reasons.append("HTTP_ERROR")
            if diag_mode:
                logger.warning("[KIS][QUOTE][WARN] code=%s err=%s", code, repr(exc))
                return {"ask": None, "bid": None, "prpr": None, "fallback_used": "none", "reasons": reasons}
            raise
        if not isinstance(quote, dict):
            reasons.append("MISSING_OUTPUT")
            return {"ask": None, "bid": None, "prpr": None, "fallback_used": "none", "reasons": reasons}
        
        ask = quote.get("ask")
        bid = quote.get("bid")
        prpr = quote.get("prpr") or quote.get("stck_prpr") or quote.get("last")
        quote.setdefault("prpr", prpr)
        
        if ask is not None or bid is not None:
            quote["fallback_used"] = "quote"
            return quote
        
        # ask/bid가 None인 경우 reasons 수집
        raw = quote.get("raw", {})
        if not raw:
            reasons.append("MISSING_OUTPUT")
        else:
            # rt_cd 체크
            if str(raw.get("rt_cd", "")) != "0":
                reasons.append("KIS_RT_CD_NOT_0")
            # 필드 존재 체크
            if ask is None and "askp1" not in raw and "askp" not in raw and "ask" not in raw:
                reasons.append("MISSING_FIELDS")
            if bid is None and "bidp1" not in raw and "bidp" not in raw and "bid" not in raw:
                reasons.append("MISSING_FIELDS")
            # 값이 0이나 빈 문자열인지 체크
            for field in ["askp1", "askp", "ask", "bidp1", "bidp", "bid"]:
                val = raw.get(field)
                if val is not None and (val == 0 or val == "" or str(val).strip() == ""):
                    reasons.append("ZERO_OR_BLANK")
                    break
            # 장외/휴장 메시지 체크
            msg1 = str(raw.get("msg1", "")).lower()
            if any(keyword in msg1 for keyword in ["휴장", "장종료", "장전", "단일가", "거래정지"]):
                reasons.append("AFTER_HOURS")
        
        if prpr is not None:
            price_only = self.get_price_only(code, attempts=attempts)
            if price_only.get("prpr") is not None:
                price_only["reasons"] = reasons
                return price_only
        
        return {"ask": None, "bid": None, "prpr": None, "fallback_used": "none", "reasons": reasons}

    def get_quote(self, code: str) -> dict:
        """
        Get quote with last, ask, bid. If ask/bid missing from inquire-price, fallback to inquire-asking-price-exp-ccn.
        Returns: {"last": int, "ask": Optional[int], "bid": Optional[int], "src": "..."}
        """
        c = safe_strip(code)
        if not c:
            raise ValueError("Invalid code")

        # Try inquire-price
        try:
            quote = self.get_price_quote(c, diag_mode=False, attempts=1)
            last = safe_int(quote.get("stck_prpr") or quote.get("prpr") or quote.get("last"))
            ask = safe_int(quote.get("askp1") or quote.get("askp") or quote.get("ask"))
            bid = safe_int(quote.get("bidp1") or quote.get("bidp") or quote.get("bid"))
            if last is not None:
                if ask is not None or bid is not None:
                    return {"last": last, "ask": ask, "bid": bid, "src": "inquire-price"}
                # Fallback to hoga
                try:
                    hoga = self._get_hoga_quote(c)
                    ask = safe_int(hoga.get("askp1"))
                    bid = safe_int(hoga.get("bidp1"))
                    return {"last": last, "ask": ask, "bid": bid, "src": "hoga-fallback"}
                except Exception as e:
                    logger.warning("[QUOTE][HOGA_FAIL] code=%s err=%s", c, e)
                    return {"last": last, "ask": None, "bid": None, "src": "price-only"}
            else:
                raise RuntimeError("No last price")
        except Exception as e:
            logger.warning("[QUOTE][PRICE_FAIL] code=%s err=%s", c, e)
            # Try hoga only
            try:
                hoga = self._get_hoga_quote(c)
                last = safe_int(hoga.get("stck_prpr"))
                ask = safe_int(hoga.get("askp1"))
                bid = safe_int(hoga.get("bidp1"))
                return {"last": last, "ask": ask, "bid": bid, "src": "hoga-only"}
            except Exception as e2:
                logger.warning("[QUOTE][HOGA_FAIL] code=%s err=%s", c, e2)
                raise RuntimeError(f"Failed to get quote for {c}")

    def _get_hoga_quote(self, code: str) -> dict:
        """Get hoga quote using inquire-asking-price-exp-ccn"""
        tr_id = _pick_tr(self.env, "ORDERBOOK")[0]  # FHKST01010200
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn"
        headers = self._headers(tr_id)
        params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}  # Assume J, can adjust
        resp = self._safe_request("GET", url, headers=headers, params=params, timeout=(3.0, 5.0))
        data = resp.json()
        if resp.status_code != 200 or data.get("rt_cd") != "0":
            logger.warning("[HOGA][FAIL] status=%s rt_cd=%s msg_cd=%s msg1=%s resp_text=%s",
                           resp.status_code, data.get("rt_cd"), data.get("msg_cd"), data.get("msg1"), resp.text[:300])
            raise RuntimeError("Hoga API failed")
        return data.get("output", {})

    def get_current_price(self, code: str) -> float:
        """기존 경량 버전(호환용). 내부적으로 get_last_price 사용."""
        return self.get_last_price(code)

    # --- 시초가 캐시 ---
    def _get_cached_today_open(self, code: str) -> Optional[float]:
        try:
            op, ts = self._today_open_cache.get(code, (None, 0.0))
            if op and (time.time() - ts) < self._today_open_ttl:
                return op
        except Exception:
            pass
        return None

    def _set_cached_today_open(self, code: str, price: float):
        try:
            if price and price > 0:
                self._today_open_cache[code] = (float(price), time.time())
        except Exception:
            pass

    def get_today_open(self, code: str) -> Optional[float]:
        """
        오늘 시초가(09:00 기준).
        1순위: inquire-price stck_oprc
        """
        cached = self._get_cached_today_open(code)
        if cached:
            return cached

        base = normalize_base_url(API_BASE_URL)
        url = f"{base}/uapi/domestic-stock/v1/quotations/inquire-price"
        self._limiter.wait("quotes-open")
        for tr in _pick_tr(self.env, "PRICE"):
            headers = self._headers(tr)
            markets = ["J", "U"]
            c = safe_strip(code)
            codes = [c, f"A{c}"] if not c.startswith("A") else [c, c[1:]]
            for market_div in markets:
                for code_fmt in codes:
                    params = {"fid_cond_mrkt_div_code": market_div, "fid_input_iscd": code_fmt}
                    try:
                        # [CHG] 안전요청 사용
                        resp = self._safe_request("GET", url, headers=headers, params=params, timeout=(3.0, 5.0))
                        data = resp.json()
                    except Exception:
                        continue
                    if "초당 거래건수" in (data.get("msg1") or ""):
                        time.sleep(0.35 + random.uniform(0, 0.15))
                        continue
                    if resp.status_code == 200 and data.get("rt_cd") == "0" and data.get("output"):
                        op_str = data["output"].get("stck_oprc")
                        try:
                            op = float(op_str) if op_str is not None else 0.0
                            if op > 0:
                                self._set_cached_today_open(code, op)
                                return op
                        except Exception:
                            pass
        return None

    def get_orderbook_strength(self, code: str) -> Optional[float]:
        base = normalize_base_url(API_BASE_URL)
        url = f"{base}/uapi/domestic-stock/v1/quotations/inquire-asking-price"
        self._limiter.wait("orderbook")
        for tr in _pick_tr(self.env, "ORDERBOOK"):
            headers = self._headers(tr)
            c = code.strip()
            params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": c}
            try:
                # [CHG] 안전요청 사용
                resp = self._safe_request("GET", url, headers=headers, params=params, timeout=(3.0, 5.0))
                data = resp.json()
            except Exception:
                continue
            if resp.status_code == 200 and data.get("rt_cd") == "0" and data.get("output"):
                out = data["output"]
                bid = sum(float(out.get(f"bidp_rsqn{i}") or 0) for i in range(1, 6))
                ask = sum(float(out.get(f"askp_rsqn{i}") or 0) for i in range(1, 6))
                if (bid + ask) > 0:
                    return 100.0 * bid / max(1.0, ask)
        return None

    # === 일봉 ===
    def get_daily_candles(self, code: str, count: int = 30) -> List[Dict[str, Any]]:
        """
        KIS 일봉 조회 (FHKST03010100)
        - 날짜 파라미터(fid_input_date_1, fid_input_date_2) 필수
        - 시장코드 J 고정
        - 종목코드 'A' 접두사 제거(6자리)
        - 0개 → DataEmptyError, 21개 미만 → DataShortError, 네트워크/게이트웨이 → NetTemporaryError
        """
        # ====================================================================
        # [DIAG 방화벽] DIAG 모드에서는 KIS 일봉 API 호출 즉시 차단
        # ====================================================================
        from trader.config import is_diag_mode
        if is_diag_mode():
            logger.error(
                "[KIS][DIAG_BLOCKED] get_daily_candles() called in DIAG mode - symbol=%s count=%s",
                code, count
            )
            raise RuntimeError(
                f"[DIAG_MODE_VIOLATION] KIS daily candles API is forbidden in DIAG mode (symbol={code}). "
                "Use DB or FDR providers only."
            )
        
        # ---- (A) .env 점검: DAILY_CAPITAL 미설정 경고 (함수 최초 1회만) ----
        try:
            global _DAILY_CAP_WARNED
            if not _DAILY_CAP_WARNED:
                if os.getenv("DAILY_CAPITAL") in (None, ""):
                    if (os.getenv("PB1_CAPITAL_MODE") or "CASH").strip().upper() == "CASH":
                        logger.info("[CAPITAL][INFO] DAILY_CAPITAL unused because CAPITAL_MODE=CASH")
                    else:
                        logger.warning(
                            "[ENV] DAILY_CAPITAL 미설정 -> settings/trader.config 기본값(%s) 사용", f"{DEFAULT_DAILY_CAPITAL:,}"
                        )
                _DAILY_CAP_WARNED = True
        except Exception:
            pass

        # ---- (1) 파라미터 구성 ----
        market_code = "J"                         # 시장코드: J 고정
        iscd = code.strip().lstrip("A")          # 종목코드: 'A' 제거(6자리)
        market = MARKET_MAP.get(iscd, "KOSPI")   # 시장 결정

        # 기간: 충분히 넉넉하게(휴장/결측 대비)
        kst = pytz.timezone("Asia/Seoul")
        now_kst = datetime.now(kst)
        to_ymd = now_kst.strftime("%Y%m%d")
        back_days = max(200, count * 4 + 100)
        from_ymd = (now_kst - timedelta(days=back_days)).strftime("%Y%m%d")

        # ---- (2) DB 캐시 조회 ----
        try:
            engine = make_engine()
            with engine.connect() as conn:
                result = conn.execute(
                    sa.select(PRICE_DAILY).where(
                        PRICE_DAILY.c.market == market,
                        PRICE_DAILY.c.code == iscd,
                        PRICE_DAILY.c.date >= from_ymd,
                        PRICE_DAILY.c.date <= to_ymd
                    ).order_by(PRICE_DAILY.c.date)
                )
                db_rows = result.fetchall()
                if len(db_rows) >= count:
                    cached_data = [
                        {
                            "date": row.date.strftime("%Y%m%d"),
                            "open": float(row.open) if row.open else None,
                            "high": float(row.high) if row.high else None,
                            "low": float(row.low) if row.low else None,
                            "close": float(row.close) if row.close else None,
                            "volume": float(row.volume) if row.volume else None,
                        }
                        for row in db_rows
                    ]
                    logger.debug("[DAILY_DB_CACHE_HIT] %s count=%d", iscd, len(cached_data))
                    return cached_data[-count:]
        except Exception as e:
            logger.debug("[DAILY_DB_CACHE_SKIP] %s err=%s", iscd, e)

        # ---- (3) 런타임 캐시 조회 ----
        cache_key = (iscd, from_ymd, to_ymd, "D", "0")
        cached_data, cached_ts = self._daily_chart_cache.get(cache_key, (None, 0))
        if cached_data and (time.time() - cached_ts) < self._daily_chart_ttl:
            logger.debug("[DAILY_CACHE_HIT] %s", iscd)
            return cached_data[-count:] if len(cached_data) > count else cached_data

        # ---- (4) 게이트 확인 및 스로틀링 ----
        gate = get_kis_gate()
        if not gate.allow("inquire-daily"):
            logger.warning("[DAILY_GATE_BLOCKED] %s", iscd)
            raise NetTemporaryError(f"GATE_BLOCKED {iscd}")
        
        # 최소 간격 보장 (스로틀링)
        sleep_time = gate.wait_if_needed("inquire-daily")
        if sleep_time > 0:
            time.sleep(sleep_time)

        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        self._limiter.wait("daily")

        last_err = None
        last_cause = "unknown"
        last_msg_cd = None
        last_msg1 = None

        for tr in _pick_tr(self.env, "DAILY_CHART"):   # TR 후보를 순차적으로 시도
            headers = self._headers(tr)
            headers.setdefault("accept", "*/*")
            headers.setdefault("tr_cont", "N")
            headers.setdefault("Connection", "keep-alive")

            params = {
                "fid_cond_mrkt_div_code": market_code,  # 반드시 'J'
                "fid_input_iscd": iscd,                 # 'A' 없이 6자리
                "fid_input_date_1": from_ymd,           # 시작일(YYYYMMDD)
                "fid_input_date_2": to_ymd,             # 종료일(YYYYMMDD)
                "fid_org_adj_prc": "0",
                "fid_period_div_code": "D",
            }

            try:
                # [CHG] 안전요청 사용 (재시도는 _safe_request에서)
                resp = self._safe_request(
                    "GET", url, headers=headers, params=params, timeout=(3.0, 7.0)
                )
                resp.raise_for_status()
                data = resp.json()
                logger.debug("[DAILY_RAW_JSON] %s TR=%s → %s", iscd, tr, data)
            except KisTemporaryError as e:
                last_err = e
                msg = str(e).lower()
                if "timeout" in msg:
                    last_cause = "timeout"
                elif "connection" in msg:
                    last_cause = "connection_error"
                else:
                    last_cause = "temporary_error"
                logger.warning("[DAILY_TEMP_FAIL] %s TR=%s err=%s", iscd, tr, e)
                continue
            except Exception as e:
                last_err = e
                msg = str(e).lower()
                if "json" in msg:
                    last_cause = "json_decode_fail"
                else:
                    last_cause = "request_exception"
                logger.warning("[DAILY_FAIL] %s TR=%s err=%s", iscd, tr, e)
                continue

            if resp.status_code != 200:
                last_cause = f"http_status_{resp.status_code}"
                last_err = RuntimeError(f"http status {resp.status_code}")
                logger.warning("[KIS][DAILY][FAIL] symbol=%s cause=%s", iscd, last_cause)
                continue

            if not isinstance(data, dict):
                last_cause = "empty_body"
                last_err = RuntimeError("empty body")
                logger.warning("[KIS][DAILY][FAIL] symbol=%s cause=%s", iscd, last_cause)
                continue

            rt_cd = data.get("rt_cd")
            last_msg_cd = data.get("msg_cd")
            last_msg1 = data.get("msg1")
            if rt_cd is not None and str(rt_cd) != "0":
                last_cause = "rt_cd_nonzero"
                last_err = RuntimeError(f"rt_cd_nonzero:{rt_cd}")
                logger.warning(
                    "[KIS][DAILY][FAIL] symbol=%s cause=rt_cd_nonzero msg_cd=%s msg1=%s",
                    iscd,
                    last_msg_cd,
                    last_msg1,
                )
                continue

            arr = data.get("output2") or data.get("output1") or data.get("output")
            if arr is None:
                last_cause = "missing_output_field"
                last_err = RuntimeError("missing output field")
                logger.warning("[KIS][DAILY][FAIL] symbol=%s cause=%s", iscd, last_cause)
                continue

            rows: List[Dict[str, Any]] = []
            for r in arr:
                try:
                    d = r.get("stck_bsop_date")
                    o = r.get("stck_oprc")
                    h = r.get("stck_hgpr")
                    l = r.get("stck_lwpr")
                    c = r.get("stck_clpr")
                    v = r.get("acml_vol") or r.get("stck_vol") or r.get("stck_trqu")
                    vol_val = float(v) if v is not None else None
                    val_val = float(r.get("stck_trqu")) if r.get("stck_trqu") else None
                    if d and o is not None and h is not None and l is not None and c is not None:
                        rows.append({
                            "date": d,
                            "open": float(o),
                            "high": float(h),
                            "low": float(l),
                            "close": float(c),
                            "volume": vol_val,
                            "value": val_val,
                        })
                except Exception as e:
                    logger.debug("[DAILY_ROW_SKIP] %s rec=%s err=%s", iscd, r, e)

            rows.sort(key=lambda x: x["date"])

            if len(rows) == 0:
                last_cause = "empty_body"
                last_err = DataEmptyError(f"A{iscd} 0 candles")
                logger.warning("[KIS][DAILY][FAIL] symbol=%s cause=%s", iscd, last_cause)
                continue
            if len(rows) < 21:
                last_cause = "insufficient_rows"
                last_err = DataShortError(f"A{iscd} {len(rows)} candles (<21)")
                logger.warning("[KIS][DAILY][FAIL] symbol=%s cause=%s", iscd, last_cause)
                continue

            # ---- (4) DB upsert ----
            try:
                engine = make_engine()
                with engine.connect() as conn:
                    for row in rows:
                        conn.execute(
                            sa.insert(PRICE_DAILY).values(
                                market=market,
                                code=iscd,
                                date=row["date"],
                                open=row["open"],
                                high=row["high"],
                                low=row["low"],
                                close=row["close"],
                                volume=row["volume"],
                                value=row["value"],
                                source="KIS"
                            ).on_conflict_do_update(
                                index_elements=["market", "code", "date"],
                                set_={
                                    "open": sa.text("EXCLUDED.open"),
                                    "high": sa.text("EXCLUDED.high"),
                                    "low": sa.text("EXCLUDED.low"),
                                    "close": sa.text("EXCLUDED.close"),
                                    "volume": sa.text("EXCLUDED.volume"),
                                    "value": sa.text("EXCLUDED.value"),
                                    "source": sa.text("EXCLUDED.source"),
                                }
                            )
                        )
                    conn.commit()
                    logger.debug("[DAILY_DB_UPSERT] %s rows=%d", iscd, len(rows))
            except Exception as e:
                logger.debug("[DAILY_DB_UPSERT_SKIP] %s err=%s", iscd, e)

            # Cache the full result
            self._daily_chart_cache[cache_key] = (rows, time.time())

            need = max(count, 21)
            return rows[-need:][-count:]

        if last_err:
            logger.warning(
                "[KIS][DAILY][FAIL] symbol=%s cause=%s msg_cd=%s msg1=%s err=%s",
                iscd,
                last_cause,
                last_msg_cd,
                last_msg1,
                last_err,
            )
        raise NetTemporaryError(f"DAILY_FAIL:{last_cause}")

    def inquire_investor(self, code: str, market: str = "KOSDAQ") -> dict:
        """주체수급 조회(inquire-investor) — 실패 시에도 예외를 던지지 않는다."""
        iscd = code.strip().lstrip("A")
        # FID_COND_MRKT_DIV_CODE는 시장(KOSPI/KOSDAQ) 코드가 아니라 상품군 코드(J=주식/ETF/ETN, W=ELW 등)로
        # 쓰이는 사례가 많다. 주식/ETF/ETN 기본값 "J"를 사용하고, 매핑에 W가 명시된 경우에만 W로 전송한다.
        mapped = MARKET_MAP.get(iscd)
        market_code = mapped if mapped in ("J", "W") else "J"
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-investor"
        headers = self._headers("FHKST01010900")
        params = {"FID_COND_MRKT_DIV_CODE": market_code, "FID_INPUT_ISCD": iscd}

        def _safe_num(val: Any) -> int:
            try:
                if val is None:
                    return 0
                if isinstance(val, (int, float)):
                    return int(val)
                return int(str(val).replace(",", ""))
            except Exception:
                return 0

        attempts = max(1, int(SUBJECT_FLOW_RETRY) + 1)
        timeout = (SUBJECT_FLOW_TIMEOUT_SEC, SUBJECT_FLOW_TIMEOUT_SEC + 0.5)

        for attempt in range(1, attempts + 1):
            try:
                self._limiter.wait("investor")
                resp = self._safe_request(
                    "get",
                    url,
                    headers=headers,
                    params=params,
                    timeout=timeout,
                )
                data = resp.json()
                output = data.get("output") or data.get("OutBlock_1") or data.get("outblock")
                if isinstance(output, list):
                    output = output[0] if output else {}
                if not isinstance(output, dict):
                    raise ValueError(f"unexpected output type: {type(output)}")
                if not output:
                    raise ValueError(f"empty output: {data}")

                inv = {
                    "prsn_ntby_tr_pbmn": _safe_num(output.get("prsn_ntby_tr_pbmn")),
                    "frgn_ntby_tr_pbmn": _safe_num(output.get("frgn_ntby_tr_pbmn")),
                    "orgn_ntby_tr_pbmn": _safe_num(output.get("orgn_ntby_tr_pbmn")),
                }
                for key in ("prsn_ntby_qty", "frgn_ntby_qty", "orgn_ntby_qty"):
                    if key in output:
                        inv[key] = _safe_num(output.get(key))
                return {"ok": True, "inv": inv}
            except Exception as e:
                logger.info("[INVESTOR_FAIL] %s attempt=%s err=%s", code, attempt, e)
                if attempt >= attempts:
                    return {"ok": False, "error": str(e), "inv": None}
                time.sleep(0.2 * (2 ** (attempt - 1)))

    def get_price_snapshot(self, code: str, market: str = "J") -> dict:
        """
        Single source of truth for inquire-price.
        - rate limited
        - ttl cached
        - inflight dedup
        - circuit breaker on EGW002
        Returns dict with keys: ask, bid, prpr, last, raw (may be empty on error).
        """
        key = (market, code)

        # 1) circuit open: do not hammer
        if _price_cache.is_circuit_open():
            logger.warning("[PRICE][CIRCUIT_OPEN] skip inquire-price key=%s until=%.0f", key, _price_cache.circuit_until)
            cached = _price_cache.get_cached(key)
            return cached or {}

        # 2) ttl cache
        cached = _price_cache.get_cached(key)
        if cached is not None:
            _price_cache.record_cache_hit()
            logger.info("[KIS][PRICE_CACHE][HIT] code=%s market=%s ttl=%ss", code, market, _PRICE_TTL_SEC)
            return cached
        _price_cache.record_cache_miss()
        logger.info("[KIS][PRICE_CACHE][MISS] code=%s market=%s", code, market)

        # 3) inflight dedup
        ev = _price_cache.begin_inflight(key)
        if ev is not None:
            data = _price_cache.wait_inflight(key, ev)
            if data:
                logger.info("[PRICE][INFLIGHT_JOIN] code=%s", code)
            return data

        # leader does the call
        try:
            # 4) global rate limit
            sleep_s = _price_rl.acquire()
            if sleep_s > 0:
                _price_cache.record_retry_wait()
                logger.debug("[PRICE][RATE_WAIT] code=%s sleep=%.2fs", code, sleep_s)
                time.sleep(sleep_s)
            if _PRICE_JITTER_MAX_SEC > 0:
                time.sleep(random.uniform(0.0, _PRICE_JITTER_MAX_SEC))

            # 5) 기존 get_price_quote 호출
            data = self.get_price_quote(code, diag_mode=False, attempts=2)

            # 6) EGW002 guard (초당 제한)
            msg_cd = str(data.get("msg_cd", ""))
            rt_cd = str(data.get("rt_cd", ""))
            msg1 = str(data.get("msg1", ""))

            if _is_egw002_error(data, msg_cd):
                logger.warning(
                    "[KIS][EGW002][BACKOFF] endpoint=inquire-price code=%s msg_cd=%s msg1=%s attempt=1",
                    code, msg_cd, msg1[:80],
                )
                _price_cache.open_circuit(rate_limited=True)
                _egw002_backoff_sleep(attempt=1)
            elif rt_cd != "0" and msg_cd.startswith("EGW002"):
                logger.warning("[PRICE][RATE_LIMITED] code=%s msg_cd=%s -> open circuit %ss", code, msg_cd, _PRICE_CIRCUIT_SEC)
                _price_cache.open_circuit(rate_limited=True)
            elif "초당 거래건수" in msg1:
                logger.warning("[PRICE][RATE_LIMITED] code=%s msg1=%s -> open circuit %ss", code, msg1, _PRICE_CIRCUIT_SEC)
                _price_cache.open_circuit(rate_limited=True)

            _price_cache.set_cached(key, data or {})
            return data or {}

        except Exception as exc:
            logger.warning("[PRICE][EXCEPTION] code=%s err=%s", code, repr(exc))
            # 예외 발생 시 빈 dict 반환
            empty = {}
            _price_cache.set_cached(key, empty)
            return empty

        finally:
            _price_cache.finish_inflight(key, _price_cache.get_cached(key) or {})

    # === ATR ===
    def get_atr(self, code: str, window: int = 14) -> Optional[float]:
        try:
            candles = self.get_daily_candles(code, count=window + 2)
            if len(candles) < window + 1:
                return None
            trs: List[float] = []
            for i in range(1, len(candles)):
                h = candles[i]["high"]
                l = candles[i]["low"]
                c_prev = candles[i - 1]["close"]
                tr = max(h - l, abs(h - c_prev), abs(l - c_prev))
                trs.append(tr)
            if not trs:
                return None
            return sum(trs[-window:]) / float(window)
        except Exception as e:
            logger.warning(f"[ATR] 계산 실패 code={code}: {e}")
            return None

    def fetch_daily_ohlcv(self, symbol: str, days: int = 30, **kwargs):
        """Backward-compatible alias for legacy callers expecting DataFrame OHLCV."""
        candles = self.get_daily_candles(symbol, count=days)
        try:
            import pandas as pd
            if not candles:
                return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
            return pd.DataFrame(candles)
        except Exception:
            return candles

    def get_intraday_candles_today(self, code: str, start_hhmm: str = "090000") -> List[Dict[str, Any]]:
        """KIS 주식당일분봉조회 (FHKST03010200 / inquire-time-itemchartprice)
        - FID_COND_MRKT_DIV_CODE: 'J'
        - FID_INPUT_ISCD: 6자리 종목코드('A' 제거)
        - FID_INPUT_HOUR_1: 시작 시간(HHMMSS), 예: '090000'
        - FID_PW_DATA_INCU_YN: 'Y'
        - FID_ETC_CLS_CODE: ''
        """
        market_code = "J"
        iscd = code.strip().lstrip("A")

        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
        self._limiter.wait("intraday")

        last_err = None

        for tr in _pick_tr(self.env, "INTRADAY_CHART"):
            headers = self._headers(tr)
            headers.setdefault("accept", "*/*")
            headers.setdefault("tr_cont", "N")
            headers.setdefault("Connection", "keep-alive")

            params = {
                "fid_cond_mrkt_div_code": market_code,
                "fid_input_iscd": iscd,
                "fid_input_hour_1": start_hhmm,
                "fid_pw_data_incu_yn": "Y",
                "fid_etc_cls_code": "",
            }

            for attempt in range(1, 4):
                try:
                    resp = self._safe_request(
                        "GET", url, headers=headers, params=params, timeout=(3.0, 7.0)
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    logger.debug("[INTRADAY_RAW_JSON] %s TR=%s attempt=%d → %s", iscd, tr, attempt, data)
                except requests.exceptions.SSLError as e:
                    last_err = e
                    logger.warning("[NET:SSL_ERROR] INTRADAY %s attempt=%s %s", iscd, attempt, e)
                    time.sleep(0.4 * attempt)
                    continue
                except requests.exceptions.RequestException as e:
                    last_err = e
                    logger.warning("[NET:REQ_ERROR] INTRADAY %s attempt=%s %s", iscd, attempt, e)
                    time.sleep(0.4 * attempt)
                    continue
                except ValueError as e:
                    last_err = e
                    logger.warning("[NET:JSON_DECODE] INTRADAY %s attempt=%s %s", iscd, attempt, e)
                    time.sleep(0.35 + random.uniform(0, 0.15))
                    continue
                except Exception as e:
                    last_err = e
                    logger.warning("[NET:UNEXPECTED] INTRADAY %s attempt=%s %s", iscd, attempt, e)
                    time.sleep(0.4 * attempt)
                    continue

                if "초당 거래건수" in str(data.get("msg1") or ""):
                    time.sleep(0.35 + random.uniform(0, 0.15))
                    continue

                arr = data.get("output2") or []
                if resp.status_code == 200 and arr:
                    rows: List[Dict[str, Any]] = []
                    for r in arr:
                        try:
                            hhmmss = r.get("stck_cntg_hour")
                            price = r.get("stck_prpr")
                            vol = r.get("cntg_vol")
                            if hhmmss and price is not None and vol is not None:
                                rows.append({
                                    "time": str(hhmmss),
                                    "price": float(price),
                                    "volume": float(vol),
                                })
                        except Exception as e:
                            logger.debug("[INTRADAY_ROW_SKIP] %s rec=%s err=%s", iscd, r, e)

                    rows.sort(key=lambda x: x["time"])
                    if len(rows) == 0:
                        raise DataEmptyError(f"A{iscd} 0 intraday candles")
                    return rows

                last_err = RuntimeError(
                    f"BAD_RESP rt_cd={data.get('rt_cd')} msg={data.get('msg1')}"
                )
                logger.warning("[INTRADAY_BAD_RESP] %s %s", iscd, data)
                time.sleep(0.4 + random.uniform(0, 0.2))

        if last_err:
            raise last_err
        raise RuntimeError(f"INTRADAY_FAIL A{iscd}")

    def get_vwap_today(self, code: str, start_hhmm: str = "090000") -> float | None:
        """당일 분봉 기준 체결 가격/거래량으로 단순 VWAP 계산."""
        try:
            candles = self.get_intraday_candles_today(code, start_hhmm=start_hhmm)
        except DataEmptyError:
            return None
        except Exception as e:
            logger.warning("[VWAP_FAIL] %s %s", code, e)
            return None

        total_vol = 0.0
        total_tr = 0.0
        for c in candles:
            try:
                v = float(c.get("volume") or 0.0)
                p = float(c.get("price") or 0.0)
            except Exception:
                continue
            if v <= 0 or p <= 0:
                continue
            total_vol += v
            total_tr += v * p

        if total_vol <= 0:
            return None
        return total_tr / total_vol

    def is_market_open(self) -> bool:
        kst = pytz.timezone("Asia/Seoul")
        now = datetime.now(kst)
        if now.weekday() >= 5:
            return False
        open_time = now.replace(hour=9, minute=0, second=0, microsecond=0)
        close_time = now.replace(hour=15, minute=20, second=0, microsecond=0)
        return open_time <= now <= close_time

    # ===== Diagnostics-safe fetchers =====
    def safe_get_daily_candles(self, code: str, count: int = 60) -> List[Dict[str, Any]]:
        try:
            return self.get_daily_candles(code, count=count)
        except Exception as e:
            logger.warning("[DIAG][FETCH] symbol=%s kind=%s error=%s", code, "daily", str(e))
            return []

    def safe_get_intraday_bars(self, code: str, interval: str = "1m") -> List[Dict[str, Any]]:
        try:
            # interval currently unused; KIS only supports 1m intraday endpoint here
            return self.get_intraday_candles_today(code)
        except Exception as e:
            logger.warning("[DIAG][FETCH] symbol=%s kind=%s error=%s", code, "intraday", str(e))
            return []

    def safe_get_prev_close(self, code: str) -> Optional[float]:
        try:
            candles = self.get_daily_candles(code, count=2)
            if candles:
                return float(candles[-1].get("close") or 0.0)
        except Exception as e:
            logger.warning("[DIAG][FETCH] symbol=%s kind=%s error=%s", code, "prev_close", str(e))
        return None

    def safe_compute_vwap(self, intraday_bars: List[Dict[str, Any]]) -> Optional[float]:
        total_vol = 0.0
        total_tr = 0.0
        for bar in intraday_bars or []:
            try:
                vol = float(bar.get("volume") or bar.get("cntg_vol") or 0.0)
                price = float(bar.get("price") or bar.get("stck_prpr") or 0.0)
            except Exception:
                continue
            if vol <= 0 or price <= 0:
                continue
            total_vol += vol
            total_tr += vol * price
        if total_vol <= 0:
            return None
        return total_tr / total_vol

    # ===== 보조 시세/지수/스냅샷 =====
    def get_close_price(self, code: str) -> Optional[float]:
        """최근 일봉 종가(전일 또는 당일 종가) → 실패 시 현재가 폴백."""
        try:
            candles = self.get_daily_candles(code, count=30)
            if candles:
                return float(candles[-1]["close"])
        except Exception as e:
            logger.warning(f"[get_close_price] fail {code}: {e}")
        try:
            return float(self.get_last_price(code))
        except Exception:
            return None

    def get_prev_close(self, code: str) -> Optional[float]:
        """전일 종가."""
        try:
            candles = self.get_daily_candles(code, count=30)
            if len(candles) >= 2:
                return float(candles[-2]["close"])
        except Exception as e:
            logger.warning(f"[get_prev_close] fail {code}: {e}")
        return None

    def get_quote_snapshot(self, code: str) -> Dict[str, Any]:
        """
        간이 스냅샷: 현재가 및 최우선 호가를 묶어서 제공.
        반환 예: {'tp': 12345.0, 'ap': 12350.0, 'bp': 12340.0, 'close': 12345.0}
        ✅ 내부적으로 get_price_snapshot을 사용하여 중복 호출 방지
        """
        out: Dict[str, Any] = {}
        try:
            # ✅ 단일 스냅샷 조회
            snapshot = self.get_price_snapshot(code, market="J")
            tp = snapshot.get("last") or snapshot.get("prpr") or snapshot.get("stck_prpr")
            ap = snapshot.get("ask") or snapshot.get("askp1") or snapshot.get("askp")
            bp = snapshot.get("bid") or snapshot.get("bidp1") or snapshot.get("bidp")
            
            out["tp"] = float(tp) if tp else None
            out["ap"] = float(ap) if ap else None
            out["bp"] = float(bp) if bp else None
            out["close"] = out.get("tp")
        except Exception as exc:
            logger.warning("[QUOTE_SNAPSHOT][FAIL] code=%s err=%s", code, repr(exc))
            out["tp"], out["ap"], out["bp"], out["close"] = None, None, None, None
        return out

    def get_best_ask(self, code: str) -> Optional[float]:
        """최우선 매도호가(askp1)."""
        # [NEW] 쿨다운 캐시 체크
        now = time.time()
        if code in self.askbid_unavailable_cache:
            until = self.askbid_unavailable_cache[code]
            if now < until:
                remaining_sec = int(until - now)
                logger.info("[ASKBID][SKIP] code=%s remaining_sec=%d", code, remaining_sec)
                # Fallback to current price
                quote = self.get_price_quote(code, diag_mode=True)
                if quote and quote.get("last"):
                    prpr = quote["last"]
                    tick_size = self._get_tick_size(prpr)
                    pseudo_ask = prpr + tick_size
                    return pseudo_ask
                return None
            else:
                del self.askbid_unavailable_cache[code]

        start_time = time.time()
        base = normalize_base_url(API_BASE_URL)
        url = f"{base}/uapi/domestic-stock/v1/quotations/inquire-asking-price"
        self._limiter.wait("orderbook-best")
        resp_data = None
        for tr in _pick_tr(self.env, "ORDERBOOK"):
            headers = self._headers(tr)
            c = code.strip()
            params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": c}
            try:
                # [CHG] 안전요청 사용
                resp = self._safe_request(
                    "GET", url, headers=headers, params=params, timeout=(3.0, 5.0)
                )
                data = resp.json()
                resp_data = data
            except Exception:
                continue
            if resp.status_code == 404:
                # [NEW] 404 영구 실패로 캐시 등록
                self.askbid_unavailable_cache[code] = time.time() + self.askbid_cooldown_sec
                logger.warning("[ASKBID][COOLDOWN] code=%s until=%s reason=404", code, datetime.fromtimestamp(self.askbid_unavailable_cache[code]).strftime('%Y-%m-%d %H:%M:%S'))
                quote = self.get_price_quote(code, diag_mode=True)
                if quote and quote.get("last"):
                    prpr = quote["last"]
                    tick_size = self._get_tick_size(prpr)
                    pseudo_ask = prpr + tick_size
                    logger.warning("[ASKBID][FALLBACK] code=%s prpr=%.0f tick_size=%d -> ask=%.0f", code, prpr, tick_size, pseudo_ask)
                    return pseudo_ask
                return None
            elif resp.status_code == 200 and data.get("rt_cd") == "0" and data.get("output"):
                elapsed_ms = (time.time() - start_time) * 1000
                self._log_kis_resp("ASKBID", code, params, data, elapsed_ms)
                try:
                    return float(data["output"].get("askp1"))
                except Exception:
                    return None
            # else continue
        elapsed_ms = (time.time() - start_time) * 1000
        self._log_kis_resp("ASKBID", code, params if 'params' in locals() else {}, resp_data, elapsed_ms)
        # [PATCH] Fallback to current price
        logger.warning("[ASKBID][FALLBACK] code=%s -> trying current price", code)
        quote = self.get_price_quote(code, diag_mode=True)
        if quote and quote.get("last"):
            prpr = quote["last"]
            tick_size = self._get_tick_size(prpr)
            pseudo_ask = prpr + tick_size
            logger.warning("[ASKBID][FALLBACK] code=%s prpr=%.0f tick_size=%d -> ask=%.0f", code, prpr, tick_size, pseudo_ask)
            return pseudo_ask
        return None

    def get_best_bid(self, code: str) -> Optional[float]:
        """최우선 매수호가(bidp1)."""
        # [NEW] 쿨다운 캐시 체크
        now = time.time()
        if code in self.askbid_unavailable_cache:
            until = self.askbid_unavailable_cache[code]
            if now < until:
                remaining_sec = int(until - now)
                logger.info("[ASKBID][SKIP] code=%s remaining_sec=%d", code, remaining_sec)
                # Fallback to current price
                quote = self.get_price_quote(code, diag_mode=True)
                if quote and quote.get("last"):
                    prpr = quote["last"]
                    pseudo_bid = prpr
                    return pseudo_bid
                return None
            else:
                del self.askbid_unavailable_cache[code]

        start_time = time.time()
        base = normalize_base_url(API_BASE_URL)
        url = f"{base}/uapi/domestic-stock/v1/quotations/inquire-asking-price"
        self._limiter.wait("orderbook-best")
        resp_data = None
        for tr in _pick_tr(self.env, "ORDERBOOK"):
            headers = self._headers(tr)
            c = code.strip()
            params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": c}
            try:
                # [CHG] 안전요청 사용
                resp = self._safe_request(
                    "GET", url, headers=headers, params=params, timeout=(3.0, 5.0)
                )
                data = resp.json()
                resp_data = data
            except Exception:
                continue
            if resp.status_code == 404:
                # [NEW] 404 영구 실패로 캐시 등록
                self.askbid_unavailable_cache[code] = time.time() + self.askbid_cooldown_sec
                logger.warning("[ASKBID][COOLDOWN] code=%s until=%s reason=404", code, datetime.fromtimestamp(self.askbid_unavailable_cache[code]).strftime('%Y-%m-%d %H:%M:%S'))
                quote = self.get_price_quote(code, diag_mode=True)
                if quote and quote.get("last"):
                    prpr = quote["last"]
                    pseudo_bid = prpr
                    logger.warning("[ASKBID][FALLBACK] code=%s prpr=%.0f -> bid=%.0f", code, prpr, pseudo_bid)
                    return pseudo_bid
                return None
            elif resp.status_code == 200 and data.get("rt_cd") == "0" and data.get("output"):
                elapsed_ms = (time.time() - start_time) * 1000
                self._log_kis_resp("ASKBID", code, params, data, elapsed_ms)
                try:
                    return float(data["output"].get("bidp1"))
                except Exception:
                    return None
            # else continue
        elapsed_ms = (time.time() - start_time) * 1000
        self._log_kis_resp("ASKBID", code, params if 'params' in locals() else {}, resp_data, elapsed_ms)
        # [PATCH] Fallback to current price
        logger.warning("[ASKBID][FALLBACK] code=%s -> trying current price", code)
        quote = self.get_price_quote(code, diag_mode=True)
        if quote and quote.get("last"):
            prpr = quote["last"]
            pseudo_bid = prpr
            logger.warning("[ASKBID][FALLBACK] code=%s prpr=%.0f -> bid=%.0f", code, prpr, pseudo_bid)
            return pseudo_bid
        return None

    def get_index_quote(self, index_code: str) -> Dict[str, Optional[float]]:
        """(간이) 지수 스냅샷 placeholder."""
        return {"price": None, "prev_close": None, "vwap": None}

    # ----- 잔고/포지션 -----
    def _normalize_cash_value(self, x: Any) -> str:
        s = safe_strip(x)
        lower = s.lower()
        if lower in {"", "none", "null", "nan"}:
            return ""
        for ch in (",", " ", "_", "+"):
            s = s.replace(ch, "")
        return s

    def _cash_to_int(self, x: Any) -> int:
        try:
            normalized = self._normalize_cash_value(x)
            if normalized == "":
                return 0
            return safe_int(normalized)
        except Exception:
            return 0

    def _parse_cash_from_output2(self, out2: Any) -> tuple[int, dict]:
        """
        ✅ 예수금 파싱 규칙:
        1) ord_psbl_cash (주문가능현금)
        2) nrcvb_buy_amt (매수가능금액)
        3) dnca_tot_amt  (예수금 총액; 결제미수 포함 가능)
        """

        row = None
        if isinstance(out2, list) and out2:
            row = out2[0]
        elif isinstance(out2, dict):
            row = out2
        else:
            return 0, {"raw_fields": {}, "selected_key": None, "clamp_applied": False}

        raw_fields = {
            "ord_psbl_cash": row.get("ord_psbl_cash"),
            "ord_psbl_amt": row.get("ord_psbl_amt"),
            "nrcvb_buy_amt": row.get("nrcvb_buy_amt"),
            "dnca_tot_amt": row.get("dnca_tot_amt"),
        }
        selected_key = None
        cash = 0
        for key in ("ord_psbl_cash", "ord_psbl_amt", "nrcvb_buy_amt", "dnca_tot_amt"):
            if key in row:
                val = self._cash_to_int(row.get(key))
                if val > 0:
                    selected_key = key
                    cash = val
                    break
        
        # ✅ 파싱 실패 시 dnca_tot_amt로 fallback
        if cash <= 0 and "dnca_tot_amt" in row:
            dnca_val = self._cash_to_int(row.get("dnca_tot_amt"))
            if dnca_val > 0:
                logger.warning(
                    "[BALANCE][FALLBACK] ord_psbl_cash parsing failed -> using dnca_tot_amt=%s",
                    dnca_val
                )
                cash = dnca_val
                selected_key = "dnca_tot_amt_fallback"
        
        clamp_applied = False
        if cash <= 0 and selected_key is None and "dnca_tot_amt" in row:
            raw_dnca = str(row.get("dnca_tot_amt") or "")
            if raw_dnca.strip().startswith("-"):
                selected_key = "dnca_tot_amt"
                clamp_applied = True
        if cash < 0:
            cash = 0
            clamp_applied = True
        
        if cash == 0:
            logger.warning(
                "[BALANCE][PARSE_ZERO] all cash fields are zero or missing. raw_fields=%s",
                raw_fields
            )
        
        return cash, {"raw_fields": raw_fields, "selected_key": selected_key, "clamp_applied": clamp_applied}

    def _parse_cash_from_psbl_order(self, resp: Any) -> tuple[int, dict]:
        # ✅ 디버깅: 응늵 구조 로깅
        if isinstance(resp, dict):
            logger.info("[PSBL][RAW_KEYS] top_level_keys=%s", list(resp.keys()))
            if "output" in resp:
                logger.info("[PSBL][RAW_SAMPLE][output] %s", str(resp.get("output"))[:300])
            if "output1" in resp:
                logger.info("[PSBL][RAW_SAMPLE][output1] %s", str(resp.get("output1"))[:300])
            if "output2" in resp:
                logger.info("[PSBL][RAW_SAMPLE][output2] %s", str(resp.get("output2"))[:300])
        
        row = None
        if isinstance(resp, dict):
            if isinstance(resp.get("output"), dict):
                row = resp.get("output")
            elif isinstance(resp.get("output1"), list) and resp.get("output1"):
                row = resp.get("output1")[0]
            elif isinstance(resp.get("output1"), dict):
                row = resp.get("output1")
            elif isinstance(resp.get("output2"), dict):
                row = resp.get("output2")
            elif isinstance(resp.get("output2"), list) and resp.get("output2"):
                row = resp.get("output2")[0]
            else:
                row = resp
        elif isinstance(resp, list) and resp:
            row = resp[0]
        else:
            logger.warning("[PSBL][PARSE_FAIL] unexpected resp type=%s", type(resp))
            return 0, {"raw_fields": {}, "selected_key": None, "clamp_applied": False}

        raw_fields = {
            "ord_psbl_cash": row.get("ord_psbl_cash"),
            "ord_psbl_amt": row.get("ord_psbl_amt"),
            "nrcvb_buy_amt": row.get("nrcvb_buy_amt"),
            "dnca_tot_amt": row.get("dnca_tot_amt"),
        }
        
        selected_key = None
        cash = 0
        for key in ("ord_psbl_cash", "ord_psbl_amt", "nrcvb_buy_amt", "dnca_tot_amt"):
            if key in row:
                val = self._cash_to_int(row.get(key))
                if val > 0:
                    selected_key = key
                    cash = val
                    break
        
        # ✅ 파싱 실패 시 dnca_tot_amt로 fallback
        if cash <= 0 and "dnca_tot_amt" in row:
            dnca_val = self._cash_to_int(row.get("dnca_tot_amt"))
            if dnca_val > 0:
                logger.warning(
                    "[PSBL][FALLBACK] ord_psbl_cash/ord_psbl_amt parsing failed -> using dnca_tot_amt=%s",
                    dnca_val
                )
                cash = dnca_val
                selected_key = "dnca_tot_amt_fallback"
        
        clamp_applied = False
        if cash < 0:
            cash = 0
            clamp_applied = True
        
        if cash == 0:
            logger.warning(
                "[PSBL][PARSE_ZERO] all cash fields are zero or missing. raw_fields=%s",
                raw_fields
            )
        
        return cash, {"raw_fields": raw_fields, "selected_key": selected_key, "clamp_applied": clamp_applied}

    def _write_orderable_cash_status(self, *, cache_stale: bool, source: str, value: int) -> None:
        try:
            path = botstate_path("runtime", "status", "orderable_cash.json")
            payload = {
                "ts": now_kst().isoformat(),
                "cache_stale": cache_stale,
                "source": source,
                "value": int(value),
            }
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            logger.warning("[CASH][STATUS][FAIL] cache_stale=%s source=%s", cache_stale, source, exc_info=True)

    def _inquire_psbl_order(self, code_hint: str, price_hint: float | None = None) -> dict:
        """주문가능조회 호출."""
        # ✅ DIAG 모드에서 KIS HTTP 차단 시 stub 반환
        if not kis_http_enabled():
            logger.warning("[CASH][PSBL][HTTP_DISABLED] mode=%s → returning stub", os.getenv("STRATEGY_MODE"))
            return {
                "output": {"ord_psbl_cash": "10000000"},
                "rt_cd": "0",
                "_diag_stub": True,
            }
        
        tr_list = _pick_tr(self.env, "PSBL_ORDER")
        if not tr_list:
            raise RuntimeError("PSBL_ORDER TR 미구성")
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-psbl-order"
        tr = tr_list[0]
        headers = self._headers(tr)
        try:
            ord_unpr = int(float(price_hint)) if price_hint is not None else 1
        except Exception:
            ord_unpr = 1
        params = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": self.ACNT_PRDT_CD,
            "PDNO": safe_strip(code_hint) or "005930",
            "ORD_UNPR": str(max(ord_unpr, 1)),
            "ORD_DVSN": "00",
            "ORD_DVSN_CD": "00",
            "CMA_EVLU_AMT_ICLD_YN": "N",
            "OVRS_ICLD_YN": "N",
        }
        self._limiter.wait("psbl-order")
        last_exc: Exception | None = None
        for attempt in range(1, 4):
            try:
                resp = self._safe_request("GET", url, headers=headers, params=params, timeout=(3.0, 7.0))
                return resp.json()
            except KisTemporaryError as exc:
                last_exc = exc
                logger.warning("[PSBL_ORDER][RETRY] attempt=%s err=%s", attempt, exc)
                if attempt >= 3:
                    self._set_safe_mode(reason="psbl_order_temp_error", err=exc)
                    logger.error("[PSBL_ORDER][SAFE_MODE] entry_blocked=1 err=%s", exc)
                    raise
                time.sleep(0.5 * attempt)
        if last_exc:
            raise last_exc
        raise KisTemporaryError("psbl_order_failed")

    def _inquire_balance_page(self, fk: str, nk: str) -> dict:
        """잔고 1페이지 호출(예외는 상위에서 처리)."""
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-balance"
        tr_list = _pick_tr(self.env, "BALANCE")
        if not tr_list:
            raise RuntimeError("BALANCE TR 미구성")
        tr = tr_list[0]
        ok, reason = self._validate_account_params()
        if not ok:
            meta = self._account_param_meta()
            logger.error(
                "[BALANCE][PARAM_INVALID] reason=%s env=%s cano_len=%s acnt_prdt_cd_len=%s cano=%s acnt_prdt_cd=%s",
                reason,
                meta.get("env"),
                meta.get("cano_len"),
                meta.get("acnt_prdt_cd_len"),
                meta.get("cano_masked"),
                meta.get("acnt_prdt_cd_masked"),
            )
            raise KisPermanentError(f"BALANCE_ACCOUNT_PARAM_INVALID:{reason}")
        if os.getenv("KIS_FORCE_500_BALANCE", "0") == "1":
            logger.warning("[BALANCE][FORCE_500] env=KIS_FORCE_500_BALANCE=1 -> simulate temp error")
            _breaker_record_temp_failure("GET", url)
            raise KisTemporaryError("forced_500_balance")
        headers = self._headers(tr)
        params = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": self.ACNT_PRDT_CD,
            "AFHR_FLPR_YN": "N",
            "UNPR_YN": "N",
            "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N",
            "PRCS_DVSN": "01",
            "OFL_YN": "N",
            "INQR_DVSN": "02",
            "CTX_AREA_FK100": fk,
            "CTX_AREA_NK100": nk,
        }
        logger.info(
            "[BALANCE][REQ_SUMMARY] env=%s cano=%s acnt_prdt_cd=%s ctx_fk=%s ctx_nk=%s inqr_dvsn=%s",
            self.env,
            meta.get("cano_masked"),
            meta.get("acnt_prdt_cd_masked"),
            str(params.get("CTX_AREA_FK100") or "")[:4],
            str(params.get("CTX_AREA_NK100") or "")[:4],
            params.get("INQR_DVSN"),
        )
        # [CHG] 안전요청 사용
        resp = self._safe_request("GET", url, headers=headers, params=params, timeout=(3.0, 7.0))
        payload = resp.json()
        if str(payload.get("rt_cd") or "") != "0":
            meta = self._account_param_meta()
            logger.error(
                "[BALANCE][API_FAIL] rt_cd=%s msg_cd=%s msg1=%s env=%s cano_len=%s acnt_prdt_cd_len=%s cano=%s acnt_prdt_cd=%s",
                payload.get("rt_cd"),
                payload.get("msg_cd"),
                payload.get("msg1"),
                meta.get("env"),
                meta.get("cano_len"),
                meta.get("acnt_prdt_cd_len"),
                meta.get("cano_masked"),
                meta.get("acnt_prdt_cd_masked"),
            )
        return payload

    def inquire_balance_all(self, *, max_empty_retry: int = 2) -> dict:
        """
        ✅ 페이징/디바운스 적용 잔고 전체 조회
        반환: {'output1': [...], 'output2': {...}, 'ctx_area_fk100': '...', 'ctx_area_nk100': '...'}
        """
        # ✅ DIAG 모드에서 KIS HTTP 차단 시 stub 반환 (output2는 list[dict] 형태로 통일)
        if not kis_http_enabled():
            if os.getenv("RESET_PRACTICE_ACCOUNT") == "1":
                raise RuntimeError("[BALANCE][HTTP_DISABLED][RESET_ABORT] real KIS balance required for account reset")
            logger.warning("[BALANCE][HTTP_DISABLED] mode=%s → returning stub", os.getenv("STRATEGY_MODE"))
            return {
                "output1": [],
                "output2": [{
                    "dnca_tot_amt": "10000000",
                    "nxdy_excc_amt": "10000000",
                    "prvs_rcdl_excc_amt": "10000000",
                    "ord_psbl_cash": "10000000",
                }],
                "ctx_area_fk100": "",
                "ctx_area_nk100": "",
                "_diag_stub": True,
                "_stub": True,
                "_source": "http_disabled_stub",
            }
        
        fk = nk = ""
        all_rows: List[dict] = []
        out2_last = None  # 🔸 요약 블록(예수금 등) → '첫 페이지' 것만 유지
        empty_cnt = 0
        last_error: Exception | None = None
        while True:
            try:
                j = self._inquire_balance_page(fk, nk)
            except Exception as e:
                logger.error("[잔고조회 예외] %s", e)
                last_error = e
                if empty_cnt < max_empty_retry:
                    empty_cnt += 1
                    time.sleep(0.7)
                    continue
                raise KisBalanceUnavailable(str(e)) from e

            output2_summary = _as_first_dict(j.get("output2")) if "_as_first_dict" in globals() else (j.get("output2") if isinstance(j.get("output2"), dict) else {})
            logger.info(
                "[BALANCE][RESP_SUMMARY] rt_cd=%s msg_cd=%s rows=%s has_output2=%s cash=%s market_value=%s",
                j.get("rt_cd"),
                j.get("msg_cd"),
                len(j.get("output1") or []),
                int(bool(j.get("output2"))),
                (output2_summary or {}).get("ord_psbl_cash") or (output2_summary or {}).get("dnca_tot_amt") or 0,
                (output2_summary or {}).get("scts_evlu_amt") or (output2_summary or {}).get("tot_evlu_amt") or 0,
            )

            rows = j.get("output1") or []
            if not rows:
                out2 = j.get("output2")
                rt_cd = str(j.get("rt_cd") or "0")
                if out2 is not None and rt_cd == "0":
                    if out2_last is None:
                        out2_last = out2
                    fk = (j.get("ctx_area_fk100") or "").strip()
                    nk = (j.get("ctx_area_nk100") or "").strip()
                    break
                empty_cnt += 1
                if empty_cnt <= max_empty_retry:
                    time.sleep(0.6)
                    continue
                else:
                    detail = "empty_response"
                    if last_error:
                        detail = f"{detail}:{last_error}"
                    raise KisBalanceUnavailable(detail)
            empty_cnt = 0
            all_rows.extend(rows)

            # ✅ '처음 나온' output2만 요약으로 사용 (마지막 페이지 값으로 덮어쓰지 않음)
            out2 = j.get("output2")
            if out2 is not None and out2_last is None:
                out2_last = out2

            fk = (j.get("ctx_area_fk100") or "").strip()
            nk = (j.get("ctx_area_nk100") or "").strip()
            if not fk and not nk:
                break

        return {"output1": all_rows, "output2": out2_last, "ctx_area_fk100": fk, "ctx_area_nk100": nk}

    def get_cash_balance(self) -> int:
        """
        ✅ 예수금: output2.ord_psbl_cash 우선.
        실패/0원 시 최근 캐시(self._last_cash) 폴백.
        """
        cash, _meta = self.get_orderable_cash()
        return cash

    def get_positions(self) -> List[Dict]:
        """보유 종목 전체(페이징 병합)."""
        snap = self.get_balance_cached()
        return snap.get("output1") or []

    def get_balance_map(self) -> Dict[str, int]:
        pos = self.get_positions()
        mp: Dict[str, int] = {}
        for row in pos or []:
            try:
                pdno = safe_strip(row.get("pdno"))
                hldg = safe_int(row.get("hldg_qty", "0"))
                ord_psbl = safe_int(row.get("ord_psbl_qty", "0"))
                qty = hldg if hldg > 0 else ord_psbl
                if pdno and qty > 0:
                    mp[pdno] = qty
            except Exception:
                continue
        logger.info(f"[보유수량맵] {len(mp)}종목")
        return mp

    def get_balance_cached(
        self,
        force: bool = False,
        *,
        return_source: bool = False,
        return_raw: bool = False,
    ) -> Dict[str, object] | tuple[Dict[str, object], str] | tuple[Dict[str, object], str, Dict[str, object]]:
        source = "api"
        raw_snapshot: Dict[str, object] | None = None
        if os.getenv("KIS_FORCE_500_BALANCE", "0") == "1":
            logger.warning("[BALANCE][FORCE_500] env=KIS_FORCE_500_BALANCE=1 -> skip cache and raise")
            raise KisTemporaryError("forced_500_balance")
        if not force and self._balance_cache is not None:
            age_s = (now_kst() - self._balance_cache_at).total_seconds() if self._balance_cache_at else 0.0
            cached = _deepcopy_json(self._balance_cache)
            normalized = _normalize_balance_snapshot(cached)
            if normalized:
                logger.info("[BALANCE][CACHE] hit=True age_s=%.1f", age_s)
                self._balance_cache = _deepcopy_json(normalized)
                source = "wrapper_cache"
                if return_source and return_raw:
                    return normalized, source, _deepcopy_json(normalized)
                if return_source:
                    return normalized, source
                return normalized
            global _BALANCE_CACHE_INVALID_LOGGED
            if not _BALANCE_CACHE_INVALID_LOGGED:
                if isinstance(cached, dict) and cached.get("rt_cd") not in (None, "0"):
                    reason = f"rt_cd_{cached.get('rt_cd')}"
                else:
                    reason = "normalize_failed"
                logger.warning("[BALANCE][CACHE][INVALID] reason=%s -> refetching raw", reason)
                _BALANCE_CACHE_INVALID_LOGGED = True
            force = True
        logger.info("[BALANCE][CACHE] hit=False force=%s", force)
        snap: dict = {}
        try:
            snap = self.inquire_balance_all()
            raw_snapshot = _deepcopy_json(snap)
            normalized = _normalize_balance_snapshot(snap)
            if normalized is None:
                if isinstance(snap, dict) and snap.get("rt_cd") not in (None, "0"):
                    reason = f"rt_cd_{snap.get('rt_cd')}"
                else:
                    reason = "normalize_failed"
                logger.warning("[BALANCE][CACHE][INVALID] reason=%s source=api", reason)
            else:
                cache_value = _deepcopy_json(normalized)
                self._balance_cache = cache_value
                self._balance_cache_at = now_kst()
                snap = normalized
        except KisBalanceUnavailable as e:
            logger.error("[GET_BALANCE_FAIL] %s", e)
            raise
        except RuntimeError:
            raise
        except KisTemporaryError as e:
            logger.error("[GET_BALANCE_FAIL] %s", e)
            raise KisBalanceUnavailable(str(e)) from e
        except Exception as e:
            logger.error("[GET_BALANCE_FAIL] %s", e)
            raise KisBalanceUnavailable(str(e)) from e
        snap_copy = _deepcopy_json(snap)
        if return_source and return_raw:
            return snap_copy, source, _deepcopy_json(raw_snapshot or snap_copy)
        if return_source:
            return snap_copy, source
        return snap_copy

    # --- 호환 셔임(기존 trader.py 호출 대응) ---
    def get_balance(self, force: bool = False, **kwargs) -> Dict[str, object]:
        """
        force=True: 캐시 무시하고 강제 조회.
        과거/미래 호출부 호환을 위해 **kwargs 허용(알 수 없는 인자 무시).
        """
        force = bool(
            force
            or kwargs.get("force_refresh", False)
            or kwargs.get("bypass_cache", False)
        )
        return_source = bool(kwargs.get("return_source", False))
        return_raw = bool(kwargs.get("return_raw", False))
        return self.get_balance_cached(
            force=force,
            return_source=return_source,
            return_raw=return_raw,
        )

    def get_balance_all(self) -> Dict[str, object]:
        """trader.py의 _fetch_balances에서 우선 호출되는 호환용 메서드."""
        return self.get_balance_cached()

    def get_balance_snapshot_safe(self) -> dict:
        """
        잔고조회 결과에서 '총액/주문가능/예수금'을 최대한 안전하게 추출한다.
        실패해도 예외를 최소화하고, 원인 확인을 위한 raw_keys를 남긴다.
        """
        raw = self.get_balance_cached(force=True)
        snap = {
            "total_asset_krw": None,
            "total_eval_krw": None,
            "cash_total_krw": None,
            "orderable_cash_krw": None,
            "deposit_like_krw": None,
            "raw_keys": [],
        }

        if not isinstance(raw, dict):
            return snap

        snap["raw_keys"] = sorted(list(raw.keys()))
        summary = raw.get("output2")
        if isinstance(summary, list):
            summary = summary[0] if summary else None
        if not isinstance(summary, dict):
            summary = raw

        def _to_int(value):
            try:
                if value is None:
                    return None
                if isinstance(value, (int, float)):
                    return int(value)
                text = str(value).replace(",", "").strip()
                return int(float(text))
            except Exception:
                return None

        snap["cash_total_krw"] = _to_int(
            summary.get("dnca_tot_amt") or summary.get("cash_total") or summary.get("cash")
        )
        snap["orderable_cash_krw"] = _to_int(summary.get("ord_psbl_cash") or summary.get("orderable_cash"))
        snap["total_eval_krw"] = _to_int(summary.get("tot_evlu_amt") or summary.get("total_eval"))
        snap["total_asset_krw"] = _to_int(summary.get("tot_asst_amt") or summary.get("total_asset"))
        snap["deposit_like_krw"] = snap["cash_total_krw"]

        return snap

    def inquire_daily_ccld(self, *, start_date: str, end_date: str) -> dict:
        """당일 주문/체결 조회."""
        # ✅ DIAG 모드에서 KIS HTTP 차단 시 stub 반환
        if not kis_http_enabled():
            logger.warning("[RECONCILE][HTTP_DISABLED] mode=%s endpoint=inquire-daily-ccld → returning empty", os.getenv("STRATEGY_MODE"))
            return {
                "rt_cd": "0",
                "msg1": "KIS_HTTP_DISABLED",
                "output1": [],
                "output2": [],
                "_diag_stub": True,
            }
        
        def _empty_daily_ccld(reason: str) -> dict:
            return {"rt_cd": "-1", "msg": reason, "output1": [], "output2": []}

        tr_ids = _pick_tr(self.env, "DAILY_CCLD")
        if not tr_ids:
            raise ValueError("KIS daily reconcile TR_ID not configured")
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
        params = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": self.ACNT_PRDT_CD,
            "INQR_STRT_DT": start_date,
            "INQR_END_DT": end_date,
            "SLL_BUY_DVSN_CD": "00",
            "INQR_DVSN": "00",
            "PDNO": "",
            "CCLD_DVSN": "00",
            "ORD_GNO_BRNO": "",
            "ODNO": "",
            "INQR_DVSN": "00",
            "SORT_SQN": "00",
        }
        retryable_statuses = {500, 502, 503, 504}
        backoff_seq = [0.5, 1.0, 2.0]
        last_err: Exception | None = None
        for attempt in range(1, len(backoff_seq) + 1):
            for tr_id in tr_ids:
                try:
                    headers = self._headers(tr_id)
                    resp = self.session.request(
                        "GET",
                        url,
                        headers=headers,
                        params=params,
                        timeout=(3.0, 7.0),
                    )
                    status = resp.status_code
                    if status in (401, 403):
                        logger.warning("[RECONCILE][AUTH] status=%s tr_id=%s -> refresh token", status, tr_id)
                        self.refresh_token()
                        raise KisTemporaryError(f"AUTH_REFRESH {status}")
                    if status in retryable_statuses:
                        raise KisTemporaryError(f"HTTP {status}")
                    if 400 <= status < 500:
                        raise KisPermanentError(f"HTTP {status} for {url}")
                    return resp.json()
                except requests.exceptions.Timeout as exc:
                    last_err = exc
                    logger.warning(
                        "[RECONCILE][TEMP] attempt=%s tr_id=%s err=timeout",
                        attempt,
                        tr_id,
                    )
                except KisTemporaryError as exc:
                    last_err = exc
                    retryable = any(token in str(exc) for token in ("HTTP 500", "HTTP 502", "HTTP 503", "HTTP 504", "timeout"))
                    logger.warning(
                        "[RECONCILE][TEMP] attempt=%s tr_id=%s err=%s retryable=%s",
                        attempt,
                        tr_id,
                        exc,
                        int(retryable),
                    )
                    if not retryable:
                        return _empty_daily_ccld("TEMP_FAIL")
                except KisPermanentError as exc:
                    last_err = exc
                    logger.warning("[RECONCILE][FAIL] tr_id=%s err=%s", tr_id, exc)
                    return _empty_daily_ccld("PERM_FAIL")
                except requests.exceptions.RequestException as exc:
                    last_err = exc
                    logger.warning("[RECONCILE][FAIL] tr_id=%s err=%s", tr_id, exc)
                    return _empty_daily_ccld("REQ_FAIL")
                except Exception as exc:
                    last_err = exc
                    logger.warning("[RECONCILE][FAIL] tr_id=%s err=%s", tr_id, exc)
                    return _empty_daily_ccld("TEMP_FAIL")
            if attempt < len(backoff_seq):
                backoff = backoff_seq[attempt - 1]
                jitter = random.uniform(0.0, min(0.2, backoff * 0.2))
                time.sleep(backoff + jitter)
        if last_err:
            logger.warning("[RECONCILE][DEGRADED] daily_ccld_failed err=%s", last_err)
        self._reset_session()
        return _empty_daily_ccld("TEMP_FAIL")

    # -------------------------------
    # 주문 공통, 시장가/지정가, 매수/매도
    # -------------------------------
    def _order_cash(self, body: dict, *, is_sell: bool) -> Optional[dict]:
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
        
        # Live Gate 체크: config에서 계산된 정책 사용
        from trader.config import ALLOW_LIVE_GATE, FORCE_BLOCK_LIVE, LIVE_GATE_STATUS
        
        if not ALLOW_LIVE_GATE or FORCE_BLOCK_LIVE:
            logger.warning(
                "[ORDER][BLOCKED] reason=%s code=%s side=%s qty=%s",
                LIVE_GATE_STATUS.reason,
                body.get("PDNO"),
                "SELL" if is_sell else "BUY",
                body.get("ORD_QTY"),
            )
            return {
                "blocked": True,
                "reason": LIVE_GATE_STATUS.reason,
                "rt_cd": "1",
                "msg_cd": "LIVE_GATE_BLOCKED",
                "msg1": LIVE_GATE_STATUS.reason,
            }
        _assert_orders_allowed("order_cash")

        # ✅ NO_TRADE 가드: 주문 차단 모드일 때 실제 주문 전송 차단 (intent는 저장됨)
        if os.getenv("NO_TRADE", "0") == "1":
            logger.warning(
                "[NO_TRADE] blocked order submit (test mode) - intent only. code=%s side=%s qty=%s",
                body.get("PDNO"),
                "SELL" if is_sell else "BUY",
                body.get("ORD_QTY")
            )
            return {"blocked": True, "reason": "NO_TRADE", "rt_cd": "1", "msg_cd": "NO_TRADE", "msg1": "NO_TRADE mode - order blocked for testing"}

        # TR 후보 순차 시도
        tr_list = _pick_tr(self.env, "ORDER_SELL" if is_sell else "ORDER_BUY")

        now = now_kst()
        block_reason = _order_block_reason(now)
        if block_reason:
            logger.warning("[ORDER_BLOCK] %s code=%s qty=%s", block_reason, body.get("PDNO"), body.get("ORD_QTY"))
            return {"rt_cd": "1", "msg_cd": "ORDER_BLOCK", "msg1": block_reason, "output": {}}

        # Fallback: 시장가 → IOC시장가 → 최유리
        ord_dvsn_chain = ["01", "13", "03"]
        last_err = None

        for tr_id in tr_list:
            for ord_dvsn in ord_dvsn_chain:
                body["ORD_DVSN"] = ord_dvsn
                body["ORD_UNPR"] = "0"
                if is_sell and not body.get("SLL_TYPE"):
                    body["SLL_TYPE"] = "01"
                body.setdefault("EXCG_ID_DVSN_CD", "KRX")

                # HashKey
                try:
                    hk = self._create_hashkey(body)
                except Exception as e:
                    logger.error(f"[ORDER_HASH_FAIL] body={body} ex={e}")
                    last_err = e
                    continue

                headers = self._headers(tr_id, hk)

                # 레이트리밋(주문은 hashkey 이후 최소 간격 포함)
                self._wait_before_order_submit()

                # [NEW] FORCE_RUN 모드에서 주문 직전 로깅 강화
                log_body_masked = {
                    k: (v if k not in ("CANO", "ACNT_PRDT_CD") else "***")
                    for k, v in body.items()
                }
                from trader.utils.env import env_bool
                dry_run = env_bool("DRY_RUN", default=True)
                live_trading = env_bool("LIVE_TRADING_ENABLED", default=False)
                force_run = env_bool("FORCE_RUN", default=False)
                intended_live = env_bool("INTENDED_LIVE", default=False)
                
                # ✅ CRITICAL ASSERTION: LIVE 의도인데 dry_run이면 즉시 실패
                if intended_live and dry_run:
                    raise RuntimeError(
                        "FATAL: intended_live=True but dry_run=True. This must never happen. "
                        f"INTENDED_LIVE={intended_live} DRY_RUN={dry_run} LIVE_TRADING_ENABLED={live_trading}"
                    )
                
                logger.info(
                    "[ORDER_READY] code=%s side=%s qty=%s price=%s tr_id=%s ord_dvsn=%s DRY_RUN=%s LIVE=%s FORCE_RUN=%s INTENDED_LIVE=%s body=%s",
                    body.get("PDNO"),
                    "SELL" if is_sell else "BUY",
                    body.get("ORD_QTY"),
                    body.get("ORD_UNPR"),
                    tr_id,
                    ord_dvsn,
                    dry_run,
                    live_trading,
                    force_run,
                    intended_live,
                    log_body_masked,
                )
                
                # ✅ ORDER_SENT 로그: 실제 주문 발생 증거
                logger.warning(
                    "[ORDER_SENT] LIVE=%s dry_run=%s code=%s side=%s qty=%s price=%s tr_id=%s ord_dvsn=%s",
                    intended_live,
                    dry_run,
                    body.get("PDNO"),
                    "SELL" if is_sell else "BUY",
                    body.get("ORD_QTY"),
                    body.get("ORD_UNPR"),
                    tr_id,
                    ord_dvsn,
                )
                
                # ✅ ORDER_SENT DB 이벤트 저장
                try:
                    run_id = os.getenv("TRADER_RUN_ID", "unknown")
                    emit_event(
                        as_of=datetime.now(pytz.timezone("Asia/Seoul")).date().isoformat(),
                        event="ORDER_SENT",
                        code=body.get("PDNO"),
                        side="SELL" if is_sell else "BUY",
                        qty=body.get("ORD_QTY"),
                        price=body.get("ORD_UNPR"),
                        tr_id=tr_id,
                        ord_dvsn=ord_dvsn,
                        kis_env=self.env,
                        run_id=run_id,
                        intended_live=intended_live,
                        dry_run=dry_run,
                    )
                except Exception as exc:
                    logger.warning("[ORDER_SENT][EVENT_FAIL] %s", exc)

                # 네트워크/게이트웨이 재시도
                for attempt in range(1, 4):
                    try:
                        # [CHG] 안전요청 사용
                        resp = self._safe_request(
                            "POST",
                            url,
                            headers=headers,
                            data=_json_dumps(body).encode("utf-8"),
                        )
                        data = resp.json()
                    except Exception as e:
                        backoff = min(0.6 * (1.7 ** (attempt - 1)), 5.0) + random.uniform(0, 0.35)
                        logger.error(
                            f"[ORDER_NET_EX] tr_id={tr_id} ord_dvsn={ord_dvsn} attempt={attempt} "
                            f"ex={e} → sleep {backoff:.2f}s"
                        )
                        time.sleep(backoff)
                        last_err = e
                        continue

                    if resp.status_code == 200 and data.get("rt_cd") == "0":
                        logger.info(
                            f"[ORDER_OK] tr_id={tr_id} ord_dvsn={ord_dvsn} output={data.get('output')}"
                        )
                        # 주문 성공 → fills에 기록 (추정 체결가 사용)
                        try:
                            out = data.get("output") or {}
                            odno = out.get("ODNO") or out.get("ord_no") or ""
                            pdno = safe_strip(body.get("PDNO", ""))
                            qty = int(float(body.get("ORD_QTY", "0")))
                            # 가능한 경우 지정가 사용, 아니면 현재가로 추정
                            price_for_fill = None
                            try:
                                ord_unpr = body.get("ORD_UNPR")
                                if ord_unpr and str(ord_unpr) not in ("0", "0.0", ""):
                                    price_for_fill = float(ord_unpr)
                                else:
                                    try:
                                        price_for_fill = float(self.get_last_price(pdno))
                                    except Exception:
                                        price_for_fill = 0.0
                            except Exception:
                                price_for_fill = 0.0

                            side = "SELL" if is_sell else "BUY"
                            append_fill(
                                side=side,
                                code=pdno,
                                name="",
                                qty=qty,
                                price=price_for_fill,
                                odno=odno,
                                note=f"tr={tr_id},ord_dvsn={ord_dvsn}",
                                reason="order_cash",
                            )
                        except Exception as e:
                            logger.warning(f"[APPEND_FILL_EX] ex={e} resp={data}")
                        return data

                    msg_cd = data.get("msg_cd", "")
                    msg1 = data.get("msg1", "")
                    # 게이트웨이/서버 에러류는 재시도
                    if msg_cd == "IGW00008" or "MCA" in msg1 or resp.status_code >= 500:
                        backoff = min(0.6 * (1.7 ** (attempt - 1)), 5.0) + random.uniform(0, 0.35)
                        logger.error(
                            f"[ORDER_FAIL_GATEWAY] tr_id={tr_id} ord_dvsn={ord_dvsn} attempt={attempt} "
                            f"resp={data} → sleep {backoff:.2f}s"
                        )
                        time.sleep(backoff)
                        last_err = data
                        continue

                    logger.error(f"[ORDER_FAIL_BIZ] tr_id={tr_id} ord_dvsn={ord_dvsn} resp={data}")
                    blocked = _is_order_disallowed(data)
                    if blocked:
                        _mark_order_blocked(blocked, now)
                    return None

                logger.warning(f"[ORDER_FALLBACK] tr_id={tr_id} ord_dvsn={ord_dvsn} 실패 → 다음 방식 시도")

        raise Exception(f"주문 실패: {last_err}")

    # -------------------------------
    # 매수/매도 (기본)
    # -------------------------------
    def buy_stock_market(self, pdno: str, qty: int) -> Optional[dict]:
        logger.info("[KIS][ORDER][REQUEST] type=MARKET side=BUY code=%s qty=%s price=0", pdno, qty)
        body = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": self.ACNT_PRDT_CD,
            "PDNO": safe_strip(pdno),
            "ORD_QTY": str(int(qty)),
            "ORD_DVSN": "01",  # 시장가
            "ORD_UNPR": "0",
        }
        response = self._order_cash(body, is_sell=False)
        masked = mask_order_response(response)
        logger.info(
            "[KIS][ORDER][RESPONSE] type=MARKET side=BUY code=%s rt_cd=%s msg_cd=%s msg1=%s odno=%s",
            pdno,
            masked.get("rt_cd"),
            masked.get("msg_cd"),
            masked.get("msg1"),
            masked.get("odno"),
        )
        return response

    def sell_stock_market(self, pdno: str, qty: int) -> Optional[dict]:
        # --- 강화된 사전점검: 보유수량 우선 ---
        pos = self.get_positions() or []
        hldg = 0
        ord_psbl = 0
        for r in pos:
            if safe_strip(r.get("pdno")) == safe_strip(pdno):
                hldg = int(float(r.get("hldg_qty", "0")))
                ord_psbl = int(float(r.get("ord_psbl_qty", "0")))
                break

        base_qty = hldg if hldg > 0 else ord_psbl
        if base_qty <= 0:
            logger.error(
                "[SELL_PRECHECK][NO_KIS_HOLDING] pdno=%s hldg=%s ord_psbl=%s action=block",
                pdno, hldg, ord_psbl,
            )
            return {
                "rt_cd": "PB1_BLOCKED",
                "msg_cd": "SELL_BLOCKED_NO_KIS_HOLDING",
                "msg1": "KIS actual holding qty is zero. Sell blocked before API order.",
                "blocked": True,
                "skip_reason": "SELL_BLOCKED_NO_KIS_HOLDING",
                "pdno": safe_strip(pdno),
                "hldg_qty": hldg,
                "ord_psbl_qty": ord_psbl,
            }

        if qty > base_qty:
            logger.warning(
                f"[SELL_PRECHECK] 수량 보정: req={qty} -> base={base_qty} (hldg={hldg}, ord_psbl={ord_psbl})"
            )
            qty = base_qty

        # --- 중복 매도 방지(메모리 기반) ---
        now_ts = time.time()
        with self._recent_sells_lock:
            last = self._recent_sells.get(pdno)
            if last and (now_ts - last) < self._recent_sells_cooldown:
                logger.warning(
                    f"[SELL_DUP_BLOCK] 최근 매도 기록으로 중복 매도 차단 pdno={pdno} "
                    f"last={last} age={now_ts-last:.1f}s"
                )
                return {"status": "SKIPPED", "skip_reason": "DUP_BLOCK"}

        body = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": self.ACNT_PRDT_CD,
            "PDNO": safe_strip(pdno),
            "SLL_TYPE": "01",  # 일반매도
            "ORD_QTY": str(int(qty)),
            "ORD_DVSN": "01",
            "ORD_UNPR": "0",
            "EXCG_ID_DVSN_CD": "KRX",
        }
        resp = self._order_cash(body, is_sell=True)
        if resp and isinstance(resp, dict) and resp.get("rt_cd") == "0":
            with self._recent_sells_lock:
                self._recent_sells[pdno] = time.time()
                cutoff = time.time() - (self._recent_sells_cooldown * 5)
                keys_to_del = [k for k, v in self._recent_sells.items() if v < cutoff]
                for k in keys_to_del:
                    del self._recent_sells[k]
        return resp

    def buy_stock_limit(self, pdno: str, qty: int, price: int) -> Optional[dict]:
        # Live Gate 체크: config에서 계산된 정책 사용
        from trader.config import ALLOW_LIVE_GATE, FORCE_BLOCK_LIVE, LIVE_GATE_STATUS
        
        # ✅ 진입 로그: wrapper까지 주문이 도달했는지 즉시 확인
        logger.info(
            "[ORDER][WRAPPER][ENTER] func=buy_stock_limit code=%s qty=%s price=%s allow_live_gate=%s force_block_live=%s reason=%s NO_TRADE=%s",
            pdno, qty, price,
            int(ALLOW_LIVE_GATE),
            int(FORCE_BLOCK_LIVE),
            LIVE_GATE_STATUS.reason,
            os.getenv("NO_TRADE", "0")
        )
        
        if not ALLOW_LIVE_GATE or FORCE_BLOCK_LIVE:
            logger.warning(
                "[ORDER][BLOCKED] reason=%s code=%s side=BUY qty=%s",
                LIVE_GATE_STATUS.reason,
                pdno,
                qty,
            )
            return {
                "blocked": True,
                "reason": LIVE_GATE_STATUS.reason,
                "rt_cd": "1",
                "msg_cd": "LIVE_GATE_BLOCKED",
                "msg1": LIVE_GATE_STATUS.reason,
            }
        _assert_orders_allowed("buy_stock_limit")
        
        # ✅ NO_TRADE 가드: 주문 차단 모드일 때 실제 주문 전송 차단 (intent는 저장됨)
        if os.getenv("NO_TRADE", "0") == "1":
            logger.warning(
                "[NO_TRADE] blocked order submit (test mode) - intent only. code=%s side=BUY qty=%s price=%s",
                pdno, qty, price
            )
            return {"blocked": True, "reason": "NO_TRADE", "rt_cd": "1", "msg_cd": "NO_TRADE", "msg1": "NO_TRADE mode - order blocked for testing"}
        
        now = now_kst()
        block_reason = _order_block_reason(now)
        if block_reason:
            logger.warning("[ORDER_BLOCK] %s code=%s qty=%s", block_reason, pdno, qty)
            return {"rt_cd": "1", "msg_cd": "ORDER_BLOCK", "msg1": block_reason, "output": {}}

        body = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": self.ACNT_PRDT_CD,
            "PDNO": safe_strip(pdno),
            "ORD_QTY": str(int(qty)),
            "ORD_DVSN": "00",  # 지정가
            "ORD_UNPR": str(int(price)),
            "EXCG_ID_DVSN_CD": "KRX",
        }
        hk = self._create_hashkey(body)
        tr_list = _pick_tr(self.env, "ORDER_BUY")
        if not tr_list:
            raise Exception("ORDER_BUY TR 미구성")
        tr_id = tr_list[0]
        headers = self._headers(tr_id, hk)
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
        logger.info("[KIS][ORDER][REQUEST] type=LIMIT side=BUY code=%s qty=%s price=%s", pdno, qty, price)
        self._wait_before_order_submit()
        # [CHG] 안전요청 사용
        resp = self._safe_request(
            "POST", url, headers=headers, data=_json_dumps(body).encode("utf-8"), timeout=(3.0, 7.0)
        )
        data = resp.json()
        masked = mask_order_response(data)
        logger.info(
            "[KIS][ORDER][RESPONSE] type=LIMIT side=BUY code=%s rt_cd=%s msg_cd=%s msg1=%s odno=%s",
            pdno,
            masked.get("rt_cd"),
            masked.get("msg_cd"),
            masked.get("msg1"),
            masked.get("odno"),
        )
        if resp.status_code == 200 and data.get("rt_cd") == "0":
            logger.info(f"[BUY_LIMIT_OK] output={data.get('output')}")
            try:
                out = data.get("output") or {}
                odno = out.get("ODNO") or out.get("ord_no") or ""
                pdno = safe_strip(body.get("PDNO", ""))
                qty_int = int(float(body.get("ORD_QTY", "0")))
                price_for_fill = float(body.get("ORD_UNPR", 0))
                append_fill(
                    side="BUY",
                    code=pdno,
                    name="",
                    qty=qty_int,
                    price=price_for_fill,
                    odno=odno,
                    note=f"limit,tr={tr_id}",
                )
            except Exception as e:
                logger.warning(f"[APPEND_FILL_LIMIT_BUY_FAIL] ex={e}")
            return data
        logger.error(f"[BUY_LIMIT_FAIL] {data}")
        blocked = _is_order_disallowed(data)
        if blocked:
            _mark_order_blocked(blocked, now)
        return None

    def sell_stock_limit(self, pdno: str, qty: int, price: int) -> Optional[dict]:
        # Live Gate 체크: config에서 계산된 정책 사용
        from trader.config import ALLOW_LIVE_GATE, FORCE_BLOCK_LIVE, LIVE_GATE_STATUS
        
        if not ALLOW_LIVE_GATE or FORCE_BLOCK_LIVE:
            logger.warning(
                "[ORDER][BLOCKED] reason=%s code=%s side=SELL qty=%s",
                LIVE_GATE_STATUS.reason,
                pdno,
                qty,
            )
            return {
                "blocked": True,
                "reason": LIVE_GATE_STATUS.reason,
                "rt_cd": "1",
                "msg_cd": "LIVE_GATE_BLOCKED",
                "msg1": LIVE_GATE_STATUS.reason,
            }
        _assert_orders_allowed("sell_stock_limit")
        
        # ✅ NO_TRADE 가드: 주문 차단 모드일 때 실제 주문 전송 차단 (intent는 저장됨)
        if os.getenv("NO_TRADE", "0") == "1":
            logger.warning(
                "[NO_TRADE] blocked order submit (test mode) - intent only. code=%s side=SELL qty=%s price=%s",
                pdno, qty, price
            )
            return {"blocked": True, "reason": "NO_TRADE", "rt_cd": "1", "msg_cd": "NO_TRADE", "msg1": "NO_TRADE mode - order blocked for testing"}
        
        now = now_kst()
        block_reason = _order_block_reason(now)
        if block_reason:
            logger.warning("[ORDER_BLOCK] %s code=%s qty=%s", block_reason, pdno, qty)
            return {"rt_cd": "1", "msg_cd": "ORDER_BLOCK", "msg1": block_reason, "output": {}}

        # --- 강화된 사전점검: 보유수량 우선 ---
        pos = self.get_positions() or []
        hldg = 0
        ord_psbl = 0
        for r in pos:
            if safe_strip(r.get("pdno")) == safe_strip(pdno):
                hldg = int(float(r.get("hldg_qty", "0")))
                ord_psbl = int(float(r.get("ord_psbl_qty", "0")))
                break

        base_qty = hldg if hldg > 0 else ord_psbl
        if base_qty <= 0:
            logger.error(
                f"[SELL_LIMIT_PRECHECK] 보유 없음/수량 0 pdno={pdno} hldg={hldg} ord_psbl={ord_psbl}"
            )
            return None

        if qty > base_qty:
            logger.warning(
                f"[SELL_LIMIT_PRECHECK] 수량 보정: req={qty} -> base={base_qty} "
                f"(hldg={hldg}, ord_psbl={ord_psbl})"
            )
            qty = base_qty

        # 중복 매도 방지(메모리 기반)
        now_ts = time.time()
        with self._recent_sells_lock:
            last = self._recent_sells.get(pdno)
            if last and (now_ts - last) < self._recent_sells_cooldown:
                logger.warning(
                    f"[SELL_DUP_BLOCK_LIMIT] 최근 매도 기록으로 중복 매도 차단 pdno={pdno} "
                    f"last={last} age={now_ts-last:.1f}s"
                )
                return {"status": "SKIPPED", "skip_reason": "DUP_BLOCK"}

        body = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": self.ACNT_PRDT_CD,
            "PDNO": safe_strip(pdno),
            "SLL_TYPE": "01",
            "ORD_QTY": str(int(qty)),
            "ORD_DVSN": "00",  # 지정가
            "ORD_UNPR": str(int(price)),
            "EXCG_ID_DVSN_CD": "KRX",
        }
        hk = self._create_hashkey(body)
        tr_list = _pick_tr(self.env, "ORDER_SELL")
        if not tr_list:
            raise Exception("ORDER_SELL TR 미구성")
        tr_id = tr_list[0]
        headers = self._headers(tr_id, hk)
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
        self._wait_before_order_submit()
        # [CHG] 안전요청 사용
        resp = self._safe_request(
            "POST", url, headers=headers, data=_json_dumps(body).encode("utf-8"), timeout=(3.0, 7.0)
        )
        data = resp.json()
        if resp.status_code == 200 and data.get("rt_cd") == "0":
            logger.info(f"[SELL_LIMIT_OK] output={data.get('output')}")
            try:
                out = data.get("output") or {}
                odno = out.get("ODNO") or out.get("ord_no") or ""
                pdno = safe_strip(body.get("PDNO", ""))
                qty_int = int(float(body.get("ORD_QTY", "0")))
                price_for_fill = float(body.get("ORD_UNPR", 0))
                append_fill(
                    side="SELL",
                    code=pdno,
                    name="",
                    qty=qty_int,
                    price=price_for_fill,
                    odno=odno,
                    note=f"limit,tr={tr_id}",
                    reason="sell_limit",
                )
            except Exception as e:
                logger.warning(f"[APPEND_FILL_LIMIT_SELL_FAIL] ex={e}")
            with self._recent_sells_lock:
                self._recent_sells[pdno] = time.time()
            return data
        logger.error(f"[SELL_LIMIT_FAIL] {data}")
        blocked = _is_order_disallowed(data)
        if blocked:
            _mark_order_blocked(blocked, now)
        return None

    # -------------------------------
    # 매수/매도 (신규 가드 사용 버전)
    # -------------------------------
    def buy_stock_limit_guarded(self, code: str, qty: int, limit_price: int, **kwargs):
        """
        지정가 매수 시 예수금 부족/과매수 자동 축소 또는 스킵.
        ✅ practice 환경에서는 KIS에게 직접 판단을 맡기고, 내부 가드는 생략.
        """
        # 🔸 모의투자(practice) 계좌에서는 예수금 가드 사용 X → 바로 KIS로 주문
        if self.env == "practice":
            logger.info(
                f"[BUY_GUARD] practice env → guard 생략, 직접 지정가 주문 "
                f"(code={code}, qty={qty}, limit={limit_price})"
            )
            return self.buy_stock_limit(code, qty, limit_price)

        try:
            limit_price = int(limit_price)
        except Exception:
            limit_price = 0
        if limit_price <= 0 or int(qty) <= 0:
            raise ValueError("invalid limit buy params")

        # 기준가격: 지정가와 현재가 중 더 보수적인 값 사용(더 높은 값)
        try:
            cur = self.get_last_price(code)
            ref_px = float(cur) if cur is not None else None
        except Exception:
            ref_px = None
        ref_px = float(limit_price) if ref_px is None else max(float(limit_price), float(ref_px))

        adj_qty = self.affordable_qty(code, ref_px, qty)
        if adj_qty <= 0:
            logger.warning(f"[BUY_GUARD] {code} 예수금 부족 → 매수 스킵 (req={qty}, px={ref_px})")
            return {"rt_cd": "1", "msg1": "INSUFFICIENT_CASH", "output": {}}

        if adj_qty < qty:
            logger.info(f"[BUY_GUARD] {code} 요청 {qty} → 가능한 {adj_qty}로 축소 (px={ref_px})")

        # 기존 지정가 매수 호출
        return self.buy_stock_limit(code, adj_qty, limit_price)

    def buy_stock_market_guarded(self, code: str, qty: int, **kwargs):
        """
        시장가 매수 시 예수금 부족/과매수 자동 축소 또는 스킵.
        ✅ practice 환경에서는 KIS에게 직접 판단을 맡기고, 내부 가드는 생략.
        """
        # 🔸 모의투자(practice) 계좌에서는 예수금 가드 사용 X → 바로 KIS로 주문
        if self.env == "practice":
            logger.info(
                f"[BUY_GUARD] practice env → guard 생략, 직접 시장가 주문 "
                f"(code={code}, qty={qty})"
            )
            return self.buy_stock_market(code, qty)

        try:
            cur = self.get_last_price(code)
            ref_px = float(cur) if cur is not None else 0.0
        except Exception:
            ref_px = 0.0

        if ref_px <= 0:
            snap = self.get_quote_snapshot(code)
            ref_px = float(snap.get("tp") or 0.0)

        adj_qty = self.affordable_qty(code, ref_px, qty)
        if adj_qty <= 0:
            logger.warning(
                f"[BUY_GUARD] {code} 예수금 부족 → 매수 스킵 (req={qty}, px≈{ref_px})"
            )
            return {"rt_cd": "1", "msg1": "INSUFFICIENT_CASH", "output": {}}

        if adj_qty < qty:
            logger.info(
                f"[BUY_GUARD] {code} 요청 {qty} → 가능한 {adj_qty}로 축소 (px≈{ref_px})"
            )

        return self.buy_stock_market(code, adj_qty)

    # --- 호환 셔임(기존 trader.py 호출 대응) ---
    def buy_stock(self, code: str, qty: int, price: Optional[int] = None):
        """기존 코드 호환용."""
        if price is None:
            return self.buy_stock_market(code, qty)
        return self.buy_stock_limit(code, qty, price)

    def sell_stock(self, code: str, qty: int, price: Optional[int] = None):
        """기존 코드 호환용."""
        if price is None:
            return self.sell_stock_market(code, qty)
        return self.sell_stock_limit(code, qty, price)

    # ===== [NEW] 주문 후 확인/보조: 체결 후 잔고 동기화 =====
    def refresh_after_order(self, wait_sec: float = 3.0, max_tries: int = 5) -> dict:
        """
        체결 직후 잔고/현금 재조회 (네트워크/지연 내성).
        - 여러 번(기본 5회) 짧게 시도하여 output1/2가 채워진 시점에 반환
        - 실패 시 마지막 성공 스냅샷 또는 빈 dict
        """
        snap: dict = {}
        tries = max(1, int(max_tries))
        delay = max(0.2, float(wait_sec) / tries)
        for i in range(tries):
            try:
                j = self.inquire_balance_all()
                if j and (j.get("output1") or j.get("output2")):
                    snap = j
                    logger.info("[SYNC] balance refreshed (try=%s)", i + 1)
                    break
            except Exception as e:
                logger.warning("[SYNC] balance refresh failed: %s", e)
            time.sleep(delay)
        return snap

    def check_filled(self, order_resp: Optional[dict]) -> bool:
        """간이 체결 확인: 응답 rt_cd == '0'이면 성공으로 간주."""
        try:
            return bool(order_resp and isinstance(order_resp, dict) and order_resp.get("rt_cd") == "0")
        except Exception:
            return False

    def smoke_test_askbid(self, code: str = "005930") -> bool:
        """
        호가 조회 smoke test: 404 캐시 동작 확인.
        """
        logger.info("[SMOKE][ASKBID] Testing code=%s", code)
        try:
            ask1 = self.get_best_ask(code)
            bid1 = self.get_best_bid(code)
            logger.info("[SMOKE][ASKBID] First call: ask=%.0f bid=%.0f", ask1 or 0, bid1 or 0)
            # 두 번째 호출: 캐시되어 API 호출 안 함
            ask2 = self.get_best_ask(code)
            bid2 = self.get_best_bid(code)
            logger.info("[SMOKE][ASKBID] Second call: ask=%.0f bid=%.0f", ask2 or 0, bid2 or 0)
            # 캐시 상태 확인
            if code in self.askbid_unavailable_cache:
                logger.info("[SMOKE][ASKBID] Cache hit for code=%s", code)
            else:
                logger.info("[SMOKE][ASKBID] No cache for code=%s", code)
            return True
        except Exception as e:
            logger.error("[SMOKE][ASKBID] Failed: %s", repr(e))
            return False
