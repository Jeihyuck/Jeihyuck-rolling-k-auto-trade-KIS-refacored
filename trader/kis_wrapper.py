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

import os
import json
import time
import random
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any

import requests
import pytz
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from settings import APP_KEY, APP_SECRET, API_BASE_URL, CANO, ACNT_PRDT_CD, KIS_ENV
from trader.time_utils import is_trading_day, is_trading_window, now_kst
from trader.config import MARKET_MAP, SUBJECT_FLOW_TIMEOUT_SEC, SUBJECT_FLOW_RETRY
from trader.fills import append_fill

logger = logging.getLogger(__name__)
_ORDER_BLOCK_STATE: Dict[str, Any] = {"date": None, "reason": None}


class NetTemporaryError(Exception):
    """네트워크/SSL 등 일시적 오류를 의미 (제외 금지, 루프 스킵)."""
    pass


class DataEmptyError(Exception):
    """정상응답이나 캔들이 0개 (실제 데이터 없음)."""
    pass


class DataShortError(Exception):
    """정상응답이나 캔들이 need_n 미만."""
    pass


def _build_session():
    s = requests.Session()
    retry = Retry(
        total=6, connect=5, read=5, status=3,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
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


def _json_dumps(body: dict) -> str:
    return json.dumps(body, ensure_ascii=False, separators=(",", ":"), sort_keys=False)


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
                time.sleep(self.min_interval - delta + random.uniform(0, 0.03))
            self.last_at[key] = time.time()


TR_MAP = {
    "practice": {
        "ORDER_BUY": [os.getenv("KIS_TR_ID_ORDER_BUY", "VTTC0012U"), "VTTC0802U"],
        "ORDER_SELL": [os.getenv("KIS_TR_ID_ORDER_SELL", "VTTC0011U"), "VTTC0801U"],
        "BALANCE": [os.getenv("KIS_TR_ID_BALANCE", "VTTC8434R")],
        "PRICE": [os.getenv("KIS_TR_ID_PRICE", "FHKST01010100")],
        "ORDERBOOK": [os.getenv("KIS_TR_ID_ORDERBOOK", "FHKST01010200")],
        "DAILY_CHART": [os.getenv("KIS_TR_ID_DAILY_CHART", "FHKST03010100")],
        "INTRADAY_CHART": [os.getenv("KIS_TR_ID_INTRADAY_CHART", "FHKST03010200")],
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
    _token_cache = {"token": None, "expires_at": 0, "last_issued": 0}
    _cache_path = "kis_token_cache.json"
    _token_lock = threading.Lock()

    def should_cooldown(self, now_kst: datetime | None = None) -> bool:
        """
        VWAP / 롤링K 메인 루프에서 '잠깐 쉬어야 하는 구간'을 체크하는 헬퍼.

        지금은 최소 구현 버전:
        - 항상 False를 리턴해서 쿨다운을 사용하지 않는다.
        - 나중에 점심시간 / 장 마감 직전 / 과열 구간 등 세부 로직을 여기로 옮기면 된다.
        """
        return False

    def __init__(self):
        self.CANO = safe_strip(CANO)
        self.ACNT_PRDT_CD = safe_strip(ACNT_PRDT_CD)
        self.env = safe_strip(KIS_ENV or "practice").lower()
        if self.env not in ("practice", "real"):
            self.env = "practice"

        # [CHG] 세션 생성 → 멤버로 보관
        self.session = _build_session()

        # [NEW] 네트워크 안전 요청 백오프/세션리셋 파라미터
        self._safe_attempts = 5
        self._safe_backoff_base = 0.2

        self._limiter = _RateLimiter(min_interval_sec=0.20)
        self._recent_sells: Dict[str, float] = {}
        self._recent_sells_lock = threading.Lock()
        self._recent_sells_cooldown = 60.0

        self._last_cash: Optional[int] = None  # ✅ 예수금 캐시(네트워크 실패/0원 응답 대응)

        self.token = self.get_valid_token()
        logger.info(f"[생성자 체크] CANO={repr(self.CANO)}, ACNT_PRDT_CD={repr(self.ACNT_PRDT_CD)}, ENV={self.env}")

        self._today_open_cache: Dict[str, Tuple[float, float]] = {}  # code -> (open_price, ts)
        self._today_open_ttl = 60 * 60 * 9  # 9시간 TTL (당일만 유효)

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

    def _safe_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        공통 안전요청 래퍼:
        - SSLError/일시 오류 시 지수형 백오프 + 세션 리셋 후 재시도
        - 기본 시도 self._safe_attempts
        """
        attempts = self._safe_attempts
        for i in range(1, attempts + 1):
            try:
                return self.session.request(
                    method,
                    url,
                    timeout=kwargs.pop("timeout", (3.0, 7.0)),
                    **kwargs,
                )
            except requests.exceptions.SSLError as e:
                logger.warning("[NET:SSL_ERROR] attempt=%s url=%s err=%s", i, url, e)
                self._reset_session()
            except requests.exceptions.RequestException as e:
                logger.warning("[NET:REQ_ERROR] attempt=%s url=%s err=%s", i, url, e)
                if i in (1, 2):  # 초기 2회엔 세션 리셋도 수행
                    self._reset_session()
            # backoff
            time.sleep((2 ** i) * self._safe_backoff_base + random.uniform(0, 0.2))
        raise NetTemporaryError(f"request failed after retries: {url}")

    # ===== 토큰 처리 =====
    def get_valid_token(self):
        with KisAPI._token_lock:
            now = time.time()
            if self._token_cache["token"] and now < self._token_cache["expires_at"] - 300:
                return self._token_cache["token"]

            if os.path.exists(self._cache_path):
                try:
                    with open(self._cache_path, "r", encoding="utf-8") as f:
                        cache = json.load(f)
                    if "access_token" in cache and now < cache["expires_at"] - 300:
                        self._token_cache.update({
                            "token": cache["access_token"],
                            "expires_at": cache["expires_at"],
                            "last_issued": cache.get("last_issued", 0),
                        })
                        logger.info(
                            f"[토큰캐시] 파일캐시 사용: {cache['access_token'][:10]}... 만료:{cache['expires_at']}"
                        )
                        return cache["access_token"]
                except Exception as e:
                    logger.warning(f"[토큰캐시 읽기 실패] {e}")

            if now - self._token_cache["last_issued"] < 61:
                logger.warning("[토큰] 1분 이내 재발급 시도 차단, 기존 토큰 재사용")
                if self._token_cache["token"]:
                    return self._token_cache["token"]
                raise Exception("토큰 발급 제한(1분 1회), 잠시 후 재시도 필요")

            token, expires_in = self._issue_token_and_expire()
            expires_at = now + int(expires_in)
            self._token_cache.update({"token": token, "expires_at": expires_at, "last_issued": now})
            try:
                with open(self._cache_path, "w", encoding="utf-8") as f:
                    json.dump(
                        {"access_token": token, "expires_at": expires_at, "last_issued": now},
                        f,
                        ensure_ascii=False,
                    )
            except Exception as e:
                logger.warning(f"[토큰캐시 쓰기 실패] {e}")
            logger.info("[토큰캐시] 새 토큰 발급 및 캐시")
            return token

    def _issue_token_and_expire(self):
        token_path = TR_MAP[self.env]["TOKEN"]
        url = f"{API_BASE_URL}{token_path}"
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
            logger.info(f"[🔑 토큰발급] 성공: {j}")
            return j["access_token"], j.get("expires_in", 86400)
        logger.error(f"[🔑 토큰발급 실패] {j.get('error_description', j)}")
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
                KisAPI._token_cache = {"token": None, "expires_at": 0, "last_issued": 0}
                if os.path.exists(self._cache_path):
                    try:
                        os.remove(self._cache_path)
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
        return hk

    # ===== 신규: 예수금/과매수 방지 유틸 =====
    def get_cash_available_today(self) -> int:
        """
        당일 매수 가능 예수금(가용현금) 반환.
        ✅ output2.ord_psbl_cash → nrcvb_buy_amt → dnca_tot_amt 순으로 파싱.
        실패/0원 시 최근 조회값 캐시 사용.
        """
        try:
            cash = self.get_cash_balance()
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
    def _inquire_price_once(self, tr_id: str, market_div: str, code_fmt: str) -> Optional[float]:
        """단일 TR/마켓/코드 조합으로 현재가 1회 조회(성공시 float 반환, 실패/0원시 None)."""
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
        headers = self._headers(tr_id)
        params = {"fid_cond_mrkt_div_code": market_div, "fid_input_iscd": code_fmt}
        try:
            # [CHG] 안전요청 사용
            resp = self._safe_request("GET", url, headers=headers, params=params, timeout=(3.0, 5.0))
            data = resp.json()
        except Exception as e:
            logger.debug("[PRICE_ONCE_EX] %s/%s %s → %s", market_div, code_fmt, tr_id, e)
            return None

        if "초당 거래건수" in (data.get("msg1") or ""):
            return None
        if resp.status_code == 200 and data.get("rt_cd") == "0" and data.get("output"):
            try:
                px = float(data["output"].get("stck_prpr") or 0)
                return px if px > 0 else None
            except Exception:
                return None
        return None

    def get_last_price(self, code: str, *, attempts: int = 2) -> float:
        """
        견고한 현재가 조회:
        - J/U 교차 + 'A' 접두/무접두 교차
        - 0원/실패 시 지수 백오프 후 재시도
        """
        c = safe_strip(code)
        code_variants = [c, f"A{c}"] if not c.startswith("A") else [c, c[1:]]
        markets = ("J", "U")
        tr_list = _pick_tr(self.env, "PRICE")
        for round_i in range(attempts):
            for tr in tr_list:
                for m in markets:
                    for cf in code_variants:
                        px = self._inquire_price_once(tr, m, cf)
                        if px and px > 0:
                            return px
            # 백오프 후 재시도
            time.sleep(0.6 * (1.5 ** round_i) + random.uniform(0, 0.2))
        raise RuntimeError(f"invalid last price 0 for {code}")

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

        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
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
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-askprice"
        self._limiter.wait("orderbook")
        for tr in _pick_tr(self.env, "ORDERBOOK"):
            headers = self._headers(tr)
            markets = ["J", "U"]
            c = code.strip()
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
        # ---- (A) .env 점검: DAILY_CAPITAL 미설정 경고 (함수 최초 1회만) ----
        try:
            if not getattr(self, "_env_checked_daily_capital", False):
                if os.getenv("DAILY_CAPITAL") in (None, ""):
                    logger.warning(
                        "[ENV] DAILY_CAPITAL 이 .env에 설정되지 않았습니다. "
                        "settings의 기본값(10,000,000)이 사용될 수 있습니다."
                    )
                self._env_checked_daily_capital = True
        except Exception:
            pass

        # ---- (1) 파라미터 구성 ----
        market_code = "J"                         # 시장코드: J 고정
        iscd = code.strip().lstrip("A")          # 종목코드: 'A' 제거(6자리)

        # 기간: 충분히 넉넉하게(휴장/결측 대비)
        kst = pytz.timezone("Asia/Seoul")
        now_kst = datetime.now(kst)
        to_ymd = now_kst.strftime("%Y%m%d")
        back_days = max(200, count * 4 + 100)
        from_ymd = (now_kst - timedelta(days=back_days)).strftime("%Y%m%d")

        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        self._limiter.wait("daily")

        last_err = None

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

            for attempt in range(1, 4):  # 가벼운 재시도
                try:
                    # [CHG] 안전요청 사용
                    resp = self._safe_request(
                        "GET", url, headers=headers, params=params, timeout=(3.0, 7.0)
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    logger.debug("[DAILY_RAW_JSON] %s TR=%s attempt=%d → %s", iscd, tr, attempt, data)
                except requests.exceptions.SSLError as e:
                    last_err = e
                    logger.warning("[NET:SSL_ERROR] DAILY %s attempt=%s %s", iscd, attempt, e)
                    time.sleep(0.4 * attempt)
                    continue
                except requests.exceptions.RequestException as e:
                    last_err = e
                    logger.warning("[NET:REQ_ERROR] DAILY %s attempt=%s %s", iscd, attempt, e)
                    time.sleep(0.4 * attempt)
                    continue
                except ValueError as e:
                    last_err = e
                    logger.warning("[NET:JSON_DECODE] DAILY %s attempt=%s %s", iscd, attempt, e)
                    time.sleep(0.35 + random.uniform(0, 0.15))
                    continue
                except Exception as e:
                    last_err = e
                    logger.warning("[NET:UNEXPECTED] DAILY %s attempt=%s %s", iscd, attempt, e)
                    time.sleep(0.4 * attempt)
                    continue

                if "초당 거래건수" in str(data.get("msg1") or ""):
                    time.sleep(0.35 + random.uniform(0, 0.15))
                    continue

                arr = data.get("output2") or data.get("output1") or data.get("output")

                if resp.status_code == 200 and arr:
                    rows: List[Dict[str, Any]] = []
                    for r in arr:
                        try:
                            d = r.get("stck_bsop_date")
                            o = r.get("stck_oprc")
                            h = r.get("stck_hgpr")
                            l = r.get("stck_lwpr")
                            c = r.get("stck_clpr")
                            if d and o is not None and h is not None and l is not None and c is not None:
                                rows.append({
                                    "date": d,
                                    "open": float(o),
                                    "high": float(h),
                                    "low": float(l),
                                    "close": float(c),
                                })
                        except Exception as e:
                            logger.debug("[DAILY_ROW_SKIP] %s rec=%s err=%s", iscd, r, e)

                    rows.sort(key=lambda x: x["date"])

                    if len(rows) == 0:
                        raise DataEmptyError(f"A{iscd} 0 candles")
                    if len(rows) < 21:
                        raise DataShortError(f"A{iscd} {len(rows)} candles (<21)")

                    need = max(count, 21)
                    return rows[-need:][-count:]

                last_err = RuntimeError(
                    f"BAD_RESP rt_cd={data.get('rt_cd')} msg={data.get('msg1')} arr=None"
                )
                logger.warning("[DAILY_FAIL] A%s: %s | raw=%s", iscd, last_err, data)
                time.sleep(0.35 + random.uniform(0, 0.15))

        if last_err:
            logger.warning("[DAILY_FAIL] A%s: %s", iscd, last_err)
        raise NetTemporaryError(f"DAILY A{iscd} net fail")

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
        """
        out: Dict[str, Any] = {}
        try:
            out["tp"] = float(self.get_last_price(code))
        except Exception:
            out["tp"] = None
        try:
            ask = self.get_best_ask(code)
            bid = self.get_best_bid(code)
            out["ap"] = float(ask) if ask is not None else None
            out["bp"] = float(bid) if bid is not None else None
        except Exception:
            out["ap"], out["bp"] = None, None
        out["close"] = out.get("tp")
        return out

    def get_best_ask(self, code: str) -> Optional[float]:
        """최우선 매도호가(askp1)."""
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-askprice"
        self._limiter.wait("orderbook-best")
        for tr in _pick_tr(self.env, "ORDERBOOK"):
            headers = self._headers(tr)
            markets = ["J", "U"]
            c = code.strip()
            codes = [c, f"A{c}"] if not c.startswith("A") else [c, c[1:]]
            for market_div in markets:
                for code_fmt in codes:
                    params = {"fid_cond_mrkt_div_code": market_div, "fid_input_iscd": code_fmt}
                    try:
                        # [CHG] 안전요청 사용
                        resp = self._safe_request(
                            "GET", url, headers=headers, params=params, timeout=(3.0, 5.0)
                        )
                        data = resp.json()
                    except Exception:
                        continue
                    if resp.status_code == 200 and data.get("rt_cd") == "0" and data.get("output"):
                        try:
                            return float(data["output"].get("askp1"))
                        except Exception:
                            return None
        return None

    def get_best_bid(self, code: str) -> Optional[float]:
        """최우선 매수호가(bidp1)."""
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-askprice"
        self._limiter.wait("orderbook-best")
        for tr in _pick_tr(self.env, "ORDERBOOK"):
            headers = self._headers(tr)
            markets = ["J", "U"]
            c = code.strip()
            codes = [c, f"A{c}"] if not c.startswith("A") else [c, c[1:]]
            for market_div in markets:
                for code_fmt in codes:
                    params = {"fid_cond_mrkt_div_code": market_div, "fid_input_iscd": code_fmt}
                    try:
                        # [CHG] 안전요청 사용
                        resp = self._safe_request(
                            "GET", url, headers=headers, params=params, timeout=(3.0, 5.0)
                        )
                        data = resp.json()
                    except Exception:
                        continue
                    if resp.status_code == 200 and data.get("rt_cd") == "0" and data.get("output"):
                        try:
                            return float(data["output"].get("bidp1"))
                        except Exception:
                            return None
        return None

    def get_index_quote(self, index_code: str) -> Dict[str, Optional[float]]:
        """(간이) 지수 스냅샷 placeholder."""
        return {"price": None, "prev_close": None, "vwap": None}

    # ----- 잔고/포지션 -----
    def _parse_cash_from_output2(self, out2: Any) -> tuple[int, dict]:
        """
        ✅ 예수금 파싱 규칙:
        1) ord_psbl_cash (주문가능현금)
        2) nrcvb_buy_amt (매수가능금액)
        3) dnca_tot_amt  (예수금 총액; 결제미수 포함 가능)
        """

        def _to_int(x) -> int:
            try:
                s = safe_strip(x)
                if s == "":
                    return 0
                return int(float(s))
            except Exception:
                return 0

        row = None
        if isinstance(out2, list) and out2:
            row = out2[0]
        elif isinstance(out2, dict):
            row = out2
        else:
            return 0, {}

        raw_fields = {
            "ord_psbl_cash": row.get("ord_psbl_cash"),
            "nrcvb_buy_amt": row.get("nrcvb_buy_amt"),
            "dnca_tot_amt": row.get("dnca_tot_amt"),
        }
        selected_key = None
        cash = 0
        for key in ("ord_psbl_cash", "nrcvb_buy_amt", "dnca_tot_amt"):
            if key in row:
                selected_key = key
                cash = _to_int(row.get(key))
                break
        clamp_applied = False
        return cash, {"raw_fields": raw_fields, "selected_key": selected_key, "clamp_applied": clamp_applied}

    def _inquire_balance_page(self, fk: str, nk: str) -> dict:
        """잔고 1페이지 호출(예외는 상위에서 처리)."""
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-balance"
        tr_list = _pick_tr(self.env, "BALANCE")
        if not tr_list:
            raise RuntimeError("BALANCE TR 미구성")
        tr = tr_list[0]
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
        logger.info(f"[잔고조회 요청파라미터] {params}")
        # [CHG] 안전요청 사용
        resp = self._safe_request("GET", url, headers=headers, params=params, timeout=(3.0, 7.0))
        return resp.json()

    def inquire_balance_all(self, *, max_empty_retry: int = 2) -> dict:
        """
        ✅ 페이징/디바운스 적용 잔고 전체 조회
        반환: {'output1': [...], 'output2': {...}, 'ctx_area_fk100': '...', 'ctx_area_nk100': '...'}
        """
        fk = nk = ""
        all_rows: List[dict] = []
        out2_last = None  # 🔸 요약 블록(예수금 등) → '첫 페이지' 것만 유지
        empty_cnt = 0
        while True:
            try:
                j = self._inquire_balance_page(fk, nk)
            except Exception as e:
                logger.error("[잔고조회 예외] %s", e)
                if empty_cnt < max_empty_retry:
                    empty_cnt += 1
                    time.sleep(0.7)
                    continue
                break

            logger.info(f"[잔고조회 응답] {j}")

            rows = j.get("output1") or []
            if not rows:
                empty_cnt += 1
                if empty_cnt <= max_empty_retry:
                    time.sleep(0.6)
                    continue
                else:
                    break
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
        try:
            j = self.inquire_balance_all()
            out2 = j.get("output2")
            cash, meta = self._parse_cash_from_output2(out2)
            logger.info(
                "[CASH] raw=%s orderable=%s source_fields=%s clamp_applied=%s",
                meta.get("raw_fields"),
                cash,
                meta.get("selected_key"),
                meta.get("clamp_applied"),
            )
            if cash > 0:
                self._last_cash = cash
                logger.info("[CASH_BALANCE_OK] ord_psbl_cash≈%s원", f"{cash:,}")
                return cash
            # 0원이면 캐시 폴백
            if self._last_cash is not None and self._last_cash > 0:
                logger.warning("[CASH_FALLBACK] live=0 → use last=%s", f"{self._last_cash:,}")
                return self._last_cash
        except Exception as e:
            logger.error(f"[CASH_BALANCE_FAIL] {e}")
            if self._last_cash is not None and self._last_cash > 0:
                logger.warning("[CASH_FALLBACK] netfail → use last=%s", f"{self._last_cash:,}")
                return self._last_cash
        return 0

    def get_positions(self) -> List[Dict]:
        """보유 종목 전체(페이징 병합)."""
        try:
            j = self.inquire_balance_all()
            return j.get("output1") or []
        except Exception as e:
            logger.error("[GET_POSITIONS_FAIL] %s", e)
            return []

    def get_balance_map(self) -> Dict[str, int]:
        pos = self.get_positions()
        mp: Dict[str, int] = {}
        for row in pos or []:
            try:
                pdno = safe_strip(row.get("pdno"))
                hldg = int(float(row.get("hldg_qty", "0")))
                ord_psbl = int(float(row.get("ord_psbl_qty", "0")))
                qty = hldg if hldg > 0 else ord_psbl
                if pdno and qty > 0:
                    mp[pdno] = qty
            except Exception:
                continue
        logger.info(f"[보유수량맵] {len(mp)}종목")
        return mp

    # --- 호환 셔임(기존 trader.py 호출 대응) ---
    def get_balance(self) -> Dict[str, object]:
        return {"cash": self.get_cash_balance(), "positions": self.get_positions()}

    def get_balance_all(self) -> Dict[str, object]:
        """trader.py의 _fetch_balances에서 우선 호출되는 호환용 메서드."""
        return self.get_balance()

    # -------------------------------
    # 주문 공통, 시장가/지정가, 매수/매도
    # -------------------------------
    def _order_cash(self, body: dict, *, is_sell: bool) -> Optional[dict]:
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"

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

                # 레이트리밋(주문은 별 키)
                self._limiter.wait("orders")

                # 로깅(민감 Mask)
                log_body_masked = {
                    k: (v if k not in ("CANO", "ACNT_PRDT_CD") else "***")
                    for k, v in body.items()
                }
                logger.info(f"[주문요청] tr_id={tr_id} ord_dvsn={ord_dvsn} body={log_body_masked}")

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
        body = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": self.ACNT_PRDT_CD,
            "PDNO": safe_strip(pdno),
            "ORD_QTY": str(int(qty)),
            "ORD_DVSN": "01",  # 시장가
            "ORD_UNPR": "0",
        }
        return self._order_cash(body, is_sell=False)

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
            logger.error(f"[SELL_PRECHECK] 보유 없음/수량 0 pdno={pdno} hldg={hldg} ord_psbl={ord_psbl}")
            return None

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
        # [CHG] 안전요청 사용
        resp = self._safe_request(
            "POST", url, headers=headers, data=_json_dumps(body).encode("utf-8"), timeout=(3.0, 7.0)
        )
        data = resp.json()
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
