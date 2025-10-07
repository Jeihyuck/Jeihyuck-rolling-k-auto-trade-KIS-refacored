import os
import json
import time
import random
import logging
import threading
import csv
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

import requests
import pytz
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from settings import APP_KEY, APP_SECRET, API_BASE_URL, CANO, ACNT_PRDT_CD, KIS_ENV
from .slippage import Quote  # get_quote 반환 호환

logger = logging.getLogger(__name__)

# =====================================================
# 환경변수/기본값 fallback
# =====================================================
if not API_BASE_URL:
    API_BASE_URL = os.getenv("API_BASE_URL", "https://openapivts.koreainvestment.com:29443")

# 표준/별칭 ENV 동시 지원을 위한 헬퍼
_DEF = object()

def _env_first(*keys: str, default: Optional[str] = None) -> Optional[str]:
    for k in keys:
        v = os.getenv(k, _DEF)
        if v is not _DEF and v is not None and str(v).strip() != "":
            return str(v)
    return default


def _bool_env(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "y", "yes", "on"}


logger.info(f"[환경변수 체크] APP_KEY={repr(APP_KEY)}")
logger.info(f"[환경변수 체크] CANO={repr(CANO)}")
logger.info(f"[환경변수 체크] ACNT_PRDT_CD={repr(ACNT_PRDT_CD)}")
logger.info(f"[환경변수 체크] API_BASE_URL={repr(API_BASE_URL)}")
logger.info(f"[환경변수 체크] KIS_ENV={repr(KIS_ENV)}")

# =====================================================
# 유틸
# =====================================================

def safe_strip(val):
    if val is None:
        return ""
    if isinstance(val, str):
        return val.replace("\n", "").replace("\r", "").strip()
    return str(val).strip()


def _json_dumps(body: dict) -> str:
    """HashKey/주문 본문 모두 동일 직렬화 문자열을 사용."""
    return json.dumps(body, ensure_ascii=False, separators=(",", ":"), sort_keys=False)

# =====================================================
# 체결 기록 CSV 저장
# =====================================================

def append_fill(side: str, code: str, name: str, qty: int, price: float, odno: str, note: str = ""):
    try:
        os.makedirs("fills", exist_ok=True)
        path = f"fills/fills_{datetime.now().strftime('%Y%m%d')}.csv"
        header = ["ts", "side", "code", "name", "qty", "price", "ODNO", "note"]
        row = [
            datetime.now().isoformat(),
            side,
            code,
            name or "",
            int(qty),
            float(price) if price is not None else 0.0,
            str(odno) if odno is not None else "",
            note or "",
        ]
        new = not os.path.exists(path)
        with open(path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if new:
                w.writerow(header)
            w.writerow(row)
        logger.info(f"[APPEND_FILL] {side} {code} qty={qty} price={price} odno={odno}")
    except Exception as e:
        logger.warning(f"[APPEND_FILL_FAIL] side={side} code={code} ex={e}")

# =====================================================
# 간단 레이트리미터(엔드포인트별 최소 간격 유지)
# =====================================================

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

# =====================================================
# TR_ID 맵(최신 스펙 반영) + 환경변수 오버라이드
# =====================================================
TR_MAP = {
    "practice": {
        "ORDER_BUY": [os.getenv("KIS_TR_ID_ORDER_BUY", "VTTC0012U"), "VTTC0802U"],
        "ORDER_SELL": [os.getenv("KIS_TR_ID_ORDER_SELL", "VTTC0011U"), "VTTC0801U"],
        "BALANCE": [os.getenv("KIS_TR_ID_BALANCE", "VTTC8434R")],
        "PRICE": [os.getenv("KIS_TR_ID_PRICE", "FHKST01010100")],
        "ORDERBOOK": [os.getenv("KIS_TR_ID_ORDERBOOK", "FHKST01010200")],
        "DAILY_CHART": [os.getenv("KIS_TR_ID_DAILY_CHART", "FHKST03010100")],
        "TOKEN": "/oauth2/tokenP",
    },
    "real": {
        "ORDER_BUY": [os.getenv("KIS_TR_ID_ORDER_BUY_REAL", "TTTC0012U")],
        "ORDER_SELL": [os.getenv("KIS_TR_ID_ORDER_SELL_REAL", "TTTC0011U")],
        "BALANCE": [os.getenv("KIS_TR_ID_BALANCE_REAL", "TTTC8434R")],
        "PRICE": [os.getenv("KIS_TR_ID_PRICE_REAL", "FHKST01010100")],
        "ORDERBOOK": [os.getenv("KIS_TR_ID_ORDERBOOK_REAL", "FHKST01010200")],
        "DAILY_CHART": [os.getenv("KIS_TR_ID_DAILY_CHART_REAL", "FHKST03010100")],
        "TOKEN": "/oauth2/token",
    },
}


def _pick_tr(env: str, key: str) -> List[str]:
    try:
        return TR_MAP[env][key]
    except Exception:
        return []

# =====================================================
# 본체
# =====================================================

class KisAPI:
    """
    - 최신 TR 및 파라미터 대소문자(엑셀 스펙) 정합
    - HashKey 의무 적용(API 문서 기준)
    - 견고한 토큰 캐시/재발급, 지수형 백오프
    - 호환 메서드 유지 + 신규(place_limit_ioc/place_market/get_quote)
    - degraded mode:
        * 계좌정보(CANO/ACNT_PRDT_CD) 미설정 시 주문 차단(시세/조회만 허용)
        * 토큰 발급 불가 시 전체를 읽기전용(시세/조회만)으로 시작
    """

    _token_cache = {"token": None, "expires_at": 0, "last_issued": 0}
    _cache_path = "kis_token_cache.json"
    _token_lock = threading.Lock()

    def __init__(self):
        # 1) 기본 settings 값을 우선, 비었으면 별칭 ENV(KIS_*)로 보강
        self.CANO = safe_strip(CANO) or safe_strip(_env_first("KIS_CANO", "CANO", default=""))
        self.ACNT_PRDT_CD = safe_strip(ACNT_PRDT_CD) or safe_strip(_env_first("KIS_ACNT_PRDT_CD", "ACNT_PRDT_CD", default=""))
        self.env = safe_strip(KIS_ENV or _env_first("KIS_ENV", default="practice")).lower()
        if self.env not in ("practice", "real"):
            self.env = "practice"

        # 2) 세션 + 재시도 어댑터
        self.session = requests.Session()
        retry = Retry(
            total=3,
            connect=3,
            read=3,
            status=3,
            backoff_factor=0.5,
            status_forcelist=(500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        self._limiter = _RateLimiter(min_interval_sec=0.20)

        # 최근 매도 방지(메모리 기반)
        self._recent_sells: Dict[str, float] = {}
        self._recent_sells_lock = threading.Lock()
        self._recent_sells_cooldown = 60.0

        # degraded 사유 분리
        self._degraded_account = False  # 계좌정보 누락
        self._degraded_token = False    # 토큰 발급 실패

        # 계좌 필수값 확인(주문 필요시)
        if not (self.CANO and self.ACNT_PRDT_CD):
            self._degraded_account = True
            logger.warning("[KisAPI init] 계좌 ENV 누락(CANO/ACNT_PRDT_CD) → 주문은 차단됩니다. 시세/조회만 허용")

        # 토큰 확보 시도 (시세/조회에 필요). 실패해도 전체 동작은 계속(조회 일부 제한)
        try:
            self.token = self.get_valid_token()
        except Exception as e:
            self._degraded_token = True
            self.token = self._token_cache.get("token")
            logger.warning(f"[KisAPI init] 토큰 발급 실패 → 읽기전용 모드 강화: {e}")

        self.degraded = self._degraded_account or self._degraded_token
        logger.info(
            f"[KisAPI init] CANO={repr('***' if self.CANO else '')} env={self.env} "
            f"degraded_account={self._degraded_account} degraded_token={self._degraded_token}"
        )

    # -------------------------------
    # 토큰
    # -------------------------------
    def get_valid_token(self):
        with KisAPI._token_lock:
            now = time.time()
            # 캐시 유효하면 사용
            if self._token_cache["token"] and now < self._token_cache["expires_at"] - 600:
                return self._token_cache["token"]

            # 파일 캐시 읽기 우선
            if os.path.exists(self._cache_path):
                try:
                    with open(self._cache_path, "r", encoding="utf-8") as f:
                        cache = json.load(f)
                    if "access_token" in cache and now < cache["expires_at"] - 600:
                        self._token_cache.update({
                            "token": cache["access_token"],
                            "expires_at": cache["expires_at"],
                            "last_issued": cache.get("last_issued", 0),
                        })
                        logger.info(f"[토큰캐시] 파일캐시 사용: {cache['access_token'][:10]}... 만료:{cache['expires_at']}")
                        return cache["access_token"]
                except Exception as e:
                    logger.warning(f"[토큰캐시 읽기 실패] {e}")

            # 1분 내 재발급 차단
            if now - self._token_cache["last_issued"] < 61:
                logger.warning("[토큰] 1분 이내 재발급 시도 차단, 기존 토큰 재사용")
                if self._token_cache["token"]:
                    return self._token_cache["token"]
                raise Exception("토큰 발급 제한(1분 1회), 잠시 후 재시도 필요")

            # 정상 발급 시도
            token, expires_in = self._issue_token_and_expire()
            expires_at = now + int(expires_in)
            self._token_cache.update({"token": token, "expires_at": expires_at, "last_issued": now})
            try:
                with open(self._cache_path, "w", encoding="utf-8") as f:
                    json.dump({"access_token": token, "expires_at": expires_at, "last_issued": now}, f, ensure_ascii=False)
            except Exception as e:
                logger.warning(f"[토큰캐시 쓰기 실패] {e}")
            logger.info("[토큰캐시] 새 토큰 발급 및 캐시")
            return token

    def _issue_token_and_expire(self):
        token_path = TR_MAP[self.env]["TOKEN"]
        url = f"{API_BASE_URL}{token_path}"
        headers = {"content-type": "application/json"}
        # APP_*와 KIS_* 별칭 모두 지원
        app_key = APP_KEY or _env_first("APP_KEY", "KIS_APP_KEY", default="")
        app_secret = APP_SECRET or _env_first("APP_SECRET", "KIS_APP_SECRET", default="")
        data = {"grant_type": "client_credentials", "appkey": app_key, "appsecret": app_secret}

        last_exc = None
        # 지수 백오프 재시도 (추가 로컬 재시도 외에도 session adapter retry 있음)
        for attempt in range(1, 5):
            try:
                resp = self.session.post(url, json=data, headers=headers, timeout=(7.0, 14.0))
                j = resp.json()
                if "access_token" in j:
                    logger.info(f"[🔑 토큰발급] 성공: {j}")
                    return j["access_token"], j.get("expires_in", 86400)
                logger.error(f"[🔑 토큰발급 실패] {j.get('error_description', j)}")
                last_exc = Exception(j)
                break
            except Exception as e:
                backoff = min(1.0 * (2 ** (attempt - 1)), 16) + random.uniform(0, 0.6)
                logger.warning(f"[토큰발급 예외] attempt={attempt} err={e} → sleep {backoff:.2f}s")
                time.sleep(backoff)
                last_exc = e
                continue

        logger.error(f"[토큰발급 최종 실패] last_exc={last_exc}")
        raise last_exc or Exception('토큰 발급 실패(최종)')

    # -------------------------------
    # 헤더/HashKey
    # -------------------------------
    def _headers(self, tr_id: str, hashkey: Optional[str] = None):
        # APP_*와 KIS_* 별칭 모두 지원
        app_key = APP_KEY or _env_first("APP_KEY", "KIS_APP_KEY", default="")
        app_secret = APP_SECRET or _env_first("APP_SECRET", "KIS_APP_SECRET", default="")
        h = {
            "appkey": app_key,
            "appsecret": app_secret,
            "tr_id": tr_id,
            "custtype": "P",
            "content-type": "application/json; charset=utf-8",
        }
        # authorization header only added when token exists
        token = None
        try:
            token = self.get_valid_token()
        except Exception:
            token = self._token_cache.get("token")

        if token:
            h["authorization"] = f"Bearer {token}"
        if hashkey:
            h["hashkey"] = hashkey
        return h

    def _create_hashkey(self, body_dict: dict) -> str:
        # APP_*와 KIS_* 별칭 모두 지원
        app_key = APP_KEY or _env_first("APP_KEY", "KIS_APP_KEY", default="")
        app_secret = APP_SECRET or _env_first("APP_SECRET", "KIS_APP_SECRET", default="")
        url = f"{API_BASE_URL}/uapi/hashkey"
        headers = {
            "content-type": "application/json; charset=utf-8",
            "appkey": app_key,
            "appsecret": app_secret,
        }
        body_str = _json_dumps(body_dict)
        try:
            r = self.session.post(url, headers=headers, data=body_str.encode("utf-8"), timeout=(3.0, 5.0))
            j = r.json()
        except Exception as e:
            logger.error(f"[HASHKEY 예외] {e}")
            raise
        hk = j.get("HASH") or j.get("hash") or j.get("hashkey")
        if not hk:
            logger.error(f"[HASHKEY 실패] resp={j}")
            raise Exception(f"HashKey 생성 실패: {j}")
        return hk

    # -------------------------------
    # 시세/장운영
    # -------------------------------
    def get_current_price(self, code: str) -> float:
        """
        - API: /uapi/domestic-stock/v1/quotations/inquire-price (v1_국내주식-008)
        - Query(엑셀 대문자 기준): FID_COND_MRKT_DIV_CODE, FID_INPUT_ISCD
        - 초과 시 '초당 거래건수' 안내 → 짧게 backoff 후 재시도
        """
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
        tried: List[Tuple[str, str, Any]] = []
        self._limiter.wait("quotes")

        for tr in _pick_tr(self.env, "PRICE"):
            headers = self._headers(tr)
            markets = ["J", "U"]
            c = code.strip()
            codes = [c, f"A{c}"] if not c.startswith("A") else [c, c[1:]]
            for market_div in markets:
                for code_fmt in codes:
                    params = {"FID_COND_MRKT_DIV_CODE": market_div, "FID_INPUT_ISCD": code_fmt}
                    try:
                        resp = self.session.get(url, headers=headers, params=params, timeout=(3.0, 5.0))
                        data = resp.json()
                    except Exception as e:
                        tried.append((market_div, code_fmt, f"EXC:{e}"))
                        continue
                    tried.append((market_div, code_fmt, data.get("rt_cd"), data.get("msg1")))
                    if "초당 거래건수" in (data.get("msg1") or ""):
                        time.sleep(0.35 + random.uniform(0, 0.15))
                        continue
                    if resp.status_code == 200 and data.get("rt_cd") == "0" and data.get("output"):
                        try:
                            return float(data["output"].get("stck_prpr"))
                        except Exception:
                            pass
        raise Exception(f"현재가 조회 실패({code}): tried={tried}")

    def get_orderbook_strength(self, code: str) -> Optional[float]:
        """5단 잔량 기준 간이 체결강도. 실패 시 None"""
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-askprice"
        self._limiter.wait("orderbook")
        for tr in _pick_tr(self.env, "ORDERBOOK"):
            headers = self._headers(tr)
            markets = ["J", "U"]
            c = code.strip()
            codes = [c, f"A{c}"] if not c.startswith("A") else [c, c[1:]]
            for market_div in markets:
                for code_fmt in codes:
                    params = {"FID_COND_MRKT_DIV_CODE": market_div, "FID_INPUT_ISCD": code_fmt}
                    try:
                        resp = self.session.get(url, headers=headers, params=params, timeout=(3.0, 5.0))
                        data = resp.json()
                    except Exception:
                        continue
                    if resp.status_code == 200 and data.get("rt_cd") == "0" and data.get("output"):
                        out = data["output"]
                        # 잔량
                        bid = sum(float(out.get(f"bidp_rsqn{i}") or 0) for i in range(1, 6))
                        ask = sum(float(out.get(f"askp_rsqn{i}") or 0) for i in range(1, 6))
                        if (bid + ask) > 0:
                            return 100.0 * bid / max(1.0, ask)
        return None

    def get_quote(self, code: str) -> Quote:
        """최우선 호가/현재가를 묶어 반환 (OrderRouter/Slippage 사용).
        - 1순위: 호가 API(/inquire-askprice)의 bidp1/askp1
        - 2순위: 현재가 API(/inquire-price)의 stck_prpr (bid/ask는 None)
        """
        # 1) 호가
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-askprice"
        self._limiter.wait("orderbook")
        for tr in _pick_tr(self.env, "ORDERBOOK"):
            headers = self._headers(tr)
            markets = ["J", "U"]
            c = code.strip()
            codes = [c, f"A{c}"] if not c.startswith("A") else [c, c[1:]]
            for market_div in markets:
                for code_fmt in codes:
                    params = {"FID_COND_MRKT_DIV_CODE": market_div, "FID_INPUT_ISCD": code_fmt}
                    try:
                        resp = self.session.get(url, headers=headers, params=params, timeout=(3.0, 5.0))
                        data = resp.json()
                    except Exception:
                        continue
                    if resp.status_code == 200 and data.get("rt_cd") == "0" and data.get("output"):
                        out = data["output"]
                        # 다양한 키 케이스 방어
                        ask = None
                        bid = None
                        for k in ("askp1", "askp_prc_1", "askp", "askp0"):
                            if out.get(k) is not None:
                                try:
                                    ask = float(out.get(k))
                                    break
                                except Exception:
                                    pass
                        for k in ("bidp1", "bidp_prc_1", "bidp", "bidp0"):
                            if out.get(k) is not None:
                                try:
                                    bid = float(out.get(k))
                                    break
                                except Exception:
                                    pass
                        last = None
                        try:
                            last = float(out.get("stck_prpr")) if out.get("stck_prpr") is not None else None
                        except Exception:
                            last = None
                        if ask is not None or bid is not None:
                            return Quote(code=code, bid=bid, ask=ask, last=last)
        # 2) 현재가만
        try:
            last = self.get_current_price(code)
        except Exception:
            last = None
        return Quote(code=code, bid=None, ask=None, last=last)

    def get_daily_candles(self, code: str, count: int = 30) -> List[Dict[str, Any]]:
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        self._limiter.wait("daily")
        for tr in _pick_tr(self.env, "DAILY_CHART"):
            headers = self._headers(tr)
            params = {
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": code if code.startswith("A") else f"A{code}",
                "FID_ORG_ADJ_PRC": "0",
                "FID_PERIOD_DIV_CODE": "D",
            }
            try:
                resp = self.session.get(url, headers=headers, params=params, timeout=(3.0, 7.0))
                data = resp.json()
            except Exception:
                continue
            if resp.status_code == 200 and data.get("rt_cd") == "0" and data.get("output"):
                arr = data["output"]
                rows = [
                    {
                        "date": r.get("stck_bsop_date"),
                        "open": float(r.get("stck_oprc")),
                        "high": float(r.get("stck_hgpr")),
                        "low": float(r.get("stck_lwpr")),
                        "close": float(r.get("stck_clpr")),
                    }
                    for r in arr[: max(count, 20)] if r.get("stck_oprc") is not None
                ]
                rows.sort(key=lambda x: x["date"])
                return rows[-count:]
        return []

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

    def is_market_open(self) -> bool:
        kst = pytz.timezone("Asia/Seoul")
        now = datetime.now(kst)
        if now.weekday() >= 5:
            return False
        open_time = now.replace(hour=9, minute=0, second=0, microsecond=0)
        close_time = now.replace(hour=15, minute=20, second=0, microsecond=0)
        return open_time <= now <= close_time

    # -------------------------------
    # 잔고/포지션
    # -------------------------------
    def get_cash_balance(self) -> int:
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-balance"
        headers = None
        for tr in _pick_tr(self.env, "BALANCE"):
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
                "CTX_AREA_FK100": "",
                "CTX_AREA_NK100": "",
            }
            logger.info(f"[잔고조회 요청파라미터] {params}")
            try:
                resp = self.session.get(url, headers=headers, params=params, timeout=(3.0, 7.0))
                j = resp.json()
            except Exception as e:
                logger.error(f"[잔고조회 예외] {e}")
                continue
            logger.info(f"[잔고조회 응답] {j}")
            if j.get("rt_cd") == "0" and "output2" in j and j["output2"]:
                try:
                    cash = int(j["output2"][0]["dnca_tot_amt"])
                    logger.info(f"[CASH_BALANCE] 현재 예수금: {cash:,}원")
                    return cash
                except Exception as e:
                    logger.error(f"[CASH_BALANCE_PARSE_FAIL] {e}")
                    continue
        logger.error("[CASH_BALANCE_FAIL] 모든 TR 실패")
        return 0

    def get_positions(self) -> List[Dict]:
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-balance"
        for tr in _pick_tr(self.env, "BALANCE"):
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
                "CTX_AREA_FK100": "",
                "CTX_AREA_NK100": "",
            }
            try:
                resp = self.session.get(url, headers=headers, params=params, timeout=(3.0, 7.0))
                j = resp.json()
            except Exception:
                continue
            if j.get("rt_cd") == "0" and j.get("output1") is not None:
                return j.get("output1") or []
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

    # =====================================================
    # 주문 공통(신규: 라우터 연동을 위한 메서드 제공)
    # =====================================================

    def _order_block_if_degraded(self) -> Optional[dict]:
        """주문 전 degraded 여부 검사. 차단 시 사유를 명확히 답한다."""
        if self._degraded_account:
            return {"status": "fail", "error": "missing_account_env", "message": "CANO/ACNT_PRDT_CD not set"}
        if self._degraded_token:
            return {"status": "fail", "error": "token_unavailable", "message": "access token unavailable"}
        return None

    def place_limit_ioc(self, *, code: str, side: str, qty: int, price: float) -> Dict[str, Any]:
        """
        - 실제 IOC 지정가: 실계좌(SOR 가능)에서만 ORD_DVSN='11' + EXCG_ID_DVSN_CD='SOR'
        - 모의계좌(KRX만 가능): ORD_DVSN='00'(지정가)로 에뮬레이션
        """
        blocked = self._order_block_if_degraded()
        if blocked:
            return blocked

        pdno = safe_strip(code)
        body = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": self.ACNT_PRDT_CD,
            "PDNO": pdno,
            "ORD_QTY": str(int(qty)),
            "ORD_UNPR": str(int(price)),
            "EXCG_ID_DVSN_CD": "KRX",
        }
        is_sell = str(side).upper() == "SELL"
        if is_sell:
            body["SLL_TYPE"] = "01"  # 일반매도

        if self.env == "real":
            # 실계좌에서만 SOR IOC 사용 시도
            body["EXCG_ID_DVSN_CD"] = "SOR"
            body["ORD_DVSN"] = "11"  # IOC 지정가
        else:
            body["ORD_DVSN"] = "00"  # 지정가 (모의는 KRX만 허용)

        hk = self._create_hashkey(body)
        tr_list = _pick_tr(self.env, "ORDER_SELL" if is_sell else "ORDER_BUY")
        tr_id = tr_list[0]
        headers = self._headers(tr_id, hk)
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"

        self._limiter.wait("orders")
        log_body_masked = {k: (v if k not in ("CANO", "ACNT_PRDT_CD") else "***") for k, v in body.items()}
        logger.info(f"[ORDER IOC-LIMIT REQ] tr_id={tr_id} body={log_body_masked}")
        try:
            resp = self.session.post(url, headers=headers, data=_json_dumps(body).encode("utf-8"), timeout=(3.0, 7.0))
            data = resp.json()
        except Exception as e:
            return {"status": "fail", "error": str(e)}
        if resp.status_code == 200 and data.get("rt_cd") == "0":
            out = data.get("output") or {}
            return {"status": "ok", "order_id": out.get("ODNO"), "filled_qty": int(body["ORD_QTY"]), "remaining_qty": 0, "raw": data}
        return {"status": "fail", "error_code": data.get("msg_cd"), "error_msg": data.get("msg1"), "raw": data}

    def place_market(self, *, code: str, side: str, qty: int) -> Dict[str, Any]:
        blocked = self._order_block_if_degraded()
        if blocked:
            return blocked

        pdno = safe_strip(code)
        body = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": self.ACNT_PRDT_CD,
            "PDNO": pdno,
            "ORD_QTY": str(int(qty)),
            "ORD_DVSN": "01",  # 시장가
            "ORD_UNPR": "0",
            "EXCG_ID_DVSN_CD": "KRX",
        }
        is_sell = str(side).upper() == "SELL"
        if is_sell:
            body["SLL_TYPE"] = "01"

        hk = self._create_hashkey(body)
        tr_list = _pick_tr(self.env, "ORDER_SELL" if is_sell else "ORDER_BUY")
        tr_id = tr_list[0]
        headers = self._headers(tr_id, hk)
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"

        self._limiter.wait("orders")
        log_body_masked = {k: (v if k not in ("CANO", "ACNT_PRDT_CD") else "***") for k, v in body.items()}
        logger.info(f"[ORDER MARKET REQ] tr_id={tr_id} body={log_body_masked}")
        try:
            resp = self.session.post(url, headers=headers, data=_json_dumps(body).encode("utf-8"), timeout=(3.0, 7.0))
            data = resp.json()
        except Exception as e:
            return {"status": "fail", "error": str(e)}
        if resp.status_code == 200 and data.get("rt_cd") == "0":
            out = data.get("output") or {}
            return {"status": "ok", "order_id": out.get("ODNO"), "filled_qty": int(body["ORD_QTY"]), "remaining_qty": 0, "raw": data}
        return {"status": "fail", "error_code": data.get("msg_cd"), "error_msg": data.get("msg1"), "raw": data}

    # -------------------------------
    # (기존) 범용 주문 래퍼 + 호환 buy/sell_* API
    # -------------------------------
    def _order_cash(self, body: dict, *, is_sell: bool) -> Optional[dict]:
        blocked = self._order_block_if_degraded()
        if blocked:
            logger.warning("[ORDER_BLOCKED] degraded mode - 주문 차단")
            return blocked

        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
        tr_list = _pick_tr(self.env, "ORDER_SELL" if is_sell else "ORDER_BUY")

        # Fallback: 시장가 → (실계좌) IOC시장가 → 최유리
        ord_dvsn_chain = ["01"]
        if self.env == "real":
            ord_dvsn_chain.append("13")  # IOC시장가(SOR)
        ord_dvsn_chain.append("03")     # 최유리

        last_err = None

        for tr_id in tr_list:
            for ord_dvsn in ord_dvsn_chain:
                body["ORD_DVSN"] = ord_dvsn
                if ord_dvsn == "01":
                    body["ORD_UNPR"] = "0"
                if is_sell and not body.get("SLL_TYPE"):
                    body["SLL_TYPE"] = "01"
                body.setdefault("EXCG_ID_DVSN_CD", "KRX")

                try:
                    hk = self._create_hashkey(body)
                except Exception as e:
                    logger.error(f"[ORDER_HASH_FAIL] body={body} ex={e}")
                    last_err = e
                    continue

                headers = self._headers(tr_id, hk)
                self._limiter.wait("orders")
                log_body_masked = {k: (v if k not in ("CANO", "ACNT_PRDT_CD") else "***") for k, v in body.items()}
                logger.info(f"[주문요청] tr_id={tr_id} ord_dvsn={ord_dvsn} body={log_body_masked}")

                for attempt in range(1, 4):
                    try:
                        resp = self.session.post(
                            url, headers=headers, data=_json_dumps(body).encode("utf-8"), timeout=(3.0, 7.0)
                        )
                        data = resp.json()
                    except Exception as e:
                        backoff = min(0.6 * (1.7 ** (attempt - 1)), 5.0) + random.uniform(0, 0.35)
                        logger.error(
                            f"[ORDER_NET_EX] tr_id={tr_id} ord_dvsn={ord_dvsn} attempt={attempt} ex={e} → sleep {backoff:.2f}s"
                        )
                        time.sleep(backoff)
                        last_err = e
                        continue

                    if resp.status_code == 200 and data.get("rt_cd") == "0":
                        logger.info(f"[ORDER_OK] tr_id={tr_id} ord_dvsn={ord_dvsn} output={data.get('output')}")
                        try:
                            out = data.get("output") or {}
                            odno = out.get("ODNO") or out.get("ord_no") or ""
                            pdno = safe_strip(body.get("PDNO", ""))
                            qty = int(float(body.get("ORD_QTY", "0")))
                            price_for_fill = 0.0
                            ord_unpr = body.get("ORD_UNPR")
                            if ord_unpr and str(ord_unpr) not in ("0", "0.0", ""):
                                price_for_fill = float(ord_unpr)
                            else:
                                try:
                                    price_for_fill = float(self.get_current_price(pdno))
                                except Exception:
                                    price_for_fill = 0.0
                            side = "SELL" if is_sell else "BUY"
                            append_fill(side=side, code=pdno, name="", qty=qty, price=price_for_fill, odno=odno, note=f"tr={tr_id},ord_dvsn={ord_dvsn}")
                        except Exception as e:
                            logger.warning(f"[APPEND_FILL_EX] ex={e} resp={data}")
                        return data

                    msg_cd = data.get("msg_cd", "")
                    msg1 = data.get("msg1", "")
                    if msg_cd == "IGW00008" or "MCA" in msg1 or resp.status_code >= 500:
                        backoff = min(0.6 * (1.7 ** (attempt - 1)), 5.0) + random.uniform(0, 0.35)
                        logger.error(
                            f"[ORDER_FAIL_GATEWAY] tr_id={tr_id} ord_dvsn={ord_dvsn} attempt={attempt} resp={data} → sleep {backoff:.2f}s"
                        )
                        time.sleep(backoff)
                        last_err = data
                        continue

                    logger.error(f"[ORDER_FAIL_BIZ] tr_id={tr_id} ord_dvsn={ord_dvsn} resp={data}")
                    return None

                logger.warning(f"[ORDER_FALLBACK] tr_id={tr_id} ord_dvsn={ord_dvsn} 실패 → 다음 방식 시도")

        raise Exception(f"주문 실패: {last_err}")

    # -------------------------------
    # 매수/매도 (신규 + 호환)
    # -------------------------------
    def buy_stock_market(self, pdno: str, qty: int) -> Optional[dict]:
        body = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": self.ACNT_PRDT_CD,
            "PDNO": safe_strip(pdno),
            "ORD_QTY": str(int(qty)),
            "ORD_DVSN": "01",  # 시장가
            "ORD_UNPR": "0",
            "EXCG_ID_DVSN_CD": "KRX",
        }
        return self._order_cash(body, is_sell=False)

    def sell_stock_market(self, pdno: str, qty: int) -> Optional[dict]:
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
            logger.warning(f"[SELL_PRECHECK] 수량 보정: req={qty} -> base={base_qty} (hldg={hldg}, ord_psbl={ord_psbl})")
            qty = base_qty

        body = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": self.ACNT_PRDT_CD,
            "PDNO": safe_strip(pdno),
            "SLL_TYPE": "01",
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
                for k in [k for k, v in self._recent_sells.items() if v < cutoff]:
                    del self._recent_sells[k]
        return resp

    def buy_stock_limit(self, pdno: str, qty: int, price: int) -> Optional[dict]:
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
        resp = self.session.post(url, headers=headers, data=_json_dumps(body).encode("utf-8"), timeout=(3.0, 7.0))
        data = resp.json()
        if resp.status_code == 200 and data.get("rt_cd") == "0":
            logger.info(f"[BUY_LIMIT_OK] output={data.get('output')}")
            try:
                out = data.get("output") or {}
                odno = out.get("ODNO") or out.get("ord_no") or ""
                pdno = safe_strip(body.get("PDNO", ""))
                qty_int = int(float(body.get("ORD_QTY", "0")))
                price_for_fill = float(body.get("ORD_UNPR", 0))
                append_fill(side="BUY", code=pdno, name="", qty=qty_int, price=price_for_fill, odno=odno, note=f"limit,tr={tr_id}")
            except Exception as e:
                logger.warning(f"[APPEND_FILL_LIMIT_BUY_FAIL] ex={e}")
            return data
        logger.error(f"[BUY_LIMIT_FAIL] {data}")
        return None

    def sell_stock_limit(self, pdno: str, qty: int, price: int) -> Optional[dict]:
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
            logger.error(f"[SELL_LIMIT_PRECHECK] 보유 없음/수량 0 pdno={pdno} hldg={hldg} ord_psbl={ord_psbl}")
            return None
        if qty > base_qty:
            logger.warning(f"[SELL_LIMIT_PRECHECK] 수량 보정: req={qty} -> base={base_qty} (hldg={hldg}, ord_psbl={ord_psbl})")
            qty = base_qty

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
        resp = self.session.post(url, headers=headers, data=_json_dumps(body).encode("utf-8"), timeout=(3.0, 7.0))
        data = resp.json()
        if resp.status_code == 200 and data.get("rt_cd") == "0":
            logger.info(f"[SELL_LIMIT_OK] output={data.get('output')}")
            try:
                out = data.get("output") or {}
                odno = out.get("ODNO") or out.get("ord_no") or ""
                pdno = safe_strip(body.get("PDNO", ""))
                qty_int = int(float(body.get("ORD_QTY", "0")))
                price_for_fill = float(body.get("ORD_UNPR", 0))
                append_fill(side="SELL", code=pdno, name="", qty=qty_int, price=price_for_fill, odno=odno, note=f"limit,tr={tr_id}")
            except Exception as e:
                logger.warning(f"[APPEND_FILL_LIMIT_SELL_FAIL] ex={e}")
            with self._recent_sells_lock:
                self._recent_sells[pdno] = time.time()
            return data
        logger.error(f"[SELL_LIMIT_FAIL] {data}")
        return None

    # --- 호환 셔임 ---
    def buy_stock(self, code: str, qty: int, price: Optional[int] = None):
        if price is None:
            return self.buy_stock_market(code, qty)
        return self.buy_stock_limit(code, qty, price)

    def sell_stock(self, code: str, qty: int, price: Optional[int] = None):
        if price is None:
            return self.sell_stock_market(code, qty)
        return self.sell_stock_limit(code, qty, price)

    # --- 강제청산(시장가) 유틸 ---
    def close_position_market(self, code: str, qty: int) -> Dict[str, Any]:
        res = self.place_market(code=code, side="SELL", qty=qty)
        return res
