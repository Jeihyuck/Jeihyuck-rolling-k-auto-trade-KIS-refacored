import os
import json
import time
import random
import logging
import threading
import csv
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union

import requests
import pytz
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from settings import APP_KEY, APP_SECRET, API_BASE_URL, CANO, ACNT_PRDT_CD, KIS_ENV

logger = logging.getLogger(__name__)

# -------------------------------------------------
# Safe utilities
# -------------------------------------------------

def safe_strip(val: Any) -> str:
    """Remove CR/LF and trim spaces safely regardless of editor/OS.
    Uses translate with codepoints (10=LF, 13=CR) to avoid brittle escape sequences.
    """
    if val is None:
        return ""
    s = str(val)
    # Remove control chars without relying on escape literals
    try:
        s = s.translate({10: None, 13: None})  # LF, CR
    except Exception:
        s = "".join(ch for ch in s if ord(ch) not in (10, 13))
    return s.strip()


def _json_dumps(body: dict) -> str:
    """Stable JSON dumps for KIS HashKey/signature consistency."""
    return json.dumps(body, ensure_ascii=False, separators=(",", ":"), sort_keys=False)


logger.info(f"[환경변수 체크] APP_KEY={repr(APP_KEY)}")
logger.info(f"[환경변수 체크] CANO={repr(CANO)}")
logger.info(f"[환경변수 체크] ACNT_PRDT_CD={repr(ACNT_PRDT_CD)}")
logger.info(f"[환경변수 체크] API_BASE_URL={repr(API_BASE_URL)}")
logger.info(f"[환경변수 체크] KIS_ENV={repr(KIS_ENV)}")

# -------------------------------------------------
# Fills CSV helper (used by trader + here on order success)
# -------------------------------------------------

def append_fill(side: str, code: str, name: str, qty: int, price: float, odno: str, note: str = "") -> None:
    """Append a fill-like record to ./fills/YYYYMMDD.csv for auditing."""
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


# -------------------------------------------------
# Simple per-key rate limiter (best-effort)
# -------------------------------------------------

class _RateLimiter:
    def __init__(self, min_interval_sec: float = 0.20):
        self.min_interval = float(min_interval_sec)
        self._last_at: Dict[str, float] = {}
        self._lock = threading.Lock()

    def wait(self, key: str) -> None:
        with self._lock:
            now = time.time()
            last = self._last_at.get(key, 0.0)
            delta = now - last
            if delta < self.min_interval:
                time.sleep(self.min_interval - delta + random.uniform(0, 0.03))
            self._last_at[key] = time.time()


# -------------------------------------------------
# TR_ID map (env-override + fallback list)
# -------------------------------------------------

TR_MAP: Dict[str, Dict[str, List[str] | str]] = {
    "practice": {
        "ORDER_BUY": [os.getenv("KIS_TR_ID_ORDER_BUY", "VTTC0012U"), "VTTC0802U"],
        "ORDER_SELL": [os.getenv("KIS_TR_ID_ORDER_SELL", "VTTC0011U"), "VTTC0801U"],
        "BALANCE": [os.getenv("KIS_TR_ID_BALANCE", "VTTC8434R")],
        "PRICE": [os.getenv("KIS_TR_ID_PRICE", "FHKST01010100")],
        "ORDERBOOK": [os.getenv("KIS_TR_ID_ORDERBOOK", "FHKST01010200")],
        "DAILY_CHART": [os.getenv("KIS_TR_ID_DAILY_CHART", "FHKST03010100")],
        "ORDER_STATUS": [os.getenv("KIS_TR_ID_ORDER_STATUS", "VTTC0081R")],  # 주식일별주문체결조회(신TR)
        "TOKEN": "/oauth2/tokenP",
    },
    "real": {
        "ORDER_BUY": [os.getenv("KIS_TR_ID_ORDER_BUY_REAL", "TTTC0012U")],
        "ORDER_SELL": [os.getenv("KIS_TR_ID_ORDER_SELL_REAL", "TTTC0011U")],
        "BALANCE": [os.getenv("KIS_TR_ID_BALANCE_REAL", "TTTC8434R")],
        "PRICE": [os.getenv("KIS_TR_ID_PRICE_REAL", "FHKST01010100")],
        "ORDERBOOK": [os.getenv("KIS_TR_ID_ORDERBOOK_REAL", "FHKST01010200")],
        "DAILY_CHART": [os.getenv("KIS_TR_ID_DAILY_CHART_REAL", "FHKST03010100")],
        "ORDER_STATUS": [os.getenv("KIS_TR_ID_ORDER_STATUS_REAL", "TTTC0081R")],  # 주식일별주문체결조회(신TR)
        "TOKEN": "/oauth2/token",
    },
}


def _pick_tr(env: str, key: str) -> List[str]:
    try:
        v = TR_MAP[env][key]
        return list(v) if isinstance(v, list) else [str(v)]
    except Exception:
        return []


# -------------------------------------------------
# KIS API wrapper
# -------------------------------------------------

class KisAPI:
    """
    - TR_ID 최신 스펙 + 환경변수 오버라이드 + 후보 폴백 구현
    - HashKey 필수 적용
    - 시세/호가/일봉/ATR + (신규) 오늘시가/전일고저 + (신규) 주문체결조회(check_filled)
    - 레이트리밋 & 백오프 & 네트워크 재시도 강화
    - get_balance / buy_stock 등 기존 호출부 호환
    - (중요) 토큰은 지연 발급(lazy): 실제 API 호출 시점에만 발급/갱신
    """

    _token_cache: Dict[str, Any] = {"token": None, "expires_at": 0, "last_issued": 0}
    _cache_path = "kis_token_cache.json"
    _token_lock = threading.Lock()

    def __init__(self) -> None:
        self.CANO = safe_strip(CANO)
        self.ACNT_PRDT_CD = safe_strip(ACNT_PRDT_CD)
        self.env = safe_strip(KIS_ENV or "practice").lower()
        if self.env not in ("practice", "real"):
            self.env = "practice"

        # Session with retries
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

        # recent sell cooldown to avoid duplicates
        self._recent_sells: Dict[str, float] = {}
        self._recent_sells_lock = threading.Lock()
        self._recent_sells_cooldown = 60.0

        # DO NOT issue token in __init__ (prevents crash when many workflows import this)
        logger.info(
            f"[KIS] ENV={self.env} API_BASE_URL={API_BASE_URL} CANO={repr(self.CANO)} ACNT={repr(self.ACNT_PRDT_CD)}"
        )

        # last order pdno for optional status lookups
        self._last_order_pdno: Optional[str] = None

    # -------------------------------
    # Token management (lazy + 403 guard)
    # -------------------------------
    def get_valid_token(self) -> str:
        with KisAPI._token_lock:
            now = time.time()
            tok = KisAPI._token_cache["token"]
            exp = KisAPI._token_cache["expires_at"]
            if tok and now < exp - 300:
                return tok

            # file cache
            if os.path.exists(KisAPI._cache_path):
                try:
                    with open(KisAPI._cache_path, "r", encoding="utf-8") as f:
                        cache = json.load(f)
                    if cache.get("access_token") and now < cache.get("expires_at", 0) - 300:
                        KisAPI._token_cache.update({
                            "token": cache["access_token"],
                            "expires_at": cache.get("expires_at", 0),
                            "last_issued": cache.get("last_issued", 0),
                        })
                        logger.info("[토큰캐시] 파일캐시 사용")
                        return cache["access_token"]
                except Exception as e:
                    logger.warning(f"[토큰캐시 읽기 실패] {e}")

            # 1/min guard
            if now - KisAPI._token_cache.get("last_issued", 0) < 61:
                logger.warning("[토큰] 1분 이내 재발급 시도 차단, 기존/파일 캐시 재사용 시도")
                if KisAPI._token_cache.get("token"):
                    return KisAPI._token_cache["token"]
                raise Exception("토큰 발급 제한(1분 1회), 잠시 후 재시도 필요")

            token, expires_in = self._issue_token_and_expire()
            expires_at = now + int(expires_in)
            KisAPI._token_cache.update({"token": token, "expires_at": expires_at, "last_issued": now})
            try:
                with open(KisAPI._cache_path, "w", encoding="utf-8") as f:
                    json.dump({"access_token": token, "expires_at": expires_at, "last_issued": now}, f, ensure_ascii=False)
            except Exception as e:
                logger.warning(f"[토큰캐시 쓰기 실패] {e}")
            logger.info("[토큰캐시] 새 토큰 발급 및 캐시")
            return token

    def refresh_token(self) -> None:
        """Clear caches so next call will re-issue a token."""
        with KisAPI._token_lock:
            KisAPI._token_cache.update({"token": None, "expires_at": 0, "last_issued": 0})
            try:
                if os.path.exists(KisAPI._cache_path):
                    os.remove(KisAPI._cache_path)
            except Exception as e:
                logger.warning(f"[refresh_token] 캐시 파일 삭제 실패: {e}")
        logger.info("[refresh_token] 토큰 캐시 비움 → 다음 호출 시 재발급")

    def _issue_token_and_expire(self) -> Tuple[str, int]:
        token_path = str(TR_MAP[self.env]["TOKEN"])
        url = f"{API_BASE_URL}{token_path}"
        headers = {"content-type": "application/json"}
        data = {"grant_type": "client_credentials", "appkey": APP_KEY, "appsecret": APP_SECRET}
        last_err_desc = ""
        for attempt in range(1, 5):
            try:
                resp = self.session.post(url, json=data, headers=headers, timeout=(3.0, 7.0))
                j = resp.json()
            except Exception as e:
                back = min(0.8 * (1.6 ** (attempt - 1)), 6.0) + random.uniform(0, 0.5)
                logger.error(f"[🔑 토큰발급 네트워크 예외] attempt={attempt} ex={e} → sleep {back:.2f}s")
                time.sleep(back)
                last_err_desc = str(e)
                continue

            desc = (j.get("error_description") or "").strip()
            last_err_desc = desc or last_err_desc
            if resp.status_code == 200 and j.get("access_token"):
                logger.info("[🔑 토큰발급] 성공")
                return j["access_token"], int(j.get("expires_in", 86400))
            if resp.status_code == 403 and ("1분당 1회" in desc or "잠시 후 다시 시도" in desc):
                back = 62 + random.uniform(0, 2)
                logger.warning(f"[🔑 토큰발급 제한 감지] {desc} → sleep {back:.1f}s 후 재시도")
                time.sleep(back)
                continue
            back = min(1.0 * (1.7 ** (attempt - 1)), 8.0) + random.uniform(0, 0.7)
            logger.error(f"[🔑 토큰발급 실패] status={resp.status_code} resp={j} → sleep {back:.2f}s")
            time.sleep(back)
        raise Exception(f"토큰 발급 실패: {last_err_desc or 'unknown error'}")

    # -------------------------------
    # Headers / HashKey
    # -------------------------------
    def _headers(self, tr_id: str, hashkey: Optional[str] = None) -> Dict[str, str]:
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

    def _create_hashkey(self, body_dict: dict) -> str:
        url = f"{API_BASE_URL}/uapi/hashkey"
        headers = {
            "content-type": "application/json; charset=utf-8",
            "appkey": APP_KEY,
            "appsecret": APP_SECRET,
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
    # Market data
    # -------------------------------
    def get_current_price(self, code: str) -> float:
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
                    params = {"fid_cond_mrkt_div_code": market_div, "fid_input_iscd": code_fmt}
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
                        resp = self.session.get(url, headers=headers, params=params, timeout=(3.0, 5.0))
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

    def get_daily_candles(self, code: str, count: int = 30) -> List[Dict[str, Any]]:
        """Return most-recent N daily candles (oldest → newest)."""
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        self._limiter.wait("daily")
        for tr in _pick_tr(self.env, "DAILY_CHART"):
            headers = self._headers(tr)
            params = {
                "fid_cond_mrkt_div_code": "J",
                "fid_input_iscd": code if code.startswith("A") else f"A{code}",
                "fid_org_adj_prc": "0",
                "fid_period_div_code": "D",
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
                rows.sort(key=lambda x: x["date"])  # oldest → newest
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

    def get_today_open(self, code: str) -> Optional[float]:
        """Try price endpoint first; fallback to last candle open if date==today."""
        try:
            url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
            headers = self._headers(_pick_tr(self.env, "PRICE")[0])
            params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code if code.startswith("A") else f"A{code}"}
            self._limiter.wait("quotes")
            resp = self.session.get(url, headers=headers, params=params, timeout=(3.0, 5.0))
            j = resp.json()
            if resp.status_code == 200 and j.get("rt_cd") == "0" and j.get("output"):
                op = j["output"].get("stck_oprc")
                if op is not None:
                    return float(op)
        except Exception:
            pass
        try:
            kst = pytz.timezone("Asia/Seoul")
            today_str = datetime.now(kst).strftime("%Y%m%d")
            c = self.get_daily_candles(code, count=2)
            if c:
                last = c[-1]
                if str(last.get("date")) == today_str:
                    return float(last.get("open"))
        except Exception:
            pass
        return None

    def get_prev_high_low(self, code: str) -> Optional[Dict[str, float]]:
        """Return previous day's high/low using daily candles."""
        try:
            kst = pytz.timezone("Asia/Seoul")
            today_str = datetime.now(kst).strftime("%Y%m%d")
            c = self.get_daily_candles(code, count=3)
            if len(c) == 0:
                return None
            if str(c[-1].get("date")) == today_str and len(c) >= 2:
                prev = c[-2]
            else:
                prev = c[-1]
            return {"high": float(prev.get("high")), "low": float(prev.get("low"))}
        except Exception:
            return None

    # -------------------------------
    # Market session
    # -------------------------------
    def is_market_open(self) -> bool:
        kst = pytz.timezone("Asia/Seoul")
        now = datetime.now(kst)
        if now.weekday() >= 5:
            return False
        open_time = now.replace(hour=9, minute=0, second=0, microsecond=0)
        close_time = now.replace(hour=15, minute=20, second=0, microsecond=0)
        return open_time <= now <= close_time

    # -------------------------------
    # Balance / positions
    # -------------------------------
    def _inquire_balance_raw(self) -> Optional[dict]:
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
            logger.info(f"[잔고조회 요청파라미터] {params}")
            try:
                resp = self.session.get(url, headers=headers, params=params, timeout=(3.0, 7.0))
                j = resp.json()
            except Exception as e:
                logger.error(f"[잔고조회 예외] {e}")
                continue
            logger.info(f"[잔고조회 응답] {j}")
            if j.get("rt_cd") == "0":
                return j
        return None

    def get_cash_balance(self) -> int:
        j = self._inquire_balance_raw()
        if j and j.get("output2"):
            try:
                return int(j["output2"][0]["dnca_tot_amt"])
            except Exception as e:
                logger.error(f"[CASH_BALANCE_PARSE_FAIL] {e}")
        logger.error("[CASH_BALANCE_FAIL] 응답 없음")
        return 0

    def get_positions(self) -> List[Dict[str, Any]]:
        j = self._inquire_balance_raw()
        if j and j.get("output1") is not None:
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

    # Backward-compat for trader.py
    def get_balance(self) -> Dict[str, object]:
        return {"cash": self.get_cash_balance(), "positions": self.get_positions()}

    def get_balance_all(self) -> Dict[str, object]:
        """Compatibility shim returning same structure as get_balance()."""
        return self.get_balance()

    # -------------------------------
    # Order APIs (with robust fallbacks)
    # -------------------------------
    def _order_cash(self, body: dict, *, is_sell: bool) -> Optional[dict]:
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"

        # keep last order pdno for optional status lookups
        try:
            self._last_order_pdno = safe_strip(body.get("PDNO", "")) or self._last_order_pdno
        except Exception:
            pass

        tr_list = _pick_tr(self.env, "ORDER_SELL" if is_sell else "ORDER_BUY")
        ord_dvsn_chain = ["01", "13", "03"]  # market → IOC market → best
        last_err: Any = None

        for tr_id in tr_list:
            for ord_dvsn in ord_dvsn_chain:
                body["ORD_DVSN"] = ord_dvsn
                body["ORD_UNPR"] = "0"  # market price (or ignored for limit path)
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
                        # record fill (approx price)
                        try:
                            out = data.get("output") or {}
                            odno = out.get("ODNO") or out.get("ord_no") or ""
                            pdno = safe_strip(body.get("PDNO", ""))
                            qty = int(float(body.get("ORD_QTY", "0")))
                            ord_unpr = body.get("ORD_UNPR")
                            if ord_unpr and str(ord_unpr) not in ("0", "0.0", ""):
                                price_for_fill = float(ord_unpr)
                            else:
                                try:
                                    price_for_fill = float(self.get_current_price(pdno))
                                except Exception:
                                    price_for_fill = 0.0
                            side = "SELL" if is_sell else "BUY"
                            append_fill(side=side, code=pdno, name="", qty=qty, price=price_for_fill, odno=odno,
                                        note=f"tr={tr_id},ord_dvsn={ord_dvsn}")
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

    def buy_stock_market(self, pdno: str, qty: int) -> Optional[dict]:
        body = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": self.ACNT_PRDT_CD,
            "PDNO": safe_strip(pdno),
            "ORD_QTY": str(int(qty)),
            "ORD_DVSN": "01",
            "ORD_UNPR": "0",
        }
        return self._order_cash(body, is_sell=False)

    def sell_stock_market(self, pdno: str, qty: int) -> Optional[dict]:
        # precheck based on positions
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

        now_ts = time.time()
        with self._recent_sells_lock:
            last = self._recent_sells.get(pdno)
            if last and (now_ts - last) < self._recent_sells_cooldown:
                logger.warning(f"[SELL_DUP_BLOCK] 최근 매도 기록으로 중복 매도 차단 pdno={pdno} age={now_ts-last:.1f}s")
                return None

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
                for k, v in list(self._recent_sells.items()):
                    if v < cutoff:
                        del self._recent_sells[k]
        return resp

    def buy_stock_limit(self, pdno: str, qty: int, price: int) -> Optional[dict]:
        body = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": self.ACNT_PRDT_CD,
            "PDNO": safe_strip(pdno),
            "ORD_QTY": str(int(qty)),
            "ORD_DVSN": "00",
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
                pdno_s = safe_strip(body.get("PDNO", ""))
                qty_int = int(float(body.get("ORD_QTY", "0")))
                price_for_fill = float(body.get("ORD_UNPR", 0))
                append_fill(side="BUY", code=pdno_s, name="", qty=qty_int, price=price_for_fill, odno=odno, note=f"limit,tr={tr_id}")
            except Exception as e:
                logger.warning(f"[APPEND_FILL_LIMIT_BUY_FAIL] ex={e}")
            return data
        logger.error(f"[BUY_LIMIT_FAIL] {data}")
        return None

    def sell_stock_limit(self, pdno: str, qty: int, price: int) -> Optional[dict]:
        # precheck
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
            logger.warning(f"[SELL_LIMIT_PRECHECK] 수량 보정: req={qty} -> base={base_qty}")
            qty = base_qty

        now_ts = time.time()
        with self._recent_sells_lock:
            last = self._recent_sells.get(pdno)
            if last and (now_ts - last) < self._recent_sells_cooldown:
                logger.warning(f"[SELL_DUP_BLOCK_LIMIT] 최근 매도 기록으로 중복 매도 차단 pdno={pdno} age={now_ts-last:.1f}s")
                return None

        body = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": self.ACNT_PRDT_CD,
            "PDNO": safe_strip(pdno),
            "SLL_TYPE": "01",
            "ORD_QTY": str(int(qty)),
            "ORD_DVSN": "00",
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
                pdno_s = safe_strip(body.get("PDNO", ""))
                qty_int = int(float(body.get("ORD_QTY", "0")))
                price_for_fill = float(body.get("ORD_UNPR", 0))
                append_fill(side="SELL", code=pdno_s, name="", qty=qty_int, price=price_for_fill, odno=odno, note=f"limit,tr={tr_id}")
            except Exception as e:
                logger.warning(f"[APPEND_FILL_LIMIT_SELL_FAIL] ex={e}")
            with self._recent_sells_lock:
                self._recent_sells[pdno] = time.time()
            return data
        logger.error(f"[SELL_LIMIT_FAIL] {data}")
        return None

    # Compatibility shims
    def buy_stock(self, code: str, qty: int, price: Optional[int] = None):
        return self.buy_stock_market(code, qty) if price is None else self.buy_stock_limit(code, qty, price)

    def sell_stock(self, code: str, qty: int, price: Optional[int] = None):
        return self.sell_stock_market(code, qty) if price is None else self.sell_stock_limit(code, qty, price)

    # -------------------------------
    # Order status helper (best-effort via daily ccld inquiry)
    # -------------------------------
    def check_filled(self, order_resp_or_odno: Union[str, dict], pdno: Optional[str] = None) -> bool:
        """Check if an order was filled/partially filled today using daily CCLD inquiry.
        Accepts ODNO string or full order response dict. Returns True if any fill detected.
        """
        try:
            if isinstance(order_resp_or_odno, dict):
                out = order_resp_or_odno.get("output") or {}
                odno = out.get("ODNO") or out.get("ord_no") or ""
            else:
                odno = str(order_resp_or_odno)
        except Exception:
            odno = ""
        if not odno:
            logger.warning("[check_filled] ODNO 없음 → False")
            return False

        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
        tr_list = _pick_tr(self.env, "ORDER_STATUS")
        if not tr_list:
            logger.warning("[check_filled] ORDER_STATUS TR 미구성")
            return False
        headers = self._headers(tr_list[0])

        kst = pytz.timezone("Asia/Seoul")
        today = datetime.now(kst).strftime("%Y%m%d")
        params = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": self.ACNT_PRDT_CD,
            "INQR_STRT_DT": today,
            "INQR_END_DT": today,
            "SLL_BUY_DVSN_CD": "00",
            "INQR_DVSN": "00",
            "PDNO": safe_strip(pdno) if pdno else safe_strip(self._last_order_pdno or ""),
            "CCLD_DVSN": "00",
            "ORD_GNO_BRNO": "",
            "ODNO": safe_strip(odno),
            "INQR_DVSN_3": "00",
            "INQR_DVSN_1": "",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }
        try:
            self._limiter.wait("status")
            resp = self.session.get(url, headers=headers, params=params, timeout=(3.0, 7.0))
            j = resp.json()
        except Exception as e:
            logger.warning(f"[check_filled] 예외: {e}")
            return False

        if resp.status_code == 200 and j.get("rt_cd") == "0":
            arr = j.get("output1") or []
            if not isinstance(arr, list):
                logger.debug(f"[check_filled] output1 형식 이상: {type(arr)}")
                return False
            for row in arr:
                try:
                    r_odno = safe_strip(row.get("odno"))
                    if r_odno != safe_strip(odno):
                        continue
                    tot_ccld_qty = int(float(row.get("tot_ccld_qty", "0")))
                    rmn_qty = int(float(row.get("rmn_qty", "0")))
                    if tot_ccld_qty > 0 or rmn_qty == 0:
                        return True
                except Exception:
                    continue
        logger.info(f"[check_filled] 미체결/미검출 odno={odno}")
        return False
