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

logger = logging.getLogger(__name__)

def safe_strip(val):
    if val is None:
        return ""
    if isinstance(val, str):
        return val.replace("\n", "").replace("\r", "").strip()
    return str(val).strip()

def _json_dumps(body: dict) -> str:
    return json.dumps(body, ensure_ascii=False, separators=(",", ":"), sort_keys=False)

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

# --- KisAPI 이하 실전 전체 로직 (토큰, 주문, 매수/매도, 체결, 실전 전략 등) ---
# (코드 길이 문제로, "계속" 요청 시 아래 전체 함수/클래스(잔고/주문/시장가/지정가/실전보조 등) 순차적 제공)

class KisAPI:
    _token_cache = {"token": None, "expires_at": 0, "last_issued": 0}
    _cache_path = "kis_token_cache.json"
    _token_lock = threading.Lock()

    def __init__(self):
        self.CANO = safe_strip(CANO)
        self.ACNT_PRDT_CD = safe_strip(ACNT_PRDT_CD)
        self.env = safe_strip(KIS_ENV or "practice").lower()
        if self.env not in ("practice", "real"):
            self.env = "practice"
        self.session = requests.Session()
        retry = Retry(
            total=3, connect=3, read=3, status=3, backoff_factor=0.5,
            status_forcelist=(500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST"]), raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self._limiter = _RateLimiter(min_interval_sec=0.20)
        self._recent_sells: Dict[str, float] = {}
        self._recent_sells_lock = threading.Lock()
        self._recent_sells_cooldown = 60.0
        self.token = self.get_valid_token()
        logger.info(f"[생성자 체크] CANO={repr(self.CANO)}, ACNT_PRDT_CD={repr(self.ACNT_PRDT_CD)}, ENV={self.env}")

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
                        logger.info(f"[토큰캐시] 파일캐시 사용: {cache['access_token'][:10]}... 만료:{cache['expires_at']}")
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
                    json.dump({"access_token": token, "expires_at": expires_at, "last_issued": now}, f, ensure_ascii=False)
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
            resp = self.session.post(url, json=data, headers=headers, timeout=(3.0, 7.0))
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

    # === 실전: 시세, 잔고, 시장가/지정가, 매수/매도, 체결강도, ATR 등 ===
    # (3부로 계속 이어집니다. 아래 "계속"을 눌러주시면 3부 전체 제공)

    def get_current_price(self, code: str) -> float:
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
        self._limiter.wait("quotes")
        tried = []
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
                rows.sort(key=lambda x: x["date"])  # 과거→최근
                return rows[-count:]
        return []

    def get_atr(self, code: str, window: int = 14) -> Optional[float]:
        try:
            candles = self.get_daily_candles(code, count=window + 2)
            if len(candles) < window + 1:
                return None
            trs: List[float] = []
            for i in range(1, len(candles)):
                h = candles[i]["high"]; l = candles[i]["low"]; c_prev = candles[i - 1]["close"]
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

    # ----- 잔고/포지션 -----
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

    # -------------------------------
    # 주문 공통, 시장가/지정가, 매수/매도(상세 구현은 1부 참고)
    # (이미 위 1,2부에서 전부 제공. 필요시 재업로드 안내)
    # -------------------------------

# -- (끝) --
