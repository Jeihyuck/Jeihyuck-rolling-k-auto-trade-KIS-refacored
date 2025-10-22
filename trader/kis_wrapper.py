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
    s.headers.update({"User-Agent": "RKMax/1.0"})
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
        self._today_open_cache: Dict[str, Tuple[float, float]] = {}  # code -> (open_price, ts)
        self._today_open_ttl = 60 * 60 * 9  # 9시간 TTL (당일만 유효)

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
        오늘 시초가(09:00 기준)를 반환한다.
        1순위: 실시간 스냅샷(inquire-price)의 stck_oprc
        2순위: 시간체결(첫 틱가) 등 보조 수단(미구현 시 생략 가능)
        """
        code = safe_strip(code)
        # 0) 캐시
        cached = self._get_cached_today_open(code)
        if cached:
            return cached

        # 1) 스냅샷에서 stck_oprc (장중에도 유지됨)
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
        self._limiter.wait("quotes-open")
        tried = []
        for tr in _pick_tr(self.env, "PRICE"):
            headers = self._headers(tr)
            # 스냅샷은 보통 접두사 없이 '277810' 형태가 기본이지만, 혼용을 대비해 둘 다 시도
            markets = ["J", "U"]
            c = code
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
                        op_str = data["output"].get("stck_oprc")
                        try:
                            op = float(op_str) if op_str is not None else 0.0
                            if op > 0:
                                self._set_cached_today_open(code, op)
                                return op
                        except Exception:
                            pass
        # 2) (옵션) 시간체결 첫 틱가 보조 → 필요하면 별도 구현
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

    from typing import List, Dict, Any
    import os, time, random, requests

    def get_daily_candles(self, code: str, count: int = 30) -> List[Dict[str, Any]]:
        """
        일봉 조회 (시장코드 고정, 네트워크 실패/데이터 부족 분리)
        - 네트워크/SSL 실패: NetTemporaryError (제외 금지, 상위 루프에서 TEMP_SKIP)
        - 데이터 없음(0개): DataEmptyError (연속 확인 후 제외)
        - 데이터 부족(<21개): DataShortError (즉시 제외)

        추가:
        - 응답 RAW 디버깅 로그 출력
        - output2 / output1 / output 자동 탐색
        - .env의 DAILY_CAPITAL 미설정 시 1회 경고 로그
        """
        # ---- (A) .env 점검: DAILY_CAPITAL 미설정 경고 (함수 최초 1회만) -----------------------
        try:
            if not getattr(self, "_env_checked_daily_capital", False):
                if os.getenv("DAILY_CAPITAL") in (None, ""):
                    # settings에서 기본값(10_000_000)으로 떨어질 가능성을 알림
                    logger.warning("[ENV] DAILY_CAPITAL 이 .env에 설정되지 않았습니다. "
                                   "settings의 기본값(10,000,000)이 사용될 수 있습니다.")
                self._env_checked_daily_capital = True
        except Exception:
            # 환경 점검 로직은 거래와 무관하므로 안전하게 무시
            pass

        # ---- (1) 시장코드 고정 (J/U 스왑 금지) ------------------------------------------------
        # 실전은 마스터테이블/캐시에서 로드. 모르면 J로 고정
        market_code = self.market_map.get(code.lstrip("A"), "J") if hasattr(self, "market_map") else "J"

        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        self._limiter.wait("daily")

        # A접두 처리
        iscd = code if code.startswith("A") else f"A{code}"

        last_err = None

        for tr in _pick_tr(self.env, "DAILY_CHART"):   # TR 후보를 순차적으로 시도
            headers = self._headers(tr)
            params = {
                "fid_cond_mrkt_div_code": market_code,  # J: 코스피, U: 코스닥 등
                "fid_input_iscd": iscd,
                "fid_org_adj_prc": "0",
                "fid_period_div_code": "D",
            }

            for attempt in range(1, 4):  # 가벼운 재시도 (세션 레벨 Retry와 중첩)
                try:
                    resp = self.session.get(url, headers=headers, params=params, timeout=(3.0, 7.0))
                    resp.raise_for_status()
                    data = resp.json()
                    # ✅ RAW 응답 디버깅
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
                    # JSON 디코드 실패 → 일시적 응답깨짐으로 보고 재시도
                    last_err = e
                    logger.warning("[NET:JSON_DECODE] DAILY %s attempt=%s %s", iscd, attempt, e)
                    time.sleep(0.35 + random.uniform(0, 0.15))
                    continue
                except Exception as e:
                    last_err = e
                    logger.warning("[NET:UNEXPECTED] DAILY %s attempt=%s %s", iscd, attempt, e)
                    time.sleep(0.4 * attempt)
                    continue

                # 게이트웨이/쿼터 문구면 잠깐 쉼 후 재시도
                if "초당 거래건수" in str(data.get("msg1") or ""):
                    time.sleep(0.35 + random.uniform(0, 0.15))
                    continue

                # ---- (2) 성공 케이스 판정: rt_cd / output 키 셀렉션 --------------------------
                rt_cd = data.get("rt_cd", "")
               # 일부 환경(특히 모의)에서는 rt_cd가 아예 비어있는 {}가 올 수 있음 → 키 존재에 의존 X
                arr = data.get("output2") or data.get("output1") or data.get("output")

                if resp.status_code == 200 and arr:
                    # 정상 파싱
                    rows = []
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
                                    "low":  float(l),
                                    "close":float(c),
                                })
                        except Exception as e:
                            # 개별 레코드 파싱 실패는 스킵
                            logger.debug("[DAILY_ROW_SKIP] %s rec=%s err=%s", iscd, r, e)

                    # 날짜 오름차순 정렬
                    rows.sort(key=lambda x: x["date"])

                    # --- 핵심 판정 로직 ---
                    if len(rows) == 0:
                        # 진짜 데이터 없음 → DataEmptyError
                        raise DataEmptyError(f"{iscd} 0 candles")
                    if len(rows) < 21:
                        # 21개 미만 → DataShortError
                        raise DataShortError(f"{iscd} {len(rows)} candles (<21)")

                    # 최소 21개 확보 보장. count가 21보다 작아도 21은 확보된 상태
                    # 최근 count개 반환 (기본 동작 유지)
                    need = max(count, 21)
                    return rows[-need:][-count:]

                # 여기까지 왔는데 arr가 비었으면, 응답 자체가 비정상인 케이스
                # rt_cd가 "0"이 아니거나, 아예 키가 없는 {} 등
                last_err = RuntimeError(f"BAD_RESP rt_cd={rt_cd} msg={data.get('msg1')} arr=None")
                logger.warning("[DAILY_FAIL] %s: %s | raw=%s", iscd, last_err, data)
                time.sleep(0.35 + random.uniform(0, 0.15))

        # 모든 TR/재시도 실패 → 네트워크/게이트웨이 등으로 정상 확보 실패
        if last_err:
            logger.warning("[DAILY_FAIL] %s: %s", iscd, last_err)
        # ❗ 네트워크 실패를 []로 내려보내면 '0캔들'로 오인됨 → 예외로 올려서 TEMP_SKIP 처리
        raise NetTemporaryError(f"DAILY {iscd} net fail")




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
    # -------------------------------
    # 주문 공통
    # -------------------------------
    def _order_cash(self, body: dict, *, is_sell: bool) -> Optional[dict]:
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"

        # TR 후보 순차 시도
        tr_list = _pick_tr(self.env, "ORDER_SELL" if is_sell else "ORDER_BUY")

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
                log_body_masked = {k: (v if k not in ("CANO", "ACNT_PRDT_CD") else "***") for k, v in body.items()}
                logger.info(f"[주문요청] tr_id={tr_id} ord_dvsn={ord_dvsn} body={log_body_masked}")

                # 네트워크/게이트웨이 재시도
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
                                        price_for_fill = float(self.get_current_price(pdno))
                                    except Exception:
                                        price_for_fill = 0.0
                            except Exception:
                                price_for_fill = 0.0

                            side = "SELL" if is_sell else "BUY"
                            append_fill(side=side, code=pdno, name="", qty=qty, price=price_for_fill, odno=odno, note=f"tr={tr_id},ord_dvsn={ord_dvsn}")
                        except Exception as e:
                            logger.warning(f"[APPEND_FILL_EX] ex={e} resp={data}")
                        return data

                    msg_cd = data.get("msg_cd", "")
                    msg1 = data.get("msg1", "")
                    # 게이트웨이/서버 에러류는 재시도
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
    # 매수/매도 (신규)
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
                logger.warning(f"[SELL_DUP_BLOCK] 최근 매도 기록으로 중복 매도 차단 pdno={pdno} last={last} age={now_ts-last:.1f}s")
                return None

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
            logger.error(f"[SELL_LIMIT_PRECHECK] 보유 없음/수량 0 pdno={pdno} hldg={hldg} ord_psbl={ord_psbl}")
            return None

        if qty > base_qty:
            logger.warning(
                f"[SELL_LIMIT_PRECHECK] 수량 보정: req={qty} -> base={base_qty} (hldg={hldg}, ord_psbl={ord_psbl})"
            )
            qty = base_qty

        # 중복 매도 방지(메모리 기반)
        now_ts = time.time()
        with self._recent_sells_lock:
            last = self._recent_sells.get(pdno)
            if last and (now_ts - last) < self._recent_sells_cooldown:
                logger.warning(f"[SELL_DUP_BLOCK_LIMIT] 최근 매도 기록으로 중복 매도 차단 pdno={pdno} last={last} age={now_ts-last:.1f}s")
                return None

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

    # --- 호환 셔임(기존 trader.py 호출 대응) ---
    def buy_stock(self, code: str, qty: int, price: Optional[int] = None):
        """
        기존 코드 호환용:
        - price 가 None → 시장가 매수
        - price 지정 → 지정가 매수
        """
        if price is None:
            return self.buy_stock_market(code, qty)
        return self.buy_stock_limit(code, qty, price)

    def sell_stock(self, code: str, qty: int, price: Optional[int] = None):
        """
        기존 코드 호환용:
        - price 가 None → 시장가 매도
        - price 지정 → 지정가 매도
        """
        if price is None:
            return self.sell_stock_market(code, qty)
        return self.sell_stock_limit(code, qty, price)
