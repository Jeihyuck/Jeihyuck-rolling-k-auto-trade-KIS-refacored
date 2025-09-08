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

# -------------------------------
# 유틸
# -------------------------------

def safe_strip(val):
    """개행과 캐리지리턴 제거 + 앞뒤 공백 제거.
    복사/붙여넣기 과정에서 문자열 리터럴의 "
"/"
"가 깨지는 문제를 피하기 위해
    숫자 코드(10=
, 13=
)를 사용한 translate로 처리합니다.
    """
    if val is None:
        return ""
    # 문자열/비문자 모두 translate로 제어문자 제거 후 strip
    s = val if isinstance(val, str) else str(val)
    try:
        # 10: '
', 13: '
'
        s = s.translate({10: None, 13: None})
    except Exception:
        # translate가 실패할 경우를 대비한 보조 경로
        s = "".join(ch for ch in s if ord(ch) not in (10, 13))
    return s.strip()


def _json_dumps(body: dict) -> str:
    # HashKey/주문 본문 모두 동일 직렬화 문자열을 사용하도록 고정
    return json.dumps(body, ensure_ascii=False, separators=(",", ":"), sort_keys=False)


logger.info(f"[환경변수 체크] APP_KEY={repr(APP_KEY)}")
logger.info(f"[환경변수 체크] CANO={repr(CANO)}")
logger.info(f"[환경변수 체크] ACNT_PRDT_CD={repr(ACNT_PRDT_CD)}")
logger.info(f"[환경변수 체크] API_BASE_URL={repr(API_BASE_URL)}")
logger.info(f"[환경변수 체크] KIS_ENV={repr(KIS_ENV)}")

# -------------------------------
# 체결 기록 저장 유틸
# -------------------------------

def append_fill(side: str, code: str, name: str, qty: int, price: float, odno: str, note: str = ""):
    """
    체결(주문 성공) 기록을 CSV로 저장
    - side: "BUY" 또는 "SELL"
    - code: 종목 코드 (문자열)
    - name: 종목명 (가능하면 전달, 없으면 빈 문자열)
    - qty: 체결 수량 (int)
    - price: 체결가(또는 추정가, float)
    - odno: 주문번호(ODNO) 등 식별자
    - note: 추가 메모
    """
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


# -------------------------------
# 간단 레이트리미터(엔드포인트별 최소 간격 유지)
# -------------------------------

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


# -------------------------------
# TR_ID 환경변수 오버라이드 + 후보 폴백
# -------------------------------

TR_MAP = {
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
        return TR_MAP[env][key]
    except Exception:
        return []


# -------------------------------
# 본체
# -------------------------------

class KisAPI:
    """
    - TR_ID 최신 스펙 + 환경변수 오버라이드 + 후보 폴백 구현
    - HashKey 필수 적용
    - 시세/호가/일봉/ATR 제공 + (신규) 오늘시가/전일고저 + (신규) 주문체결조회 check_filled
    - 레이트리밋 & 백오프 & 네트워크 재시도 강화
    - get_balance / buy_stock 등 기존 호출부 호환
    - (중요) 토큰은 **지연 발급(lazy)**: 실제 API 호출 시점에만 발급/갱신
    """

    _token_cache = {"token": None, "expires_at": 0, "last_issued": 0}
    _cache_path = "kis_token_cache.json"
    _token_lock = threading.Lock()

    def __init__(self):
        self.CANO = safe_strip(CANO)
        self.ACNT_PRDT_CD = safe_strip(ACNT_PRDT_CD)
        self.env = safe_strip(KIS_ENV or "practice").lower()
        if self.env not in ("practice", "real"):
            self.env = "practice"

        # 세션 + 재시도 어댑터
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

        # (중요) 여기서 토큰을 발급하지 않는다. headers() 호출 시 get_valid_token() 수행.
        logger.info(f"[생성자 체크] CANO={repr(self.CANO)}, ACNT_PRDT_CD={repr(self.ACNT_PRDT_CD)}, ENV={self.env}")

        # 마지막 주문 종목(체결 확인용 보조)
        self._last_order_pdno: Optional[str] = None

    # -------------------------------
    # 토큰 (lazy + 403 백오프 보호)
    # -------------------------------

    def get_valid_token(self):
        with KisAPI._token_lock:
            now = time.time()
            # 메모리 캐시
            if self._token_cache["token"] and now < self._token_cache["expires_at"] - 300:
                return self._token_cache["token"]

            # 파일 캐시
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

            # 1분 내 재발급 차단 (동시 실행 보호)
            if now - self._token_cache["last_issued"] < 61:
                logger.warning("[토큰] 1분 이내 재발급 시도 차단, 기존 토큰 재사용(없으면 예외)")
                if self._token_cache["token"]:
                    return self._token_cache["token"]
                raise Exception("토큰 발급 제한(1분 1회), 잠시 후 재시도 필요")

            # 발급 시도 (403 메시지에 따른 지연 처리)
            try:
                token, expires_in = self._issue_token_and_expire()
            except Exception as e:
                msg = str(e)
                if "1분당 1회" in msg or "잠시 후 다시 시도" in msg:
                    wait_s = 65 + random.uniform(0, 3)
                    logger.error(f"[토큰발급 대기] 제한 메시지 감지 → {wait_s:.1f}s 대기 후 재시도")
                    time.sleep(wait_s)
                    token, expires_in = self._issue_token_and_expire()
                else:
                    raise

            expires_at = now + int(expires_in)
            self._token_cache.update({"token": token, "expires_at": expires_at, "last_issued": time.time()})
            try:
                with open(self._cache_path, "w", encoding="utf-8") as f:
                    json.dump({"access_token": token, "expires_at": expires_at, "last_issued": self._token_cache["last_issued"]}, f, ensure_ascii=False)
            except Exception as e:
                logger.warning(f"[토큰캐시 쓰기 실패] {e}")
            logger.info("[토큰캐시] 새 토큰 발급 및 캐시")
            return token

    def refresh_token(self):
        """호출부 호환용: 캐시 삭제 후 다음 호출에서 재발급되도록 플래그만 갱신."""
        with KisAPI._token_lock:
            self._token_cache.update({"token": None, "expires_at": 0, "last_issued": 0})
            try:
                if os.path.exists(self._cache_path):
                    os.remove(self._cache_path)
            except Exception as e:
                logger.warning(f"[refresh_token] 캐시 파일 삭제 실패: {e}")
        logger.info("[refresh_token] 토큰 캐시 비움 → 다음 호출 시 재발급")

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

    # -------------------------------
    # 헤더/HashKey
    # -------------------------------

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

    # -------------------------------
    # 시세/장운영
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
        """일봉 최근 N개 (과거→최근 정렬)."""
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

    # --- 신규: 오늘 시가/전일 고저 ---

    def get_today_open(self, code: str) -> Optional[float]:
        try:
            kst = pytz.timezone("Asia/Seoul")
            today = datetime.now(kst).strftime("%Y%m%d")
            candles = self.get_daily_candles(code, count=3)
            if not candles:
                return None
            last = candles[-1]
            if str(last.get("date")) == today:
                return float(last.get("open")) if last.get("open") is not None else None
            return None
        except Exception as e:
            logger.warning(f"[get_today_open] 실패 code={code} ex={e}")
            return None

    def get_prev_high_low(self, code: str) -> Optional[Dict[str, float]]:
        try:
            candles = self.get_daily_candles(code, count=3)
            if len(candles) < 2:
                return None
            prev = candles[-2]
            return {"high": float(prev.get("high")), "low": float(prev.get("low"))}
        except Exception as e:
            logger.warning(f"[get_prev_high_low] 실패 code={code} ex={e}")
            return None

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
        """
        매도/체크에 사용할 수량은 '주문가능수량(ord_psbl_qty)'이 0일 수 있어
        '보유수량(hldg_qty)'을 우선 사용한다. (체결대기/락 상황 보호)
        """
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
    # 주문 공통
    # -------------------------------

    def _order_cash(self, body: dict, *, is_sell: bool) -> Optional[dict]:
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"

        # 마지막 주문 종목 보조 저장(체결확인에 활용)
        try:
            self._last_order_pdno = safe_strip(body.get("PDNO", "")) or self._last_order_pdno
        except Exception:
            pass

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
                        try:
                            out = data.get("output") or {}
                            odno = out.get("ODNO") or out.get("ord_no") or ""
                            pdno = safe_strip(body.get("PDNO", ""))
                            qty = int(float(body.get("ORD_QTY", "0")))
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
        if price is None:
            return self.buy_stock_market(code, qty)
        return self.buy_stock_limit(code, qty, price)

    def sell_stock(self, code: str, qty: int, price: Optional[int] = None):
        if price is None:
            return self.sell_stock_market(code, qty)
        return self.sell_stock_limit(code, qty, price)

    # -------------------------------
    # (신규) 주문 체결 확인: 주식일별주문체결조회
    # -------------------------------

    def check_filled(self, order_resp_or_odno: Union[str, dict], pdno: Optional[str] = None) -> bool:
        """
        지정가 주문 직후 체결 여부 확인.
        - 입력: 주문 응답(dict) 또는 ODNO(str)
        - pdno는 선택. 미제공 시 내부에 저장한 self._last_order_pdno 사용. (없어도 쿼리 가능)
        성공 시 True(체결/부분체결), 실패/미체결/오류 시 False 반환.
        """
        try:
            if isinstance(order_resp_or_odno, dict):
                out = (order_resp_or_odno.get("output") if isinstance(order_resp_or_odno.get("output"), dict) else {}) if order_resp_or_odno else {}
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
