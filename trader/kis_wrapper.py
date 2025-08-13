# kis_wrapper.py
import os
import json
import time
import random
import logging
import threading
from datetime import datetime
from typing import Dict, List, Optional

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
    if val is None:
        return ""
    if isinstance(val, str):
        return val.replace("\n", "").replace("\r", "").strip()
    return str(val).strip()

def _json_dumps(body: dict) -> str:
    # HashKey/주문 본문 모두 동일 직렬화 문자열을 사용하도록 고정
    return json.dumps(body, ensure_ascii=False, separators=(",", ":"), sort_keys=False)

logger.info(f"[환경변수 체크] APP_KEY={repr(APP_KEY)}")
logger.info(f"[환경변수 체크] CANO={repr(CANO)}")
logger.info(f"[환경변수 체크] ACNT_PRDT_CD={repr(ACNT_PRDT_CD)}")
logger.info(f"[환경변수 체크] API_BASE_URL={repr(API_BASE_URL)}")
logger.info(f"[환경변수 체크] KIS_ENV={repr(KIS_ENV)}")

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
# 본체
# -------------------------------
class KisAPI:
    """
    - TR_ID 최신 스펙
      * (모의) 매수 VTTC0012U / 매도 VTTC0011U
      * (실전) 매수 TTTC0012U / 매도 TTTC0011U
    - HashKey 필수 적용
    - 시세 조회 레이트리밋 & 백오프
    - get_balance / buy_stock 호환 셔임(옛 호출부 대응)
    """
    _token_cache = {"token": None, "expires_at": 0, "last_issued": 0}
    _cache_path = "kis_token_cache.json"
    _token_lock = threading.Lock()

    def __init__(self):
        self.CANO = safe_strip(CANO)
        self.ACNT_PRDT_CD = safe_strip(ACNT_PRDT_CD)
        self.env = safe_strip(KIS_ENV or "practice").lower()

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

        self._limiter = _RateLimiter(min_interval_sec=0.20)  # 초당 제한 회피

        self.token = self.get_valid_token()
        logger.info(f"[생성자 체크] CANO={repr(self.CANO)}, ACNT_PRDT_CD={repr(self.ACNT_PRDT_CD)}, ENV={self.env}")

    # -------------------------------
    # 토큰
    # -------------------------------
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
                        self._token_cache.update(
                            {"token": cache["access_token"], "expires_at": cache["expires_at"], "last_issued": cache.get("last_issued", 0)}
                        )
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
        token_path = "/oauth2/tokenP" if self.env == "practice" else "/oauth2/token"
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
        """
        - KIS 시세 쿼터(초당 제한)에 걸리면 메시지에 '초당 거래건수'가 포함됨.
          → 짧게 sleep 후 재시도.
        - 시장구분은 'J'(KRX)와 'U'만 사용 (불필요 'UN' 제거)
        - 종목코드는 기본 6자리. 필요 시 'A' prefix도 함께 시도.
        """
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
        tr_id = "FHKST01010100"
        headers = self._headers(tr_id)

        tried = []
        markets = ["J", "U"]
        codes = []
        c = code.strip()
        if c.startswith("A"):
            codes = [c, c[1:]]
        else:
            codes = [c, f"A{c}"]

        # 레이트리밋(엔드포인트 키: quotes)
        self._limiter.wait("quotes")

        for market_div in markets:
            for code_fmt in codes:
                params = {
                    "fid_cond_mrkt_div_code": market_div,
                    "fid_input_iscd": code_fmt,
                }
                try:
                    resp = self.session.get(url, headers=headers, params=params, timeout=(3.0, 5.0))
                    data = resp.json()
                except Exception as e:
                    tried.append((market_div, code_fmt, f"EXC:{e}"))
                    continue

                tried.append((market_div, code_fmt, data.get("rt_cd"), data.get("msg1")))
                # 초당 제한 → 짧은 백오프 후 재시도(다음 루프)
                if "초당 거래건수를 초과" in (data.get("msg1") or ""):
                    time.sleep(0.35 + random.uniform(0, 0.15))
                    continue

                if resp.status_code == 200 and data.get("rt_cd") == "0" and "output" in data:
                    pr = data["output"].get("stck_prpr")
                    try:
                        return float(pr)
                    except Exception:
                        pass

        raise Exception(f"현재가 조회 실패({code}): tried={tried}")

    def is_market_open(self) -> bool:
        kst = pytz.timezone("Asia/Seoul")
        now = datetime.now(kst)
        if now.weekday() >= 5:
            return False
        open_time = now.replace(hour=9, minute=0, second=0, microsecond=0)
        close_time = now.replace(hour=15, minute=20, second=0, microsecond=0)
        return open_time <= now <= close_time

    # -------- 분봉/일봉 조회 (신규 추가) --------
    def get_minute_bars(self, code: str, unit: int = 1, count: int = 200):
        """
        종목/ETF 1분봉(기본) 시세 조회
        return: [{"ts": "HHMMSS", "open":..., "high":..., "low":..., "close":..., "vol":...}, ...]
        """
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
        tr_id = "FHKST03010200"
        headers = self._headers(tr_id)

        tried = []
        markets = ["J", "U"]
        code_variants = [code.strip()]
        if not code_variants[0].startswith("A"):
            code_variants.insert(0, f"A{code_variants[0]}")
        else:
            code_variants.append(code_variants[0][1:])  # 'A' 제외한 6자리도 시도

        self._limiter.wait("minute_bars")

        for mkt in markets:
            for c in code_variants:
                params = {
                    "fid_cond_mrkt_div_code": mkt,   # J: KRX, U: KOSDAQ
                    "fid_input_iscd": c,             # A+6자리
                    "fid_time_unit": str(int(unit)), # 1,3,5,10...
                    "fid_pw_data_incu_yn": "Y",      # 과거 연속조회 포함
                    "fid_org_adj_prc": "1",          # 수정주가 반영
                }
                try:
                    r = self.session.get(url, headers=headers, params=params, timeout=(3.0, 7.0))
                    j = r.json()
                except Exception as e:
                    tried.append((mkt, c, f"EXC:{e}"))
                    continue

                if r.status_code == 200 and j.get("rt_cd") == "0" and "output2" in j:
                    rows = j["output2"][:count]
                    bars = []
                    for row in rows:
                        try:
                            bars.append({
                                "ts": row.get("stck_cntg_hour") or row.get("cntg_time") or "",
                                "open": float(row.get("stck_oprc") or row.get("open", 0) or 0),
                                "high": float(row.get("stck_hgpr") or row.get("high", 0) or 0),
                                "low": float(row.get("stck_lwpr") or row.get("low", 0) or 0),
                                "close": float(row.get("stck_prpr") or row.get("close", 0) or 0),
                                "vol": int(float(row.get("acml_vol") or row.get("cum_vol") or 0)),
                            })
                        except Exception:
                            continue
                    bars = [b for b in bars if b["close"] > 0]
                    bars.sort(key=lambda x: x["ts"])
                    if len(bars) >= 10:
                        return bars
                tried.append((mkt, c, j.get("rt_cd"), j.get("msg1")))
        raise Exception(f"[분봉조회 실패] {code} tried={tried}")

    def get_daily_bars(self, code: str, count: int = 60):
        """
        일봉 시세 조회
        return: [{"date":"YYYYMMDD","open":...,"high":...,"low":...,"close":...,"vol":...}, ...]
        """
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        tr_id = "FHKST03010100"
        headers = self._headers(tr_id)

        markets = ["J", "U"]
        code_variants = [code.strip()]
        if not code_variants[0].startswith("A"):
            code_variants.insert(0, f"A{code_variants[0]}")
        else:
            code_variants.append(code_variants[0][1:])

        self._limiter.wait("daily_bars")

        tried = []
        for mkt in markets:
            for c in code_variants:
                params = {
                    "fid_cond_mrkt_div_code": mkt,
                    "fid_input_iscd": c,
                    "fid_org_adj_prc": "1",
                }
                try:
                    r = self.session.get(url, headers=headers, params=params, timeout=(3.0, 7.0))
                    j = r.json()
                except Exception as e:
                    tried.append((mkt, c, f"EXC:{e}"))
                    continue

                if r.status_code == 200 and j.get("rt_cd") == "0" and "output2" in j:
                    rows = j["output2"][:count]
                    bars = []
                    for row in rows:
                        try:
                            bars.append({
                                "date": row.get("stck_bsop_date") or row.get("date", ""),
                                "open": float(row.get("stck_oprc", 0) or 0),
                                "high": float(row.get("stck_hgpr", 0) or 0),
                                "low": float(row.get("stck_lwpr", 0) or 0),
                                "close": float(row.get("stck_clpr", 0) or 0),
                                "vol": int(float(row.get("acml_vol", 0) or 0)),
                            })
                        except Exception:
                            continue
                    bars = [b for b in bars if b["close"] > 0]
                    bars.sort(key=lambda x: x["date"])
                    if len(bars) >= 10:
                        return bars
                tried.append((mkt, c, j.get("rt_cd"), j.get("msg1")))
        raise Exception(f"[일봉조회 실패] {code} tried={tried}")

    # -------------------------------
    # 잔고/포지션
    # -------------------------------
    def get_cash_balance(self) -> int:
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-balance"
        tr_id = "VTTC8434R" if self.env == "practice" else "TTTC8434R"
        headers = self._headers(tr_id)
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
            return 0
        logger.info(f"[잔고조회 응답] {j}")
        if j.get("rt_cd") == "0" and "output2" in j and j["output2"]:
            try:
                cash = int(j["output2"][0]["dnca_tot_amt"])
                logger.info(f"[CASH_BALANCE] 현재 예수금: {cash:,}원")
                return cash
            except Exception as e:
                logger.error(f"[CASH_BALANCE_PARSE_FAIL] {e}")
                return 0
        logger.error(f"[CASH_BALANCE_PARSE_FAIL] {j}")
        return 0

    def get_positions(self) -> List[Dict]:
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-balance"
        tr_id = "VTTC8434R" if self.env == "practice" else "TTTC8434R"
        headers = self._headers(tr_id)
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
        except Exception as e:
            logger.error(f"[포지션조회 예외] {e}")
            return []
        return j.get("output1") or []

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
        """
        기존 코드 호환용:
        반환 구조: {"cash": <int>, "positions": <list[dict]>}
        """
        return {"cash": self.get_cash_balance(), "positions": self.get_positions()}

    # -------------------------------
    # 주문 공통
    # -------------------------------
    def _order_cash(self, body: dict, *, is_sell: bool) -> Optional[dict]:
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
        tr_id = (
            ("VTTC0011U" if self.env == "practice" else "TTTC0011U")
            if is_sell
            else ("VTTC0012U" if self.env == "practice" else "TTTC0012U")
        )

        # Fallback: 시장가 → IOC시장가 → 최유리
        ord_dvsn_chain = ["01", "13", "03"]
        last_err = None

        for ord_dvsn in ord_dvsn_chain:
            body["ORD_DVSN"] = ord_dvsn
            body["ORD_UNPR"] = "0"
            if is_sell and not body.get("SLL_TYPE"):
                body["SLL_TYPE"] = "01"
            body.setdefault("EXCG_ID_DVSN_CD", "KRX")

            # HashKey
            hk = self._create_hashkey(body)
            headers = self._headers(tr_id, hk)

            # 레이트리밋(주문은 별 키)
            self._limiter.wait("orders")

            # 로깅(민감 Mask)
            log_body_masked = {k: (v if k not in ("CANO", "ACNT_PRDT_CD") else "***") for k, v in body.items()}
            logger.info(f"[주문요청] tr_id={tr_id} ord_dvsn={ord_dvsn} body={log_body_masked}")

            # 네트워크/게이트웨이 재시도
            for attempt in range(1, 4):
                try:
                    resp = self.session.post(url, headers=headers, data=_json_dumps(body).encode("utf-8"), timeout=(3.0, 7.0))
                    data = resp.json()
                except Exception as e:
                    backoff = min(0.6 * (1.7 ** (attempt - 1)), 5.0) + random.uniform(0, 0.35)
                    logger.error(f"[ORDER_NET_EX] ord_dvsn={ord_dvsn} attempt={attempt} ex={e} → sleep {backoff:.2f}s")
                    time.sleep(backoff)
                    last_err = e
                    continue

                if resp.status_code == 200 and data.get("rt_cd") == "0":
                    logger.info(f"[ORDER_OK] ord_dvsn={ord_dvsn} output={data.get('output')}")
                    return data

                msg_cd = data.get("msg_cd", "")
                msg1 = data.get("msg1", "")
                if msg_cd == "IGW00008" or "MCA" in msg1 or resp.status_code >= 500:
                    backoff = min(0.6 * (1.7 ** (attempt - 1)), 5.0) + random.uniform(0, 0.35)
                    logger.error(f"[ORDER_FAIL_GATEWAY] ord_dvsn={ord_dvsn} attempt={attempt} resp={data} → sleep {backoff:.2f}s")
                    time.sleep(backoff)
                    last_err = data
                    continue

                logger.error(f"[ORDER_FAIL_BIZ] ord_dvsn={ord_dvsn} resp={data}")
                return None

            logger.warning(f"[ORDER_FALLBACK] ord_dvsn={ord_dvsn} 실패 → 다음 방식 시도")

        raise Exception(f"주문 실패: {last_err}")

    # -------------------------------
    # 매수/매도
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
            logger.warning(f"[SELL_PRECHECK] 수량 보정: req={qty} -> base={base_qty} (hldg={hldg}, ord_psbl={ord_psbl})")
            qty = base_qty

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
        return self._order_cash(body, is_sell=True)

    def buy_stock_limit(self, pdno: str, qty: int, price: int) -> Optional[dict]:
        body = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": self.ACNT_PRDT_CD,
            "PDNO": safe_strip(pdno),
            "ORD_QTY": str(int(qty)),
            "ORD_DVSN": "00",   # 지정가
            "ORD_UNPR": str(int(price)),
            "EXCG_ID_DVSN_CD": "KRX",
        }
        hk = self._create_hashkey(body)
        tr_id = "VTTC0012U" if self.env == "practice" else "TTTC0012U"
        headers = self._headers(tr_id, hk)
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
        resp = self.session.post(url, headers=headers, data=_json_dumps(body).encode("utf-8"), timeout=(3.0, 7.0))
        data = resp.json()
        if resp.status_code == 200 and data.get("rt_cd") == "0":
            logger.info(f"[BUY_LIMIT_OK] output={data.get('output')}")
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
            logger.warning(f"[SELL_LIMIT_PRECHECK] 수량 보정: req={qty} -> base={base_qty} (hldg={hldg}, ord_psbl={ord_psbl})")
            qty = base_qty

        body = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": self.ACNT_PRDT_CD,
            "PDNO": safe_strip(pdno),
            "SLL_TYPE": "01",
            "ORD_QTY": str(int(qty)),
            "ORD_DVSN": "00",   # 지정가
            "ORD_UNPR": str(int(price)),
            "EXCG_ID_DVSN_CD": "KRX",
        }
        hk = self._create_hashkey(body)
        tr_id = "VTTC0011U" if self.env == "practice" else "TTTC0011U"
        headers = self._headers(tr_id, hk)
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
        resp = self.session.post(url, headers=headers, data=_json_dumps(body).encode("utf-8"), timeout=(3.0, 7.0))
        data = resp.json()
        if resp.status_code == 200 and data.get("rt_cd") == "0":
            logger.info(f"[SELL_LIMIT_OK] output={data.get('output')}")
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
