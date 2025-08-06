import requests, os, json, time, logging
from settings import APP_KEY, APP_SECRET, API_BASE_URL, CANO, ACNT_PRDT_CD, KIS_ENV
from datetime import datetime
import pytz
import threading

logger = logging.getLogger(__name__)

def safe_strip(val):
    if val is None:
        return ''
    if isinstance(val, str):
        return val.replace('\n', '').replace('\r', '').strip()
    return str(val).strip()

logger.info(f"[환경변수 체크] APP_KEY={repr(APP_KEY)}")
logger.info(f"[환경변수 체크] CANO={repr(CANO)}")
logger.info(f"[환경변수 체크] ACNT_PRDT_CD={repr(ACNT_PRDT_CD)}")
logger.info(f"[환경변수 체크] API_BASE_URL={repr(API_BASE_URL)}")
logger.info(f"[환경변수 체크] KIS_ENV={repr(KIS_ENV)}")

class KisAPI:
    _token_cache = {"token": None, "expires_at": 0, "last_issued": 0}
    _cache_path = "kis_token_cache.json"
    _token_lock = threading.Lock()

    def __init__(self):
        self.CANO = safe_strip(CANO)
        self.ACNT_PRDT_CD = safe_strip(ACNT_PRDT_CD)
        self.token = self.get_valid_token()
        logger.info(f"[생성자 체크] CANO={repr(self.CANO)}, ACNT_PRDT_CD={repr(self.ACNT_PRDT_CD)}")

    def get_valid_token(self):
        with KisAPI._token_lock:
            now = time.time()
            if self._token_cache["token"] and now < self._token_cache["expires_at"] - 300:
                return self._token_cache["token"]
            if os.path.exists(self._cache_path):
                with open(self._cache_path, "r") as f:
                    cache = json.load(f)
                if "access_token" in cache and now < cache["expires_at"] - 300:
                    self._token_cache["token"] = cache["access_token"]
                    self._token_cache["expires_at"] = cache["expires_at"]
                    self._token_cache["last_issued"] = cache.get("last_issued", 0)
                    logger.info(f"[토큰캐시] 파일캐시 사용: {cache['access_token'][:10]}... 만료:{cache['expires_at']}")
                    return cache["access_token"]
            if now - self._token_cache["last_issued"] < 61:
                logger.warning(f"[토큰] 1분 이내 재발급 시도 차단, 기존 토큰 재사용")
                if self._token_cache["token"]:
                    return self._token_cache["token"]
                else:
                    raise Exception("토큰 발급 제한(1분 1회), 잠시 후 재시도 필요")
            token, expires_in = self._issue_token_and_expire()
            expires_at = now + int(expires_in)
            self._token_cache["token"] = token
            self._token_cache["expires_at"] = expires_at
            self._token_cache["last_issued"] = now
            with open(self._cache_path, "w") as f:
                json.dump({
                    "access_token": token,
                    "expires_at": expires_at,
                    "last_issued": now
                }, f)
            logger.info(f"[토큰캐시] 새 토큰 발급 및 캐시")
            return token

    def _issue_token_and_expire(self):
        url = f"{API_BASE_URL}/oauth2/tokenP"
        headers = {"content-type": "application/json"}
        data = {"grant_type": "client_credentials", "appkey": APP_KEY, "appsecret": APP_SECRET}
        resp = requests.post(url, json=data, headers=headers).json()
        if "access_token" in resp:
            logger.info(f"[🔑 토큰발급] 성공: {resp}")
            return resp["access_token"], resp["expires_in"]
        else:
            logger.error(f"[🔑 토큰발급 실패]: {resp.get('error_description')}")
            raise Exception(f"토큰 발급 실패: {resp.get('error_description')}")

    def _headers(self, tr_id):
        return {
            "authorization": f"Bearer {self.get_valid_token()}",
            "appkey": APP_KEY,
            "appsecret": APP_SECRET,
            "tr_id": tr_id,
            "custtype": "P",
            "content-type": "application/json"
        }

    def get_current_price(self, code):
        tried = []
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
        headers = self._headers("FHKST01010100")
        for market_div in ["J", "U"]:
            for code_fmt in [code, f"A{code}", code[1:] if code.startswith("A") else code]:
                params = {
                    "fid_cond_mrkt_div_code": market_div,
                    "fid_input_iscd": code_fmt
                }
                resp = requests.get(url, headers=headers, params=params).json()
                tried.append((market_div, code_fmt, resp.get("rt_cd"), resp.get("msg1")))
                if resp.get("rt_cd") == "0" and "output" in resp:
                    return float(resp["output"]["stck_prpr"])
        raise Exception(f"현재가 조회 실패({code}): tried={tried}")

    def buy_stock(self, code, qty, price=None):
        tr_id = "VTTC0012U" if KIS_ENV == "practice" else "TTTC0012U"
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
        headers = self._headers(tr_id)
        if price is None:
            price = self.get_current_price(code)
        data = {
            "CANO": safe_strip(self.CANO),
            "ACNT_PRDT_CD": safe_strip(self.ACNT_PRDT_CD),
            "PDNO": str(code).strip(),
            "ORD_DVSN": "00",  # 시장가
            "ORD_QTY": str(int(float(qty))).strip(),
            "ORD_UNPR": str(int(float(price))).strip(),
        }
        logger.info(f"[매수주문 요청파라미터] {data}")
        resp = requests.post(url, headers=headers, json=data).json()
        if resp.get("rt_cd") == "0":
            logger.info(f"[매수 체결 응답] {resp}")
            return resp["output"]
        elif resp.get("msg1") == "모의투자 장종료 입니다.":
            logger.warning("⏰ [KIS] 장운영시간 외 주문시도 — 주문 무시(정상)")
            return None
        elif "초과" in resp.get("msg1", ""):
            logger.warning(f"⏰ [KIS] API 사용량 초과(Throttle) — 주문 무시(정상): {resp.get('msg1')}")
            return None
        else:
            logger.error(f"[ORDER_FAIL] {resp}")
            raise Exception(f"매수주문 실패({code}): {resp.get('msg1', resp)}")

    def sell_stock(self, code, qty, price=None):
        tr_id = "VTTC0013U" if KIS_ENV == "practice" else "TTTC0013U"
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
        headers = self._headers(tr_id)
        if price is None:
            price = self.get_current_price(code)
        data = {
            "CANO": safe_strip(self.CANO),
            "ACNT_PRDT_CD": safe_strip(self.ACNT_PRDT_CD),
            "PDNO": str(code).strip(),
            "ORD_DVSN": "00",  # 시장가
            "ORD_QTY": str(int(float(qty))).strip(),
            "ORD_UNPR": str(int(float(price))).strip(),
        }
        logger.info(f"[매도주문 요청파라미터] {data}")
        resp = requests.post(url, headers=headers, json=data).json()
        if resp.get("rt_cd") == "0":
            logger.info(f"[매도 체결 응답] {resp}")
            return resp["output"]
        elif resp.get("msg1") == "모의투자 장종료 입니다.":
            logger.warning("⏰ [KIS] 장운영시간 외 매도 주문시도 — 주문 무시(정상)")
            return None
        elif "초과" in resp.get("msg1", ""):
            logger.warning(f"⏰ [KIS] API 사용량 초과(Throttle) — 주문 무시(정상): {resp.get('msg1')}")
            return None
        else:
            logger.error(f"[SELL_ORDER_FAIL] {resp}")
            raise Exception(f"매도주문 실패({code}): {resp.get('msg1', resp)}")

    def get_cash_balance(self):
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-balance"
        headers = self._headers("VTTC8434R" if KIS_ENV == "practice" else "TTTC8434R")
        params = {
            "CANO": safe_strip(self.CANO),
            "ACNT_PRDT_CD": safe_strip(self.ACNT_PRDT_CD),
            "AFHR_FLPR_YN": "N",
            "UNPR_YN": "N",
            "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N",
            "PRCS_DVSN": "01",
            "OFL_YN": "N",
            "INQR_DVSN": "02",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": ""
        }
        logger.info(f"[잔고조회 요청파라미터] {params}")
        resp = requests.get(url, headers=headers, params=params).json()
        logger.info(f"[잔고조회 응답] {resp}")
        if resp.get("rt_cd") == "0" and "output2" in resp and resp["output2"]:
            try:
                cash = int(resp["output2"][0]["dnca_tot_amt"])
                logger.info(f"[CASH_BALANCE] 현재 예수금: {cash:,}원")
                return cash
            except Exception as e:
                logger.error(f"[CASH_BALANCE_PARSE_FAIL] {e}")
                return 0
        else:
            logger.error(f"[CASH_BALANCE_PARSE_FAIL] {resp}")
            return 0

    def is_market_open(self):
        KST = pytz.timezone('Asia/Seoul')
        now = datetime.now(KST)
        if now.weekday() >= 5:
            return False
        open_time = now.replace(hour=9, minute=0, second=0, microsecond=0)
        close_time = now.replace(hour=15, minute=20, second=0, microsecond=0)  # <=== 여기 15:20으로 변경!
        return open_time <= now <= close_time
