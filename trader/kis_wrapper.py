import requests, os, json, time, logging
from settings import APP_KEY, APP_SECRET, API_BASE_URL, CANO, ACNT_PRDT_CD, KIS_ENV

logger = logging.getLogger(__name__)

# --- 환경 변수, 파라미터 방어적 정제 함수 ---
def safe_strip(val):
    """환경 변수 등 어떤 값이 와도 안전하게 str 변환 후 개행/공백 제거."""
    if val is None:
        return ''
    if isinstance(val, str):
        return val.replace('\n', '').replace('\r', '').strip()
    return str(val).strip()

# --- 실제 환경변수 값 로그로 찍기 ---
logger.info(f"[환경변수 체크] APP_KEY={repr(APP_KEY)}")
logger.info(f"[환경변수 체크] CANO={repr(CANO)}")
logger.info(f"[환경변수 체크] ACNT_PRDT_CD={repr(ACNT_PRDT_CD)}")
logger.info(f"[환경변수 체크] API_BASE_URL={repr(API_BASE_URL)}")
logger.info(f"[환경변수 체크] KIS_ENV={repr(KIS_ENV)}")

class KisAPI:
    def __init__(self):
        self.CANO = safe_strip(CANO)
        self.ACNT_PRDT_CD = safe_strip(ACNT_PRDT_CD)
        self.token = self._get_token_with_file_cache()
        logger.info(f"[생성자 체크] CANO={repr(self.CANO)}, ACNT_PRDT_CD={repr(self.ACNT_PRDT_CD)}")

    def _get_token_with_file_cache(self):
        cache_path = "kis_token_cache.json"
        if os.path.exists(cache_path):
            with open(cache_path, "r") as f:
                cache = json.load(f)
            if time.time() < cache["expires_at"] - 300:
                logger.info(f"[토큰캐시] 캐시 사용: {cache['access_token'][:10]}... 만료:{cache['expires_at']}")
                return cache["access_token"]
        token, expires_in = self._issue_token_and_expire()
        with open(cache_path, "w") as f:
            json.dump({
                "access_token": token,
                "expires_at": time.time() + int(expires_in)
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
            "authorization": f"Bearer {self.token}",
            "appkey": APP_KEY,
            "appsecret": APP_SECRET,
            "tr_id": tr_id,
            "custtype": "P",
            "content-type": "application/json"
        }

    def get_current_price(self, code):
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
        headers = self._headers("VTTC3001R" if KIS_ENV == "practice" else "FHKST01010100")
        params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": str(code).strip()}
        resp = requests.get(url, headers=headers, params=params).json()
        if resp["rt_cd"] == "0":
            return float(resp["output"]["stck_prpr"])
        raise Exception(f"현재가 조회 실패({code}): {resp.get('msg1', resp)}")

    def buy_stock(self, code, qty):
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
        headers = self._headers("VTTC0802U" if KIS_ENV == "practice" else "TTTC0802U")
        data = {
            "CANO": safe_strip(self.CANO),
            "ACNT_PRDT_CD": safe_strip(self.ACNT_PRDT_CD),
            "PDNO": str(code).strip(),
            "ORD_DVSN": "00",  # 시장가
            "ORD_QTY": str(qty).strip(),
            "ORD_UNPR": "0"
        }
        logger.info(f"[매수주문 요청파라미터] {data}")
        resp = requests.post(url, headers=headers, json=data).json()
        # Graceful handling for 장종료, 초과 등
        if resp["rt_cd"] == "0":
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
