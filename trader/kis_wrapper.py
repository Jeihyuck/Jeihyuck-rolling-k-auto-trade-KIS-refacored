import requests, os, json, time, logging
from settings import APP_KEY, APP_SECRET, API_BASE_URL, CANO, ACNT_PRDT_CD, KIS_ENV

logger = logging.getLogger(__name__)

def strip_env(val):
    # 환경 변수 및 파라미터 방어적 정제 (None도 방지)
    return str(val or '').replace('\n', '').replace('\r', '').strip()

class KisAPI:
    def __init__(self):
        # 항상 strip된 버전으로 멤버에 저장
        self.CANO = strip_env(CANO)
        self.ACNT_PRDT_CD = strip_env(ACNT_PRDT_CD)
        self.token = self._get_token_with_file_cache()

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
        params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}
        resp = requests.get(url, headers=headers, params=params).json()
        if resp["rt_cd"] == "0":
            return float(resp["output"]["stck_prpr"])
        raise Exception(f"현재가 조회 실패({code}): {resp.get('msg1', resp)}")

    def buy_stock(self, code, qty):
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
        headers = self._headers("VTTC0802U" if KIS_ENV == "practice" else "TTTC0802U")
        data = {
            "CANO": strip_env(self.CANO),
            "ACNT_PRDT_CD": strip_env(self.ACNT_PRDT_CD),
            "PDNO": str(code).strip(),
            "ORD_DVSN": "01",  # 시장가
            "ORD_QTY": str(qty).strip(),
            "ORD_UNPR": "0"
        }
        # 로그로 한 번 더 실제 값 점검
        logger.info(f"[매수주문 요청파라미터] {data}")
        resp = requests.post(url, headers=headers, json=data).json()
        if resp["rt_cd"] == "0":
            return resp["output"]
        raise Exception(f"매수주문 실패({code}): {resp.get('msg1', resp)}")

