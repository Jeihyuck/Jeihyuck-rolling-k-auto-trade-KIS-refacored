import requests, logging
from settings import APP_KEY, APP_SECRET, API_BASE_URL, CANO, ACNT_PRDT_CD, KIS_ENV

logger = logging.getLogger(__name__)

class KisAPI:
    def __init__(self):
        self.token = self._issue_token()

    def _issue_token(self):
        url = f"{API_BASE_URL}/oauth2/tokenP"
        headers = {"content-type": "application/json"}
        data = {"grant_type": "client_credentials", "appkey": APP_KEY, "appsecret": APP_SECRET}
        resp = requests.post(url, json=data, headers=headers).json()
        logger.info(f"[🔑 토큰발급] 응답: {resp}")
        return resp["access_token"]

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
        raise Exception(f"현재가 조회 실패({code}): {resp['msg1']}")

    def buy_stock(self, code, qty):
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
        headers = self._headers("VTTC0802U" if KIS_ENV == "practice" else "TTTC0802U")
        data = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "PDNO": code,
            "ORD_DVSN": "01",  # 시장가
            "ORD_QTY": str(qty),
            "ORD_UNPR": "0"
        }
        resp = requests.post(url, headers=headers, json=data).json()
        if resp["rt_cd"] == "0":
            return resp["output"]
        raise Exception(f"매수주문 실패({code}): {resp['msg1']}")
