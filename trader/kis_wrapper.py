import requests
from settings import APP_KEY, APP_SECRET

class KisAPI:
    def __init__(self):
        self.token = None

    def authenticate(self):
        resp = requests.post(
            "https://openapi.koreainvestment.com:9443/oauth2/tokenP",
            json={"grant_type": "client_credentials", "appkey": APP_KEY, "appsecret": APP_SECRET}
        )
        data = resp.json()
        print("🔐 Auth response:", data)

        # 응답에서 토큰 추출 (유연하게 대응)
        if "access_token" in data:
            self.token = data["access_token"]
        elif "accessToken" in data:
            self.token = data["accessToken"]
        else:
            raise RuntimeError(f"❗ 인증 실패 — 응답에 토큰 없음: {data}")

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    def get_current_price(self, code):
        resp = requests.get(
            "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-price",
            headers=self._headers(),
            params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}
        )
        return float(resp.json()["output"]["stck_prpr"])

    def order_cash(self, code, qty, order_type="market", side="1"):
        payload = {
            "CANO": "계좌번호",
            "ACNT_PRDT_CD": "01",
            "PDNO": code,
            "ORD_QTY": str(qty),
            "ORD_UNPR": "0"
        }
        resp = requests.post(
            "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/order-cash",
            headers=self._headers(),
            json=payload
        )
        return resp.json()

    def get_open_orders(self):
        resp = requests.get(
            "https://openapi.koreainvestment.com/.../inquire-order",
            headers=self._headers()
        )
        return resp.json().get("output", [])

    def inquire_order(self, order_no):
        resp = requests.get(
            "https://openapi.koreainvestment.com/.../inquire-order-detail",
            headers=self._headers(),
            params={"order_no": order_no}
        )
        return resp.json()

    def get_balance(self):
        resp = requests.get(
            "https://openapi.koreainvestment.com/.../inquire-balance",
            headers=self._headers()
        )
        return resp.json().get("output", [])
