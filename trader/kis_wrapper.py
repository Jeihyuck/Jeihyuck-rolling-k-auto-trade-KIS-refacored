import requests
import time
from settings import APP_KEY, APP_SECRET, CANO, ACNT_PRDT_CD

class KisAPI:
    def __init__(self):
        self.token = None

    def authenticate(self):
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            resp = requests.post(
                "https://openapi.koreainvestment.com:9443/oauth2/tokenP",
                json={"grant_type": "client_credentials", "appkey": APP_KEY, "appsecret": APP_SECRET},
                timeout=10
            )
            data = resp.json()
            print(f"🔐 Auth response (attempt {attempt}):", data)
            if "access_token" in data:
                self.token = data["access_token"]; return
            if "accessToken" in data:
                self.token = data["accessToken"]; return
            print(f"⚠️ 인증 실패 (code {data.get('error_code') or data.get('error')}) 재시도 중...")
            if attempt < max_retries: time.sleep(2 ** attempt)
        raise RuntimeError(f"🚫 인증 3회 실패 — 최종 응답: {data}")

    def _headers(self):
        if not self.token:
            raise RuntimeError("⚠️ 인증 필요: authenticate() 먼저 호출하세요.")
        return {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}

    def get_current_price(self, code):
        resp = requests.get(
            "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-price",
            headers=self._headers(),
            params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}
        )
        return float(resp.json()["output"]["stck_prpr"])

    def order_cash(self, code, qty, order_type="00", side="1"):
        payload = {"CANO": CANO, "ACNT_PRDT_CD": ACNT_PRDT_CD, "PDNO": code, "ORD_QTY": str(qty), "ORD_UNPR": "0"}
        resp = requests.post(
            "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/order-cash",
            headers=self._headers(), json=payload
        )
        return resp.json()

    def get_open_orders(self):
        resp = requests.get(
            "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/inquire-orders",
            headers=self._headers(),
            params={"CANO": CANO, "ACNT_PRDT_CD": ACNT_PRDT_CD}
        )
        return resp.json().get("output", [])

    def inquire_order(self, order_no):
        resp = requests.get(
            "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/inquire-order-detail",
            headers=self._headers(),
            params={"CANO": CANO, "ACNT_PRDT_CD": ACNT_PRDT_CD, "ORD_NO": order_no}
        )
        return resp.json()

    def get_balance(self):
        resp = requests.get(
            "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/inquire-balance",
            headers=self._headers(),
            params={"CANO": CANO, "ACNT_PRDT_CD": ACNT_PRDT_CD}
        )
        return resp.json().get("output", [])
