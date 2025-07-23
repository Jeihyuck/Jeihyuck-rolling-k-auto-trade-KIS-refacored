import requests
from datetime import datetime, timedelta
from settings import APP_KEY, APP_SECRET, CANO, ACNT_PRDT_CD

class KisAPI:
    def __init__(self):
        self.token = None
        self.token_expiry = datetime.min

    def authenticate(self):
        if self.token and datetime.now() < self.token_expiry:
            return

        resp = requests.post(
            "https://openapi.koreainvestment.com:9443/oauth2/tokenP",
            json={"grant_type": "client_credentials", "appkey": APP_KEY, "appsecret": APP_SECRET},
            timeout=10
        )
        data = resp.json()
        print("🔐 Auth response:", data)

        if "access_token" not in data:
            raise RuntimeError(f"🚫 인증 실패 — 응답: {data}")

        self.token = data["access_token"]
        self.token_expiry = datetime.now() + timedelta(seconds=int(data.get("expires_in", 86400)) - 60)
        print(f"✅ New token, expires at {self.token_expiry}")

    def _headers(self):
        self.authenticate()
        return {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}

    def get_current_price(self, code):
        # 기본적으로 KOSDAQ (J), 필요시 KOSPI (U)로 변경 테스트 가능
        params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}
        url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-price"
        resp = requests.get(url, headers=self._headers(), params=params)
        data = resp.json()
        print(f"📈 get_current_price response for {code}:", data)

        if resp.status_code != 200:
            raise RuntimeError(f"❌ HTTP 오류: {resp.status_code} - {resp.text}")

        if data.get("rt_cd") != "0":
            raise RuntimeError(f"📉 가격 조회 실패 — 코드: {data.get('rt_cd')}, 메시지: {data.get('msg1')}")

        if "output" not in data or "stck_prpr" not in data["output"]:
            raise RuntimeError(f"📉 가격 정보 없음 — 응답: {data}")

        return float(data["output"]["stck_prpr"])

    def order_cash(self, code, qty):
        payload = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "PDNO": code,
            "ORD_QTY": str(qty),
            "ORD_UNPR": "0"
        }
        url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/order-cash"
        resp = requests.post(url, headers=self._headers(), json=payload)
        data = resp.json()
        print(f"💸 order_cash response for {code}:", data)
        return data

    def get_open_orders(self):
        resp = requests.get(
            "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/inquire-orders",
            headers=self._headers(),
            params={"CANO": CANO, "ACNT_PRDT_CD": ACNT_PRDT_CD}
        )
        data = resp.json()
        print("📂 get_open_orders response:", data)
        return data.get("output", [])

    def inquire_order(self, order_no):
        resp = requests.get(
            "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/inquire-order-detail",
            headers=self._headers(),
            params={"CANO": CANO, "ACNT_PRDT_CD": ACNT_PRDT_CD, "ORD_NO": order_no}
        )
        data = resp.json()
        print(f"🧾 inquire_order response for {order_no}:", data)
        return data

    def get_balance(self):
        resp = requests.get(
            "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/inquire-balance",
            headers=self._headers(),
            params={"CANO": CANO, "ACNT_PRDT_CD": ACNT_PRDT_CD}
        )
        data = resp.json()
        print("💰 get_balance response:", data)
        return data.get("output", [])


