import requests
from datetime import datetime, timedelta
from settings import APP_KEY, APP_SECRET, CANO, ACNT_PRDT_CD, API_BASE_URL

class KisAPI:
    def __init__(self):
        self.token = None
        self.token_expiry = datetime.min

    def authenticate(self):
        if self.token and datetime.now() < self.token_expiry:
            return
        resp = requests.post(
            f"{API_BASE_URL}/oauth2/tokenP",
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
        tried = []
        for market_div in ["U", "J"]:
            # "005930"와 "A005930" 모두 시도
            for code_fmt in [code, "A" + code if not code.startswith("A") else code[1:]]:
                params = {"fid_cond_mrkt_div_code": market_div, "fid_input_iscd": code_fmt}
                url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
                resp = requests.get(url, headers=self._headers(), params=params)
                data = resp.json()
                tried.append({
                    "market_div": market_div,
                    "code_fmt": code_fmt,
                    "status_code": resp.status_code,
                    "response": data
                })
                print(f"📈 get_current_price try: {params} status={resp.status_code} resp={data}")
                if resp.status_code == 200 and data.get("rt_cd") == "0" and "output" in data and "stck_prpr" in data["output"]:
                    return float(data["output"]["stck_prpr"])
        raise RuntimeError(f"❌ 가격 조회 모두 실패 — tried: {tried}")

    def order_cash(self, code, qty, price=0):
        payload = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "PDNO": code,
            "ORD_QTY": str(qty),
            "ORD_UNPR": str(price),
            "ORD_DVSN": "01",
            "CVI": ""
        }
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
        resp = requests.post(url, headers=self._headers(), json=payload)
        data = resp.json()
        print(f"💸 order_cash response for {code}:", data)
        return data

    def get_open_orders(self):
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-orders"
        resp = requests.get(
            url,
            headers=self._headers(),
            params={"CANO": CANO, "ACNT_PRDT_CD": ACNT_PRDT_CD}
        )
        data = resp.json()
        print("📂 get_open_orders response:", data)
        return data.get("output", [])

    def inquire_order(self, order_no):
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-order-detail"
        resp = requests.get(
            url,
            headers=self._headers(),
            params={"CANO": CANO, "ACNT_PRDT_CD": ACNT_PRDT_CD, "ORD_NO": order_no}
        )
        data = resp.json()
        print(f"🧾 inquire_order response for {order_no}:", data)
        return data

    def get_balance(self):
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-balance"
        resp = requests.get(
            url,
            headers=self._headers(),
            params={"CANO": CANO, "ACNT_PRDT_CD": ACNT_PRDT_CD}
        )
        data = resp.json()
        print("💰 get_balance response:", data)
        return data.get("output", [])

