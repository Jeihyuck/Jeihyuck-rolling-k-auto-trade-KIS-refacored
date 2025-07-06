import os, requests
from dotenv import load_dotenv

load_dotenv()

APP_KEY = os.getenv("KIS_APP_KEY")
APP_SECRET = os.getenv("KIS_APP_SECRET")
ACCESS_TOKEN = os.getenv("KIS_ACCESS_TOKEN")
ACCOUNT = os.getenv("KIS_ACCOUNT")
BASE_URL = "https://openapivts.koreainvestment.com:29443"

HEADERS = {
    "content-type": "application/json; charset=utf-8",
    "authorization": f"Bearer {ACCESS_TOKEN}",
    "appkey": APP_KEY,
    "appsecret": APP_SECRET,
}


def get_price_data(stock_code):
    url = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-price"
    params = {
        "fid_cond_mrkt_div_code": "J",
        "fid_input_iscd": stock_code,
        "fid_period_div_code": "D",
        "fid_org_adj_prc": "1",
    }
    res = requests.get(
        url, headers={**HEADERS, "tr_id": "FHKST03010100"}, params=params
    )
    return res.json()


def send_order(stock_code, qty=1, side="buy"):
    url = f"{BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
    payload = {
        "CANO": ACCOUNT[:8],
        "ACNT_PRDT_CD": ACCOUNT[8:],
        "PDNO": stock_code,
        "ORD_DVSN": "01",  # 시장가
        "ORD_QTY": str(qty),
        "ORD_UNPR": "0",
        "SLL_BUY_DVSN_CD": "01" if side == "buy" else "02",
        "ORD_DVSN": "01",
    }
    res = requests.post(url, headers={**HEADERS, "tr_id": "VTTC0802U"}, json=payload)
    return res.json()


def send_sell(stock_code, qty=1):
    return send_order(stock_code, qty=qty, side="sell")
