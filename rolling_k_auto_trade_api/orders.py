from fastapi import APIRouter
from rolling_k_auto_trade_api.models import BuyOrderRequest, SellOrderRequest
from datetime import datetime
import json
import os
import requests
from dotenv import load_dotenv

router = APIRouter()
load_dotenv()

LOG_DIR = "./rolling_k_auto_trade_api/logs"
os.makedirs(LOG_DIR, exist_ok=True)

KIS_APP_KEY = os.getenv("KIS_APP_KEY")
KIS_APP_SECRET = os.getenv("KIS_APP_SECRET")
KIS_ACCESS_TOKEN = os.getenv("KIS_ACCESS_TOKEN")
KIS_REST_URL = os.getenv("KIS_REST_URL", "https://openapivts.koreainvestment.com:29443")

def log_order(data: dict, order_type: str):
    log_file = os.path.join(LOG_DIR, f"{order_type}_orders.log")
    with open(log_file, "a") as f:
        f.write(json.dumps(data, ensure_ascii=False) + "\n")

@router.post("/order/buy")
def buy_stock(order: BuyOrderRequest):
    url = f"{KIS_REST_URL}/uapi/domestic-stock/v1/trading/order-cash"
    headers = {
        "content-type": "application/json",
        "authorization": f"Bearer {KIS_ACCESS_TOKEN}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": "VTTC0012U",
        "custtype": "P"
    }
    payload = {
        "CANO": order.account_no,
        "ACNT_PRDT_CD": order.product_code,
        "PDNO": order.code,
        "ORD_DVSN": order.order_type,
        "ORD_QTY": order.quantity,
        "ORD_UNPR": order.price
    }
    response = requests.post(url, headers=headers, json=payload)
    res_data = response.json()
    log_order(res_data, "buy")
    return res_data

@router.post("/order/sell")
def sell_stock(order: SellOrderRequest):
    url = f"{KIS_REST_URL}/uapi/domestic-stock/v1/trading/order-cash"
    headers = {
        "content-type": "application/json",
        "authorization": f"Bearer {KIS_ACCESS_TOKEN}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": "VTTC0011U",
        "custtype": "P"
    }
    payload = {
        "CANO": order.account_no,
        "ACNT_PRDT_CD": order.product_code,
        "PDNO": order.code,
        "ORD_DVSN": order.order_type,
        "ORD_QTY": order.quantity,
        "ORD_UNPR": order.price
    }
    response = requests.post(url, headers=headers, json=payload)
    res_data = response.json()
    log_order(res_data, "sell")
    return res_data
