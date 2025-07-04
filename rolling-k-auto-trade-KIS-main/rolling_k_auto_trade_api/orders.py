# rolling_k_auto_trade_api/orders.py
from fastapi import APIRouter
from rolling_k_auto_trade_api.models import BuyOrderRequest, SellOrderRequest
import json
import os
from datetime import datetime

router = APIRouter()
TRADE_STATE = {}

LOG_DIR = "./rolling_k_auto_trade_api/logs"
os.makedirs(LOG_DIR, exist_ok=True)

def log_order(data: dict, order_type: str):
    log_file = os.path.join(LOG_DIR, f"{order_type}_orders.log")
    with open(log_file, "a") as f:
        f.write(json.dumps(data, ensure_ascii=False) + "\n")

@router.post("/order/buy")
def buy_stock(order: BuyOrderRequest):
    order_data = order.dict()
    order_data["timestamp"] = datetime.now().isoformat()
    log_order(order_data, "buy")
    TRADE_STATE[order.code] = order_data
    return {"message": "Buy order logged", "data": order_data}

@router.post("/order/sell")
def sell_stock(order: SellOrderRequest):
    order_data = order.dict()
    order_data["timestamp"] = datetime.now().isoformat()
    log_order(order_data, "sell")
    if order.code in TRADE_STATE:
        del TRADE_STATE[order.code]
    return {"message": "Sell order logged", "data": order_data}

def get_order_status():
    return {"open_positions": TRADE_STATE, "count": len(TRADE_STATE)}

from rolling_k_auto_trade_api.models import BuyOrderRequest, SellOrderRequest
import logging

def execute_buy_order(request: BuyOrderRequest):
    # 여기선 예시로, 실제 API 연동 대신 로그만 남깁니다
    logging.info(f"[BUY] {request.ticker} - {request.amount}주 매수 요청됨")
    return {"result": "buy order received", "ticker": request.ticker, "amount": request.amount}

def execute_sell_order(request: SellOrderRequest):
    logging.info(f"[SELL] {request.ticker} - {request.amount}주 매도 요청됨")
    return {"result": "sell order received", "ticker": request.ticker, "amount": request.amount}

