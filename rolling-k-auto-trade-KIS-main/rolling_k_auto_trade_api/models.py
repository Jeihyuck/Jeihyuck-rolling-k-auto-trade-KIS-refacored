# rolling_k_auto_trade_api/models.py
from pydantic import BaseModel
from typing import Optional

class BuyOrderRequest(BaseModel):
    code: str
    name: str
    buy_price: float
    quantity: int
    signal_date: str

class SellOrderRequest(BaseModel):
    code: str
    name: str
    sell_price: float
    sell_date: str
    reason: Optional[str] = None
