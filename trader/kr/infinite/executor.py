from __future__ import annotations
from typing import Any
from .models import BrokerPosition, Decision, Action

class KISExecutor:
    """Narrow adapter over existing public domestic KIS methods."""
    def __init__(self,kis:Any): self.kis=kis
    def position(self,symbol:str)->BrokerPosition:
        raw=self.kis.get_balance(force=True); holdings=raw.get("output1",raw.get("stocks",[])) if isinstance(raw,dict) else []
        row=next((x for x in holdings if str(x.get("pdno") or x.get("code") or "").lstrip("A")==symbol),{})
        qty=int(float(row.get("hldg_qty") or row.get("qty") or 0)); orderable=int(float(row.get("ord_psbl_qty") or row.get("orderable_qty") or qty))
        avg=float(row.get("pchs_avg_pric") or row.get("avg_price") or 0); price=float(self.kis.get_current_price(symbol))
        return BrokerPosition(qty,orderable,avg,price)
    def orderable_cash(self,symbol:str,price:float)->float:return float(self.kis.get_orderable_cash(symbol,price)[0])
    def submit(self,d:Decision,symbol:str):
        if d.action in {Action.BUY,Action.RECOVERY}: return self.kis.buy_stock_limit(symbol,d.qty,int(d.notional/d.qty))
        if d.action==Action.SELL_ALL:return self.kis.sell_stock(symbol,d.qty)
        raise ValueError("NON_ORDER_DECISION")
