from __future__ import annotations

from datetime import date
from typing import Any

from trader.kis_wrapper import extract_order_no, is_order_accepted

from .models import Action, BrokerOrderState, BrokerPosition, Decision, OrderIntent


class KISExecutor:
    """Narrow adapter over existing public domestic KIS methods."""

    def __init__(self, kis: Any, kis_env: str):
        self.kis = kis
        self.kis_env = kis_env

    def _balance(self) -> dict:
        raw = self.kis.get_balance(force=True)
        if not isinstance(raw, dict) or raw.get("_stub"):
            raise RuntimeError("KR_INF_RECONCILE_BROKER_UNAVAILABLE")
        return raw

    def position(self, symbol: str) -> BrokerPosition:
        raw = self._balance()
        holdings = raw.get("output1") or raw.get("stocks") or []
        row = next((item for item in holdings if str(item.get("pdno") or item.get("code") or "").lstrip("A") == symbol), {})
        qty = int(float(row.get("hldg_qty") or row.get("qty") or 0))
        orderable = int(float(row.get("ord_psbl_qty") or row.get("orderable_qty") or qty))
        average = float(row.get("pchs_avg_pric") or row.get("avg_price") or 0)
        return BrokerPosition(qty, orderable, average, float(self.kis.get_current_price(symbol)))

    def account_equity(self) -> float:
        raw = self._balance()
        summary = raw.get("output2") or {}
        if isinstance(summary, list):
            summary = summary[0] if summary else {}
        for key in ("tot_evlu_amt", "tot_asst_icdc_amt", "nass_amt"):
            value = float(summary.get(key) or 0)
            if value > 0:
                return value
        raise RuntimeError("KR_INF_ACCOUNT_EQUITY_UNAVAILABLE")

    def orderable_cash(self, symbol: str, price: float) -> float:
        return float(self.kis.get_orderable_cash(symbol, price)[0])

    def submit(self, decision: Decision, symbol: str) -> tuple[str, dict]:
        if decision.action in {Action.BUY, Action.RECOVERY}:
            response = self.kis.buy_stock_limit(symbol, decision.qty, int(decision.notional / decision.qty))
        elif decision.action == Action.SELL_ALL:
            response = self.kis.sell_stock(symbol, decision.qty)
        else:
            raise ValueError("NON_ORDER_DECISION")
        if not is_order_accepted(response, kis_env=self.kis_env):
            raise RuntimeError("KR_INF_BROKER_ORDER_REJECTED")
        order_id = extract_order_no(response)
        if not order_id and self.kis_env.lower() in {"real", "live"}:
            raise RuntimeError("KR_INF_BROKER_ORDER_ID_MISSING")
        return order_id or f"PRACTICE-{decision.idempotency_key}", response

    def order_state(self, intent: OrderIntent, trade_date: date) -> BrokerOrderState:
        if not intent.broker_order_id:
            return BrokerOrderState("INTENT_CREATED")
        raw = self.kis.inquire_daily_ccld(start_date=trade_date.strftime("%Y%m%d"), end_date=trade_date.strftime("%Y%m%d"))
        rows = raw.get("output1") or raw.get("output") or [] if isinstance(raw, dict) else []
        row = next((item for item in rows if str(item.get("odno") or item.get("ODNO") or item.get("ord_no") or "") == intent.broker_order_id), None)
        if row is None:
            return BrokerOrderState("RECONCILE_PENDING")
        qty = int(float(row.get("tot_ccld_qty") or row.get("ccld_qty") or row.get("filled_qty") or 0))
        avg = float(row.get("avg_prvs") or row.get("avg_price") or row.get("ccld_unpr") or 0)
        status = "FILLED" if qty >= intent.requested_qty else ("PARTIALLY_FILLED" if qty > 0 else "PENDING")
        return BrokerOrderState(status, qty, qty * avg, avg or None)
