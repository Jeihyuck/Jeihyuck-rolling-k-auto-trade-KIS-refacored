from __future__ import annotations

from datetime import date
from typing import Any

from trader.kis_wrapper import extract_order_no, is_order_accepted

from .models import Action, BrokerOrderState, BrokerPosition, Decision, OrderIntent


class KISExecutor:
    """Narrow adapter over existing public domestic KIS methods."""

    def __init__(self, kis: Any, kis_env: str, balance_snapshot: dict | None = None):
        self.kis = kis
        self.kis_env = kis_env
        self.balance_snapshot = balance_snapshot

    def _balance(self) -> dict:
        raw = self.balance_snapshot if self.balance_snapshot is not None else self.kis.get_balance()
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

    def submit(self, decision: Decision, symbol: str) -> tuple[str | None, dict]:
        if decision.action in {Action.BUY, Action.RECOVERY}:
            response = self.kis.buy_stock_limit(symbol, decision.qty, int(decision.notional / decision.qty))
        elif decision.action in {Action.SELL_PARTIAL, Action.SELL_ALL}:
            response = self.kis.sell_stock(symbol, decision.qty)
        else:
            raise ValueError("NON_ORDER_DECISION")
        if not is_order_accepted(response, kis_env=self.kis_env):
            raise RuntimeError("KR_INF_BROKER_ORDER_REJECTED")
        order_id = extract_order_no(response)
        if not order_id and self.kis_env.lower() in {"real", "live"}:
            raise RuntimeError("KR_INF_BROKER_ORDER_ID_MISSING")
        return order_id, response

    def order_state(self, intent: OrderIntent, trade_date: date) -> BrokerOrderState:
        # Reconcile against the order's original trading day, not the current
        # session day. Domestic day orders do not migrate to a later trade date;
        # using today's date can make an old SELL stay RECONCILE_PENDING forever.
        lookup_date = intent.trade_date or trade_date
        raw = self.kis.inquire_daily_ccld(
            start_date=lookup_date.strftime("%Y%m%d"),
            end_date=lookup_date.strftime("%Y%m%d"),
        )
        rows = (raw.get("output1") or raw.get("output") or []) if isinstance(raw, dict) else []
        if intent.broker_order_id:
            matches = [item for item in rows if str(item.get("odno") or item.get("ODNO") or item.get("ord_no") or "") == intent.broker_order_id]
        else:
            expected_side = "BUY" if intent.side in {"BUY", "RECOVERY"} else "SELL"
            def correlated(item) -> bool:
                symbol = str(item.get("pdno") or item.get("code") or item.get("symbol") or "").lstrip("A")
                side_raw = str(item.get("sll_buy_dvsn_cd") or item.get("side") or item.get("sll_buy_dvsn_name") or "").upper()
                side = "BUY" if side_raw in {"02", "BUY", "매수"} or "BUY" in side_raw else "SELL" if side_raw in {"01", "SELL", "매도"} or "SELL" in side_raw else ""
                requested = int(float(item.get("ord_qty") or item.get("requested_qty") or item.get("tot_ord_qty") or 0))
                return symbol == "122630" and side == expected_side and requested == intent.requested_qty
            matches = [item for item in rows if correlated(item)]
            if len(matches) > 1:
                raise RuntimeError("KR_INF_PENDING_ORDER_AMBIGUOUS")
        row = matches[0] if len(matches) == 1 else None
        if row is None:
            # A successful historical inquiry with no matching row can only
            # close a zero-fill day order.  Durable partial-fill evidence stays
            # fenced because re-sizing a replacement from current holdings
            # could overshoot the intended TP stage.
            inquiry_ok = isinstance(raw, dict) and str(raw.get("rt_cd") or "0").strip() in {"", "0"}
            if lookup_date < trade_date and inquiry_ok and int(intent.filled_qty or 0) == 0:
                return BrokerOrderState("EXPIRED")
            return BrokerOrderState("RECONCILE_PENDING")
        qty = int(float(row.get("tot_ccld_qty") or row.get("ccld_qty") or row.get("filled_qty") or 0))
        avg = float(row.get("avg_prvs") or row.get("avg_price") or row.get("ccld_unpr") or 0)
        if qty >= intent.requested_qty:
            status = "FILLED"
        elif qty > 0:
            # A historical partial fill needs explicit lifecycle review; do not
            # silently advance TP stage or create a replacement SELL.
            status = "PARTIALLY_FILLED"
        elif lookup_date < trade_date:
            # A zero-fill domestic day order cannot remain live overnight.
            status = "EXPIRED"
        else:
            status = "PENDING"
        return BrokerOrderState(status, qty, qty * avg, avg or None)
