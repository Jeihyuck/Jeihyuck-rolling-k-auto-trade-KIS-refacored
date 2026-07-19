import json
from contextlib import contextmanager

import trader.us.db.repos as repos

class R:
    def __init__(self, rows=None, rowcount=0): self.rows=rows or []; self.rowcount=rowcount
    def mappings(self): return self
    def all(self): return self.rows
    def first(self): return self.rows[0] if self.rows else None

class Engine:
    def __init__(self, fail_insert=False):
        self.order={"id":1,"trade_date":"2026-07-16","client_order_key":"K","symbol":"AMD","exchange":"NASDAQ","side":"SELL","order_no":"O1","meta":{},"qty_requested":10,"qty_filled":3}
        self.fills=[{"id":1,"trade_date":"2026-07-16","order_no":"O1","client_order_key":"K","symbol":"AMD","side":"SELL","qty":3,"meta":{"is_synthetic":True,"fill_evidence_type":"BALANCE_DELTA_SYNTHETIC","cumulative_filled_qty":3}}]
        self.fail_insert=fail_insert
        self.snap=None
    @contextmanager
    def begin(self):
        self.snap=(dict(self.order), [dict(f, meta=dict(f["meta"])) for f in self.fills])
        try:
            yield self
        except Exception:
            self.order, self.fills = self.snap
            raise
    def execute(self, stmt, params=None):
        sql=str(stmt); params=params or {}
        if "FROM us_orders" in sql and "FOR UPDATE" in sql:
            return R([dict(self.order)])
        if "UPDATE us_orders" in sql:
            self.order.update({"status":params["status"],"qty_filled":params["qty"],"avg_price_usd":params["price"],"meta":json.loads(params["meta"])})
            return R(rowcount=1)
        if "SELECT qty, meta FROM us_fills" in sql or "SELECT id, qty, meta FROM us_fills" in sql:
            rows=[dict(f) for f in self.fills if f["meta"].get("accounting_active", True)]
            return R(rows)
        if "UPDATE us_fills SET meta" in sql:
            for f in self.fills:
                if f["id"]==params["id"]: f["meta"]=json.loads(params["meta"])
            return R(rowcount=1)
        if "SELECT 1 FROM us_fills" in sql:
            return R([])
        if "INSERT INTO us_fills" in sql:
            if self.fail_insert: raise RuntimeError("forced insert failure")
            self.fills.append({"id":len(self.fills)+1,"trade_date":params["td"],"order_no":params["order_no"],"client_order_key":params["cok"],"symbol":params["symbol"],"side":params["side"],"qty":params["qty"],"meta":json.loads(params["meta"]),"fill_idempotency_key":params["idem"]})
            return R(rowcount=1)
        return R([])

def test_postgres_synthetic_promotion_actual_cumulative_full(monkeypatch):
    e=Engine(); monkeypatch.setattr(repos,"_get_engine_or_none",lambda:e)
    r=repos.mark_order_filled_by_reconcile(order_no="O1",client_order_key="K",symbol="AMD",side="SELL",filled_qty=6,requested_qty=10,cumulative_filled_qty=6,avg_price_usd=100,trade_date="2026-07-16",evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",source="fills_by_order_no")
    assert r["status"]=="OK" and e.order["qty_filled"]==6
    assert e.fills[0]["meta"]["accounting_active"] is False
    assert e.fills[1]["qty"]==6 and "KIS_ORDER_CUMULATIVE_ACTUAL" in e.fills[1]["fill_idempotency_key"]


def test_postgres_promotion_rollback_keeps_synthetic_active(monkeypatch):
    e=Engine(fail_insert=True); monkeypatch.setattr(repos,"_get_engine_or_none",lambda:e)
    r=repos.mark_order_filled_by_reconcile(order_no="O1",client_order_key="K",symbol="AMD",side="SELL",filled_qty=6,requested_qty=10,cumulative_filled_qty=6,avg_price_usd=100,trade_date="2026-07-16",evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",source="fills_by_order_no")
    assert r["status"]=="RECONCILE_UPDATE_FAILED"
    assert e.order["qty_filled"]==3 and e.fills[0]["meta"].get("accounting_active", True) is True


def test_postgres_actual_smaller_than_synthetic_conflicts_before_update(monkeypatch):
    e=Engine(); e.order["qty_filled"]=10; e.fills[0]["qty"]=10; e.fills[0]["meta"]["cumulative_filled_qty"]=10
    monkeypatch.setattr(repos,"_get_engine_or_none",lambda:e)
    r=repos.mark_order_filled_by_reconcile(order_no="O1",client_order_key="K",symbol="AMD",side="SELL",filled_qty=7,requested_qty=10,cumulative_filled_qty=7,avg_price_usd=100,trade_date="2026-07-16",evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",source="fills_by_order_no")
    assert r["status"]=="EVIDENCE_QUANTITY_CONFLICT"
    assert e.order["qty_filled"]==10 and e.fills[0]["meta"].get("accounting_active", True) is True
