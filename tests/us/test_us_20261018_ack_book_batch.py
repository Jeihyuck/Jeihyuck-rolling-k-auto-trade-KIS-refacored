"""Safe per-ACK-batch broker scope re-use, with deterministic call reduction.

No network or DB writes. Mocked per-request latency is only a simulation.
"""
from trader.us.data_provider import USDataProvider
from trader.us.execution import reconcile


def _orders(count=6):
    return [{
        "trade_date": "2026-10-16", "symbol": f"T{i}", "side": "SELL",
        "order_no": f"000000{i + 1:04d}", "qty_requested": 1,
        "client_order_key": f"key-{i}", "status": "ACK",
        "pre_order_position_qty": 1,
        "meta": {},
    } for i in range(count)]


class Broker:
    _offline = False
    get_fills_by_order_no = USDataProvider.get_fills_by_order_no

    def __init__(self, orders, *, provenance_ok=True):
        self.orders = orders
        self.calls = 0
        self.provenance_ok = provenance_ok

    def get_today_orders(self, trade_date=None, **kwargs):
        self.calls += 1
        return [{
            "order_no": o["order_no"], "symbol": o["symbol"],
            "side": "SELL", "requested_qty": 1,
            "filled_qty": 0, "filled_qty_present": True,
            "remaining_qty": 1, "status": "OPEN",
            "normalization_result": "normalized",
        } for o in self.orders]

    def get_order_lookup_provenance(self, trade_date):
        return {
            "verified": self.provenance_ok,
            "source": "kis_authenticated_order_book",
            "query_trade_date": trade_date, "market": "US",
            "account_id": "only-scoped-mock-account",
            "account_product_code": "01", "env": "practice",
        }

    def get_balance(self, force_refresh=False):
        return {"positions": [
            {"symbol": o["symbol"], "qty": 1, "orderable_qty": 0}
            for o in self.orders
        ]}


def test_six_open_orders_share_one_verified_broker_ccnl(monkeypatch):
    from trader.us.db import repos
    orders = _orders()
    broker = Broker(orders)
    monkeypatch.setattr(repos, "load_pending_ack_orders", lambda **kw: orders)
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)

    result = reconcile.reconcile_ack_orders_with_balance(
        provider=broker, trade_date="2026-10-16", env="practice",
    )
    assert broker.calls == 1
    assert result["pending_count"] == 6
    assert result["open_order_pending_count"] == 6
    assert result["confirmed_count"] == 0
    assert result["balance_reconcile_count"] == 0
    assert result["status"] == "WARN"
    # 18 seconds per identical mocked KIS HTTP call gives 108->18 sec
    # *simulated*, not measured wall-clock or a guaranteed 60s sell.
    assert 6 * 18 > 60
    assert broker.calls * 18 <= 60


def test_unverified_account_scope_reverts_to_fresh_order_reads(monkeypatch):
    from trader.us.db import repos
    orders = _orders(3)
    broker = Broker(orders, provenance_ok=False)
    monkeypatch.setattr(repos, "load_pending_ack_orders", lambda **kw: orders)
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    result = reconcile.reconcile_ack_orders_with_balance(
        provider=broker, trade_date="2026-10-16", env="practice",
    )
    assert broker.calls == 3, "never share unverified broker evidence"
    assert result["open_order_pending_count"] == 3
    assert result["confirmed_count"] == 0


def test_missing_order_from_shared_book_remains_unresolved(monkeypatch):
    from trader.us.db import repos
    orders = _orders(2)
    broker = Broker(orders)
    def empty_order_book(trade_date=None, **kwargs):
        broker.calls += 1
        return []
    broker.get_today_orders = empty_order_book
    monkeypatch.setattr(repos, "load_pending_ack_orders", lambda **kw: orders)
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    result = reconcile.reconcile_ack_orders_with_balance(
        provider=broker, trade_date="2026-10-16", env="practice",
    )
    assert broker.calls == 1
    assert result["confirmed_count"] == 0
    assert result["unresolved_count"] >= 1
    assert result["cancelled_count"] == 0
    assert result["expired_count"] == 0
