from datetime import datetime, timezone

from trader.us.infinite.integration import reconcile_tqqq_open_buy_ttl


NOW = datetime(2026, 9, 5, 14, 0, tzinfo=timezone.utc)


def _order(**extra):
    return {
        "trade_date": "2026-09-05",
        "client_order_key": "TQQQ_INF_V3:cycle-a:2026-09-05:BUY",
        "order_no": "original-broker-order",
        "symbol": "TQQQ",
        "side": "BUY",
        "qty_requested": 2,
        "status": "OPEN",
        "meta": {},
        **extra,
    }


class _Repository:
    def __init__(self, orders=None):
        self.orders = list(orders or [_order()])
        self.cancel_requests = []
        self.terminal_observations = []

    def load_expired_open_buy_orders(self, **kwargs):
        assert kwargs["symbol"] == "TQQQ"
        return self.orders

    def mark_ttl_cancel_requested(self, order, **kwargs):
        self.cancel_requests.append((order, kwargs))
        order["meta"]["tqqq_ttl_cancel_requested_at"] = kwargs["requested_at"].isoformat()

    def apply_ttl_terminal_observation(self, order, observation):
        self.terminal_observations.append((order, observation))
        order["status"] = observation["status"]
        return {"status": "OK"}


def _run(repo, *, cancel=lambda **_: {"status": "ACK"}, query=lambda **_: {"status": "OPEN"}):
    return reconcile_tqqq_open_buy_ttl(
        repository=repo, now=NOW, ttl_seconds=60,
        cancel_order=cancel, query_order=query,
    )


def test_tqqq_open_buy_blocks_only_tqqq_buy():
    repo = _Repository()
    calls = []
    _run(repo, cancel=lambda **identity: calls.append(identity) or {"status": "ACK"})
    assert calls == [{
        "order_no": "original-broker-order",
        "client_order_key": "TQQQ_INF_V3:cycle-a:2026-09-05:BUY",
        "symbol": "TQQQ", "side": "BUY",
    }]
    assert repo.orders[0]["status"] == "OPEN"


def test_tqqq_open_buy_does_not_block_tqqq_sell():
    repo = _Repository()
    queried = []
    _run(repo, query=lambda **identity: queried.append(identity) or {"status": "OPEN"})
    assert queried[0]["side"] == "BUY"
    assert repo.orders[0]["status"] == "OPEN"


def test_tqqq_open_buy_does_not_block_standard_sell():
    repo = _Repository()
    standard_sell = {"symbol": "any-current-position", "side": "SELL"}
    _run(repo)
    assert standard_sell == {"symbol": "any-current-position", "side": "SELL"}
    assert repo.terminal_observations == []


def test_tqqq_ttl_cancel_uses_original_order_identity():
    repo = _Repository()
    cancel_calls, query_calls = [], []
    _run(
        repo,
        cancel=lambda **identity: cancel_calls.append(identity) or {"status": "ACK"},
        query=lambda **identity: query_calls.append(identity) or {"status": "OPEN"},
    )
    assert cancel_calls == query_calls
    assert cancel_calls[0]["order_no"] == repo.orders[0]["order_no"]
    assert cancel_calls[0]["client_order_key"] == repo.orders[0]["client_order_key"]


def test_tqqq_cancel_ack_does_not_terminalize_order():
    repo = _Repository()
    result = _run(repo, query=lambda **_: {})
    assert result == {"expired": 1, "cancel_requested": 1, "terminal": 0, "pending": 1}
    assert repo.orders[0]["status"] == "OPEN"
    assert repo.terminal_observations == []


def test_tqqq_broker_confirmed_cancel_terminalizes_order():
    repo = _Repository()
    result = _run(repo, query=lambda **_: {
        "order_no": "original-broker-order", "symbol": "TQQQ", "side": "BUY",
        "status": "CANCELLED", "filled_qty": 0,
    })
    assert result["terminal"] == 1
    assert repo.orders[0]["status"] == "CANCELLED"


def test_tqqq_unknown_cancel_result_keeps_pending():
    repo = _Repository()
    result = _run(repo, cancel=lambda **_: {"status": "UNKNOWN"}, query=lambda **_: {"status": "UNKNOWN"})
    assert result["cancel_requested"] == 1
    assert result["pending"] == 1
    assert repo.orders[0]["status"] == "OPEN"


def test_tqqq_production_callbacks_cancel_and_requery_same_broker_identity():
    from trader.us.runner.trade_tick_runner import _build_tqqq_ttl_callbacks

    calls = {"cancel": [], "query": []}

    class Client:
        def cancel_us_order(self, **kwargs):
            calls["cancel"].append(kwargs)
            return {"status": "ACK"}

    class Provider:
        def get_fills_by_order_no(self, **kwargs):
            calls["query"].append(kwargs)
            return {
                "order_no": kwargs["order_no"], "symbol": kwargs["symbol"],
                "side": "BUY", "status": "CANCELLED", "filled_qty": 0,
            }

    cancel, query = _build_tqqq_ttl_callbacks(
        Provider(), Client(), trade_date="2026-09-08", symbol="TQQQ"
    )
    identity = {
        "order_no": "original-broker-order",
        "client_order_key": "TQQQ_INF_V3:cycle-a:2026-09-08:BUY",
        "symbol": "TQQQ", "side": "BUY",
    }
    assert cancel(**identity)["status"] == "ACK"
    observed = query(**identity)
    assert observed["status"] == "CANCELLED"
    assert calls["cancel"] == [{
        "symbol": "TQQQ", "exchange": "NASDAQ", "order_no": "original-broker-order",
    }]
    assert calls["query"] == [{
        "order_no": "original-broker-order", "symbol": "TQQQ", "trade_date": "2026-09-08",
    }]
