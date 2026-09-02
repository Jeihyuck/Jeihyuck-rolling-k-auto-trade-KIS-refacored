import time

import pytest

from trader.us.execution.kis_us_client import KisUSClient, KisUSTemporaryError
from trader.us.execution.order_economics import order_intent_economics_valid
from trader.us.execution.tick_context import TickExecutionContext
from trader.us.pb1.us_exit_engine import _make_exit_intent
from trader.us.runner.trade_session_runner import effective_child_tick_budget
from trader.us.runner.trade_tick_runner import calculate_latency_accounting


@pytest.mark.parametrize("symbol,decision_price,qty", [
    ("MSFT", 499.79, 1),
    ("JPM", 301.25, 2),
])
def test_pb1_sell_uses_final_limit_price_for_economics(monkeypatch, symbol, decision_price, qty):
    monkeypatch.setattr(
        "trader.us.pb1.us_exit_engine.should_skip_exit_due_to_pending_sell",
        lambda *_: (False, None, None),
    )
    intent = _make_exit_intent(
        symbol, "NASDAQ", qty, decision_price, decision_price * 1.1,
        "hard_stop_loss", "incident replay", -10.0, -0.1,
        holding_qty=qty, orderable_qty=qty,
    )
    expected_limit = round(decision_price * 0.998, 4)
    assert intent["limit_price"] == expected_limit
    assert intent["notional_usd"] == round(qty * expected_limit, 4)
    assert intent["meta"]["decision_price"] == decision_price
    assert order_intent_economics_valid(intent)


def test_kis_retry_aborts_before_shared_tick_deadline(monkeypatch):
    requests = pytest.importorskip("requests")
    client = KisUSClient(offline=False)
    context = TickExecutionContext(
        "2026-09-01", "am", "run", 1, "tick",
        deadline=time.monotonic() + 0.08,
    )
    client.bind_tick_context(context)
    client._apply_rate_limit = lambda _path: None

    def timeout(*_args, **_kwargs):
        raise requests.exceptions.Timeout("slow KIS")

    monkeypatch.setattr(requests, "get", timeout)
    started = time.monotonic()
    with pytest.raises(KisUSTemporaryError, match="deadline budget exhausted"):
        client._get("/uapi/overseas-stock/v1/trading/inquire-balance", {}, {})
    assert time.monotonic() - started < 0.2
    assert client.stats["get_retry_count"] == 1


def test_order_post_timeout_is_never_blind_retried(monkeypatch):
    requests = pytest.importorskip("requests")
    client = KisUSClient(offline=False)
    calls = []
    client._apply_rate_limit = lambda _path: None
    monkeypatch.setattr(requests, "post", lambda *a, **k: calls.append((a, k)) or (_ for _ in ()).throw(requests.Timeout("accepted but response lost")))
    with pytest.raises(KisUSTemporaryError):
        client._post("/uapi/overseas-stock/v1/trading/order", {}, {})
    assert len(calls) == 1
    assert client.stats["post_retry_count"] == 0


def test_route_created_client_is_bound_to_same_context(monkeypatch):
    from trader.us.execution import order_router
    seen = {}

    class Client:
        def __init__(self, **_kwargs): pass
        def bind_tick_context(self, context):
            self._tick_context = context
            seen["context"] = context
            return self
        def place_us_buy_order(self, *_args):
            return {"output": {"ODNO": "BOUND-1"}}

    context = TickExecutionContext("2026-09-01", "am", "run", 1, "tick")
    monkeypatch.setenv("DRY_RUN", "0")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setattr("trader.us.execution.kis_us_client.KisUSClient", Client)
    monkeypatch.setattr(order_router, "canonical_order_risk_check", lambda *a, **k: None)
    monkeypatch.setattr("trader.us.db.repos.load_today_order_keys", lambda **k: set())
    monkeypatch.setattr("trader.us.db.repos.save_order_intent", lambda *a, **k: True)
    monkeypatch.setattr("trader.us.db.repos.save_order_ack", lambda *a, **k: True)
    result = order_router.route_order({
        "symbol": "MSFT", "exchange": "NASDAQ", "side": "BUY", "qty": 1,
        "limit_price": 100, "notional_usd": 100, "client_order_key": "bound-route",
        "trade_date": "2026-09-01",
    }, context=context)
    assert seen["context"] is context
    assert result["status"] == "ACK"


def test_token_rate_limit_wait_obeys_short_deadline(monkeypatch):
    import trader.us.execution.kis_us_client as module
    client = KisUSClient(offline=False).bind_tick_context(TickExecutionContext(
        "2026-09-01", "am", "run", 1, "token", deadline=time.monotonic() + 2,
    ))
    module._TOKEN_CACHE.update(access_token=None, expires_at=None)
    monkeypatch.setattr(module, "_read_token_file", lambda _env: {})
    monkeypatch.setattr(client, "_request_new_token", lambda: (_ for _ in ()).throw(TimeoutError("timeout")))
    started = time.monotonic()
    with pytest.raises(KisUSTemporaryError, match="token refresh wait"):
        client.get_access_token()
    assert time.monotonic() - started < 0.2


def test_child_budget_reserves_parent_cleanup_and_honors_configured_deadline():
    assert effective_child_tick_budget(watchdog_sec=240, configured_deadline_sec=240, cleanup_reserve_sec=5) == 235
    assert effective_child_tick_budget(watchdog_sec=240, configured_deadline_sec=120, cleanup_reserve_sec=5) == 120


def test_latency_accounting_ignores_nested_child_timers_and_is_bounded():
    accounted, unaccounted = calculate_latency_accounting(100, {
        "position_reconcile_ms": 70, "balance_snapshot_ms": 60, "fill_fetch_ms": 50,
        "entry_engine_ms": 10,
    })
    assert accounted == 80
    assert unaccounted == 20
    assert 0 <= unaccounted <= 100
