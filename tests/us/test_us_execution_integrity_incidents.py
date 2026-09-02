import time

import pytest

from trader.us.execution.kis_us_client import KisUSClient, KisUSTemporaryError
from trader.us.execution.order_economics import order_intent_economics_valid
from trader.us.execution.tick_context import TickExecutionContext
from trader.us.pb1.us_exit_engine import _make_exit_intent


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

