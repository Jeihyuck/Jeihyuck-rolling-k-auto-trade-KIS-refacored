"""Exercise the ACTUAL durable route_order with fake KIS and two SELL attempts.

Neither fake broker call performs HTTP. This verifies that diagnostics do
not change route outcomes and selected vs first-candidate metrics stay split.
"""
import time

from trader.us.execution.order_router import route_order
from trader.us.execution.tick_context import TickExecutionContext
from tests.us.test_us_route_order_claim_boundary import _intent, _route_fixture


def test_exact_selected_and_symbol_first_remain_separate_across_two_attempts(monkeypatch):
    _engine, _claims, broker = _route_fixture(monkeypatch)
    context = TickExecutionContext(
        "2026-10-02", "am", "trace-run", 1, "trace-tick",
        deadline=time.monotonic() + 30,
    )
    symbols = ["TEST_SYMBOL", "OTHER_SYMBOL"]
    keys = ["trace-one", "trace-two"]
    attempts = []

    for idx, (symbol, key) in enumerate(zip(symbols, keys)):
        now = time.monotonic()
        context._sell_selected_ready_by_symbol = {symbol: now - .01}
        context._sell_signal_ready_by_symbol = {symbol: now - .03}
        context._sell_selected_trace_id_by_symbol = {symbol: "signal-" + key}
        intent = _intent(
            "2026-10-02", side="SELL", client_order_key=key,
            action="HARD_STOP_" + str(idx),
        )
        intent["symbol"] = symbol
        intent["meta"] = {"strategy_owner": "US_STANDARD"}
        result = route_order(
            intent, kis_client=broker, context=context,
            current_position_symbols=set(symbols),
        )
        assert result["status"] == "ACK", result
        attempt = str(result["intent"]["submit_attempt_id"])
        attempts.append(attempt)
        chosen = context.metrics["sell_signal_to_broker_boundary_by_attempt_ms"][attempt]
        first = context.metrics["sell_first_candidate_to_broker_by_attempt_ms"][attempt]
        assert first > chosen
        assert first - chosen >= 15.0

    assert broker.calls == 2
    assert len(set(attempts)) == 2
    assert len(context.metrics["sell_signal_to_broker_boundary_by_attempt_ms"]) == 2
    assert len(context.metrics["sell_first_candidate_to_broker_by_attempt_ms"]) == 2
