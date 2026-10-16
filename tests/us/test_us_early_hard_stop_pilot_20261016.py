"""Synthetic, no-broker proof for per-position hard-stop streaming.

Not an operating latency benchmark: it isolates B's unrelated quote delay.
"""
from __future__ import annotations

import time

from trader.us.pb1 import us_exit_engine
from trader.us.runner.trade_tick_runner import route_exit_orders_immediately


def test_hard_stop_route_precedes_next_symbols_slow_quote(monkeypatch):
    from trader.us.pb1 import us_exit_router
    events: list[str] = []

    class QuoteProvider:
        def get_current_price(self, symbol, exchange):
            events.append("quote:" + symbol)
            if symbol == "BBB":
                time.sleep(0.06)
            return {"last": "90.0"}

    monkeypatch.setattr(
        us_exit_router, "route_exit_by_book_horizon",
        lambda **kw: (
            {"symbol": "AAA", "side": "SELL", "qty": 1,
             "exit_type": "hard_stop_loss", "meta": {}}
            if kw["position"]["symbol"] == "AAA" else None
        ),
    )
    monkeypatch.setattr(us_exit_engine, "build_us_exit_explanation", lambda **kw: {})
    monkeypatch.setattr(us_exit_engine, "log_us_exit_decision", lambda *a, **k: None)
    monkeypatch.setattr(us_exit_engine, "validate_explanations_batch", lambda rows: {
        "total_count": len(rows), "full_count": 0, "partial_count": 0,
        "minimal_count": 0, "missing_count": 0,
        "quality_summary": "synthetic", "quality_warning": False,
    })

    submission_times: list[float] = []
    def fake_route_order(intent, **kwargs):
        events.append("submit:" + intent["symbol"])
        submission_times.append(time.monotonic())
        return {"status": "ACK", "side": "SELL",
                "symbol": intent["symbol"], "intent": intent}

    monkeypatch.setattr("trader.us.execution.order_router.route_order", fake_route_order)
    positions = [
        {"symbol": "AAA", "strategy_owner": "US_STANDARD",
         "exchange": "NASDAQ", "qty": 1, "entry_price": 0},
        {"symbol": "BBB", "strategy_owner": "US_STANDARD",
         "exchange": "NASDAQ", "qty": 1, "entry_price": 0},
    ]

    def submit_from_snapshot(snapshot):
        intents = us_exit_engine.generate_exit_intents(
            [], prepared_snapshots=[snapshot], include_trend_time=False,
        )
        if intents:
            route_exit_orders_immediately(
                intents, buy_daily_notional=0.0,
                position_count=2, effective_budget=1000.0,
                signal_only=False, kis_order_allowed=False,
                current_position_symbols={"AAA", "BBB"},
            )

    early_started = time.monotonic()
    us_exit_engine.prepare_exit_position_snapshots(
        [dict(x) for x in positions], QuoteProvider(),
        on_snapshot_ready=submit_from_snapshot,
    )
    early_submit_ms = (submission_times[0] - early_started) * 1000.0
    assert events.index("submit:AAA") < events.index("quote:BBB")

    events.clear()
    submission_times.clear()
    baseline_started = time.monotonic()
    snapshots = us_exit_engine.prepare_exit_position_snapshots(
        [dict(x) for x in positions], QuoteProvider(),
    )
    normal_intents = us_exit_engine.generate_exit_intents(
        [], prepared_snapshots=snapshots, include_trend_time=False,
    )
    route_exit_orders_immediately(
        normal_intents, buy_daily_notional=0.0,
        position_count=2, effective_budget=1000.0,
        signal_only=False, kis_order_allowed=False,
        current_position_symbols={"AAA", "BBB"},
    )
    baseline_submit_ms = (submission_times[0] - baseline_started) * 1000.0
    assert events.index("quote:BBB") < events.index("submit:AAA")
    assert events.count("submit:AAA") == 1
    # Synthetic delay injection proves causal isolation of B's 60-ms wait.
    # This is NOT a claim about real WSL/KIS execution.
    assert baseline_submit_ms - early_submit_ms >= 40.0
