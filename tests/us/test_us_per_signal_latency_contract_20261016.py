"""Behavioral regression for PB1 per-signal telemetry (no KIS submission)."""
from datetime import datetime, timezone

from trader.us.pb1 import us_exit_engine as exit_engine


def test_observer_failure_does_not_change_hard_stop(monkeypatch):
    from trader.us.pb1 import us_exit_router
    monkeypatch.setattr(
        us_exit_router, "route_exit_by_book_horizon",
        lambda **kwargs: {
            "side": "SELL", "symbol": kwargs["position"]["symbol"],
            "qty": 1, "exit_type": "hard_stop_loss", "meta": {},
        },
    )
    monkeypatch.setattr(exit_engine, "build_us_exit_explanation", lambda **kwargs: {})
    monkeypatch.setattr(exit_engine, "log_us_exit_decision", lambda *args, **kwargs: None)
    monkeypatch.setattr(exit_engine, "validate_explanations_batch", lambda rows: {
        "total_count": len(rows), "full_count": 0, "partial_count": 0,
        "minimal_count": 0, "missing_count": 0,
        "quality_summary": "test", "quality_warning": False,
    })
    snapshots = [
        {"symbol": "AAA", "resolved_current_price": 90.0},
        {"symbol": "BBB", "resolved_current_price": 80.0},
    ]
    now = datetime(2026, 10, 16, tzinfo=timezone.utc)
    baseline = exit_engine.generate_exit_intents(
        [], prepared_snapshots=[dict(x) for x in snapshots],
        now=now, include_trend_time=False,
    )
    observed = []
    with_observer = exit_engine.generate_exit_intents(
        [], prepared_snapshots=[dict(x) for x in snapshots],
        now=now, include_trend_time=False,
        on_sell_ready=lambda intent, t: observed.append((intent["symbol"], t)),
    )
    assert baseline == with_observer
    assert [s for s, _ in observed] == ["AAA", "BBB"]

    def broken_observer(*args):
        raise RuntimeError("telemetry failure")
    on_error = exit_engine.generate_exit_intents(
        [], prepared_snapshots=[dict(x) for x in snapshots],
        now=now, include_trend_time=False,
        on_sell_ready=broken_observer,
    )
    assert on_error == baseline


def test_finished_signal_observed_before_next_symbol(monkeypatch):
    from trader.us.pb1 import us_exit_router
    order = []
    def fake_router(**kwargs):
        sym = kwargs["position"]["symbol"]
        order.append("evaluate_" + sym)
        return {"side": "SELL", "symbol": sym, "qty": 1, "exit_type": "hard_stop_loss", "meta": {}}
    monkeypatch.setattr(us_exit_router, "route_exit_by_book_horizon", fake_router)
    monkeypatch.setattr(exit_engine, "build_us_exit_explanation", lambda **kwargs: {})
    monkeypatch.setattr(exit_engine, "log_us_exit_decision", lambda *args, **kwargs: None)
    monkeypatch.setattr(exit_engine, "validate_explanations_batch", lambda rows: {
        "total_count": len(rows), "full_count": 0, "partial_count": 0,
        "minimal_count": 0, "missing_count": 0,
        "quality_summary": "test", "quality_warning": False,
    })
    exit_engine.generate_exit_intents(
        [], prepared_snapshots=[
            {"symbol": "AAA", "resolved_current_price": 90.0},
            {"symbol": "BBB", "resolved_current_price": 80.0},
        ], include_trend_time=False,
        on_sell_ready=lambda intent, _: order.append("signal_" + intent["symbol"]),
    )
    assert order == ["evaluate_AAA", "signal_AAA", "evaluate_BBB", "signal_BBB"]
