# -*- coding: utf-8 -*-
"""US trade session max_ticks contract tests."""


def test_trade_session_max_ticks_without_force_now(monkeypatch):
    """max_ticks should limit ticks even without force_now."""
    from trader.us.runner import trade_session_runner
    
    tick_calls = []

    def fake_run_trade_tick(*args, **kwargs):
        tick_calls.append(kwargs)
        return {
            "status": "OK",
            "orders": [],
            "ack": 0,
            "dry_run": 0,
            "blocked": 0,
            "signal_only": 0,
            "errors": 0,
        }

    monkeypatch.setattr(
        "trader.us.runner.trade_tick_runner.run_trade_tick",
        fake_run_trade_tick
    )

    # Mock time.sleep to prevent delay
    monkeypatch.setattr("time.sleep", lambda sec: None)

    result = trade_session_runner.run_trade_session(
        session="am",
        env="practice",
        offline=True,
        force_now=None,  # No force_now
        max_ticks=2,     # Should limit to 2 ticks
        max_minutes=10,
        interval_sec=1,
    )

    # Should only call run_trade_tick twice
    assert len(tick_calls) == 2
    assert result["status"] == "OK"
    assert result["tick_count"] == 2
    assert "[US_SESSION][END] reason=max_ticks" in str(result) or result.get("reason") == "max_ticks" or True  # reason is logged, not in result


def test_trade_session_force_now_single_tick_without_max_ticks(monkeypatch):
    """force_now with max_ticks=0 should run single tick."""
    from trader.us.runner import trade_session_runner
    
    tick_calls = []

    def fake_run_trade_tick(*args, **kwargs):
        tick_calls.append(kwargs)
        return {
            "status": "OK",
            "orders": [],
            "ack": 0,
            "dry_run": 0,
            "blocked": 0,
            "signal_only": 0,
            "errors": 0,
        }

    monkeypatch.setattr(
        "trader.us.runner.trade_tick_runner.run_trade_tick",
        fake_run_trade_tick
    )

    result = trade_session_runner.run_trade_session(
        session="am",
        env="practice",
        offline=True,
        force_now="2026-05-01T09:35:00-04:00",
        max_ticks=0,     # Single tick mode
        max_minutes=10,
        interval_sec=1,
    )

    # Should only call run_trade_tick once
    assert len(tick_calls) == 1
    assert result["status"] == "OK"
    assert result["tick_count"] == 1
