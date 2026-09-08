from __future__ import annotations

from trader.kr.pb1.exit_policy import resolve_exit_policy


def test_resolve_exit_policy_prefers_hard_stop_then_trail_then_soft_then_time():
    hard_stop = resolve_exit_policy(
        days_held=2,
        holding_bars=10,
        stop_hit=True,
        trail_stop_price=90.0,
        mark=85.0,
        ma20=88.0,
        ma50=87.0,
        time_stop_hit=True,
        risk_off_signal=True,
    )
    assert hard_stop["final_reason"] == "EXIT_HARD_STOP"
    assert hard_stop["family"] == "EXIT_STOP"
    assert hard_stop["exit_ok"] is True
    assert hard_stop["triggered"] == ["EXIT_HARD_STOP"]

    trail = resolve_exit_policy(
        days_held=2,
        holding_bars=10,
        stop_hit=False,
        trail_stop_price=90.0,
        mark=89.0,
        ma20=88.0,
        ma50=87.0,
        time_stop_hit=True,
        risk_off_signal=False,
    )
    assert trail["final_reason"] == "EXIT_TRAIL"
    assert trail["family"] == "EXIT_TRAIL"
    assert trail["exit_ok"] is True
    assert trail["triggered"] == ["EXIT_TRAIL"]

    soft = resolve_exit_policy(
        days_held=2,
        holding_bars=10,
        stop_hit=False,
        trail_stop_price=80.0,
        mark=84.0,
        ma20=85.0,
        ma50=86.0,
        time_stop_hit=True,
        risk_off_signal=True,
    )
    assert soft["final_reason"] == "EXIT_SOFT_RISK_OFF"
    assert soft["family"] == "EXIT_RISK_OFF"
    assert soft["exit_ok"] is True
    assert soft["triggered"] == ["EXIT_SOFT_RISK_OFF"]

    time_based = resolve_exit_policy(
        days_held=2,
        holding_bars=10,
        stop_hit=False,
        trail_stop_price=80.0,
        mark=90.0,
        ma20=85.0,
        ma50=86.0,
        time_stop_hit=True,
        risk_off_signal=False,
    )
    assert time_based["final_reason"] == "EXIT_TIME_BASED"
    assert time_based["family"] == "EXIT_TIME"
    assert time_based["exit_ok"] is True
    assert time_based["triggered"] == ["EXIT_TIME_BASED"]


def test_resolve_exit_policy_returns_skip_when_no_exit_signals():
    policy = resolve_exit_policy(
        days_held=0,
        holding_bars=0,
        stop_hit=False,
        trail_stop_price=None,
        mark=100.0,
        ma20=None,
        ma50=None,
        time_stop_hit=False,
        risk_off_signal=False,
    )
    assert policy["same_day_entry"] is True
    assert policy["final_reason"] == "NO_EXIT_SIGNAL"
    assert policy["family"] == "SKIP"
    assert policy["exit_ok"] is False
    assert policy["triggered"] == []
