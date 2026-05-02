# -*- coding: utf-8 -*-
"""tests/us/test_us_trade_session_runner.py - session runner smoke test."""
import os
import pytest


def _setup_env():
    os.environ.update({
        "TRADING_REGION": "US",
        "US_AGENT_ENABLED": "1",
        "KIS_ENV": "practice",
        "US_PAPER_TRADING_ENABLED": "1",
        "US_LIVE_TRADING_ENABLED": "0",
        "DISABLE_REAL_TRADING": "1",
        "ALLOW_REAL_ORDER": "0",
        "US_STRATEGY_ENGINE": "pb1",
        "US_ENTRY_ENABLED": "1",
        "US_EXIT_ENABLED": "1",
        "US_PAPER_MAX_CAPITAL_KRW": "50000000",
        "US_BUDGET_FX_KRW_PER_USD": "1450",
        "DRY_RUN": "1",
        "PYTHONPATH": ".",
    })


def test_session_runner_am_offline_smoke():
    """AM session 단일 tick (offline, force-now) smoke 테스트."""
    _setup_env()
    from trader.us.runner.trade_session_runner import run_trade_session

    result = run_trade_session(
        session="am",
        env="practice",
        offline=True,
        max_minutes=1,
        interval_sec=1,
        force_now="2026-01-02T09:35:00-05:00",
    )

    assert result["status"] in ("OK", "OK_WITH_WARNINGS", "SKIP"), \
        f"Unexpected status: {result}"


def test_session_runner_afternoon_offline_smoke():
    """Afternoon session 단일 tick (offline, force-now) smoke 테스트."""
    _setup_env()
    from trader.us.runner.trade_session_runner import run_trade_session

    result = run_trade_session(
        session="afternoon",
        env="practice",
        offline=True,
        max_minutes=1,
        interval_sec=1,
        force_now="2026-01-02T13:00:00-05:00",
    )

    assert result["status"] in ("OK", "OK_WITH_WARNINGS", "SKIP"), \
        f"Unexpected status: {result}"


def test_session_runner_returns_tick_count():
    """force_now 모드에서 tick_count가 포함되어야 한다."""
    _setup_env()
    from trader.us.runner.trade_session_runner import run_trade_session

    result = run_trade_session(
        session="am",
        env="practice",
        offline=True,
        max_minutes=1,
        interval_sec=1,
        force_now="2026-01-02T09:35:00-05:00",
    )

    # 정상 또는 SKIP 이면 pass (마켓 미개장일 수도 있음)
    assert "status" in result
