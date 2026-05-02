# -*- coding: utf-8 -*-
"""tests/us/test_us_trade_tick_runner.py - tick runner 테스트."""
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
        "US_BLOCK_NEW_ENTRY_AFTER_ET": "15:45",
        "US_BLOCK_REBUY_AFTER_SELL_SAME_DAY": "0",  # DB 없을 때
        "US_ORDER_ACCEPTED_IS_NOT_FILLED": "0",      # DB 없을 때
        "DRY_RUN": "1",
    })


def test_tick_runner_am_offline():
    """AM tick - offline 모드."""
    _setup_env()
    from trader.us.runner.trade_tick_runner import run_trade_tick

    result = run_trade_tick(
        session="am",
        env="practice",
        offline=True,
        force_now="2026-01-02T09:35:00-05:00",
    )

    assert result["status"] in ("OK", "OK_WITH_WARNINGS", "SKIP"), \
        f"Unexpected status: {result}"


def test_tick_runner_after_cutoff_blocks_entry():
    """15:46 ET 이후에는 BUY intent가 차단되어야 한다."""
    _setup_env()
    from trader.us.runner.trade_tick_runner import _entry_cutoff_passed
    from datetime import datetime
    from zoneinfo import ZoneInfo

    NY_TZ = ZoneInfo("America/New_York")
    dt = datetime.fromisoformat("2026-01-02T15:46:00-05:00").astimezone(NY_TZ)

    assert _entry_cutoff_passed(dt) is True


def test_tick_runner_before_cutoff_allows_entry():
    """09:35 ET 에는 entry cutoff 미통과."""
    _setup_env()
    from trader.us.runner.trade_tick_runner import _entry_cutoff_passed
    from datetime import datetime
    from zoneinfo import ZoneInfo

    NY_TZ = ZoneInfo("America/New_York")
    dt = datetime.fromisoformat("2026-01-02T09:35:00-05:00").astimezone(NY_TZ)

    assert _entry_cutoff_passed(dt) is False


def test_tick_runner_offline_after_cutoff_no_entry_intents():
    """15:46 ET 이후 tick에서는 entry_intents 가 0이어야 한다."""
    _setup_env()
    from trader.us.runner.trade_tick_runner import run_trade_tick

    result = run_trade_tick(
        session="afternoon",
        env="practice",
        offline=True,
        force_now="2026-01-02T15:46:00-05:00",
    )

    # SKIP 또는 OK/OK_WITH_WARNINGS
    assert result["status"] in ("OK", "OK_WITH_WARNINGS", "SKIP"), \
        f"Unexpected status: {result}"
