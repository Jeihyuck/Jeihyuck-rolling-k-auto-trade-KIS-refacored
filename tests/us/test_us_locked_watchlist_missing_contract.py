# -*- coding: utf-8 -*-
"""US locked watchlist missing contract tests."""

from pathlib import Path


def test_trade_tick_locked_watchlist_missing_is_error_not_ok_no_trade():
    text = Path("trader/us/runner/trade_tick_runner.py").read_text(encoding="utf-8")

    assert "locked_watchlist_missing" in text
    assert "reason=locked_watchlist_missing" in text
    assert 'status=ERROR reason=locked_watchlist_missing' in text or '"status": "ERROR"' in text


def test_session_runner_hard_errors_include_locked_watchlist_missing():
    text = Path("trader/us/runner/trade_session_runner.py").read_text(encoding="utf-8")

    assert "hard_error_reasons" in text
    assert "locked_watchlist_missing" in text
    assert "fills_contract_error" in text
