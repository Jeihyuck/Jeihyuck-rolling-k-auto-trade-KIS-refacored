# -*- coding: utf-8 -*-
"""Phase 13: close workflow entry policy 검증.

- US_CLOSE_ENTRY_ENABLED=0이면 close에서 신규 entry 금지
- run_trade_close 결과에 close_entry_enabled 필드 포함
"""
from __future__ import annotations

import os
import pytest
from unittest.mock import patch, MagicMock


def test_close_entry_disabled_by_default():
    """US_CLOSE_ENTRY_ENABLED=0 (기본)이면 close_entry_enabled=False."""
    from trader.us.runner.trade_close_runner import run_trade_close

    with patch.dict(os.environ, {"US_CLOSE_ENTRY_ENABLED": "0"}):
        with (
            patch("trader.us.data_provider.USDataProvider.__init__", return_value=None),
            patch("trader.us.db.repos.save_fills", return_value=None),
            patch("trader.us.db.repos.save_position_snapshot", return_value=None),
            patch("trader.us.db.repos.save_reconcile_log", return_value=None),
            patch("trader.us.runner.daily_report_runner.run_daily_report", return_value={}),
            patch("trader.us.data_provider.USDataProvider.get_balance", return_value={}),
        ):
            result = run_trade_close(env="practice", offline=True)

    assert result.get("close_entry_enabled") is False


def test_close_entry_enabled_when_set():
    """US_CLOSE_ENTRY_ENABLED=1이면 close_entry_enabled=True."""
    from trader.us.runner.trade_close_runner import run_trade_close

    with patch.dict(os.environ, {"US_CLOSE_ENTRY_ENABLED": "1"}):
        with (
            patch("trader.us.data_provider.USDataProvider.__init__", return_value=None),
            patch("trader.us.db.repos.save_fills", return_value=None),
            patch("trader.us.db.repos.save_position_snapshot", return_value=None),
            patch("trader.us.db.repos.save_reconcile_log", return_value=None),
            patch("trader.us.runner.daily_report_runner.run_daily_report", return_value={}),
            patch("trader.us.data_provider.USDataProvider.get_balance", return_value={}),
        ):
            result = run_trade_close(env="practice", offline=True)

    assert result.get("close_entry_enabled") is True


def test_close_returns_required_fields():
    """run_trade_close 결과에 필수 필드 포함."""
    from trader.us.runner.trade_close_runner import run_trade_close

    with (
        patch("trader.us.data_provider.USDataProvider.__init__", return_value=None),
        patch("trader.us.db.repos.save_fills", return_value=None),
        patch("trader.us.db.repos.save_position_snapshot", return_value=None),
        patch("trader.us.db.repos.save_reconcile_log", return_value=None),
        patch("trader.us.runner.daily_report_runner.run_daily_report", return_value={}),
        patch("trader.us.data_provider.USDataProvider.get_balance", return_value={}),
    ):
        result = run_trade_close(env="practice", offline=True)

    assert "status" in result
    assert "close_entry_enabled" in result
    assert "fills_status" in result
    assert "reconcile_status" in result
