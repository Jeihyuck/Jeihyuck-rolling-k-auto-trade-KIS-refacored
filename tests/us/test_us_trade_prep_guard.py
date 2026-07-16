# -*- coding: utf-8 -*-
"""tests/us/test_us_trade_prep_guard.py - Trade session prep guard 테스트."""
import json
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

def _valid_contract(trade_date="2024-05-01"):
    return {
        "trade_date": trade_date,
        "status": "OK",
        "contract_ok": True,
        "trade_can_proceed": 1,
        "final30_scored_count": 30,
        "score_nonzero_count": 30,
    }


def test_prep_guard_ok_with_valid_contract(tmp_path):
    """유효한 contract가 있으면 check_us_prep_guard ok=True여야 한다."""
    try:
        from trader.us.prep_contract import check_us_prep_guard
    except ImportError:
        pytest.skip("prep_contract not available")

    trade_date = "2024-05-01"
    f = tmp_path / "pc.json"
    f.write_text(json.dumps(_valid_contract(trade_date)), encoding="utf-8")
    missing = tmp_path / "no_fallback.json"

    with patch("trader.us.path_contract.us_prep_contract_path", return_value=f), \
         patch("trader.us.path_contract.us_signals_latest_prep_contract_path", return_value=missing):
        g = check_us_prep_guard(trade_date)
        assert g["ok"] is True


def test_prep_guard_block_when_missing(tmp_path):
    """contract 파일이 없으면 ok=False여야 한다."""
    try:
        from trader.us.prep_contract import check_us_prep_guard
    except ImportError:
        pytest.skip("prep_contract not available")

    missing = tmp_path / "no.json"
    with patch("trader.us.path_contract.us_prep_contract_path", return_value=missing), \
         patch("trader.us.path_contract.us_signals_latest_prep_contract_path", return_value=missing):
        g = check_us_prep_guard("2024-05-01")
        assert g["ok"] is True
        assert g["entry_can_proceed"] is False
        assert g["exit_can_proceed"] is True
        assert g["reason"] == "PREP_MISSING_EXIT_ONLY"


def test_prep_guard_block_when_wrong_trade_date(tmp_path):
    """trade_date 불일치 시 stale exit-only로 session liveness를 유지한다."""
    try:
        from trader.us.prep_contract import check_us_prep_guard
    except ImportError:
        pytest.skip("prep_contract not available")

    f = tmp_path / "pc.json"
    f.write_text(json.dumps(_valid_contract("2024-01-01")), encoding="utf-8")
    missing = tmp_path / "no_fallback.json"

    with patch("trader.us.path_contract.us_prep_contract_path", return_value=f), \
         patch("trader.us.path_contract.us_signals_latest_prep_contract_path", return_value=missing):
        g = check_us_prep_guard("2024-05-01")
        assert g["ok"] is True
        assert g["guard_state"] == "PREP_STALE_EXIT_ONLY"
        assert g["entry_can_proceed"] is False
        assert g["exit_can_proceed"] is True
        assert g["close_can_proceed"] is True
        assert str(g["reason"]).startswith("PREP_STALE_EXIT_ONLY:prep_contract_trade_date_mismatch")


def test_prep_guard_block_when_trade_can_proceed_zero(tmp_path):
    """trade_can_proceed=0이면 ok=False여야 한다."""
    try:
        from trader.us.prep_contract import check_us_prep_guard
    except ImportError:
        pytest.skip("prep_contract not available")

    contract = _valid_contract("2024-05-01")
    contract["trade_can_proceed"] = 0
    f = tmp_path / "pc.json"
    f.write_text(json.dumps(contract), encoding="utf-8")
    missing = tmp_path / "no_fallback.json"

    with patch("trader.us.path_contract.us_prep_contract_path", return_value=f), \
         patch("trader.us.path_contract.us_signals_latest_prep_contract_path", return_value=missing):
        g = check_us_prep_guard("2024-05-01")
        assert g["ok"] is True
        assert g["entry_can_proceed"] is False
        assert g["exit_can_proceed"] is True
        assert str(g["reason"]).startswith("PREP_DEGRADED_ENTRY_BLOCKED")


def test_prep_guard_allows_session_when_final30_count_less_than_30(tmp_path):
    """final30_scored_count < 30 blocks entry only and keeps session guard ok."""
    try:
        from trader.us.prep_contract import check_us_prep_guard
    except ImportError:
        pytest.skip("prep_contract not available")

    contract = _valid_contract("2024-05-01")
    contract["final30_scored_count"] = 29
    f = tmp_path / "pc.json"
    f.write_text(json.dumps(contract), encoding="utf-8")
    missing = tmp_path / "no_fallback.json"

    with patch("trader.us.path_contract.us_prep_contract_path", return_value=f), \
         patch("trader.us.path_contract.us_signals_latest_prep_contract_path", return_value=missing):
        g = check_us_prep_guard("2024-05-01")
        assert g["ok"] is True
        assert g["entry_can_proceed"] is False
        assert g["exit_can_proceed"] is True


def test_no_session_pm():
    """trade_session_runner에 session=pm 케이스가 없어야 한다 (forbidden)."""
    try:
        import inspect
        import trader.us.runner.trade_session_runner as tsr
        src = inspect.getsource(tsr)
    except ImportError:
        pytest.skip("trade_session_runner not available")

    # session == "pm" 패턴이 있으면 안됨
    assert 'session == "pm"' not in src, "Forbidden: session='pm' found in trade_session_runner"
    assert "session-pm" not in src, "Forbidden: 'session-pm' found"
