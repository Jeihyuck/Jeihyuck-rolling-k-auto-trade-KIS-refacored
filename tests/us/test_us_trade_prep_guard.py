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
        assert g["ok"] is False


def test_prep_guard_block_when_wrong_trade_date(tmp_path):
    """trade_date 불일치 시 ok=False여야 한다."""
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
        assert g["ok"] is False


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
        assert g["ok"] is False


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



def test_trade_session_prep_guard_exception_is_exit_only(monkeypatch):
    """check_us_prep_guard 예외는 신규매수 차단 + exit-only tick으로 전달된다."""
    from trader.us.runner import trade_session_runner as mod
    import trader.us.market_calendar as cal
    import trader.us.prep_contract as pc
    import trader.us.utils.session_guard as sg
    import trader.us.runner.trade_tick_runner as tick_mod

    captured = {}

    monkeypatch.setattr(cal, "is_us_trading_day", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(sg, "acquire_us_session_running_lock", lambda *a, **k: {"acquired": True})
    monkeypatch.setattr(sg, "release_us_session_running_lock", lambda *a, **k: None)
    monkeypatch.setattr(sg, "check_us_session_file_guard", lambda *a, **k: {"already_ran": False, "payload": {}, "guard_status": "OK"})
    monkeypatch.setattr(sg, "write_us_session_done_file", lambda *a, **k: None)
    monkeypatch.setattr(mod, "write_heartbeat_file", lambda *a, **k: None)
    monkeypatch.setattr(mod, "_write_us_session_report", lambda *a, **k: None)
    monkeypatch.setattr(mod, "_write_us_schedule_health", lambda *a, **k: None)
    monkeypatch.setattr(pc, "check_us_prep_guard", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))

    def fake_tick(**kwargs):
        captured.update(kwargs)
        return {"status": "OK", "reason": "unit", "orders_blocked": 0, "block_reasons": {}}

    monkeypatch.setattr(tick_mod, "load_watchlist_from_artifact", lambda *_a, **_k: [])
    monkeypatch.setattr(tick_mod, "run_trade_tick", fake_tick)
    monkeypatch.setenv("US_TICK_TIMEOUT_SEC", "60")

    result = mod.run_trade_session(
        session="am",
        env="practice",
        offline=False,
        max_minutes=1,
        interval_sec=1,
        force_now="2026-07-15T09:35:00-04:00",
        max_ticks=1,
    )

    assert result["status"] in {"OK", "OK_WITH_WARNINGS"}
    assert captured["entry_can_proceed"] is False
    assert captured["exit_can_proceed"] is True
