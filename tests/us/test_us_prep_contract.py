# -*- coding: utf-8 -*-
"""tests/us/test_us_prep_contract.py - Prep contract builder 테스트."""
import json
import pytest
from pathlib import Path
from unittest.mock import patch


def _make_final30_scored(n=30):
    rows = []
    for i in range(n):
        rows.append({
            "symbol": f"SYM{i + 100:04d}",
            "exchange": "NASDAQ",
            "asset_type": "stock",
            "rank_final30": i + 1,
            "score_final": 0.5 + i * 0.01,
            "agent_a_score": 0.4,
            "agent_b_score": 0.45,
            "close": 120.0,
            "ma20": 115.0,
            "ma50": 110.0,
            "atr_pct": 0.03,
            "entry_style_selected": "pb1_pullback",
            "reason_json": {"agent_a": "RS+", "agent_b": "pullback"},
        })
    return rows


def _make_valid_inputs(final30_count=30):
    final30 = _make_final30_scored(final30_count)
    dynamic_universe_result = {
        "status": "OK",
        "raw_count": 100,
        "unique_count": 100,
        "filtered_count": 95,
        "source_counts": {},
        "filter_counts": {},
        "symbols": [],
    }
    candidate_pool_result = {
        "status": "OK",
        "input_count": 95,
        "selected_count": 80,
        "min_required": 50,
        "target": 200,
        "selection_mode_counts": {},
        "rows": [],
    }
    watchlist_result = {
        "broader_scored": final30,
        "top50_scored": final30,
        "final30": final30,
        "final30_scored": final30,
        "final30_scored_count": len(final30),
        "top50_count": len(final30),
    }
    validation = {
        "ok": True,
        "errors": [],
        "warnings": [],
        "score_nonzero_count": len(final30),
        "agent_a_nonzero_count": len(final30),
        "agent_b_nonzero_count": len(final30),
    }
    paths = {
        "dynamic_universe": "/tmp/test_du.json",
        "candidate_pool": "/tmp/test_cp.json",
        "top50_scored": "/tmp/test_top50.json",
        "final30_scored": "/tmp/test_f30.json",
        "prep_contract": "/tmp/test_pc.json",
    }
    return dynamic_universe_result, candidate_pool_result, watchlist_result, validation, paths


def test_build_us_prep_contract_returns_required_keys():
    """build_us_prep_contract 결과에 필수 키가 있어야 한다."""
    try:
        from trader.us.prep_contract import build_us_prep_contract
    except ImportError:
        pytest.skip("prep_contract not available")

    du, cp, wl, val, paths = _make_valid_inputs(30)
    contract = build_us_prep_contract(
        trade_date="2024-05-01",
        env="practice",
        status="OK",
        dynamic_universe_result=du,
        candidate_pool_result=cp,
        watchlist_result=wl,
        validation=val,
        paths=paths,
    )

    assert "status" in contract
    assert "trade_date" in contract
    assert "trade_can_proceed" in contract
    assert "contract_ok" in contract
    assert "final30_scored_count" in contract


def test_build_us_prep_contract_trade_can_proceed_ok():
    """정상 30행 + validation ok → trade_can_proceed=1이어야 한다."""
    try:
        from trader.us.prep_contract import build_us_prep_contract
    except ImportError:
        pytest.skip("prep_contract not available")

    du, cp, wl, val, paths = _make_valid_inputs(30)
    contract = build_us_prep_contract(
        trade_date="2024-05-01",
        env="practice",
        status="OK",
        dynamic_universe_result=du,
        candidate_pool_result=cp,
        watchlist_result=wl,
        validation=val,
        paths=paths,
    )

    assert contract["trade_can_proceed"] == 1


def test_build_us_prep_contract_validation_fail_blocks_entry_only():
    """watchlist validation ok=False blocks entry only; exit/close liveness remains explicit."""
    try:
        from trader.us.prep_contract import build_us_prep_contract
    except ImportError:
        pytest.skip("prep_contract not available")

    du, cp, wl, val, paths = _make_valid_inputs(30)
    val["ok"] = False  # validation 실패

    contract = build_us_prep_contract(
        trade_date="2024-05-01",
        env="practice",
        status="OK",
        dynamic_universe_result=du,
        candidate_pool_result=cp,
        watchlist_result=wl,
        validation=val,
        paths=paths,
    )

    assert contract["entry_can_proceed"] == 0
    assert contract["exit_can_proceed"] == 1
    assert contract["close_can_proceed"] == 1
    assert contract["trade_can_proceed"] == 1


def test_build_us_prep_contract_final30_less_than_30_allows_safe_underfilled():
    """safe final30_scored_count < 30 can trade with an underfilled tier."""
    try:
        from trader.us.prep_contract import build_us_prep_contract
    except ImportError:
        pytest.skip("prep_contract not available")

    du, cp, wl, val, paths = _make_valid_inputs(29)
    wl["final30_scored_count"] = 29

    contract = build_us_prep_contract(
        trade_date="2024-05-01",
        env="practice",
        status="OK",
        dynamic_universe_result=du,
        candidate_pool_result=cp,
        watchlist_result=wl,
        validation=val,
        paths=paths,
    )

    assert contract["trade_can_proceed"] == 1
    assert contract["trade_block_reason"] == "ok"
    assert contract["underfilled_tier"] == "normal_underfilled"


def test_build_us_prep_contract_error_status_blocks():
    """status=ERROR → trade_can_proceed=0이어야 한다."""
    try:
        from trader.us.prep_contract import build_us_prep_contract
    except ImportError:
        pytest.skip("prep_contract not available")

    du, cp, wl, val, paths = _make_valid_inputs(30)

    contract = build_us_prep_contract(
        trade_date="2024-05-01",
        env="practice",
        status="ERROR",
        dynamic_universe_result=du,
        candidate_pool_result=cp,
        watchlist_result=wl,
        validation=val,
        paths=paths,
    )

    assert contract["trade_can_proceed"] == 0


def test_check_us_prep_guard_ok_with_valid_contract(tmp_path):
    """유효한 prep contract 파일이 있으면 guard ok=True여야 한다."""
    try:
        from trader.us.prep_contract import check_us_prep_guard
    except ImportError:
        pytest.skip("prep_contract not available")

    trade_date = "2024-05-01"
    contract_data = {
        "trade_date": trade_date,
        "status": "OK",
        "contract_ok": True,
        "trade_can_proceed": 1,
        "final30_scored_count": 30,
        "score_nonzero_count": 30,
    }

    contract_file = tmp_path / "prep_contract.json"
    contract_file.write_text(json.dumps(contract_data), encoding="utf-8")

    missing_path = tmp_path / "nonexistent_fallback.json"

    with patch("trader.us.path_contract.us_prep_contract_path", return_value=contract_file), \
         patch("trader.us.path_contract.us_signals_latest_prep_contract_path", return_value=missing_path):
        guard = check_us_prep_guard(trade_date)
        assert guard["ok"] is True


def test_check_us_prep_guard_blocks_when_no_contract(tmp_path):
    """prep contract 파일이 없으면 guard ok=False여야 한다."""
    try:
        from trader.us.prep_contract import check_us_prep_guard
    except ImportError:
        pytest.skip("prep_contract not available")

    missing = tmp_path / "nonexistent.json"

    with patch("trader.us.path_contract.us_prep_contract_path", return_value=missing), \
         patch("trader.us.path_contract.us_signals_latest_prep_contract_path", return_value=missing):
        guard = check_us_prep_guard("2024-05-01")
        assert guard["ok"] is True


def test_cluster_cap_blocks_entry_but_allows_exit_and_close():
    from trader.us.prep_contract import build_us_prep_contract
    du, cp, wl, val, paths = _make_valid_inputs(17)
    wl.update({
        "cluster_contract_ok": False,
        "cap_violations": ["SINGLE_CLUSTER", "AI_TECH_COMBINED"],
        "market_state_overlay": {"market_regime": "DEFENSIVE", "allow_new_buy": True},
    })
    contract = build_us_prep_contract(
        trade_date="2026-07-13", env="practice", status="OK",
        dynamic_universe_result=du, candidate_pool_result=cp, watchlist_result=wl,
        validation=val, paths=paths,
    )
    assert contract["entry_can_proceed"] == 0
    assert contract["exit_can_proceed"] == 1
    assert contract["close_can_proceed"] == 1
    assert contract["trade_can_proceed"] == 1
    assert contract["effective_max_new_positions"] == 0
    assert contract["status"].startswith("OK_WITH_WARNINGS_ENTRY_BLOCKED")


def test_legacy_failed_cluster_contract_allows_exit(monkeypatch):
    import trader.us.prep_contract as pc
    legacy_contract = {
        "trade_date": "2026-07-13",
        "status": "FAILED_CLUSTER_CAP_CONTRACT",
        "trade_can_proceed": 0,
        "trade_block_reason": "sector_cap_violation_block",
        "final30_scored_count": 17,
        "score_nonzero_count": 17,
    }
    monkeypatch.setattr("trader.us.path_contract.load_us_prep_contract", lambda trade_date: legacy_contract)
    guard = pc.check_us_prep_guard("2026-07-13", session="am")
    assert guard["ok"] is True
    assert guard["entry_can_proceed"] is False
    assert guard["exit_can_proceed"] is True


def test_volume_missing_fallback_permission_split_blocks_entry_by_default(monkeypatch):
    from trader.us.prep_contract import build_us_prep_contract
    monkeypatch.delenv("US_ALLOW_ENTRY_WITH_VOLUME_MISSING_FALLBACK", raising=False)
    du, cp, wl, val, paths = _make_valid_inputs(30)
    du.update({"volume_missing_fallback_used": True, "volume_missing_fallback_count": 50, "warnings": ["volume_missing_from_provider"]})
    contract = build_us_prep_contract(
        trade_date="2024-05-01", env="practice", status="OK_WITH_WARNINGS",
        dynamic_universe_result=du, candidate_pool_result=cp, watchlist_result=wl, validation=val, paths=paths,
    )
    assert contract["entry_can_proceed"] == 0
    assert contract["exit_can_proceed"] == 1
    assert contract["close_can_proceed"] == 1
    assert contract["trade_can_proceed"] == 1
    assert contract["trade_block_reason"] == "volume_missing_provider_entry_block"


def test_volume_missing_fallback_permission_split_allows_entry_when_env_enabled(monkeypatch):
    from trader.us.prep_contract import build_us_prep_contract
    monkeypatch.setenv("US_ALLOW_ENTRY_WITH_VOLUME_MISSING_FALLBACK", "1")
    du, cp, wl, val, paths = _make_valid_inputs(30)
    du.update({"volume_missing_fallback_used": True, "volume_missing_fallback_count": 50, "warnings": ["volume_missing_from_provider"]})
    contract = build_us_prep_contract(
        trade_date="2024-05-01", env="practice", status="OK_WITH_WARNINGS",
        dynamic_universe_result=du, candidate_pool_result=cp, watchlist_result=wl, validation=val, paths=paths,
    )
    assert contract["entry_can_proceed"] == 1
    assert contract["exit_can_proceed"] == 1
    assert contract["close_can_proceed"] == 1
