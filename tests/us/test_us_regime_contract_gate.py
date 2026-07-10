import os

from trader.us.runner.trade_tick_runner import validate_us_regime_contract_for_entry
from trader.us.prep_contract import build_us_prep_contract
from trader.us.watchlist_builder import enforce_regime_sector_caps


def test_real_trade_missing_contract_version_blocks_entry(monkeypatch):
    monkeypatch.delenv("US_ALLOW_LEGACY_PREP_FOR_TEST", raising=False)
    gate = validate_us_regime_contract_for_entry({"status": "OK"}, real_order_mode=True, kis_order_allowed=True)
    assert gate["ok"] is False
    assert gate["reason"] == "prep_contract_version_mismatch"


def test_schedule_missing_contract_version_blocks_entry(monkeypatch):
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.setenv("US_ALLOW_LEGACY_PREP_FOR_TEST", "1")
    gate = validate_us_regime_contract_for_entry({"status": "OK"}, real_order_mode=False, kis_order_allowed=False)
    assert gate["ok"] is False
    assert gate["reason"] == "prep_contract_version_mismatch"


def test_legacy_contract_allowed_only_in_test_env(monkeypatch):
    monkeypatch.delenv("GITHUB_EVENT_NAME", raising=False)
    monkeypatch.setenv("US_ALLOW_LEGACY_PREP_FOR_TEST", "1")
    assert validate_us_regime_contract_for_entry({"status": "OK"}, real_order_mode=False, kis_order_allowed=False)["ok"] is True
    blocked = validate_us_regime_contract_for_entry({"status": "OK"}, real_order_mode=True, kis_order_allowed=True)
    assert blocked["ok"] is False
    assert blocked["reason"] == "prep_contract_version_mismatch"


def test_sector_cap_violation_blocks_trade():
    rows = [{"symbol": f"AI{i}", "theme_cluster": "AI_SEMI", "score_final": 1.0 - i * 0.001} for i in range(21)] + [{"symbol": f"H{i}", "theme_cluster": "HEALTHCARE", "score_final": 0.7} for i in range(9)]
    final, meta = enforce_regime_sector_caps(rows, rows, {"market_regime": "NEUTRAL", "max_ai_tech_ratio": 0.35, "max_single_cluster_ratio": 1.0}, 30)
    assert "AI_TECH_COMBINED" in meta["cap_violations"]
    contract = build_us_prep_contract(
        trade_date="2026-07-09",
        env="practice",
        status="OK",
        dynamic_universe_result={"filtered_count": 50},
        candidate_pool_result={"selected_count": 30, "status": "OK"},
        watchlist_result={"top50_count": 30, "final30_count": 30, "final30_scored_count": 30, "cluster_contract_ok": False, "final30_cluster_cap_clean": False, "cap_violations": meta["cap_violations"], "blocked_by_cluster_cap": meta["blocked_by_cluster_cap"], "market_state_overlay": {"market_regime": "NEUTRAL", "allow_new_buy": True}},
        validation={"ok": True, "score_nonzero_count": 30, "warnings": [], "errors": []},
        paths={},
    )
    assert contract["cluster_contract_ok"] is False
    assert contract["trade_can_proceed"] == 0
    assert contract["trade_block_reason"] in {"sector_cap_violation_block", "cluster_cap_contract_failed", "final30_incomplete"}


def test_risk_off_is_not_prep_error():
    contract = build_us_prep_contract(
        trade_date="2026-07-09",
        env="practice",
        status="OK",
        dynamic_universe_result={"filtered_count": 50},
        candidate_pool_result={"selected_count": 30, "status": "OK"},
        watchlist_result={"top50_count": 30, "final30_count": 30, "final30_scored_count": 30, "cluster_contract_ok": True, "final30_cluster_cap_clean": True, "cap_violations": [], "market_state_overlay": {"market_state": "NORMAL", "market_regime": "RISK_OFF", "capital_scale": 0.0, "max_ai_tech_ratio": 0.1, "max_single_cluster_ratio": 0.1, "allow_new_buy": False, "allow_ai_tech_buy": False, "allow_defensive_buy": False, "force_entry_block": True}},
        validation={"ok": True, "score_nonzero_count": 30, "warnings": [], "errors": []},
        paths={},
    )
    assert contract["status"] in {"RISK_OFF_ENTRY_BLOCKED", "DEFENSE_CRASH_ENTRY_BLOCKED"}
    assert contract["trade_can_proceed"] == 0
    assert contract["trade_block_reason"] == "risk_off_entry_block"
    assert contract["contract_version"] == "us_sector_rotation_v3"
    assert contract["market_regime_version"] == "us_leading_regime_v1"
