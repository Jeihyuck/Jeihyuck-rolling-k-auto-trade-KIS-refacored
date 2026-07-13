from trader.us.prep_contract import build_us_prep_contract, check_us_prep_guard
from trader.us.runner.trade_tick_runner import validate_us_regime_contract_for_entry


def _base(final30=17, cluster_ok=True, cap=None):
    return build_us_prep_contract(
        trade_date="2026-07-13", env="practice", status="OK",
        dynamic_universe_result={"filtered_count": 50},
        candidate_pool_result={"selected_count": 30, "status": "OK"},
        watchlist_result={"top50_count": 50, "final30_count": final30, "final30_scored_count": final30, "cluster_contract_ok": cluster_ok, "cap_violations": cap or [], "market_state_overlay": {"market_regime": "RISK_ON", "allow_new_buy": True, "capital_scale": 1.0, "max_new_positions": 30}},
        validation={"ok": True, "score_nonzero_count": final30, "warnings": [], "errors": []}, paths={})


def test_underfilled_final30_degrades_but_session_can_proceed():
    c = _base(17)
    assert c["trade_can_proceed"] == 1
    assert c["exit_can_proceed"] == 1
    assert c["close_can_proceed"] == 1
    assert c["raw_final30_count"] == 17
    assert c["effective_final30_count"] == 17


def test_cluster_cap_warning_allows_liveness_but_blocks_entry():
    c = _base(24, cluster_ok=False, cap=["SINGLE_CLUSTER"])
    assert c["trade_can_proceed"] == 1
    assert c["entry_can_proceed"] == 0
    assert c["exit_can_proceed"] == 1
    assert c["cluster_contract_ok"] is False
    assert c["degraded_reason"] in {"sector_cap_violation_block", "cluster_cap_contract_failed"}


def test_entry_guard_split_uses_entry_permission():
    c = _base(24, cluster_ok=False, cap=["SINGLE_CLUSTER"])
    g = validate_us_regime_contract_for_entry(c, real_order_mode=True, kis_order_allowed=True)
    assert not g["ok"]
    assert g["reason"] in {"sector_cap_violation_block", "cluster_cap_contract_failed"}
