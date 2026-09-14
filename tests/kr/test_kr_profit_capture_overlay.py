import trader.kr.market_state_overlay as overlay
from trader.execution_state import exit_stage_for_reason, legal_next_exit_stage
from trader.kr.market_state_overlay import (
    build_kr_policy_missing_adoption,
    filter_kr_entry_intent,
    generate_kr_profit_capture_intents,
    has_kr_policy_missing_adoption_claim,
    is_verified_kr_policy_missing_adoption,
)


def test_legacy_market_state_evaluator_is_not_importable_from_production():
    assert not hasattr(overlay, "evaluate_kr_market_state")
    assert not hasattr(overlay, "_index_returns")
    assert not hasattr(overlay, "apply_kr_market_state_to_budget")


def test_sell_helpers_remain_available_without_regime_evaluator():
    base={"market_state":"KR_NORMAL","data_quality":"OK","force_entry_block":False,"sector_exposure_pct":{},"portfolio_equity_krw":1_000_000,"gross_exposure_pct":0.0,"high_beta_exposure_pct":0.0}
    assert filter_kr_entry_intent({"side":"SELL","code":"005930"},base)["side"] == "SELL"
    assert isinstance(generate_kr_profit_capture_intents([],base),list)


def test_policy_missing_adoption_is_explicit_and_excludes_infinite():
    position = {
        "code": "010060", "qty": 20, "orderable_qty": 20,
        "avg_buy_price": 100.0, "last_price": 109.0,
        "entry_thesis": "POLICY_MISSING", "exit_policy_family": "POLICY_MISSING",
        "position_origin": "IMPORTED", "position_meta": {},
        "position_cycle_id": "cycle-adopt", "portfolio_epoch_id": "epoch-adopt",
    }
    adopted = build_kr_policy_missing_adoption(position, current_price=109.0)
    assert adopted is not None
    assert adopted["position_fields"]["exit_policy_family"] == "SWING_STAGED_EXIT"
    assert adopted["position_fields"]["policy_version"] == "kr_policy_missing_tp_adoption_v1"
    persisted = {**position, **adopted["position_fields"]}
    assert is_verified_kr_policy_missing_adoption(persisted)
    tampered = {**persisted, "entry_exit_plan_json": {
        **persisted["entry_exit_plan_json"],
        "profit_plan": {"tp1": {"return_fraction": 0.99, "sell_fraction": 1.0}},
    }}
    assert has_kr_policy_missing_adoption_claim(tampered)
    assert not is_verified_kr_policy_missing_adoption(tampered)
    assert build_kr_policy_missing_adoption({**position, "code": "122630"}, current_price=109.0) is None
    assert build_kr_policy_missing_adoption({**position, "owner_strategy": "KR_INFINITE"}, current_price=109.0) is None


def test_profit_capture_stages_are_sequential_and_fill_driven():
    base = {"market_state": "KR_NORMAL"}
    position = {
        "code": "010060", "qty": 20, "orderable_qty": 20,
        "unrealized_pnl_pct": 0.09, "position_meta": {},
    }
    first = generate_kr_profit_capture_intents([position], base)
    assert first[0]["reason"] == "KR_TAKE_PROFIT_TP1"
    assert first[0]["profit_capture_stage"] == "tp1"
    assert generate_kr_profit_capture_intents([
        {**position, "position_meta": {"kr_tp1_pending": True}}
    ], base) == []
    second = generate_kr_profit_capture_intents([
        {**position, "position_meta": {"kr_tp1_done": True}}
    ], base)
    assert second[0]["reason"] == "KR_TAKE_PROFIT_TP2"
    third = generate_kr_profit_capture_intents([
        {**position, "position_meta": {"kr_tp1_done": True, "kr_tp2_done": True}}
    ], base)
    assert third[0]["reason"] == "KR_TAKE_PROFIT_TP3"


def test_kr_profit_capture_reasons_keep_three_distinct_durable_stages():
    assert exit_stage_for_reason("KR_TAKE_PROFIT_TP1") == "TP1"
    assert exit_stage_for_reason("KR_TAKE_PROFIT_TP2") == "TP2"
    assert exit_stage_for_reason("KR_TAKE_PROFIT_TP3") == "TP3"
    assert legal_next_exit_stage("TP1", "TP2")
    assert legal_next_exit_stage("TP2", "TP3")
