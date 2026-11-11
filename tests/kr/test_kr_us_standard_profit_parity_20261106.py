"""KR parity TP candidate math vs original US_STANDARD quantity resolver.

No KIS order is sent; this is intentionally a pure-policy shadow test.
"""
import copy

import pytest

from trader.kr.us_standard_profit_parity import (
    build_new_swing_parity_contract,
    calculate_next_tp_stage,
    verified_contract,
)
from trader.us.market_state_overlay import resolve_profit_capture_stage_quantity


def contract(monkeypatch, mode="SHADOW", runner="US_V2"):
    monkeypatch.setenv("KR_SWING_US_PROFIT_MODE", mode)
    return build_new_swing_parity_contract(
        strategy_owner="PB1", exit_policy_family="SWING_STAGED_EXIT",
        trade_horizon="SWING", position_action="NEW_POSITION_BUY",
        account_id="practice:12345678:01", trading_epoch_id="epoch1",
        position_cycle_id="cycle1", runner_version=runner, code="005930",
    )


def test_explicit_legacy_never_replaces_existing_v1(monkeypatch):
    monkeypatch.setenv("KR_SWING_US_PROFIT_MODE", "LEGACY")
    assert build_new_swing_parity_contract(
        strategy_owner="PB1", exit_policy_family="SWING_STAGED_EXIT",
        trade_horizon="SWING", position_action="NEW_POSITION_BUY",
        account_id="practice:12345678:01", trading_epoch_id="epoch1",
        position_cycle_id="cycle1", runner_version="US_V2", code="005930",
    ) is None


def test_us_100_share_25_18_11_runner_46(monkeypatch):
    c = contract(monkeypatch)
    assert verified_contract(c)
    states = {}
    holding = 100
    for stage, gain, fraction, expected in (
        ("TP1", .03, .25, 25), ("TP2", .05, .25, 18),
        ("TP3", .08, .20, 11),
    ):
        out = calculate_next_tp_stage(
            contract=c, stage_states=states, holding_qty=holding,
            orderable_qty=holding, verified_entry_price=100,
            executable_price=100 * (1 + gain),
        )
        original = resolve_profit_capture_stage_quantity(
            holding_qty=holding, sell_fraction=fraction,
            runner_min_remain_pct=.40,
        )
        assert out.action == "SHADOW_TP" and not out.order_authorized
        assert out.stage == stage
        assert out.target_qty == original == expected
        states[stage] = {"status": "FILLED", "broker_fill_proven": True}
        holding -= expected
    assert holding == 46
    assert calculate_next_tp_stage(
        contract=c, stage_states=states, holding_qty=holding,
        orderable_qty=holding, verified_entry_price=100,
        executable_price=109,
    ).action == "RUNNER"


@pytest.mark.parametrize("q", [33, 30, 5, 2, 1])
def test_us_quantity_matches_on_small_positions(monkeypatch, q):
    c = contract(monkeypatch)
    result = calculate_next_tp_stage(
        contract=c, stage_states={}, holding_qty=q, orderable_qty=q,
        verified_entry_price=100, executable_price=109,
    )
    expected = resolve_profit_capture_stage_quantity(
        holding_qty=q, sell_fraction=.25, runner_min_remain_pct=.40,
    )
    assert result.target_qty == expected
    assert not result.order_authorized


def test_price_threshold_and_frozen_stage_sequence(monkeypatch):
    c = contract(monkeypatch)
    assert calculate_next_tp_stage(
        contract=c, stage_states={}, holding_qty=100, orderable_qty=100,
        verified_entry_price=100, executable_price=102.999,
    ).action == "HOLD"
    one = calculate_next_tp_stage(
        contract=c, stage_states={}, holding_qty=100, orderable_qty=100,
        verified_entry_price=100, executable_price=109,
    )
    assert one.stage == "TP1"
    block = calculate_next_tp_stage(
        contract=c, stage_states={"TP1": {"status": "ACK"}},
        holding_qty=100, orderable_qty=100,
        verified_entry_price=100, executable_price=109,
    )
    assert block.action == "BLOCK"
    assert calculate_next_tp_stage(
        contract=c, stage_states={"TP1": {"status": "FILLED"}},
        holding_qty=75, orderable_qty=75,
        verified_entry_price=100, executable_price=109,
    ).action == "BLOCK"


@pytest.mark.parametrize("owner,family,horizon", [
    ("KR_INFINITE", "SWING_STAGED_EXIT", "SWING"),
    ("US_STANDARD", "SWING_STAGED_EXIT", "SWING"),
    ("PB1", "INTRADAY_PROFIT_PROTECT", "DAY_TRADE"),
    ("PB1", "CORE_TREND_FOLLOW", "CORE"),
])
def test_separate_owner_and_horizon_never_inherit(monkeypatch, owner, family, horizon):
    monkeypatch.setenv("KR_SWING_US_PROFIT_MODE", "SHADOW")
    with pytest.raises(ValueError, match="OWNER_OR_HORIZON"):
        build_new_swing_parity_contract(
            strategy_owner=owner, exit_policy_family=family,
            trade_horizon=horizon, position_action="NEW_POSITION_BUY",
            account_id="acc", trading_epoch_id="epoch", position_cycle_id="cycle",
            runner_version="US_V2", code="005930",
        )


def test_active_practice_builds_signed_candidate_not_broker_permission(monkeypatch):
    monkeypatch.setenv("KIS_ENV", "practice")
    c = contract(monkeypatch, mode="ACTIVE_PRACTICE")
    assert verified_contract(c)
    assert c["activation_mode"] == "ACTIVE_PRACTICE"
    out = calculate_next_tp_stage(
        contract=c, stage_states={}, holding_qty=100, orderable_qty=100,
        verified_entry_price=100, executable_price=109,
    )
    assert out.action == "ACTIVE_TP" and out.stage == "TP1"
    assert out.target_qty == 25 and out.order_authorized is False
    monkeypatch.setenv("KIS_ENV", "real")
    with pytest.raises(ValueError, match="DEPLOY_NOT_APPROVED"):
        contract(monkeypatch, mode="ACTIVE_PRACTICE")


def test_practice_default_is_active_but_real_default_is_legacy(monkeypatch):
    monkeypatch.delenv("KR_SWING_US_PROFIT_MODE", raising=False)
    monkeypatch.setenv("KIS_ENV", "practice")
    assert verified_contract(build_new_swing_parity_contract(
        strategy_owner="PB1", exit_policy_family="SWING_STAGED_EXIT",
        trade_horizon="SWING", position_action="NEW_POSITION_BUY",
        account_id="practice:account", trading_epoch_id="epoch",
        position_cycle_id="cycle", runner_version="US_V3", code="005930",
    ))
    monkeypatch.setenv("KIS_ENV", "real")
    assert build_new_swing_parity_contract(
        strategy_owner="PB1", exit_policy_family="SWING_STAGED_EXIT",
        trade_horizon="SWING", position_action="NEW_POSITION_BUY",
        account_id="real:account", trading_epoch_id="epoch",
        position_cycle_id="cycle", runner_version="US_V3", code="005930",
    ) is None


def test_signed_contract_cannot_be_rewritten_or_used_as_order_authorization(monkeypatch):
    c = contract(monkeypatch, runner="US_V3")
    assert verified_contract(c)
    corrupted = copy.deepcopy(c)
    corrupted["runner_min_remain_pct"] = 0
    assert not verified_contract(corrupted)
    assert calculate_next_tp_stage(
        contract=corrupted, stage_states={}, holding_qty=100, orderable_qty=100,
        verified_entry_price=100, executable_price=109,
    ).action == "BLOCK"
    with pytest.raises(ValueError, match="ALREADY_EXISTS"):
        build_new_swing_parity_contract(
            strategy_owner="PB1", exit_policy_family="SWING_STAGED_EXIT",
            trade_horizon="SWING", position_action="NEW_POSITION_BUY",
            account_id="acc", trading_epoch_id="epoch", position_cycle_id="cycle",
            runner_version="US_V2", code="005930", existing_contract=c,
        )


def test_unknown_ack_or_missing_quote_blocks_without_any_broker_intent(monkeypatch):
    c = contract(monkeypatch)
    out = calculate_next_tp_stage(
        contract=c, stage_states={}, holding_qty=100, orderable_qty=100,
        verified_entry_price=100, executable_price=110,
        unresolved_sell=True,
    )
    assert out.action == "BLOCK" and not out.order_authorized
    assert calculate_next_tp_stage(
        contract=c, stage_states={}, holding_qty=100, orderable_qty=100,
        verified_entry_price=0, executable_price=110,
    ).action == "BLOCK"


def _v3_input(monkeypatch):
    from trader.kr.us_standard_profit_parity import evaluate_v3_runner_shadow
    c = contract(monkeypatch, runner="US_V3")
    stages = {
        stage: {"status": "FILLED", "broker_fill_proven": True}
        for stage in ("TP1", "TP2", "TP3")
    }
    baseline = dict(
        contract=c, stage_states=stages,
        verified_entry_price=100.0, verified_high=110.0,
        executable_price=108.0, holding_qty=46, orderable_qty=46,
        high_verified=True, trend_state="TRIM", trend_verified=True,
        atr_evidence={"valid": True, "atr14": 0.5, "stop": 105.0},
        partial_stage_status="NOT_TRIGGERED", final_stage_status="NOT_TRIGGERED",
    )
    return evaluate_v3_runner_shadow, baseline


def test_exact_us_v3_partial_runner_shadow_never_submits(monkeypatch):
    evaluate, params = _v3_input(monkeypatch)
    result = evaluate(**params)
    assert result["action"] == "SHADOW_RUNNER"
    assert result["source_policy_action"] == "SELL"
    assert result["stage"] == "PROFIT_DEFENSE_PARTIAL"
    assert result["qty"] == 23
    assert result["order_authorized"] is False


def test_us_v3_final_shadow_respects_unresolved_partial_and_full_holding(monkeypatch):
    evaluate, params = _v3_input(monkeypatch)
    params["trend_state"] = "EXIT"
    assert evaluate(**params)["stage"] == "RUNNER_FINAL"
    params["partial_stage_status"] = "ACK"
    result = evaluate(**params)
    assert result["action"] == "HOLD"
    assert result["reason"] == "PARTIAL_ORDER_UNSETTLED_FINAL_REQUIRES_RECONCILE"
    params["partial_stage_status"] = "NOT_TRIGGERED"
    params["orderable_qty"] = 40
    result = evaluate(**params)
    assert result["action"] == "HOLD"
    assert result["reason"] == "RUNNER_FINAL_RESERVED_INVENTORY_RECONCILE_REQUIRED"


def test_runner_requires_completed_tp_and_verified_high(monkeypatch):
    evaluate, params = _v3_input(monkeypatch)
    params["stage_states"]["TP3"] = {"status": "ACK"}
    assert evaluate(**params)["action"] == "BLOCK"
    params["stage_states"]["TP3"] = {"status": "FILLED", "broker_fill_proven": True}
    params["high_verified"] = False
    assert evaluate(**params)["action"] == "HOLD"
    params["high_verified"] = True
    params["unresolved_sell"] = True
    assert evaluate(**params)["action"] == "BLOCK"


@pytest.mark.parametrize("value,expected", [
    ("100", "SHADOW_TP"),
    (100, "SHADOW_TP"),
    ("2.0", "BLOCK"),
    (2.5, "BLOCK"),
    (True, "BLOCK"),
    (-1, "BLOCK"),
])
def test_verified_kis_orderable_quantity_never_rounds(value, expected, monkeypatch):
    c = contract(monkeypatch)
    out = calculate_next_tp_stage(
        contract=c, stage_states={}, holding_qty=value, orderable_qty=value,
        verified_entry_price=100, executable_price=103,
    )
    assert out.action == expected


@pytest.mark.parametrize("status", ["REJECTED", "CANCELLED", "UNRESOLVED_ACK"])
def test_failed_or_ambiguous_stage_requires_exact_broker_recheck(status, monkeypatch):
    c = contract(monkeypatch)
    result = calculate_next_tp_stage(
        contract=c, stage_states={"TP1": {"status": status}},
        holding_qty=100, orderable_qty=100,
        verified_entry_price=100, executable_price=108,
    )
    assert result.action == "BLOCK"
    assert result.order_authorized is False


def test_frozen_contract_horizon_and_lifecycle_tamper_blocks(monkeypatch):
    import hashlib
    import json

    c = contract(monkeypatch)
    c["position_cycle_id"] = ""
    payload = dict(c)
    payload.pop("sha256")
    c["sha256"] = hashlib.sha256(json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    ).encode("utf-8")).hexdigest()
    assert not verified_contract(c)
