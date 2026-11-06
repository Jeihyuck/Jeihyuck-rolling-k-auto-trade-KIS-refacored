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
        position_cycle_id="cycle1", runner_version=runner,
    )


def test_default_legacy_never_replaces_existing_v1(monkeypatch):
    monkeypatch.delenv("KR_SWING_US_PROFIT_MODE", raising=False)
    assert build_new_swing_parity_contract(
        strategy_owner="PB1", exit_policy_family="SWING_STAGED_EXIT",
        trade_horizon="SWING", position_action="NEW_POSITION_BUY",
        account_id="practice:12345678:01", trading_epoch_id="epoch1",
        position_cycle_id="cycle1", runner_version="US_V2",
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
            runner_version="US_V2",
        )


def test_active_practice_requires_approved_environment(monkeypatch):
    monkeypatch.setenv("KIS_ENV", "practice")
    with pytest.raises(ValueError, match="DEPLOY_NOT_APPROVED"):
        contract(monkeypatch, mode="ACTIVE_PRACTICE")
    monkeypatch.setenv("KR_SWING_US_PROFIT_DEPLOY_APPROVED", "1")
    assert verified_contract(contract(monkeypatch, mode="ACTIVE_PRACTICE"))
    monkeypatch.setenv("KIS_ENV", "real")
    with pytest.raises(ValueError, match="DEPLOY_NOT_APPROVED"):
        contract(monkeypatch, mode="ACTIVE_PRACTICE")


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
            runner_version="US_V2", existing_contract=c,
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
