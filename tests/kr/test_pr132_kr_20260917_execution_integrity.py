from __future__ import annotations

from datetime import date
from decimal import Decimal
import json
from types import SimpleNamespace

import pytest
import sqlalchemy as sa

from trader.db.repos import OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.kr.runtime_integrity_20260917 import (
    _adoption_verification_reasons,
    _install_kr_buy_json_roundtrip_guard,
    _json_object,
    _minimum_preexisting_age,
)


def _engine():
    engine = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(engine).metadata.create_all(engine)
    return engine


def _valid_buy_contract_dict():
    import trader.db.repos as repos

    _install_kr_buy_json_roundtrip_guard()
    request = {
        "enforce_entry_contract": True,
        "entry_exit_plan": {
            "policy_version": "pb1_entry_exit_plan_v1",
            "exit_policy_family": "SWING_STAGED_EXIT",
        },
        "pre_order_holding_qty": 0,
        "requested_qty": 1,
        "submitted_qty": 1,
        "balance_snapshot_id": "balance-1",
        "client_order_key": "kr-buy-contract-test",
        "position_cycle_id": "cycle-1",
        "portfolio_epoch_id": "epoch-1",
        "entry_contract_version": repos.KR_BUY_ENTRY_CONTRACT_VERSION,
    }
    request["entry_contract_sha256"] = repos._kr_buy_entry_contract_hash(request)
    return request


def test_kr_buy_contract_accepts_dict_and_json_string_readback():
    import trader.db.repos as repos

    request = _valid_buy_contract_dict()
    repos._assert_kr_buy_entry_contract(request)
    repos._assert_kr_buy_entry_contract(json.dumps(request, ensure_ascii=False))
    repos._assert_kr_buy_entry_contract(json.dumps(request).encode("utf-8"))


def test_kr_buy_contract_malformed_json_and_hash_mismatch_fail_closed():
    import trader.db.repos as repos

    _install_kr_buy_json_roundtrip_guard()
    with pytest.raises(RuntimeError, match="KR_BUY_ENTRY_CONTRACT_NOT_JSON"):
        repos._assert_kr_buy_entry_contract("{bad-json")

    request = _valid_buy_contract_dict()
    request["entry_contract_sha256"] = "tampered"
    with pytest.raises(RuntimeError, match="entry_contract_sha256"):
        repos._assert_kr_buy_entry_contract(json.dumps(request))


def test_pb1_buy_persistence_completes_before_broker_submit_boundary():
    _install_kr_buy_json_roundtrip_guard()
    engine = _engine()
    repo = OrdersRepo(engine)
    broker_submit_calls: list[str] = []

    order_id, created = repo.create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close", sid=1, mode=1,
        code="000660", market="KOSPI", side="BUY", ord_type="LIMIT", qty=1,
        limit_price=1_000_000, stage="PB1-ENTRY", client_order_key="pb1-buy-submit-boundary",
        request_json={
            "enforce_entry_contract": True,
            "entry_exit_plan": {"policy_version": "pb1_entry_exit_plan_v1", "exit_policy_family": "SWING_STAGED_EXIT"},
            "pre_order_holding_qty": 0,
            "requested_qty": 1,
            "submitted_qty": 1,
            "balance_snapshot_id": "balance-1",
        },
        account_id="acct",
    )
    assert created and order_id
    persisted = repo.get_order_by_client_order_key("practice", "pb1-buy-submit-boundary")
    assert persisted and persisted["request_json"]["entry_contract_sha256"]

    # This represents the next PB1 boundary after durable read-back.  The Sep-17
    # defect raised before this point, so reaching it is the regression contract.
    broker_submit_calls.append("ORDER_API_CALL_START")
    assert broker_submit_calls == ["ORDER_API_CALL_START"]


def _persisted_adoption_position(*, json_string_roundtrip: bool = False):
    import trader.kr.market_state_overlay as overlay

    engine = _engine()
    repo = PositionsRepo(engine)
    row, _ = repo.get_or_create_imported_cycle_for_kis_holding(
        env="practice", strategy="pb1_pullback_close", account_id="acct",
        sid=1, mode=1, code="005830", market="KOSPI", qty=10, avg_price=100.0,
    )
    adoption = overlay.build_kr_policy_missing_adoption(row, current_price=110.0)
    assert adoption is not None
    repo.update_position_fields(
        env="practice", strategy="pb1_pullback_close", sid=1, mode=1, code="005830",
        fields=adoption["position_fields"],
        position_cycle_id=str(row["position_cycle_id"]),
        portfolio_epoch_id=str(row["portfolio_epoch_id"]),
    )
    persisted = repo.get_position(
        env="practice", strategy="pb1_pullback_close", sid=1, mode=1, code="005830",
        position_cycle_id=str(row["position_cycle_id"]),
        portfolio_epoch_id=str(row["portfolio_epoch_id"]),
    )
    assert persisted
    if json_string_roundtrip:
        persisted = dict(persisted)
        persisted["entry_exit_plan_json"] = json.dumps(persisted["entry_exit_plan_json"], ensure_ascii=False)
        persisted["position_meta"] = json.dumps(persisted["position_meta"], ensure_ascii=False)
    return persisted


def test_policy_missing_adoption_persists_reads_back_and_verifies_after_json_string_roundtrip():
    import trader.kr.runtime_integrity_20260917 as integrity
    import trader.kr.market_state_overlay as overlay

    integrity._install_policy_adoption_roundtrip_guard()
    persisted = _persisted_adoption_position(json_string_roundtrip=True)
    reasons, normalized = _adoption_verification_reasons(persisted)
    assert reasons == []
    assert overlay.is_verified_kr_policy_missing_adoption(persisted)
    assert normalized["exit_policy_family"] == "SWING_STAGED_EXIT"


def test_policy_missing_adoption_tp_is_sequential_and_fill_driven():
    import trader.kr.runtime_integrity_20260917 as integrity
    import trader.kr.market_state_overlay as overlay

    integrity._install_policy_adoption_roundtrip_guard()
    persisted = _persisted_adoption_position()
    persisted = {**persisted, "unrealized_pnl_pct": 0.10, "orderable_qty": 10, "qty": 10}
    intents = overlay.generate_kr_profit_capture_intents([persisted], {"market_state": "KR_NORMAL"})
    assert intents and intents[0]["reason"] == "KR_TAKE_PROFIT_TP1"

    meta = dict(persisted["position_meta"])
    meta["kr_tp1_done"] = True
    persisted["position_meta"] = meta
    intents = overlay.generate_kr_profit_capture_intents([persisted], {"market_state": "KR_NORMAL"})
    assert intents and intents[0]["reason"] == "KR_TAKE_PROFIT_TP2"

    meta["kr_tp2_done"] = True
    persisted["position_meta"] = meta
    intents = overlay.generate_kr_profit_capture_intents([persisted], {"market_state": "KR_NORMAL"})
    assert intents and intents[0]["reason"] == "KR_TAKE_PROFIT_TP3"


def test_policy_missing_adoption_tamper_keeps_fail_closed_contract():
    import trader.kr.runtime_integrity_20260917 as integrity
    import trader.kr.market_state_overlay as overlay

    integrity._install_policy_adoption_roundtrip_guard()
    persisted = _persisted_adoption_position()
    plan = dict(persisted["entry_exit_plan_json"])
    contract = dict(plan["policy_adoption_contract"])
    contract["sha256"] = "tampered"
    plan["policy_adoption_contract"] = contract
    persisted["entry_exit_plan_json"] = plan
    assert not overlay.is_verified_kr_policy_missing_adoption(persisted)
    assert overlay.has_kr_policy_missing_adoption_claim(persisted)


def test_legacy_missing_entry_date_is_not_treated_as_same_day_but_real_today_buy_is():
    legacy = SimpleNamespace(
        code="039030", entry_date=None, holding_qty=5,
        days_held=0, trading_days_held=0, calendar_days_held=0, holding_bars=0,
        position_meta={"holding_age_unknown": True, "position_origin": "IMPORTED", "import_source": "KIS_HOLDING"},
    )
    _minimum_preexisting_age(legacy)
    assert legacy.days_held >= 1
    assert legacy.trading_days_held >= 1
    assert legacy.position_meta["preexisting_position_without_entry_fill"] is True

    same_day = SimpleNamespace(
        code="000660", entry_date="2026-09-17", holding_qty=1,
        days_held=0, trading_days_held=0, calendar_days_held=0, holding_bars=0,
        position_meta={"position_origin": "ORDER_FILL"},
    )
    _minimum_preexisting_age(same_day)
    assert same_day.days_held == 0
    assert "preexisting_position_without_entry_fill" not in same_day.position_meta

    adopted = SimpleNamespace(
        code="069510", entry_date=None, holding_qty=4,
        days_held=0, trading_days_held=0, calendar_days_held=0, holding_bars=0,
        position_meta={"policy_adopted": True, "policy_adopted_from": "POLICY_MISSING"},
    )
    _minimum_preexisting_age(adopted)
    assert adopted.days_held >= 1


def test_122630_ownership_remains_kr_infinite_only():
    from trader.pb1_engine import enforce_kr_order_ownership

    assert enforce_kr_order_ownership("122630", "KR_STANDARD") == (False, "KR_INF_OWNERSHIP_RESERVED")
    assert enforce_kr_order_ownership("122630", "KR_INFINITE") == (True, None)


def test_kr_infinite_mixed_money_types_buy_and_sell_reconcile_without_type_error():
    from trader.kr.infinite.accounting import apply_confirmed_fill
    from trader.kr.infinite.models import BrokerOrderState, OrderIntent, State, Status

    state = State(
        cycle_id="cycle", allocated_capital_krw=10_000_000, unit_krw=500_000,
        core_filled_notional=1_000_000, units_used=2, core_units_used=2,
        status=Status.ACTIVE, metadata={"pending_profit_stage": "TP2_SUBMITTED"},
    )
    sell_intent = OrderIntent(
        id=1, cycle_id="cycle", trade_date=date(2026, 9, 17), side="SELL_ALL",
        idempotency_key="sell", requested_qty=3, filled_qty=0,
        filled_notional_krw=Decimal("0"),
    )
    sell_broker = BrokerOrderState(status="FILLED", filled_qty=3, filled_notional_krw=450000.0)
    after_sell, qty, notional = apply_confirmed_fill(state, sell_intent, sell_broker, date(2026, 9, 17))
    assert qty == 3 and notional == 450000.0
    assert after_sell.metadata["pending_profit_stage"] is None
    assert after_sell.metadata["profit_stage"] == "TP2_FILLED"

    buy_intent = OrderIntent(
        id=2, cycle_id="cycle", trade_date=date(2026, 9, 17), side="BUY",
        idempotency_key="buy", requested_qty=2, filled_qty=0,
        filled_notional_krw=0.0, unit_sequence=3,
    )
    buy_broker = BrokerOrderState(status="FILLED", filled_qty=2, filled_notional_krw=Decimal("300000"))
    after_buy, qty, notional = apply_confirmed_fill(state, buy_intent, buy_broker, date(2026, 9, 17))
    assert qty == 2 and notional == 300000.0
    assert after_buy.units_used == 3


@pytest.mark.parametrize("broker_value,intent_value", [
    (0.0, Decimal("0")),
    (Decimal("0"), 0.0),
    (None, Decimal("0")),
])
def test_kr_infinite_zero_none_money_values_are_safe(broker_value, intent_value):
    from trader.kr.infinite.accounting import apply_confirmed_fill
    from trader.kr.infinite.models import BrokerOrderState, OrderIntent, State

    state = State(metadata={})
    intent = OrderIntent(
        id=1, cycle_id="c", trade_date=date(2026, 9, 17), side="BUY",
        idempotency_key="k", requested_qty=1, filled_notional_krw=intent_value,
    )
    broker = BrokerOrderState(status="OPEN", filled_qty=0, filled_notional_krw=broker_value)
    unchanged, qty, notional = apply_confirmed_fill(state, intent, broker, date(2026, 9, 17))
    assert unchanged == state and qty == 0 and notional == 0.0
