"""KR historical recovery lanes: explicit opt-in, durable proof or fail-closed."""

import os
from datetime import date
from types import SimpleNamespace

import pytest

from trader.kr.historical_recovery_journal import (
    RecoveryLane, choose_historical_lane, lane_enabled,
    record_historical_result,
)


class UnexpectedEngine:
    @property
    def dialect(self):
        raise AssertionError("Disabled lane must not touch the DB")


def test_default_legacy_no_extra_db_or_broker_io(monkeypatch):
    monkeypatch.delenv("KR_HISTORICAL_RECOVERY_ONCE_ENABLED", raising=False)
    monkeypatch.setenv("KIS_ENV", "practice")
    assert not lane_enabled("practice")
    lane = choose_historical_lane(
        engine=UnexpectedEngine(), kis=object(), env="practice",
        strategy="pb1_pullback_close", trading_date=date(2026, 11, 6),
    )
    assert lane.mode == "LEGACY"
    assert lane.historical_recovery


def test_no_us_or_real_money_activation(monkeypatch):
    monkeypatch.setenv("KR_HISTORICAL_RECOVERY_ONCE_ENABLED", "1")
    monkeypatch.setenv("KIS_ENV", "real")
    assert not lane_enabled("practice")
    monkeypatch.setenv("KIS_ENV", "practice")
    assert not lane_enabled("real")
    assert lane_enabled("practice")


@pytest.mark.parametrize("mode,historical,can_buy", [
    ("FULL", True, True),
    ("LEGACY", True, True),
    ("CURRENT_ONLY", False, True),
    ("CURRENT_ONLY_BLOCK", False, False),
])
def test_state_semantics_never_treat_incomplete_recovery_as_buy_proof(
    mode, historical, can_buy,
):
    lane = RecoveryLane(mode, job_key="key", fencing_token="nonce")
    assert lane.historical_recovery is historical
    assert lane.allow_buy_from_recovery is can_buy


def test_only_claimed_full_recovery_can_persist_terminal_state():
    assert not record_historical_result(
        engine=UnexpectedEngine(),
        lane=RecoveryLane("FULL"),
        result={"ok": True, "historical_repair_fastpath": True},
    )
    assert not record_historical_result(
        engine=UnexpectedEngine(),
        lane=RecoveryLane("CURRENT_ONLY_BLOCK"),
        result={"ok": True},
    )


def test_read_only_current_lane_is_not_implicitly_historical_complete():
    assert record_historical_result(
        engine=UnexpectedEngine(),
        lane=RecoveryLane("CURRENT_ONLY"),
        result={"ok": True},
    )


@pytest.mark.parametrize("lane_name,full", [("FULL", True), ("CURRENT_ONLY", False)])
def test_pr326_buy_requires_both_durable_history_and_fresh_current_tick(lane_name, full):
    from trader.kr.historical_recovery_journal import current_tick_reconcile_proves_buy_safe

    lane = RecoveryLane(lane_name, "journal-key", "nonce")
    proof = {
        "ok": True,
        "historical_recovery_executed": full,
        "historical_repair_fastpath": True,
        "frozen_contract_invariant_violations": 0,
        "guard_reason": None,
        "degraded": None,
        "unresolved_broker_activity": False,
    }
    assert current_tick_reconcile_proves_buy_safe(
        lane=lane, result=proof, durable_record_ok=True,
    )
    assert not current_tick_reconcile_proves_buy_safe(
        lane=lane, result=proof, durable_record_ok=False,
    )
    assert not current_tick_reconcile_proves_buy_safe(
        lane=lane, result=None, durable_record_ok=True,
    )
    for change in (
        {"ok": False},
        {"degraded": "KIS_DAILY_CCLD_TIMEOUT"},
        {"guard_reason": "UNRESOLVED_ACCOUNT"},
        {"unresolved_broker_activity": True},
        {"frozen_contract_invariant_violations": 1},
        {"historical_recovery_executed": not full},
    ):
        assert not current_tick_reconcile_proves_buy_safe(
            lane=lane, result={**proof, **change}, durable_record_ok=True,
        ), change
    if not full:
        assert not current_tick_reconcile_proves_buy_safe(
            lane=lane, result={**proof, "historical_repair_fastpath": False},
            durable_record_ok=True,
        )


def test_runner_pre_and_post_sell_paths_use_current_tick_reconciliation_gate():
    import inspect
    from trader.pb1_runner import run_once

    source = inspect.getsource(run_once)
    assert "current_tick_reconcile_proves_buy_safe(" in source
    assert "if not _pre_verified:" in source
    assert "if not _post_verified:" in source
    assert "_kr_post_exit_reconcile_ok = False" in source


def test_retired_september_account_orders_are_not_october_account_blockers():
    """2026-11-06 retirement: only proven foreign Epoch orders are ignored.

    The September practice account is ENDED, while an old ACKED order and
    open positions remain for audit. A missing/ambiguous owner must still
    block; the October live account must never inherit the old ACK.
    """
    from trader.kr.recovery_scope_20261104 import (
        KRRecoveryScope, relevant_open_orders_for_scope,
    )
    scope = KRRecoveryScope(
        env="practice",
        account_key="practice:50220451:01",
        trading_epoch_id="09772c87-27ab-412d-b1c7-0a4a55196462",
    )
    old_epoch = "3dc51d8e-61ce-414c-80a9-73f9b4911110"
    current = scope.trading_epoch_id
    orders = [
        {"order_id": "old-ack", "code": "028050", "env": "practice",
         "strategy": "pb1_pullback_close", "position_cycle_id": "old-cycle",
         "portfolio_epoch_id": "old-portfolio", "trading_epoch_id": old_epoch},
        {"order_id": "new-ack", "code": "122630", "env": "practice",
         "strategy": "pb1_pullback_close", "position_cycle_id": "new-cycle",
         "portfolio_epoch_id": "new-portfolio", "trading_epoch_id": current},
        {"order_id": "unowned-ack", "code": "293490", "env": "practice",
         "strategy": "pb1_pullback_close", "position_cycle_id": None,
         "portfolio_epoch_id": "", "trading_epoch_id": old_epoch},
    ]
    portfolios = {
        "old-portfolio": {
            "portfolio_epoch_id": "old-portfolio", "env": "practice",
            "account_id": "practice:50199084:01",
            "strategy": "pb1_pullback_close", "trading_epoch_id": old_epoch,
            "status": "ENDED",
        },
        "new-portfolio": {
            "portfolio_epoch_id": "new-portfolio", "env": "practice",
            "account_id": scope.account_key,
            "strategy": "pb1_pullback_close", "trading_epoch_id": current,
            "status": "ACTIVE",
        },
    }
    filtered = relevant_open_orders_for_scope(
        orders=orders, portfolios=portfolios, scope=scope,
    )
    assert [x["order_id"] for x in filtered] == ["new-ack", "unowned-ack"]
    # Only known old account/epoch evidence can be safely filtered;
    # an unscoped historical order is NOT proof of another account.
