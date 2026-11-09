"""2026-11-09 KR: prior-date claim debt cannot veto unrelated current orders."""
from datetime import date

import pytest

from trader.reconcile_kis import _execution_claim_integrity_reason
from trader.kr.historical_recovery_journal import (
    RecoveryLane, current_tick_reconcile_proves_buy_safe,
)


def _health(*, prior=0, current=0, unknown=0):
    return {
        "available": True,
        "unresolved_execution_actions": prior + current + unknown,
        "unresolved_source_date_prior": prior,
        "unresolved_source_date_current": current,
        "unresolved_source_date_other_or_unknown": unknown,
    }


def test_kakao_old_uncertain_is_recorded_not_current_day_global_veto():
    assert _execution_claim_integrity_reason(_health(prior=1)) is None
    assert _execution_claim_integrity_reason(_health(prior=100)) is None


@pytest.mark.parametrize("health", [
    _health(prior=1, current=1),
    _health(prior=1, unknown=1),
    {"available": True, "unresolved_execution_actions": 1},
    {**_health(prior=1), "unresolved_execution_actions": 2},
])
def test_current_or_unproven_claims_continue_to_block(health):
    assert _execution_claim_integrity_reason(health) is not None


def test_current_order_needs_fresh_broker_exact_fences_after_history_unresolved():
    lane = RecoveryLane("CURRENT_ONLY", "journal", reason="HISTORICAL_UNRESOLVED")
    current = {
        "ok": True, "degraded": None, "historical_recovery_executed": False,
        "current_only_fresh_broker_proven": True, "historical_repair_fastpath": False,
        "holdings": 3, "guard_reason": "kis_has_holdings",
        "unresolved_broker_activity": False,
        "frozen_contract_invariant_violations": 0,
    }
    assert current_tick_reconcile_proves_buy_safe(
        lane=lane, result=current, durable_record_ok=True,
    )
    for bad in (
        {"current_only_fresh_broker_proven": False},
        {"unresolved_broker_activity": True},
        {"frozen_contract_invariant_violations": 1},
        {"degraded": "KIS_DAILY_CCLD_TIMEOUT"},
    ):
        assert not current_tick_reconcile_proves_buy_safe(
            lane=lane, result={**current, **bad}, durable_record_ok=True,
        )


def test_journal_corruption_is_not_permission_to_submit():
    lane = RecoveryLane("CURRENT_ONLY_BLOCK", "bad_journal", reason="JOURNAL_UNAVAILABLE")
    assert not current_tick_reconcile_proves_buy_safe(
        lane=lane,
        result={
            "ok": True, "historical_recovery_executed": False,
            "current_only_fresh_broker_proven": True,
            "frozen_contract_invariant_violations": 0,
        },
        durable_record_ok=True,
    )
