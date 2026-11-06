"""Normal KIS holdings must not be confused with unhealthy broker settlement."""
import pytest

from trader.kr.reconcile_guard_classification import broker_reconcile_guard_blocks
from trader.kr.historical_recovery_journal import (
    RecoveryLane, current_tick_reconcile_proves_buy_safe,
)


@pytest.mark.parametrize("proof,expected", [
    ({"guard_reason": None, "holdings": 3}, False),
    ({"guard_reason": "kis_has_holdings", "holdings": 3}, False),
    ({"guard_reason": "kis_has_holdings", "holdings": 0}, True),
    ({"guard_reason": "kis_has_holdings"}, True),
    ({"guard_reason": "kis_has_holdings", "holdings": 3, "holdings_error": "AUTH"}, True),
    ({"guard_reason": "kis_error", "holdings": 3}, True),
    ({"guard_reason": "holdings_error", "holdings": 3}, True),
    ({"guard_reason": "unresolved_broker_activity", "holdings": 3}, True),
    ({"guard_reason": "recent_orders_or_fills", "holdings": 0}, True),
    ({"guard_reason": "empty_streak_insufficient", "holdings": 0}, True),
    ({"guard_reason": "UNKNOWN_SENTINEL", "holdings": 3}, True),
    ({"guard_reason": "kis_has_holdings", "holdings": True}, True),
])
def test_stale_purge_reason_is_not_broker_integrity_failure(proof, expected):
    assert broker_reconcile_guard_blocks(proof) is expected


def test_current_tick_allows_only_healthy_claims_and_normal_holdings():
    lane = RecoveryLane("CURRENT_ONLY", job_key="fixture", reason="full_proof")
    proof = {
        "ok": True,
        "guard_reason": "kis_has_holdings",
        "holdings": 3,
        "holdings_error": None,
        "historical_repair_fastpath": True,
        "historical_recovery_executed": False,
        "frozen_contract_invariant_violations": 0,
        "unresolved_broker_activity": False,
        "degraded": None,
    }
    assert current_tick_reconcile_proves_buy_safe(
        lane=lane, result=proof, durable_record_ok=True
    )
    for change in (
        {"degraded": "KIS_DAILY_CCLD_TIMEOUT"},
        {"ok": False},
        {"unresolved_broker_activity": True},
        {"frozen_contract_invariant_violations": 1},
        {"guard_reason": "unresolved_broker_activity"},
        {"holdings": 0},
        {"historical_repair_fastpath": False},
    ):
        assert not current_tick_reconcile_proves_buy_safe(
            lane=lane, result={**proof, **change}, durable_record_ok=True
        ), change


def test_full_historical_lane_requires_current_reconcile_and_frozen_contract():
    lane = RecoveryLane("FULL", job_key="fixture", fencing_token="nonce")
    result = {
        "ok": True, "guard_reason": "kis_has_holdings",
        "holdings": 2, "historical_recovery_executed": True,
        "frozen_contract_invariant_violations": 0,
    }
    assert current_tick_reconcile_proves_buy_safe(
        lane=lane, result=result, durable_record_ok=True
    )
    assert not current_tick_reconcile_proves_buy_safe(
        lane=lane, result={**result, "holdings_error": "KIS_AUTH_FAILURE"},
        durable_record_ok=True,
    )
