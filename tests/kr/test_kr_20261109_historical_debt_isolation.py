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


def test_historical_open_orders_exclude_only_proven_foreign_epoch_or_account():
    """Same-account KR Infinite remains a distinct blocking order, not PB1."""
    from trader.kr.recovery_scope_20261104 import (
        KRRecoveryScope, relevant_open_orders_for_scope,
    )
    scope = KRRecoveryScope(
        env="practice", account_key="practice:account-a:paper",
        trading_epoch_id="epoch-active",
    )
    def portfolio(portfolio_id, *, account, epoch, strategy):
        return {
            "portfolio_epoch_id": portfolio_id,
            "env": "practice", "account_id": account,
            "trading_epoch_id": epoch, "strategy": strategy,
        }
    portfolios = {
        "pb1-own": portfolio(
            "pb1-own", account=scope.account_key,
            epoch="epoch-active", strategy="pb1_pullback_close",
        ),
        "infinite-own": portfolio(
            "infinite-own", account=scope.account_key,
            epoch="epoch-active", strategy="KR_INFINITE_V1",
        ),
        "other-account": portfolio(
            "other-account", account="practice:account-b:paper",
            epoch="epoch-active", strategy="pb1_pullback_close",
        ),
        "other-epoch": portfolio(
            "other-epoch", account=scope.account_key,
            epoch="epoch-prior", strategy="pb1_pullback_close",
        ),
    }
    def order(portfolio_id, *, code, epoch, owner):
        return {
            "order_id": "unique-" + portfolio_id,
            "env": "practice", "strategy": owner,
            "portfolio_epoch_id": portfolio_id,
            "position_cycle_id": "cycle-" + portfolio_id,
            "trading_epoch_id": epoch, "code": code,
            "status": "SUBMITTED",
        }
    rows = [
        order("pb1-own", code="093320", epoch="epoch-active",
              owner="pb1_pullback_close"),
        order("infinite-own", code="122630", epoch="epoch-active",
              owner="KR_INFINITE_V1"),
        order("other-account", code="093320", epoch="epoch-active",
              owner="pb1_pullback_close"),
        order("other-epoch", code="293490", epoch="epoch-prior",
              owner="pb1_pullback_close"),
        order("no-portfolio-proof", code="028050", epoch="epoch-prior",
              owner="pb1_pullback_close"),
    ]
    retained = relevant_open_orders_for_scope(
        orders=rows, portfolios=portfolios, scope=scope,
    )
    assert [item["portfolio_epoch_id"] for item in retained] == [
        "pb1-own", "infinite-own", "no-portfolio-proof",
    ]
    # Do not relabel 122630 as PB1 to unblock the historical BUY gate.
    assert retained[1]["strategy"] == "KR_INFINITE_V1"
