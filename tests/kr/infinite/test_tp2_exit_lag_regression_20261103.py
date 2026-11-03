"""KR Infinite regression: a completed TP2 may precede the balance update.

Synthetic states only; absolutely no broker POST or operational DB writes.
"""
from dataclasses import replace
from datetime import date, timedelta

from trader.kr.infinite.models import BrokerPosition, State, Status
from trader.kr.infinite.reconciliation import reconcile

DAY = date(2026, 10, 27)


def _cycle(stage: str) -> State:
    return State(
        status=Status.EXIT_PENDING,
        cycle_id="KRINF-20261026-eb6285bb",
        cycle_start_date=date(2026, 10, 26),
        allocated_capital_krw=30_000_000,
        unit_krw=750_000,
        core_filled_notional=740_795,
        units_used=1,
        core_units_used=1,
        metadata={"profit_stage": stage, "pending_profit_stage": None},
    )


def test_tp2_filled_stale_balance_then_zero_closes_without_frozen():
    state = _cycle("TP2_FILLED")
    # KIS may still report the pre-TP2 six shares even after the final sell.
    waiting, reason = reconcile(
        state, BrokerPosition(6, 6, 67345, 72090), DAY,
        pending_sell=False,
    )
    assert reason == "FINAL_PROFIT_FILL_BALANCE_PENDING"
    assert waiting.status is Status.EXIT_PENDING

    # The next authoritative broker balance becomes zero.
    closed, reason = reconcile(
        waiting, BrokerPosition(0, 0, 0, 72090),
        DAY + timedelta(days=1), pending_sell=False,
    )
    assert reason == "CYCLE_COMPLETE"
    assert closed.status is Status.COMPLETE
    assert closed.last_exit_date == DAY + timedelta(days=1)
    assert closed.cycle_complete_date == DAY + timedelta(days=1)


def test_final_exit_stage_with_residual_never_rearms_existing_cycle():
    for stage in ("TP2_FILLED", "CAPITAL_RECOVERY_FILLED"):
        state, reason = reconcile(
            _cycle(stage),
            BrokerPosition(6, 6, 67345, 72090),
            DAY,
        )
        assert state.status is Status.EXIT_PENDING
        assert reason == "FINAL_PROFIT_FILL_BALANCE_PENDING"


def test_tp1_filled_residual_still_rearms_for_existing_tp2_policy():
    for stage in ("TP1_FILLED", "TP1_DEFENSE_FILLED", "TP1_REBOUND_PENDING_FILLED"):
        state, reason = reconcile(
            _cycle(stage),
            BrokerPosition(6, 6, 67345, 72090),
            DAY,
        )
        assert state.status is Status.ACTIVE
        assert reason == "PARTIAL_PROFIT_FILLED_RESIDUAL"


def test_unconfirmed_final_sell_or_pending_sell_never_completes():
    pending = replace(
        _cycle("TP2_FILLED"),
        metadata={"profit_stage": "TP1_FILLED",
                  "pending_profit_stage": "TP2_SUBMITTED"},
    )
    waiting, reason = reconcile(
        pending, BrokerPosition(0, 0, 0, 72090), DAY,
        pending_sell=True,
    )
    assert waiting.status is Status.EXIT_PENDING
    assert reason == "SELL_RECONCILE_PENDING"


def test_persisted_frozen_state_is_not_automatically_unfrozen():
    frozen = replace(_cycle("TP2_FILLED"), status=Status.FROZEN)
    unchanged, reason = reconcile(
        frozen, BrokerPosition(0, 0, 0, 72090), DAY,
        pending_sell=False,
    )
    assert unchanged.status is Status.FROZEN
    assert reason == "OK"  # Only the separately proven repair may certify.
