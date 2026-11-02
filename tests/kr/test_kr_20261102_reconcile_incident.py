"""2026-11-02 KR: broker evidence and execution health are separate contracts.

Offline tests only. No broker POST, no Supabase mutation and no account secrets.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from trader.pb1_runner import _apply_kr_broker_reconcile_health
from trader.kr import broker_truth_hardening as truth


def test_failed_reconcile_counts_as_degraded_without_erasing_accepted_orders():
    metrics = {"degraded": False, "buy_orders": 1, "api_submitted": 1, "warning_counts": {}}
    payload = {"broker_truth_reconcile_failed": 1, "broker_truth_health_status": "RED"}
    result = _apply_kr_broker_reconcile_health(metrics, payload)
    assert result["degraded"] is True
    assert result["buy_orders"] == 1
    assert result["api_submitted"] == 1
    assert result["warning_counts"]["kr_broker_reconcile_degraded_count"] == 1


@pytest.mark.parametrize("payload", [
    {"broker_truth_reconcile_degraded": 1, "broker_truth_health_status": "RED"},
    {"broker_truth_reconcile_deferred": 1, "broker_truth_health_status": "RED"},
])
def test_reconciliation_pending_does_not_disappear_from_session_metrics(payload):
    metrics = _apply_kr_broker_reconcile_health({"degraded": False, "warning_counts": {}}, payload)
    assert metrics["degraded"] is True
    assert metrics["warning_counts"]["kr_broker_reconcile_degraded_count"] == 1


def test_reconciliation_ok_keeps_tick_health_clean():
    metrics = {"degraded": False, "warning_counts": {}}
    assert _apply_kr_broker_reconcile_health(
        metrics, {"broker_truth_reconcile_degraded": 0, "broker_truth_health_status": "OK"}
    ) == metrics
    assert metrics["warning_counts"] == {}


def test_post_tick_reconcile_failure_is_red_even_when_quantity_matches(monkeypatch):
    import trader.reconcile_kis as rk

    class DummyKis:
        def get_balance_cached(self, *, force):
            return {"output1": [], "output2": {"dnca_tot_amt": "1"}}

        def invalidate_balance_cache(self, **_kwargs):
            return None

    payload = {"api_submitted": 1, "accepted": 1}
    runner = SimpleNamespace(
        dry_run=False, intended_live=True, env="practice",
        engine=object(), kis=DummyKis(), _run_summary_payload=payload,
        _exit_summary_payload={}, _balance_snapshot=None,
        STRATEGY_NAME="pb1_pullback_close",
    )
    monkeypatch.setattr(truth, "kr_tick_remaining_sec", lambda: float("inf"))
    monkeypatch.setattr(rk, "reconcile_kis", lambda **_kw: {
        "ok": False, "reason": "reconcile_today_failed", "orders": 0,
        "fills": 0, "_final_holdings_rows": [],
    })
    monkeypatch.setattr(truth, "_recover_proven_policy_positions", lambda **_kw: {
        "recovered": [], "review_required": [],
    })
    monkeypatch.setattr(truth, "_health_after_reconcile", lambda **_kw: {
        "qty_mismatch_count": 0, "stale_open_order_count": 0,
    })

    truth._post_pb1_tick_reconcile(runner)
    assert payload["broker_truth_health_status"] == "RED"
    assert payload["broker_truth_reconcile_degraded"] == 1
    assert payload["broker_truth_reconcile_reason"] == "reconcile_today_failed"
    metrics = _apply_kr_broker_reconcile_health(
        {"degraded": False, "warning_counts": {}, "api_submitted": 1}, payload,
    )
    assert metrics["degraded"] is True
    assert metrics["api_submitted"] == 1



@pytest.mark.parametrize("side", ["BUY", "SELL"])
def test_verified_kis_order_cumulative_contract_is_authorized_and_idempotent(side, monkeypatch):
    from datetime import date
    from decimal import Decimal
    from trader import reconcile_kis
    from trader.settlement import kr_cutover
    from trader.settlement.core import preview_decision

    monkeypatch.setattr(kr_cutover, "_account_scope", lambda env: "offline-account-scope")
    proof = {"qty": 85, "notional": 85 * 23700, "avg_price": 23700}
    args = reconcile_kis._kr_atomic_execution_evidence(
        cumulative_sell_evidence=proof if side == "SELL" else None,
        cumulative_buy_evidence=proof if side == "BUY" else None,
        filled_price=23900,  # a raw KIS quote must not override verified cumulative avg
        filled_qty=85,
    )
    assert args == {
        "evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
        "execution_price": 23700,
        "execution_qty": None,
    }
    order = {
        "env": "practice", "strategy": "pb1_pullback_close",
        "trading_epoch_id": "offline-epoch", "position_cycle_id": "offline-cycle",
        "client_order_key": f"offline:{side}:test", "market": "KRX",
        "kis_odno": "0000011233", "side": side,
    }
    observation = kr_cutover.build_kr_broker_observation(
        order=order, request_json={},
        broker_trade_date=date(2026, 11, 2),
        requested_qty=85, cumulative_qty=85,
        evidence_digest="offline-single-aggregate", **args,
    )
    first = preview_decision(observation)
    assert first.qty_delta == 85
    assert first.notional_delta == Decimal(85 * 23700)
    replay = preview_decision(
        observation, applied_qty=85,
        applied_notional=Decimal(85 * 23700), priced_qty=85,
        price_status="CONFIRMED",
    )
    assert replay.qty_delta == 0
    assert replay.notional_delta == 0


def test_unverified_cumulative_price_is_not_promoted_to_atomic_settlement():
    from trader import reconcile_kis
    with pytest.raises(ValueError, match="KR_ATOMIC_CUMULATIVE_PRICE_PROOF_INVALID"):
        reconcile_kis._kr_atomic_execution_evidence(
            cumulative_sell_evidence={"qty": 85, "notional": 0, "avg_price": 23700},
            cumulative_buy_evidence=None,
            filled_price=23700, filled_qty=85,
        )


def test_actual_individual_execution_keeps_individual_quantity():
    from trader import reconcile_kis
    args = reconcile_kis._kr_atomic_execution_evidence(
        cumulative_sell_evidence=None, cumulative_buy_evidence=None,
        filled_price=23700, filled_qty=2,
    )
    assert args == {
        "evidence_type": "KIS_EXECUTION_ACTUAL",
        "execution_price": 23700,
        "execution_qty": 2,
    }
