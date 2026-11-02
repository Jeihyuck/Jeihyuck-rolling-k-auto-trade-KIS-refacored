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
