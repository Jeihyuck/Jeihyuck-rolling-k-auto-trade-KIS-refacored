"""US Close five-axis source separation; offline synthetic contract tests.

No broker API, DB writes, order intent, policy changes or WSL access.
"""
from __future__ import annotations

from copy import deepcopy

import pytest

from trader.us.runner.close_audit import audit_us_close_report
from trader.us.runner.close_five_axis_audit import build_us_close_five_axis_audit


def _report():
    return {
        "trade_date": "2026-11-19",
        "order_chain_verification": {
            "status": "AUDIT_INCOMPLETE",
            "order_count": 11,
            "kis_broker_chain_verified_count": 11,
            "broker_db_quantity_conflict_count": 0,
            "policy_verdict_counts": {"PENDING_EVIDENCE": 11},
        },
        "trade_pnl_analysis": {
            "realized_pnl_usd": -51.52,
            "realized_pnl_status": "VERIFIED",
            "trade_details": [
                *[
                    {"symbol": "HCA", "side": "SELL", "filled_qty": 1,
                     "pnl_source": "AUTHORITATIVE_KIS_FILL"}
                    for _ in range(10)
                ],
                {"symbol": "TQQQ", "side": "BUY", "filled_qty": 4},
            ],
        },
        "broker_recovery_health": {
            "available": True,
            "historical_recovery_unresolved": 2,
            "historical_tp_unresolved_count": 1,
            "execution_claim_source_date_breakdown_available": True,
            "unresolved_source_date_prior": 2,
        },
        "reconciliation_scope_diagnostics": {
            "current_recovery_health_available": True,
        },
        "runtime_liveness_failures": [],
        "close_integrity_status": "OK",
        "report_consistency": "OK",
    }


def test_11_verified_broker_fills_do_not_mislabel_11_missing_policies_or_history():
    result = build_us_close_five_axis_audit(_report())
    axis = result["axes"]
    assert len(axis) == 5
    assert axis["broker_execution"]["status"] == "VERIFIED"
    assert axis["broker_execution"]["verified_broker_chains"] == 11
    assert axis["buy_policy_and_strategy"]["status"] == "PENDING_EVIDENCE"
    assert axis["buy_policy_and_strategy"]["pending_5w1h_or_policy_count"] == 11
    assert axis["economic_settlement"]["status"] == "VERIFIED"
    assert axis["economic_settlement"]["authoritative_fill_pnl_sells"] == 10
    assert axis["historical_unresolved"]["status"] == "PENDING_EVIDENCE"
    assert axis["historical_unresolved"]["prior_recovery_unresolved"] == 2
    assert axis["runtime_and_stage4"]["status"] == (
        "REPORTED_NO_LIVENESS_FAILURE_NOT_READY_PROOF"
    )
    assert result["strict_status"] == "NOT_FULLY_VERIFIED"
    assert result["verified_axis_count"] == 2
    assert result["is_order_authorization"] is False
    assert result["is_kis_live_practice_verification"] is False


def test_close_audit_embeds_five_axes_without_mutating_original_sources():
    report = _report()
    initial = deepcopy(report)
    result = audit_us_close_report(report)
    assert report == initial
    assert result["five_axis"]["axis_count"] == 5
    assert result["five_axis_requires_verification"] is True


@pytest.mark.parametrize("field", [
    "order_chain_verification",
    "trade_pnl_analysis",
    "broker_recovery_health",
    "reconciliation_scope_diagnostics",
    "runtime_liveness_failures",
])
def test_missing_axis_evidence_is_unknown_not_implicitly_zero(field):
    report = _report()
    del report[field]
    result = build_us_close_five_axis_audit(report)
    assert result["strict_status"] == "NOT_FULLY_VERIFIED"
    if field == "order_chain_verification":
        assert result["axes"]["broker_execution"]["status"] == "UNVERIFIED"
    elif field == "trade_pnl_analysis":
        assert result["axes"]["economic_settlement"]["status"] == "UNVERIFIED"
    elif field == "broker_recovery_health":
        assert result["axes"]["historical_unresolved"]["status"] == "UNVERIFIED"
    else:
        assert result["axes"]["runtime_and_stage4"]["status"] == "UNVERIFIED"


def test_no_order_day_is_not_fabricated_success():
    report = _report()
    report["order_chain_verification"] = {
        "status": "NO_ORDERS", "order_count": 0,
        "kis_broker_chain_verified_count": 0, "broker_db_quantity_conflict_count": 0,
        "policy_verdict_counts": {},
    }
    report["trade_pnl_analysis"]["trade_details"] = []
    result = build_us_close_five_axis_audit(report)
    assert result["axes"]["broker_execution"]["status"] == "NO_ORDERS_OBSERVED"
    assert result["axes"]["buy_policy_and_strategy"]["status"] == "NO_ORDERS_OBSERVED"
    assert result["axes"]["economic_settlement"]["status"] == "NO_SELLS_OBSERVED"
    assert result["strict_status"] == "NOT_FULLY_VERIFIED"


def test_broker_quantity_contradiction_separate_from_policy():
    report = _report()
    report["order_chain_verification"]["broker_db_quantity_conflict_count"] = 1
    result = build_us_close_five_axis_audit(report)
    assert result["axes"]["broker_execution"]["status"] == "CONFLICT"
    assert result["axes"]["buy_policy_and_strategy"]["status"] == "PENDING_EVIDENCE"
    assert result["axes"]["economic_settlement"]["status"] == "VERIFIED"


def test_reconstructed_sell_cost_basis_is_not_authoritative_kis_economics():
    report = _report()
    report["trade_pnl_analysis"]["trade_details"][0]["pnl_source"] = "ORDER_AUDIT_RECONSTRUCTED"
    result = build_us_close_five_axis_audit(report)
    assert result["axes"]["economic_settlement"]["status"] == "PENDING_EVIDENCE"
    assert result["axes"]["economic_settlement"]["authoritative_fill_pnl_sells"] == 9


def test_historical_zero_with_positive_prior_claim_is_not_clean():
    report = _report()
    report["broker_recovery_health"]["historical_recovery_unresolved"] = 0
    report["broker_recovery_health"]["historical_tp_unresolved_count"] = 0
    assert build_us_close_five_axis_audit(report)["axes"]["historical_unresolved"]["status"] == "PENDING_EVIDENCE"
    report["broker_recovery_health"]["unresolved_source_date_prior"] = 0
    assert build_us_close_five_axis_audit(report)["axes"]["historical_unresolved"]["status"] == (
        "REPORTED_CLEAR_UNVERIFIED_ORIGINALS"
    )


def test_freshness_never_inferred_from_empty_liveness_failures():
    report = _report()
    result = build_us_close_five_axis_audit(report)
    assert result["axes"]["runtime_and_stage4"]["status"] != "VERIFIED"
    report["runtime_liveness_failures"] = [{"session": "am", "runtime_integrity_status": "RECONCILE_REQUIRED"}]
    assert build_us_close_five_axis_audit(report)["axes"]["runtime_and_stage4"]["status"] == "CONFLICT"


def test_legacy_cross_source_findings_unchanged_for_healthy_source_counts():
    report = _report()
    report["kis_actual_fill_order_count"] = 11
    report["daily_fills_confirmed_total"] = 11
    report["unique_broker_order_count"] = 11
    # Existing independent cross-source findings remain independent.
    assert audit_us_close_report(report)["five_axis_requires_verification"] is True


@pytest.mark.parametrize("order_count,verified_count", [(0, 1), (11, 12)])
def test_broker_verified_count_exceeding_population_is_conflict(order_count, verified_count):
    report = _report()
    report["order_chain_verification"].update(
        order_count=order_count, kis_broker_chain_verified_count=verified_count)
    assert build_us_close_five_axis_audit(report)["axes"]["broker_execution"]["status"] == "CONFLICT"


@pytest.mark.parametrize("verdicts,expected", [
    ({"EXPECTED": 11, "IMPLEMENTATION_BUG": 1}, "CONFLICT"),
    ({"EXPECTED": 11, "PENDING_EVIDENCE": "unknown"}, "UNVERIFIED"),
    ({"EXPECTED": 11}, "VERIFIED"),
])
def test_policy_population_requires_consistent_counts(verdicts, expected):
    report = _report()
    report["order_chain_verification"]["policy_verdict_counts"] = verdicts
    assert build_us_close_five_axis_audit(report)["axes"]["buy_policy_and_strategy"]["status"] == expected


def test_malformed_trade_row_cannot_disappear_from_economic_axis():
    report = _report()
    report["trade_pnl_analysis"]["trade_details"].append({"filled_qty": 1})
    assert build_us_close_five_axis_audit(report)["axes"]["economic_settlement"]["status"] == "UNVERIFIED"


def test_missing_prior_claim_breakdown_cannot_report_historical_clear():
    report = _report()
    report["broker_recovery_health"].update(
        historical_recovery_unresolved=0, historical_tp_unresolved_count=0,
        execution_claim_source_date_breakdown_available=False)
    assert build_us_close_five_axis_audit(report)["axes"]["historical_unresolved"]["status"] == "UNVERIFIED"
