"""Read-only close-report cross-source integrity audit.

The report contains session, daily-local, and KIS authoritative aggregates.
A zero in one namespace is not necessarily an error in another namespace.
This audit records *differences to investigate* without inventing fills,
changing positions, or declaring an unverified order settled.
"""
from __future__ import annotations

from typing import Any


def audit_us_close_report(report: dict[str, Any]) -> dict[str, Any]:
    findings: list[dict[str, Any]] = []

    def record(code: str, severity: str, **evidence: Any) -> None:
        findings.append({"code": code, "severity": severity, "evidence": evidence})

    recovery = report.get("broker_recovery_health") or {}
    if not recovery.get("available"):
        record("BROKER_RECOVERY_HEALTH_UNAVAILABLE", "P0")
    if int(recovery.get("unresolved_execution_actions") or 0):
        record("UNRESOLVED_EXECUTION_ACTIONS", "P0",
               count=int(recovery["unresolved_execution_actions"]))
    blocked = int(recovery.get("duplicate_semantic_submit_detection_count") or 0)
    if blocked:
        record("BLOCKED_EXECUTION_CLAIM_ATTEMPTS", "P1", count=blocked,
               note="This metric counts blocked claims, not broker submissions")

    if str(report.get("report_consistency") or "").upper() == "FAILED":
        record("CLOSE_REPORT_CONSISTENCY_FAILED", "P0")
    if str(report.get("close_integrity_status") or "").upper() == "RECONCILE_REQUIRED":
        record("CLOSE_RECONCILIATION_REQUIRED", "P0")

    broker_fills = int(report.get("kis_actual_fill_order_count") or 0)
    local_fills = int(report.get("daily_fills_confirmed_total") or 0)
    if broker_fills and not local_fills:
        record("BROKER_LOCAL_DAILY_AGGREGATE_DIVERGENCE", "P1",
               kis_actual_fill_orders=broker_fills,
               daily_local_confirmed_fills=local_fills,
               note="Different source populations may explain the difference; verify before changing totals")

    unique_order_nos = int(report.get("unique_broker_order_count") or 0)
    if unique_order_nos > broker_fills:
        record("BROKER_ORDER_NUMBER_COVERAGE_REVIEW", "P1",
               unique_broker_order_count=unique_order_nos,
               kis_actual_fill_orders=broker_fills,
               note="Open/cancelled orders and number normalization require independent verification")

    return {
        "findings": findings,
        "p0_count": sum(item["severity"] == "P0" for item in findings),
        "p1_count": sum(item["severity"] == "P1" for item in findings),
        "requires_manual_verification": bool(findings),
    }
