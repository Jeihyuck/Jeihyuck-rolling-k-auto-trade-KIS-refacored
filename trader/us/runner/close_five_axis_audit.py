"""Independent, conservative five-axis US Close evidence classification.

Pure read-only reporting. A broker ACK/FILL does not certify the frozen BUY
policy, economic basis, historical recovery, or writer freshness. Unknown
is never silently converted to zero or to VERIFIED. No DB, KIS or filesystem I/O.
"""
from __future__ import annotations

from typing import Any


def _mapping(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def _nonnegative_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None or value == "":
        return None
    try:
        number = int(value)
        return number if number >= 0 and float(value) == number else None
    except (ValueError, TypeError, OverflowError):
        return None


def build_us_close_five_axis_audit(report: dict) -> dict:
    """Describe five *different* evidence dimensions, never authorize orders."""
    if not isinstance(report, dict):
        report = {}
    chain = _mapping(report.get("order_chain_verification"))
    recovery = _mapping(report.get("broker_recovery_health"))
    pnl = _mapping(report.get("trade_pnl_analysis"))
    reconciled = _mapping(report.get("reconciliation_scope_diagnostics"))
    order_n = _nonnegative_int(chain.get("order_count"))
    broker_n = _nonnegative_int(chain.get("kis_broker_chain_verified_count"))
    broker_conflicts = _nonnegative_int(chain.get("broker_db_quantity_conflict_count"))
    axes: dict[str, dict] = {}

    # The broker execution population is independent of how an entry was
    # selected. The audit may say AUDIT_INCOMPLETE only because 5W1H is missing.
    if not chain or order_n is None or broker_n is None or broker_conflicts is None:
        broker_status = "UNVERIFIED"
    elif broker_conflicts or broker_n > order_n:
        broker_status = "CONFLICT"
    elif not order_n:
        broker_status = "NO_ORDERS_OBSERVED"
    elif broker_n == order_n:
        broker_status = "VERIFIED"
    else:
        broker_status = "PENDING_EVIDENCE"
    axes["broker_execution"] = {
        "status": broker_status,
        "observed_orders": order_n,
        "verified_broker_chains": broker_n,
        "quantity_conflicts": broker_conflicts,
        "authority": "KIS_FILL_DB_JOURNAL_CHAIN",
    }

    verdicts = _mapping(chain.get("policy_verdict_counts"))
    pending = _nonnegative_int(verdicts.get("PENDING_EVIDENCE"))
    verified_policies = sum(
        _nonnegative_int(verdicts.get(v)) or 0 for v in ("EXPECTED", "VERIFIED")
    )
    verdict_numbers = [_nonnegative_int(v) for v in verdicts.values()]
    verdict_total = (sum(verdict_numbers) if all(v is not None for v in verdict_numbers)
                     else None)
    if not chain or order_n is None or verdict_total is None or (order_n and not verdicts):
        policy_status = "UNVERIFIED"
    elif verdict_total > order_n:
        policy_status = "CONFLICT"
    elif not order_n:
        policy_status = "NO_ORDERS_OBSERVED"
    elif pending or verified_policies != order_n:
        policy_status = "PENDING_EVIDENCE"
    else:
        policy_status = "VERIFIED"
    axes["buy_policy_and_strategy"] = {
        "status": policy_status,
        "pending_5w1h_or_policy_count": pending,
        "verified_policy_verdicts": verified_policies,
        "observed_orders": order_n,
        "authority": "SIGNED_SOURCE_AND_OWNER_POLICY_REPLAY",
    }

    trades = pnl.get("trade_details")
    sells = (
        [row for row in trades if isinstance(row, dict)
         and str(row.get("side") or "").upper() == "SELL"]
        if isinstance(trades, list) else None
    )
    malformed_trade = (isinstance(trades, list) and any(
        not isinstance(row, dict) or str(row.get("side") or "").upper() not in {"BUY", "SELL"}
        for row in trades
    ))
    if sells is None or malformed_trade:
        economics_status = "UNVERIFIED"
        authoritative = None
    else:
        authoritative = sum(
            str(row.get("pnl_source") or "") == "AUTHORITATIVE_KIS_FILL"
            and _nonnegative_int(row.get("filled_qty")) not in (None, 0)
            for row in sells
        )
        if not sells:
            economics_status = "NO_SELLS_OBSERVED"
        elif (pnl.get("realized_pnl_status") == "VERIFIED"
              and authoritative == len(sells)):
            economics_status = "VERIFIED"
        else:
            economics_status = "PENDING_EVIDENCE"
    axes["economic_settlement"] = {
        "status": economics_status,
        "observed_sells": len(sells) if sells is not None else None,
        "authoritative_fill_pnl_sells": authoritative,
        "realized_pnl_status": pnl.get("realized_pnl_status"),
        "authority": "NATIVE_FILL_AND_VERIFIED_COST_BASIS",
    }

    historical = _nonnegative_int(recovery.get("historical_recovery_unresolved"))
    historical_tp = _nonnegative_int(recovery.get("historical_tp_unresolved_count"))
    prior_claims = (
        _nonnegative_int(recovery.get("unresolved_source_date_prior"))
        if recovery.get("execution_claim_source_date_breakdown_available") is True
        else None
    )
    if recovery.get("available") is not True:
        historical_status = "UNVERIFIED"
    elif historical is None or historical_tp is None or prior_claims is None:
        historical_status = "UNVERIFIED"
    elif historical or historical_tp or (prior_claims or 0):
        historical_status = "PENDING_EVIDENCE"
    else:
        historical_status = "REPORTED_CLEAR_UNVERIFIED_ORIGINALS"
    axes["historical_unresolved"] = {
        "status": historical_status,
        "prior_recovery_unresolved": historical,
        "prior_tp_unresolved": historical_tp,
        "prior_date_claims_unresolved": prior_claims,
        "authority": "AM_CLOSE_HISTORICAL_BROKER_PROOF",
    }

    failures = report.get("runtime_liveness_failures")
    close_integrity = str(report.get("close_integrity_status") or "").upper()
    current_recovery = reconciled.get("current_recovery_health_available")
    if (close_integrity in {"RECONCILE_REQUIRED", "INTEGRITY_DEGRADED", "FAILED"}
            or isinstance(failures, list) and failures):
        runtime_status = "CONFLICT"
    elif not isinstance(failures, list) or current_recovery is not True:
        runtime_status = "UNVERIFIED"
    else:
        # An empty failure list is an absence of *reported* failure, not
        # proof that the current generation stayed READY at the KIS boundary.
        runtime_status = "REPORTED_NO_LIVENESS_FAILURE_NOT_READY_PROOF"
    axes["runtime_and_stage4"] = {
        "status": runtime_status,
        "reported_liveness_failures": len(failures) if isinstance(failures, list) else None,
        "current_recovery_health_available": current_recovery,
        "close_integrity_status": close_integrity or None,
        "authority": "WSL_CODE_PIN_PID_GENERATION_AND_FRESH_RECONCILE",
    }

    verified_axes = sum(item["status"] == "VERIFIED" for item in axes.values())
    # Only a separate operational verification can establish live/paper GO.
    # This read-only summary does not claim KIS external availability.
    return {
        "schema": "US_CLOSE_FIVE_AXIS_READONLY_V1",
        "strict_status": "VERIFIED" if verified_axes == 5 else "NOT_FULLY_VERIFIED",
        "verified_axis_count": verified_axes,
        "axis_count": 5,
        "axes": axes,
        "is_order_authorization": False,
        "is_kis_live_practice_verification": False,
    }
