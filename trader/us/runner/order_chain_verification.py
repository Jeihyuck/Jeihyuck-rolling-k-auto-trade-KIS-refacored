"""Read-only, proof-backed US order chain audit.

A broker fill and an executed strategy policy are different claims. This audit
never declares EXPECTED solely because KIS filled an order. Missing 5W1H,
policy inputs or explicit risk gate records remain PENDING_EVIDENCE, while
original KIS-vs-DB quantity contradictions remain visible as mismatches.
"""
from __future__ import annotations

from collections import Counter
from typing import Any

from trader.us.utils.order_no import normalize_us_order_no

_KIS_ACTUAL = frozenset({
    "KIS_ORDER_CUMULATIVE_ACTUAL", "KIS_EXECUTION_ACTUAL",
    "KIS_FILLS_ACTUAL", "KIS_ACTUAL",
})
_STAGE_EVENTS = frozenset({
    "BROKER_CLAIM_PREPARED", "BROKER_SUBMIT_STARTED",
    "BROKER_ACK_RECEIVED", "BROKER_ACK_RECOVERED",
    "DB_ACK_PERSISTED", "JOURNAL_REPLAY_FILL_CONFIRMED",
    "ORDER_PARTIALLY_FILLED", "ORDER_FILLED",
})


def _qty(value: Any) -> int | None:
    try:
        n = int(value)
        return n if float(value) == float(n) and n >= 0 else None
    except (ValueError, TypeError, OverflowError):
        return None


def _dict(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            import json
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except (TypeError, ValueError):
            pass
    return {}


def _actual_fill_qty(rows: list[dict]) -> tuple[int | None, str]:
    """Order cumulative and distinct execution rows never double count."""
    cumulative: list[int] = []
    executions: dict[str, int] = {}
    for row in rows:
        meta = _dict(row.get("meta"))
        typ = str(meta.get("fill_evidence_type") or row.get("fill_evidence_type") or "").upper()
        if typ not in _KIS_ACTUAL:
            continue
        qty = _qty(row.get("qty"))
        if qty is None:
            return None, "KIS_FILL_QUANTITY_INVALID"
        if typ == "KIS_EXECUTION_ACTUAL":
            execution_key = str(row.get("fill_idempotency_key") or meta.get("fill_idempotency_key") or "")
            if not execution_key:
                return None, "KIS_EXECUTION_ID_MISSING"
            if execution_key in executions and executions[execution_key] != qty:
                return None, "KIS_EXECUTION_ID_CONFLICT"
            executions[execution_key] = qty
        else:
            cumulative.append(_qty(meta.get("cumulative_filled_qty")) or qty)
    if not cumulative and not executions:
        return None, "KIS_ORIGINAL_FILL_EVIDENCE_MISSING"
    sum_executions = sum(executions.values())
    qty = max(max(cumulative, default=0), sum_executions)
    return qty, "KIS_ACTUAL_BROKER_EVIDENCE"


def audit_us_order_chains(
    order_audit: list[dict], fills: list[dict], journal: list[dict],
) -> dict:
    if not all(isinstance(rows, list) for rows in (order_audit, fills, journal)):
        return {"status": "UNAVAILABLE", "reason": "SOURCE_COLLECTION_UNVERIFIED"}
    grouped_events: dict[tuple[str, str], list[dict]] = {}
    blocked = set()
    for event in journal:
        if not isinstance(event, dict):
            continue
        key = str(event.get("client_order_key") or "")
        attempt = str(event.get("submit_attempt_id") or "")
        if not key or not attempt:
            continue
        grouped_events.setdefault((key, attempt), []).append(event)
        if event.get("event_type") == "BROKER_SUBMIT_ABORTED_PRE_IO":
            blocked.add((key, attempt))

    reports = []
    sessions: Counter[str] = Counter()
    statuses: Counter[str] = Counter()
    broker_verified = 0
    missing_5w1h = 0
    integrity_conflicts = 0
    for order in order_audit:
        if not isinstance(order, dict):
            continue
        key = str(order.get("client_order_key") or "")
        attempt = str(order.get("submit_attempt_id") or "")
        symbol = str(order.get("symbol") or "").upper()
        side = str(order.get("side") or "").upper()
        owner = str(order.get("strategy_owner") or "").upper()
        order_no = normalize_us_order_no(
            order.get("raw_order_no") or order.get("canonical_order_no")
        )
        expected_qty = _qty(order.get("filled_qty"))
        journal_rows = grouped_events.get((key, attempt), [])
        event_types = {str(e.get("event_type") or "") for e in journal_rows}
        acknowledgement = bool(
            event_types & {"BROKER_ACK_RECEIVED", "BROKER_ACK_RECOVERED"}
        )
        submit_proven = (
            "BROKER_CLAIM_PREPARED" in event_types
            and "BROKER_SUBMIT_STARTED" in event_types
        )
        for event in journal_rows:
            if event.get("event_type") in ("BROKER_ACK_RECEIVED", "BROKER_ACK_RECOVERED"):
                sessions[str(event.get("session") or "UNKNOWN").lower()] += (
                    1 if event.get("event_type") == "BROKER_ACK_RECEIVED" else 0
                )
                break

        matched = [
            fill for fill in fills if isinstance(fill, dict)
            and str(fill.get("client_order_key") or "") == key
            and str(fill.get("symbol") or "").upper() == symbol
            and str(fill.get("side") or "").upper() == side
            and normalize_us_order_no(fill.get("order_no")) == order_no
        ]
        actual_qty, fill_reason = _actual_fill_qty(matched)
        fill_confirmed = (
            bool(key and attempt and order_no and submit_proven and acknowledgement)
            and actual_qty is not None and expected_qty == actual_qty
            and expected_qty is not None and expected_qty > 0
            and ("ORDER_FILLED" in event_types or "ORDER_PARTIALLY_FILLED" in event_types)
        )
        if fill_confirmed:
            broker_verified += 1
            broker_status = "KIS_FILL_DB_JOURNAL_MATCHED"
        elif actual_qty is not None and expected_qty is not None and actual_qty != expected_qty:
            broker_status = "BROKER_DB_FILLED_QTY_MISMATCH"
        else:
            broker_status = fill_reason if actual_qty is None else "ORDER_CHAIN_INCOMPLETE"

        evidence_status = str(order.get("decision_evidence_status") or "").upper()
        if evidence_status != "VERIFIED":
            missing_5w1h += 1
        # An inconsistent fill quantity is an *integrity discrepancy*, not
        # automatic proof of a code implementation defect: the KIS evidence,
        # DB ingestion revision and original event ordering still need review.
        # Do not collapse implementation bugs and unverified evidence.
        if broker_status == "BROKER_DB_FILLED_QTY_MISMATCH":
            integrity_conflicts += 1
        # Owner, entry snapshot, exact signal configuration and risk-gate
        # source must be replayed against the executing revision before any
        # EXPECTED / IMPLEMENTATION BUG / POLICY REVIEW verdict is asserted.
        policy_status = "PENDING_EVIDENCE"
        statuses[policy_status] += 1
        reports.append({
            "symbol": symbol, "side": side, "strategy_owner": owner,
            "client_order_key": key, "submit_attempt_id": attempt,
            "canonical_order_no": order_no, "decision_reason": (
                order.get("strategy_reason") or order.get("reason") or ""
            ),
            "decision_evidence_status": evidence_status or "MISSING",
            "risk_gate_recorded": order.get("risk_gate_result") is not None,
            "execution_claim_event_present": "BROKER_CLAIM_PREPARED" in event_types,
            "submit_event_present": "BROKER_SUBMIT_STARTED" in event_types,
            "broker_ack_event_present": acknowledgement,
            "fill_reconciliation_event_present": (
                "JOURNAL_REPLAY_FILL_CONFIRMED" in event_types
                or "ORDER_FILLED" in event_types
            ),
            "kis_actual_fill_qty": actual_qty,
            "local_filled_qty": expected_qty,
            "broker_chain_status": broker_status,
            "policy_verdict": policy_status,
            "policy_verdict_reason": (
                "KIS_AND_DB_QUANTITY_CONTRADICTION_UNVERIFIED_CAUSE"
                if broker_status == "BROKER_DB_FILLED_QTY_MISMATCH"
                else "EXACT_OWNER_POLICY_INPUT_GATE_REPLAY_REQUIRED"
            ),
        })
    # A proven pre-HTTP abort is NOT a broker order and must never inflate
    # the eight executed US trades or be treated as ambiguous submit.
    pre_http_only = 0
    for identity in blocked:
        all_types = {str(e.get("event_type") or "") for e in grouped_events.get(identity, [])}
        if not all_types.intersection({"BROKER_ACK_RECEIVED", "BROKER_ACK_RECOVERED"}):
            pre_http_only += 1
    total = len(reports)
    return {
        "status": (
            "VERIFIED" if total and statuses["PENDING_EVIDENCE"] == 0
            and integrity_conflicts == 0
            else "AUDIT_INCOMPLETE" if total else "NO_ORDERS"
        ),
        "order_count": total,
        "kis_broker_chain_verified_count": broker_verified,
        "missing_5w1h_count": missing_5w1h,
        "broker_db_quantity_conflict_count": integrity_conflicts,
        "safe_pre_http_abort_attempt_count": pre_http_only,
        "broker_ack_by_session": dict(sessions),
        "policy_verdict_counts": dict(statuses),
        "orders": reports,
    }
