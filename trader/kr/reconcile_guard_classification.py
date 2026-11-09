"""Distinguish KIS reconciliation failure from the unrelated stale-DB purge guard.

evaluate_stale_db_guard() returns 'kis_has_holdings' solely to forbid stale
position deletion. That is NORMAL when a brokerage account owns shares, and
must not turn a successful KIS daily-ccld/claim reconciliation into DEGRADED.

Any unknown guard reason, contradictory missing broker holdings, or holdings
error remains fail-closed. This module never authorizes BUY by itself.
"""
from __future__ import annotations

from typing import Any, Mapping


def broker_reconcile_guard_blocks(result: Mapping[str, Any] | None) -> bool:
    if not isinstance(result, Mapping):
        return True
    reason = str(result.get("guard_reason") or "").strip()
    if not reason:
        return False
    if reason == "kis_has_holdings":
        # FULL AM recovery can find an old, exactly dated unresolved claim.
        # It is not today's missing KIS balance. Only a fresh authenticated
        # current broker snapshot and a complete, scoped prior-only origin
        # breakdown can make this a purge-only reason. The historical journal
        # still records UNRESOLVED; the same-order fence remains active.
        health = result.get("execution_claim_health")
        prior_only = False
        if (
            result.get("historical_recovery_executed") is True
            and result.get("current_only_fresh_broker_proven") is True
            and isinstance(health, Mapping)
            and health.get("available") is True
        ):
            fields = (
                "unresolved_execution_actions",
                "unresolved_source_date_current",
                "unresolved_source_date_prior",
                "unresolved_source_date_other_or_unknown",
            )
            try:
                if all(k in health and type(health[k]) is int for k in fields):
                    total, current, prior, unknown = (health[k] for k in fields)
                    prior_only = bool(
                        total == prior and prior > 0
                        and current == 0 and unknown == 0
                    )
            except (TypeError, ValueError):
                prior_only = False
        # Only the direct presence of a valid broker holdings snapshot
        # can establish this as a purge-only reason; never allow spoofed
        # stale/corrupt/missing balance evidence to bypass the claim gates.
        qty = result.get("holdings")
        return (
            isinstance(qty, bool) or not isinstance(qty, int) or qty < 1
            or bool(result.get("holdings_error"))
            # Mere presence of KIS shares says nothing about whether an
            # imported DB cycle owns those shares. Require independently
            # proven exact active-account/epoch/cycle/holdings parity.
            or (
                result.get("historical_repair_fastpath") is not True
                and not (
                    result.get("historical_recovery_executed") is False
                    and result.get("current_only_fresh_broker_proven") is True
                )
                and not prior_only
            )
        )
    return True
