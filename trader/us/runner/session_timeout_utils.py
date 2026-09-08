# -*- coding: utf-8 -*-
"""US session timeout / liveness helpers."""
from __future__ import annotations


def tick_has_authoritative_execution_health(tick: dict) -> bool:
    """Recovery requires all broker state, not merely a successful quote."""
    return bool(
        not tick.get("balance_fetch_failed")
        and str(tick.get("ack_reconcile_after_route_status") or tick.get("ack_reconcile_status") or "OK").upper() == "OK"
        and int(tick.get("unresolved_ack_count", 0) or 0) == 0
        and str(tick.get("fill_source_status") or "OK").upper() == "OK"
        and str(tick.get("durable_fence_status") or "ACTIVE").upper() == "ACTIVE"
    )


def advance_timeout_execution_mode(mode: str, consecutive: int, *, threshold: int, healthy_tick: dict | None = None) -> tuple[str, bool]:
    """Pure session-liveness transition used by the runner and regressions."""
    if healthy_tick is not None and tick_has_authoritative_execution_health(healthy_tick):
        return "NORMAL", True
    if consecutive >= threshold:
        return "SAFE_DEGRADED", False
    return mode, mode == "NORMAL"
