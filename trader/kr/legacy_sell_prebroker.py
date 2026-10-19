"""Exact provenance proof for the 2026-10-13 pre-order SELL misclassification.

On eb924b38 sell_stock_market/sell_stock_limit called get_positions() BEFORE
the KIS order HTTP boundary. get_positions raised KisBalanceUnavailable with
KR_BALANCE_BUDGET_EXHAUSTED_BEFORE_FETCH. The then-current error classifier
failed to recognize this proven pre-HTTP exception and persisted UNRESOLVED_ACK
and a synthetic submitted_at. Only this audited immutable branch/dated cause
is eligible. Do not use this for general missing ACKs or later revisions.
"""
from __future__ import annotations

import json
from datetime import date
from typing import Any

LEGACY_PROVEN_PRE_HTTP_SHA = "eb924b380ace9a5e2a0d9e7b65fa1ca15a379d81"
LEGACY_PROVEN_PRE_HTTP_DATE = date(2026, 10, 13)
LEGACY_PROVEN_CODES = frozenset({"036540", "039030", "042700", "066570", "067310"})
LEGACY_RESPONSE = {
    "rt_cd": "UNRESOLVED_ACK",
    "msg_cd": "BROKER_SUBMIT_OUTCOME_UNKNOWN",
    "msg1": "KR_BALANCE_BUDGET_EXHAUSTED_BEFORE_FETCH",
    "broker_submit": True,
    "reconcile_required": True,
}


def proven_oct13_prehttp_sell(row: dict[str, Any]) -> bool:
    """Proof is based on source code control flow, not missing broker IDs."""
    response = row.get("response_json")
    if isinstance(response, str):
        try:
            response = json.loads(response)
        except (ValueError, TypeError):
            return False
    if not isinstance(response, dict):
        return False
    day = row.get("trade_date")
    if isinstance(day, str):
        try:
            day = date.fromisoformat(day)
        except ValueError:
            return False
    return (
        row.get("env") == "practice"
        and row.get("git_sha") == LEGACY_PROVEN_PRE_HTTP_SHA
        and day == LEGACY_PROVEN_PRE_HTTP_DATE
        and str(row.get("code") or "").zfill(6) in LEGACY_PROVEN_CODES
        and row.get("strategy_owner") == "PB1"
        and row.get("action") == "SELL:STOP_HIT_EFFECTIVE:FULL_EXIT"
        and row.get("action_state") == "UNCERTAIN"
        and row.get("attempt_state") == "UNRESOLVED"
        and row.get("order_status") == "UNRESOLVED_ACK"
        and row.get("stage") == "FULL_EXIT"
        and row.get("side") == "SELL"
        and row.get("strategy") == "pb1_pullback_close"
        and row.get("acked_at") is None
        and not row.get("kis_odno")
        and not row.get("broker_order_id")
        and all(response.get(k) == v for k, v in LEGACY_RESPONSE.items())
    )
