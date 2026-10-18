"""Evidence policy for the US_STANDARD order reconciliation pilot.

The default is legacy behavior (OFF). Enabling this flag holds balance-only
observations pending KIS execution proof without relaxing an order/claim fence.
Owner-specific TQQQ_INFINITE processing is deliberately excluded.
"""
from __future__ import annotations

import os
from collections.abc import Mapping


def require_broker_proof_for_balance_estimate(
    order: Mapping[str, object], environ: Mapping[str, str] | None = None,
) -> bool:
    flags = os.environ if environ is None else environ
    if str(flags.get("US_STANDARD_BROKER_PROOF_ONLY", "0")).strip() != "1":
        return False
    meta = order.get("meta") or {}
    if not isinstance(meta, dict):
        meta = {}
    owner = str(order.get("strategy_owner") or meta.get("strategy_owner") or
                order.get("owner") or meta.get("owner") or "").strip().upper()
    # No owner inference from ticker, session, or market mode. A named owner
    # other than US_STANDARD must retain its own reconciliation contract.
    return owner == "US_STANDARD"
