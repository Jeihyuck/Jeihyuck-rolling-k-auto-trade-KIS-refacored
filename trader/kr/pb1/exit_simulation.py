from __future__ import annotations

import os
from typing import Any

import numpy as np

from trader.config import TP1_SELL_PCT, TP2_SELL_PCT


def resolve_force_exit_simulation(*, code: str, orderable_qty: int, exit_policy_family: str) -> dict[str, Any] | None:
    if not _env_bool("FORCE_EXIT_SIMULATION", False):
        return None
    requested_code = str(os.getenv("FORCE_EXIT_CODE") or "").strip().zfill(6)
    if requested_code and requested_code != str(code or "").zfill(6):
        return None
    requested_reason = str(os.getenv("FORCE_EXIT_REASON") or "STOP_HIT").strip().upper() or "STOP_HIT"
    valid_reasons = {"STOP_HIT", "FAILED_BREAKOUT", "TP1", "TP2", "TRAIL_STOP"}
    if requested_reason not in valid_reasons:
        requested_reason = "STOP_HIT"
    qty = int(orderable_qty or 0)
    if requested_reason == "TP1":
        qty = max(1, int(np.ceil(float(orderable_qty or 0) * float(TP1_SELL_PCT))))
    elif requested_reason == "TP2":
        qty = max(1, int(np.ceil(float(orderable_qty or 0) * float(TP2_SELL_PCT))))
    family_hint = exit_policy_family or "GENERIC_EXIT"
    if requested_reason == "FAILED_BREAKOUT":
        family_hint = "PULLBACK_EXIT"
    elif requested_reason in {"TP1", "TP2", "TRAIL_STOP"}:
        family_hint = "MOMENTUM_EXIT"
    return {
        "reason": requested_reason,
        "qty": min(max(1, qty), max(1, int(orderable_qty or 0))),
        "stage": requested_reason,
        "exit_policy_family": family_hint,
    }


def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return bool(default)
    return str(val).strip().lower() in {"1", "true", "yes", "on"}
