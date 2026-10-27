"""KR PB1 entry style provenance; a trigger is not a strategy owner."""
from __future__ import annotations

from typing import Any

from trader.trade_plan import ENTRY_STYLE_ALIAS, STYLE_PLAN_MAPPING


def entry_style_identity_proof(*, selected_style: Any, trigger_ok: bool,
                               decision_family: Any, trigger_policy: Any) -> dict[str, Any]:
    raw = str(selected_style or "").strip().upper()
    style = ENTRY_STYLE_ALIAS.get(raw, raw)
    mapped = STYLE_PLAN_MAPPING.get(style)
    if mapped is None:
        return {"state": "UNKNOWN", "reason": "STYLE_UNKNOWN", "selected_style": raw,
                "trigger_event": "BREAKOUT_PRICE_CONFIRMED" if trigger_ok else "NOT_CONFIRMED",
                "decision_family": str(decision_family or ""), "trigger_policy": str(trigger_policy or "")}
    return {
        "state": "VERIFIED",
        "reason": None,
        "selected_style": style,
        "trigger_event": "BREAKOUT_PRICE_CONFIRMED" if trigger_ok else "NOT_CONFIRMED",
        "decision_family": str(decision_family or ""),
        "trigger_policy": str(trigger_policy or ""),
        "contract_trade_horizon": mapped["trade_horizon"],
        "contract_exit_policy_family": mapped["exit_policy_family"],
        "contract_eod_action": mapped["eod_action"],
        "contract_force_eod_close": mapped["force_eod_close"],
    }
