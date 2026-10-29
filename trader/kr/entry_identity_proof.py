"""KR PB1 entry style provenance; a trigger is not a strategy owner."""
from __future__ import annotations

from typing import Any
import os

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


def kr_practice_trend_swing_enabled(*, environment: str) -> bool:
    """Enable verified NEW PB1 trend-swing entries in paper by default.

    Real accounts are always excluded. An explicit environment value '0'
    overrides the paper default, preserving the operational kill switch.
    This gate never rewrites a previously frozen position's exit contract.
    """
    env = str(environment or "").strip().lower()
    return (
        env == "practice"
        and os.getenv("PB1_KR_PRACTICE_TREND_SWING", "1").strip() == "1"
    )


def select_practice_trend_horizon(
    *, selected_style: Any, environment: str, enabled: bool,
    volume_proof: dict[str, Any] | None, price: Any,
    ma20: Any, ma50: Any, rs_percentile: Any,
    pivot: Any = None, trigger_ok: bool = False,
    explicit_day: bool = False,
) -> dict[str, str]:
    """Opt-in NEW KR practice BUY classification; never alter an existing holding.

    The opt-in is deliberately separate from entry style, broker routing, and
    final30 trigger identity. No historical replay or profit claim is implied.
    """
    import math

    style_raw = str(selected_style or "").strip().upper()
    style = ENTRY_STYLE_ALIAS.get(style_raw, style_raw)
    result = {"state": "KEEP_EXISTING_DAY", "reason": "not_eligible",
              "selected_style": style, "trade_horizon": "DAY_TRADE"}
    if style not in {"ENTRY_MOMENTUM", "ENTRY_MOMENTUM_CONTINUATION", "ENTRY_BREAKOUT"}:
        result["reason"] = "other_style_policy_unchanged"
        return result
    if not enabled:
        result["reason"] = "opt_in_disabled"
        return result
    if str(environment or "").lower() != "practice":
        result["reason"] = "practice_only"
        return result
    if explicit_day:
        result["reason"] = "explicit_day_trade"
        return result
    if not isinstance(volume_proof, dict) or volume_proof.get("confirmation_state") != "VERIFIED":
        result["reason"] = "volume_evidence_unverified"
        return result

    def positive(value: Any) -> float | None:
        try:
            number = float(value)
            return number if math.isfinite(number) and number > 0 else None
        except (TypeError, ValueError):
            return None

    last, m20, m50 = positive(price), positive(ma20), positive(ma50)
    rs = positive(rs_percentile)
    if rs is not None and rs <= 1:
        rs *= 100
    if (last is None or m20 is None or m50 is None or
            rs is None or rs < 60 or not (last >= m20 >= m50)):
        result["reason"] = "trend_or_rs_unverified"
        return result
    if style == "ENTRY_BREAKOUT":
        pivot_price = positive(pivot)
        if (not trigger_ok or not volume_proof.get("breakout_volume_confirmed")
                or pivot_price is None or last < pivot_price):
            result["reason"] = "breakout_pivot_or_volume_unverified"
            return result
    result.update(state="SWING_ELIGIBLE", reason="verified_practice_trend",
                  trade_horizon="SWING")
    return result
