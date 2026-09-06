from __future__ import annotations

from typing import Any

from trader.config import RISK_PER_TRADE_PCT
from trader.time_utils import now_kst


def infer_entry_family(
    features: dict[str, Any],
    *,
    trigger_ok: bool,
    trigger_info: dict | None = None,
) -> tuple[str, str, str]:
    raw_style = features.get("entry_style_selected") or features.get("entry_style") or features.get("selected_style") or ""
    raw_family = (
        features.get("selected_family")
        or features.get("decision_family")
        or features.get("entry_family")
        or features.get("entry_reason")
        or ""
    )
    style = str(raw_style or "").upper()
    family = str(raw_family or "").upper()
    pullback_ok = bool(features.get("pullback_ok") or features.get("pullback_pass") or style == "PULLBACK" or family == "ENTRY_PULLBACK")
    breakout_ok = bool(trigger_ok or features.get("breakout_ok") or features.get("breakout_pass") or style == "BREAKOUT" or family == "ENTRY_BREAKOUT")
    momentum_ok = bool(features.get("momentum_ok") or features.get("momentum_pass") or style == "MOMENTUM" or family == "ENTRY_MOMENTUM")
    if pullback_ok:
        return "PULLBACK", "ENTRY_PULLBACK", "PULLBACK_OVERRIDE"
    if breakout_ok:
        return "BREAKOUT", "ENTRY_BREAKOUT", "BREAKOUT_TRIGGER"
    if momentum_ok:
        return "MOMENTUM", "ENTRY_MOMENTUM", "MOMENTUM_CONTINUATION"
    if trigger_ok:
        return "BREAKOUT", "ENTRY_BREAKOUT", "BREAKOUT_TRIGGER"
    return "PULLBACK", "ENTRY_PULLBACK", "PULLBACK_OVERRIDE"


def build_entry_plan(
    *,
    cf: Any,
    gate_state: dict[str, Any],
    entry_price: float,
    order_price: float,
    stop_price: float,
    trigger_ok: bool,
    trigger_info: dict | None,
    entry_mode: str | None,
    stage: str,
    price_source: str | None = None,
) -> dict[str, Any]:
    features = cf.features or {}
    entry_style, entry_family, trigger_policy = infer_entry_family(features, trigger_ok=trigger_ok, trigger_info=trigger_info)
    qty = int(cf.planned_qty or 0)
    limit_price = float(features.get("limit_price") or features.get("order_price") or order_price or entry_price or 0.0)
    plan = {
        "version": 1,
        "code": str(cf.code).zfill(6),
        "market": cf.market,
        "side": "BUY",
        "stage": stage,
        "entry_mode": entry_mode,
        "entry_style": entry_style,
        "entry_family": entry_family,
        "entry_reason": entry_family,
        "trigger_policy": trigger_policy,
        "order_type": "LIMIT",
        "entry_price": float(entry_price or 0.0),
        "order_price": float(order_price or entry_price or 0.0),
        "limit_price": float(limit_price or 0.0),
        "stop_price": float(stop_price or 0.0),
        "initial_stop": float(stop_price or 0.0),
        "qty": qty,
        "planned_value": float(qty * limit_price),
        "risk_pct": float(RISK_PER_TRADE_PCT),
        "score": float(features.get("score") or cf.score or 0.0),
        "pivot": features.get("pivot"),
        "trigger_ok": bool(trigger_ok),
        "trigger_info": trigger_info or {},
        "price_source": price_source or features.get("price_source") or "unknown",
        "price_gate_blocked": bool(features.get("price_gate_blocked")),
        "created_at": now_kst().isoformat(),
        "setup_passed": bool(gate_state.get("setup_passed")),
        "risk_passed": bool(gate_state.get("risk_passed")),
        "sizing_passed": bool(gate_state.get("sizing_passed")),
        "buyable_passed": bool(gate_state.get("buyable_passed")),
        "authoritative_gate_passed": bool(gate_state.get("authoritative_gate_passed")),
        "sizing_reason": gate_state.get("sizing_reason"),
        "gate_state": {
            "setup_passed": bool(gate_state.get("setup_passed")),
            "risk_passed": bool(gate_state.get("risk_passed")),
            "sizing_passed": bool(gate_state.get("sizing_passed")),
            "buyable_passed": bool(gate_state.get("buyable_passed")),
            "authoritative_gate_passed": bool(gate_state.get("authoritative_gate_passed")),
            "sizing_reason": gate_state.get("sizing_reason"),
            "planned_qty": int(gate_state.get("planned_qty") or qty or 0),
            "risk_reasons": list(gate_state.get("risk_reasons") or []),
            "buyable_reasons": list(gate_state.get("buyable_reasons") or []),
        },
    }
    features["entry_style_selected"] = entry_style
    features["selected_family"] = entry_family
    features["entry_reason"] = entry_family
    features["trigger_policy"] = trigger_policy
    features["entry_plan"] = plan
    return plan


def validate_entry_plan(plan: dict | None) -> tuple[bool, list[str]]:
    return validate_entry_plan_with_window(plan, window_internal=None)


def validate_entry_plan_with_window(plan: dict | None, *, window_internal: str | None) -> tuple[bool, list[str]]:
    if not isinstance(plan, dict):
        return False, ["entry_plan_missing"]
    required = ["code", "side", "stage", "entry_style", "entry_family", "entry_reason", "trigger_policy", "entry_price", "order_price", "limit_price", "stop_price", "qty"]
    missing = [k for k in required if k not in plan or plan.get(k) in (None, "")]
    if missing:
        return False, [f"missing:{k}" for k in missing]
    reasons: list[str] = []
    try:
        qty = int(plan.get("qty") or 0)
        entry_price = float(plan.get("entry_price") or 0.0)
        order_price = float(plan.get("order_price") or 0.0)
        limit_price = float(plan.get("limit_price") or 0.0)
        stop_price = float(plan.get("stop_price") or 0.0)
    except Exception as exc:
        return False, [f"numeric_parse_error:{type(exc).__name__}"]
    if qty <= 0:
        reasons.append("qty_invalid")
    if entry_price <= 0:
        reasons.append("entry_price_invalid")
    if order_price <= 0:
        reasons.append("order_price_invalid")
    if limit_price <= 0:
        reasons.append("limit_price_invalid")
    if stop_price <= 0:
        reasons.append("stop_price_invalid")
    if stop_price >= entry_price:
        reasons.append("stop_not_below_entry")
    entry_style = str(plan.get("entry_style") or "").upper()
    entry_family = str(plan.get("entry_family") or "").upper()
    trigger_policy = str(plan.get("trigger_policy") or "").upper()
    stage = str(plan.get("stage") or "").upper()
    if entry_style not in {"PULLBACK", "BREAKOUT", "MOMENTUM"}:
        reasons.append(f"entry_style_invalid:{entry_style}")
    if entry_family not in {"ENTRY_PULLBACK", "ENTRY_BREAKOUT", "ENTRY_MOMENTUM"}:
        reasons.append(f"entry_family_invalid:{entry_family}")
    if entry_style == "PULLBACK" and trigger_policy in {"", "NONE"}:
        reasons.append("pullback_trigger_policy_missing")
    if stage == "PB1-CLOSE" and str(plan.get("side")).upper() == "BUY":
        window = str(window_internal or "").lower()
        if window in {"morning", "am", "afternoon", "pm"}:
            reasons.append("intraday_buy_stage_must_not_be_close")
    return len(reasons) == 0, reasons
