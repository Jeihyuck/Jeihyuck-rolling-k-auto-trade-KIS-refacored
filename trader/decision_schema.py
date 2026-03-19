from __future__ import annotations

from typing import Any


ENTRY_DECISION_FAMILIES = {"ENTRY_BREAKOUT", "ENTRY_PULLBACK", "ENTRY_MOMENTUM", "ENTRY_GENERIC", "SKIP"}
EXIT_DECISION_FAMILIES = {"EXIT_STOP", "EXIT_TRAIL", "EXIT_TIME", "EXIT_MA_BREAK", "EXIT_RISK_OFF", "SKIP"}
ENTRY_TRIGGER_POLICIES = {"BREAKOUT_PIVOT", "PULLBACK_REVERSAL", "MOMENTUM_CONTINUATION", "CLOSE_RECLAIM", "NONE"}


def _normalized_code(value: Any) -> str:
    return str(value or "").strip().zfill(6)


def normalize_entry_setup_family(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if raw in {"ENTRY_BREAKOUT", "BREAKOUT", "ENTRY_BREAKOUT_CONFIRMED"}:
        return "ENTRY_BREAKOUT"
    if raw in {"ENTRY_PULLBACK", "PULLBACK", "ENTRY_PULLBACK_OVERRIDE"}:
        return "ENTRY_PULLBACK"
    if raw in {"ENTRY_MOMENTUM", "MOMENTUM", "ENTRY_MOMENTUM_CONTINUATION"}:
        return "ENTRY_MOMENTUM"
    if raw in {"ENTRY_GENERIC", "GENERIC"}:
        return "ENTRY_GENERIC"
    return "SKIP"


def resolve_entry_trigger_policy(setup_family: Any, *, trigger_ok: bool, features: dict[str, Any] | None = None) -> str:
    if not trigger_ok:
        return "NONE"
    family = normalize_entry_setup_family(setup_family)
    feature_map = dict(features or {})
    explicit_policy = str(feature_map.get("entry_trigger_policy") or "").strip().upper()
    if explicit_policy in ENTRY_TRIGGER_POLICIES:
        return explicit_policy
    if family == "ENTRY_BREAKOUT":
        return "BREAKOUT_PIVOT"
    if family == "ENTRY_PULLBACK":
        if bool(feature_map.get("close_reclaim_trigger_ok")):
            return "CLOSE_RECLAIM"
        return "PULLBACK_REVERSAL"
    if family == "ENTRY_MOMENTUM":
        return "MOMENTUM_CONTINUATION"
    return "NONE"


def normalize_entry_decision_reason(reason: Any, *, trigger_policy: str = "NONE") -> str:
    raw = str(reason or "").strip()
    upper = raw.upper()
    mapping = {
        "": "UNKNOWN",
        "OK": "ORDER_READY",
        "SETUP_FILTERS_FAIL": "SETUP_FILTERS_FAIL",
        "BREAKOUT_TRIGGER_FAIL": "PIVOT_NOT_BROKEN" if trigger_policy == "BREAKOUT_PIVOT" else "TRIGGER_NOT_CONFIRMED",
        "REQUIRE_AND_FAIL": "ENTRY_CONDITION_NOT_MET",
        "REQUIRE_OR_FAIL": "ENTRY_CONDITION_NOT_MET",
        "PIVOT_NOT_BROKEN": "PIVOT_NOT_BROKEN",
        "PIVOT_OVERSHOOT": "PIVOT_OVERSHOOT",
        "BUYABLE_TODAY_BUY_EXISTS": "BUYABLE_TODAY_BUY_EXISTS",
        "BUYABLE_COOLDOWN": "BUYABLE_COOLDOWN",
        "BUYABLE_OPEN_ORDER": "BUYABLE_OPEN_ORDER",
        "BUYABLE_EXISTING_HOLDING": "BUYABLE_EXISTING_HOLDING",
        "BUYABLE_DUPLICATE": "BUYABLE_DUPLICATE",
        "BUYABLE_WINDOW_BLOCK": "BUYABLE_WINDOW_BLOCK",
        "ORDER_PX_ABOVE_POSITION_CAP": "ORDER_PX_ABOVE_POSITION_CAP",
        "ORDER_PX_ABOVE_TICK_BUDGET": "ORDER_PX_ABOVE_TICK_BUDGET",
        "ORDER_PX_ABOVE_USABLE_CASH": "ORDER_PX_ABOVE_USABLE_CASH",
        "MIN_ORDER_KRW_NOT_MET": "MIN_ORDER_KRW_NOT_MET",
        "STOP_CALC_FAIL": "STOP_CALC_FAIL",
        "STOP_ABOVE_ENTRY": "STOP_ABOVE_ENTRY",
        "MAX_POSITIONS": "MAX_POSITIONS_REACHED",
        "TARGET_NEW_POSITIONS_ZERO": "TARGET_NEW_POSITIONS_ZERO",
        "TICK_BUDGET_ZERO": "TICK_BUDGET_ZERO",
        "AVAILABLE_CASH_ZERO": "AVAILABLE_CASH_ZERO",
        "INSUFFICIENT_CASH": "INSUFFICIENT_CASH",
        "ENTRY_CAP_EXCEEDED": "ENTRY_CAP_EXCEEDED",
        "TARGET_NEW_POSITIONS_LIMIT": "TARGET_NEW_POSITIONS_LIMIT",
    }
    return mapping.get(upper, upper or "UNKNOWN")


def build_entry_evaluation(
    *,
    code: Any,
    as_of: Any,
    trade_date: Any,
    input_source: str,
    setup_ok: bool,
    score_ok: bool,
    risk_ok: bool,
    sizing_ok: bool,
    buyable_ok: bool,
    trigger_ok: bool,
    order_ready: bool,
    reasons: list[str] | None,
    setup_family: Any,
    decision_reason: Any = None,
    features: dict[str, Any] | None = None,
) -> dict[str, Any]:
    reason_list = [str(reason).strip().upper() for reason in (reasons or []) if str(reason or "").strip()]
    normalized_family = normalize_entry_setup_family(setup_family)
    trigger_policy = resolve_entry_trigger_policy(normalized_family, trigger_ok=trigger_ok, features=features)
    primary_reason = normalize_entry_decision_reason(
        decision_reason if decision_reason is not None else (reason_list[0] if reason_list else ("ORDER_READY" if order_ready else "UNKNOWN")),
        trigger_policy=trigger_policy,
    )
    return {
        "code": _normalized_code(code),
        "phase": "entry",
        "as_of": str(as_of or ""),
        "trade_date": str(trade_date or ""),
        "input_source": str(input_source or "unknown"),
        "setup_ok": bool(setup_ok),
        "score_ok": bool(score_ok),
        "risk_ok": bool(risk_ok),
        "sizing_ok": bool(sizing_ok),
        "buyable_ok": bool(buyable_ok),
        "trigger_ok": bool(trigger_ok),
        "order_ready": bool(order_ready),
        "decision": "ORDER_READY" if order_ready else "SKIP",
        "decision_family": normalized_family if normalized_family != "SKIP" else "SKIP",
        "decision_reason": primary_reason,
        "entry_setup_family": normalized_family if normalized_family != "SKIP" else "ENTRY_GENERIC",
        "entry_trigger_policy": trigger_policy,
        "reasons": reason_list or ([primary_reason] if primary_reason else []),
    }


def normalize_exit_decision(reason: Any) -> tuple[str, str]:
    upper = str(reason or "").strip().upper()
    mapping = {
        "EXIT_RISK_OFF": ("EXIT_RISK_OFF", "EXIT_RISK_OFF"),
        "EXIT_STOP_LOSS": ("EXIT_STOP", "EXIT_STOP_LOSS"),
        "EXIT_TRAILING_STOP": ("EXIT_TRAIL", "EXIT_TRAILING_STOP"),
        "EXIT_MA50_BREAK": ("EXIT_MA_BREAK", "EXIT_MA50_BREAK"),
        "EXIT_MA20_BREAK": ("EXIT_MA_BREAK", "EXIT_MA20_BREAK"),
        "EXIT_TIME_STOP": ("EXIT_TIME", "EXIT_TIME_STOP"),
        "STOP_HIT": ("EXIT_STOP", "EXIT_STOP_LOSS_HIT"),
        "PULLBACK_INVALIDATION": ("EXIT_STOP", "EXIT_STOP_LOSS_HIT"),
        "MOMENTUM_STOP": ("EXIT_STOP", "EXIT_STOP_LOSS_HIT"),
        "BREAKOUT_FAILURE": ("EXIT_RISK_OFF", "EXIT_BREAKOUT_FAILURE"),
        "REBOUND_FAILURE": ("EXIT_RISK_OFF", "EXIT_REBOUND_FAILURE"),
        "MOMENTUM_DECAY": ("EXIT_RISK_OFF", "EXIT_MOMENTUM_DECAY"),
        "MA20_BREAKDOWN": ("EXIT_MA_BREAK", "EXIT_MA20_BREAK"),
        "MA50_BREAK_HEAVY_VOLUME": ("EXIT_MA_BREAK", "EXIT_MA50_BREAK"),
        "BREAKOUT_TIME_STOP": ("EXIT_TIME", "EXIT_TIME_STOP_HIT"),
        "PULLBACK_TIME_STOP": ("EXIT_TIME", "EXIT_TIME_STOP_HIT"),
        "TIME_STOP": ("EXIT_TIME", "EXIT_TIME_STOP_HIT"),
        "TIME_STOP_HIT": ("EXIT_TIME", "EXIT_TIME_STOP_HIT"),
        "MOMENTUM_TRAIL_PARTIAL": ("EXIT_TRAIL", "EXIT_TRAIL_STOP_HIT"),
        "CLIMAX_PARTIAL": ("EXIT_TRAIL", "EXIT_TRAIL_STOP_HIT"),
        "TP1": ("EXIT_TRAIL", "EXIT_PARTIAL_TAKE_PROFIT"),
        "TP2": ("EXIT_TRAIL", "EXIT_PARTIAL_TAKE_PROFIT"),
        "OK_HOLD": ("SKIP", "NO_EXIT_SIGNAL"),
        "NO_EXIT_SIGNAL": ("SKIP", "NO_EXIT_SIGNAL"),
    }
    return mapping.get(upper, ("SKIP", upper or "NO_EXIT_SIGNAL"))


def build_exit_evaluation(
    *,
    code: Any,
    as_of: Any,
    trade_date: Any,
    holding_qty: int,
    avg_price: float,
    last_price: float,
    entry_date: Any,
    days_held: int,
    stop_loss_hit: bool,
    trailing_stop_hit: bool,
    ma20_break: bool,
    ma50_break: bool,
    time_stop_hit: bool,
    risk_off_hit: bool,
    exit_ok: bool,
    reasons: list[str] | None,
    decision_reason: Any = None,
    secondary_reasons: list[str] | None = None,
) -> dict[str, Any]:
    reason_list = [str(reason).strip().upper() for reason in (reasons or []) if str(reason or "").strip()]
    primary = str(decision_reason or (reason_list[0] if reason_list else "NO_EXIT_SIGNAL")).strip().upper()
    decision_family, normalized_reason = normalize_exit_decision(primary)
    normalized_secondary = [str(reason).strip().upper() for reason in (secondary_reasons or reason_list[1:]) if str(reason or "").strip()]
    return {
        "code": _normalized_code(code),
        "phase": "exit",
        "as_of": str(as_of or ""),
        "trade_date": str(trade_date or ""),
        "holding_qty": int(holding_qty or 0),
        "avg_price": float(avg_price or 0.0),
        "last_price": float(last_price or 0.0),
        "entry_date": entry_date,
        "days_held": int(days_held or 0),
        "stop_loss_hit": bool(stop_loss_hit),
        "trailing_stop_hit": bool(trailing_stop_hit),
        "ma20_break": bool(ma20_break),
        "ma50_break": bool(ma50_break),
        "time_stop_hit": bool(time_stop_hit),
        "risk_off_hit": bool(risk_off_hit),
        "exit_ok": bool(exit_ok),
        "decision": "SUBMIT" if exit_ok else "SKIP",
        "decision_family": decision_family,
        "decision_reason": normalized_reason,
        "reasons": reason_list,
        "secondary_reasons": normalized_secondary,
    }