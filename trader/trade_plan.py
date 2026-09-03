from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from typing import Any, Mapping

POLICY_VERSION = "pb1_entry_exit_plan_v1"

ENTRY_STYLE_ALIAS = {
    "PULLBACK": "ENTRY_PULLBACK",
    "BREAKOUT": "ENTRY_BREAKOUT",
    "MOMENTUM": "ENTRY_MOMENTUM",
    "VCP": "ENTRY_VCP",
    "PULLBACK_OVERRIDE": "ENTRY_PULLBACK_OVERRIDE",
    "BREAKOUT_TRIGGER": "ENTRY_BREAKOUT",
    "MOMENTUM_CONTINUATION": "ENTRY_MOMENTUM_CONTINUATION",
}

STYLE_PLAN_MAPPING: dict[str, dict[str, Any]] = {
    "ENTRY_BREAKOUT": {"entry_thesis": "BREAKOUT_DAYTRADE", "trade_horizon": "DAY_TRADE", "exit_policy_family": "INTRADAY_PROFIT_PROTECT", "eod_action": "FORCE_EXIT", "force_eod_close": True},
    "ENTRY_OPEN_PUSH": {"entry_thesis": "BREAKOUT_DAYTRADE", "trade_horizon": "DAY_TRADE", "exit_policy_family": "INTRADAY_PROFIT_PROTECT", "eod_action": "FORCE_EXIT", "force_eod_close": True},
    "ENTRY_MOMENTUM": {"entry_thesis": "MOMENTUM_RECLAIM", "trade_horizon": "DAY_TRADE", "exit_policy_family": "INTRADAY_PROFIT_PROTECT", "eod_action": "FORCE_EXIT", "force_eod_close": True},
    "ENTRY_MOMENTUM_CONTINUATION": {"entry_thesis": "MOMENTUM_RECLAIM", "trade_horizon": "DAY_TRADE", "exit_policy_family": "INTRADAY_PROFIT_PROTECT", "eod_action": "FORCE_EXIT", "force_eod_close": True},
    "ENTRY_PULLBACK": {"entry_thesis": "PULLBACK_CONTINUATION", "trade_horizon": "SWING", "exit_policy_family": "SWING_STAGED_EXIT", "eod_action": "CARRY_IF_NO_EXIT_SIGNAL", "force_eod_close": False},
    "ENTRY_PULLBACK_OVERRIDE": {"entry_thesis": "PULLBACK_CONTINUATION", "trade_horizon": "SWING", "exit_policy_family": "SWING_STAGED_EXIT", "eod_action": "CARRY_IF_NO_EXIT_SIGNAL", "force_eod_close": False},
    "ENTRY_VCP": {"entry_thesis": "VCP_BREAKOUT", "trade_horizon": "SWING", "exit_policy_family": "SWING_STAGED_EXIT", "eod_action": "CARRY_IF_NO_EXIT_SIGNAL", "force_eod_close": False},
    "ENTRY_MINERVINI": {"entry_thesis": "MINERVINI_TREND", "trade_horizon": "SWING", "exit_policy_family": "SWING_STAGED_EXIT", "eod_action": "CARRY_IF_NO_EXIT_SIGNAL", "force_eod_close": False},
    "ENTRY_CORE": {"entry_thesis": "CORE_TREND", "trade_horizon": "CORE", "exit_policy_family": "CORE_TREND_FOLLOW", "eod_action": "CARRY_IF_NO_EXIT_SIGNAL", "force_eod_close": False},
    "CORE_TREND": {"entry_thesis": "CORE_TREND", "trade_horizon": "CORE", "exit_policy_family": "CORE_TREND_FOLLOW", "eod_action": "CARRY_IF_NO_EXIT_SIGNAL", "force_eod_close": False},
}


@dataclass(frozen=True)
class RiskPlan:
    initial_stop: float
    stop_type: str
    risk_per_share: float
    risk_R: float
    atr_pct: float | None
    invalidation_reason: str


@dataclass(frozen=True)
class ProfitPlan:
    tp1_trigger_type: str
    tp1_r: float | None
    tp1_profit_pct: float | None
    tp1_sell_pct: float
    tp2_trigger_type: str
    tp2_r: float | None
    tp2_profit_pct: float | None
    tp2_sell_pct: float
    runner_enabled: bool


@dataclass(frozen=True)
class ProtectionPlan:
    hard_stop_enabled: bool
    trail_enabled: bool
    profit_protect_enabled: bool
    activate_profit_pct: float
    giveback_pct: float
    floor_profit_pct: float
    ma20_break_exit: bool
    ma50_break_exit: bool
    risk_off_exit: bool


@dataclass(frozen=True)
class TimePlan:
    max_trading_days: int
    time_stop_min_r: float
    same_day_eod_check: bool


@dataclass(frozen=True)
class EntryExitPlan:
    code: str
    market: str | None
    entry_thesis: str
    entry_style_selected: str
    entry_reason: str
    trade_horizon: str
    exit_policy_family: str
    eod_action: str
    force_eod_close: bool
    risk_plan: RiskPlan
    profit_plan: ProfitPlan
    protection_plan: ProtectionPlan
    time_plan: TimePlan
    policy_source: str
    entry_style_raw: str | None = None
    policy_version: str = POLICY_VERSION

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _feature(features: Any, key: str, default: Any = None) -> Any:
    if isinstance(features, Mapping):
        return features.get(key, default)
    return getattr(features, key, default)


def _float_or_none(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except (TypeError, ValueError):
        return default


def _env_int(key: str, default: int) -> int:
    try:
        return int(float(os.getenv(key, str(default))))
    except (TypeError, ValueError):
        return default




def parse_plan_bool(value: Any, *, default: bool = False) -> bool:
    """Parse booleans from DB/JSON/env-ish values without bool("False") mistakes."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "f", "no", "n", "off", "", "none", "null"}:
        return False
    return default

def seed_plan_fields_for_entry_style(entry_style_selected: Any) -> dict[str, Any]:
    style_raw = str(entry_style_selected or "").strip().upper()
    style = ENTRY_STYLE_ALIAS.get(style_raw, style_raw)
    mapped = STYLE_PLAN_MAPPING.get(style)
    if not mapped:
        return {"entry_thesis": None, "trade_horizon": None, "exit_policy_family": None, "eod_action": None, "force_eod_close": False}
    return dict(mapped)


def build_entry_exit_plan(*, code: str, market: str | None = None, entry_style_selected: str | None, entry_reason: str | None, entry_price: float, features: Any | None = None) -> EntryExitPlan:
    features = features or {}
    style_raw = str(entry_style_selected or _feature(features, "entry_style_selected") or "").strip().upper()
    style = ENTRY_STYLE_ALIAS.get(style_raw, style_raw)
    if not style:
        raise ValueError("entry_style_selected is required")
    mapped = STYLE_PLAN_MAPPING.get(style)
    if not mapped:
        raise ValueError(f"unknown entry_style_selected={style}")
    explicit_any = any(_feature(features, key) for key in ("entry_thesis", "trade_horizon", "exit_policy_family"))
    allow_style_mapping = os.getenv("PB1_ALLOW_STYLE_MAPPED_ENTRY_EXIT_PLAN", "1").strip() not in {"0", "false", "False", "FALSE"}
    if not explicit_any and not allow_style_mapping:
        raise ValueError("explicit entry exit plan seed fields required")
    price = float(entry_price or 0.0)
    if price <= 0:
        raise ValueError("entry_price must be positive")
    base = dict(mapped)
    base["entry_thesis"] = str(_feature(features, "entry_thesis") or base["entry_thesis"]).strip().upper()
    base["trade_horizon"] = str(_feature(features, "trade_horizon") or base["trade_horizon"]).strip().upper()
    base["exit_policy_family"] = str(_feature(features, "exit_policy_family") or base["exit_policy_family"]).strip().upper()
    base["eod_action"] = str(_feature(features, "eod_action") or base["eod_action"]).strip().upper()
    base["force_eod_close"] = parse_plan_bool(_feature(features, "force_eod_close", base["force_eod_close"]), default=bool(base["force_eod_close"]))

    stop = None
    stop_source = ""
    for key in ("initial_stop", "stop_price", "stop_price_at_entry", "tight_low", "pullback_low"):
        value = _float_or_none(_feature(features, key))
        if value and value > 0:
            stop = value
            stop_source = key
            break
    atr_pct = _float_or_none(_feature(features, "atr_pct"))
    if stop is None:
        fallback_pct = _env_float("PB1_PLAN_DEFAULT_ATR_STOP_PCT", 3.0)
        stop = price * (1.0 - fallback_pct / 100.0)
        stop_source = "PB1_PLAN_DEFAULT_ATR_STOP_PCT"
        if atr_pct is None:
            atr_pct = fallback_pct
    risk_r = price - float(stop)
    if risk_r <= 0:
        raise ValueError(f"risk_R must be positive entry_price={price} initial_stop={stop}")

    horizon = base["trade_horizon"]
    if horizon == "DAY_TRADE":
        tp1_r = _env_float("PB1_MOMENTUM_TP1_R", 1.5)
        tp2_r = _env_float("PB1_MOMENTUM_TP2_R", 2.5)
        tp1_profit = None
        tp2_profit = None
        tp1_sell = 0.5
        tp2_sell = 0.5
        activate = _env_float("PB1_MOMENTUM_PROFIT_ACTIVATE_PCT", 5.0)
        giveback = _env_float("PB1_MOMENTUM_PROFIT_GIVEBACK_PCT", 2.0)
        floor = 0.0
        max_days = 1
    elif horizon == "CORE":
        tp1_r = None
        tp2_r = None
        tp1_profit = _env_float("PB1_CORE_TP1_PROFIT_PCT", 20.0)
        tp2_profit = _env_float("PB1_CORE_TP2_PROFIT_PCT", 30.0)
        tp1_sell = _env_float("PB1_CORE_TP1_SELL_PCT", 0.25)
        tp2_sell = _env_float("PB1_CORE_TP2_SELL_PCT", 0.25)
        activate = tp1_profit
        giveback = _env_float("PB1_SWING_PROFIT_GIVEBACK_PCT", 3.0)
        floor = _env_float("PB1_SWING_PROFIT_FLOOR_PCT", 5.0)
        max_days = _env_int("PB1_CORE_TIME_STOP_DAYS", 20)
    else:
        tp1_r = _env_float("PB1_SWING_TP1_R", 2.0)
        tp2_r = _env_float("PB1_SWING_TP2_R", 3.0)
        tp1_profit = _env_float("PB1_SWING_TP1_PROFIT_PCT", 12.0)
        tp2_profit = _env_float("PB1_SWING_TP2_PROFIT_PCT", 18.0)
        tp1_sell = _env_float("PB1_SWING_TP1_SELL_PCT", 0.33)
        tp2_sell = _env_float("PB1_SWING_TP2_SELL_PCT", 0.33)
        activate = _env_float("PB1_SWING_PROFIT_ACTIVATE_PCT", 8.0)
        giveback = _env_float("PB1_SWING_PROFIT_GIVEBACK_PCT", 3.0)
        floor = _env_float("PB1_SWING_PROFIT_FLOOR_PCT", 5.0)
        max_days = _env_int("PB1_SWING_TIME_STOP_DAYS", 10)

    plan = EntryExitPlan(
        code=str(code).zfill(6),
        market=market,
        entry_thesis=base["entry_thesis"],
        entry_style_selected=style,
        entry_reason=str(entry_reason or _feature(features, "entry_reason") or style),
        entry_style_raw=style_raw,
        trade_horizon=horizon,
        exit_policy_family=base["exit_policy_family"],
        eod_action=base["eod_action"],
        force_eod_close=bool(base["force_eod_close"]),
        risk_plan=RiskPlan(initial_stop=float(stop), stop_type=stop_source, risk_per_share=float(risk_r), risk_R=float(risk_r), atr_pct=atr_pct, invalidation_reason=f"break below {stop_source}"),
        profit_plan=ProfitPlan(tp1_trigger_type="R" if tp1_r is not None else "PCT", tp1_r=tp1_r, tp1_profit_pct=tp1_profit, tp1_sell_pct=tp1_sell, tp2_trigger_type="R" if tp2_r is not None else "PCT", tp2_r=tp2_r, tp2_profit_pct=tp2_profit, tp2_sell_pct=tp2_sell, runner_enabled=True),
        protection_plan=ProtectionPlan(hard_stop_enabled=True, trail_enabled=horizon != "DAY_TRADE", profit_protect_enabled=True, activate_profit_pct=activate, giveback_pct=giveback, floor_profit_pct=floor, ma20_break_exit=horizon != "CORE", ma50_break_exit=True, risk_off_exit=True),
        time_plan=TimePlan(max_trading_days=max(1, int(max_days)), time_stop_min_r=0.0, same_day_eod_check=horizon == "DAY_TRADE"),
        policy_source="explicit_features" if explicit_any else "style_mapping",
    )
    validate_entry_exit_plan(plan)
    return plan


def validate_entry_exit_plan(plan: EntryExitPlan | Mapping[str, Any]) -> bool:
    data = plan.to_dict() if isinstance(plan, EntryExitPlan) else dict(plan or {})
    required = ["code", "entry_thesis", "entry_style_selected", "trade_horizon", "exit_policy_family", "eod_action", "risk_plan", "profit_plan", "protection_plan", "time_plan", "policy_version"]
    missing = [key for key in required if not data.get(key)]
    if missing:
        raise ValueError(f"missing required plan fields: {missing}")
    horizon = str(data.get("trade_horizon") or "").upper()
    eod = str(data.get("eod_action") or "").upper()
    force = parse_plan_bool(data.get("force_eod_close"), default=False)
    if horizon == "DAY_TRADE" and (eod != "FORCE_EXIT" or not force):
        raise ValueError("DAY_TRADE must FORCE_EXIT with force_eod_close=true")
    if horizon in {"SWING", "CORE"} and (eod != "CARRY_IF_NO_EXIT_SIGNAL" or force):
        raise ValueError("SWING/CORE must CARRY_IF_NO_EXIT_SIGNAL with force_eod_close=false")
    risk = data.get("risk_plan") or {}
    time_plan = data.get("time_plan") or {}
    if float(risk.get("initial_stop") or 0.0) <= 0:
        raise ValueError("initial_stop must be positive")
    if float(risk.get("risk_R") or risk.get("risk_per_share") or 0.0) <= 0:
        raise ValueError("risk_R must be positive")
    if int(time_plan.get("max_trading_days") or 0) < 1:
        raise ValueError("max_trading_days must be >= 1")
    return True


def classify_close_action_from_plan(plan: Any) -> tuple[str, str]:
    if not plan or not isinstance(plan, Mapping):
        return "SKIP", "POLICY_MISSING"
    horizon = str(plan.get("trade_horizon") or "").upper()
    eod = str(plan.get("eod_action") or "").upper()
    force = parse_plan_bool(plan.get("force_eod_close"), default=False)
    if horizon == "DAY_TRADE" and eod == "FORCE_EXIT" and force:
        return "FORCE_SELL", "EOD_FORCE_EXIT"
    if horizon == "SWING":
        return "CARRY", "SWING_CARRY"
    if horizon == "CORE":
        return "CARRY", "CORE_CARRY"
    return "SKIP", f"UNHANDLED_OR_INVALID_PLAN:horizon={horizon or 'missing'} eod_action={eod or 'missing'} force_eod_close={int(force)}"


def classify_close_action_from_position_contract(
    position: Mapping[str, Any],
) -> tuple[str, str, str]:
    """Classify close behavior without inventing a missing historical plan."""
    plan = position.get("entry_exit_plan_json")
    if isinstance(plan, Mapping) and plan:
        action, reason = classify_close_action_from_plan(plan)
        return action, reason, "OK_FULL_PLAN"
    meta = position.get("position_meta") or {}
    if isinstance(meta, str):
        try:
            import json
            meta = json.loads(meta)
        except (TypeError, ValueError):
            meta = {}
    verified = position.get("provenance_verified") or (
        isinstance(meta, Mapping) and meta.get("provenance_verified")
    )
    if verified and all(
        position.get(key) is not None
        for key in ("trade_horizon", "eod_action", "force_eod_close")
    ):
        action, reason = classify_close_action_from_plan({
            "trade_horizon": position.get("trade_horizon"),
            "eod_action": position.get("eod_action"),
            "force_eod_close": position.get("force_eod_close"),
        })
        return action, reason, "RECOVERED_VERIFIED_CONTRACT"
    return "SKIP", "POLICY_MISSING", "POLICY_MISSING"
