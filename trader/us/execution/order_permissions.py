# -*- coding: utf-8 -*-
from __future__ import annotations
from dataclasses import dataclass
from typing import Mapping

TRUE={"1","true","TRUE","yes","YES","on","ON"}
FALSE={"0","false","FALSE","no","NO","off","OFF",""}

def _bool(env: Mapping[str,str], key: str, default: bool=False)->bool:
    v=env.get(key)
    if v is None: return default
    return str(v).strip() in TRUE

@dataclass(frozen=True)
class PermissionResult:
    allowed: bool
    dry_run: bool
    live_trading_enabled: bool
    us_live_trading_enabled: bool
    us_order_armed: bool
    signal_only: bool
    reasons: list[str]


def resolve_us_order_permissions(session: str, env: str, run_mode: str | None, environ: Mapping[str,str]) -> PermissionResult:
    dry_run=_bool(environ,"DRY_RUN", True)
    disable_live=_bool(environ,"DISABLE_LIVE_TRADING", True)
    disable_real=_bool(environ,"DISABLE_REAL_TRADING", False)
    live=_bool(environ,"LIVE_TRADING_ENABLED", False)
    us_live=_bool(environ,"US_LIVE_TRADING_ENABLED", False)
    armed=_bool(environ,"US_ORDER_ARMED", False)
    signal_only=_bool(environ,"SIGNAL_ONLY", False) or str(run_mode or "").upper()=="SIGNAL_ONLY"
    mode=str(run_mode or environ.get("RUN_MODE") or environ.get("STRATEGY_MODE") or "TRADE").upper()
    reasons=[]
    if dry_run: reasons.append("dry_run_enabled")
    if signal_only: reasons.append("signal_only_enabled")
    if disable_live: reasons.append("disable_live_trading_enabled")
    if disable_real and env.lower() not in {"practice","paper"}: reasons.append("disable_real_trading_enabled")
    if not live: reasons.append("live_trading_flag_disabled")
    if not us_live: reasons.append("us_live_trading_disabled")
    if not armed: reasons.append("us_order_not_armed")
    if mode not in {"TRADE","LIVE"}: reasons.append("run_mode_not_trade")
    return PermissionResult(not reasons, dry_run, live, us_live, armed, signal_only, reasons)
