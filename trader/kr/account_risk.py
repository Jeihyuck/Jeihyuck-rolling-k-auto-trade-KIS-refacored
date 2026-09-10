from __future__ import annotations
import os
from typing import Any


def _f(k, d):
    try:
        return float(os.getenv(k, str(d)))
    except Exception:
        return d


def _fraction(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    # This module consumes fractions only. Reject obvious percentage-point
    # leakage instead of converting by magnitude and risking a false crash.
    if parsed < -1.0 or parsed > 1.0:
        return None
    return parsed


def evaluate_kr_account_risk(account_snapshot: dict | None) -> dict:
    s = account_snapshot or {}
    intraday = _fraction(s.get("account_intraday_pnl_pct"))
    five = _fraction(s.get("account_5d_pnl_pct"))
    level = None
    if intraday is not None:
        if intraday <= _f("KR_ACCOUNT_CRASH_LOSS_PCT", -0.018):
            level = "KR_DEFENSE_CRASH"
        elif intraday <= _f("KR_ACCOUNT_RISK_OFF_LOSS_PCT", -0.010):
            level = "KR_DEFENSE_RISK_OFF"
        elif intraday <= _f("KR_ACCOUNT_CAUTION_LOSS_PCT", -0.007):
            level = "KR_DEFENSE_CAUTION"
    if five is not None:
        if five <= _f("KR_ACCOUNT_5D_CRASH_LOSS_PCT", -0.050):
            level = "KR_DEFENSE_CRASH"
        elif five <= _f("KR_ACCOUNT_5D_RISK_OFF_LOSS_PCT", -0.030) and level != "KR_DEFENSE_CRASH":
            level = "KR_DEFENSE_RISK_OFF"
    return {
        "account_intraday_pnl_pct": intraday,
        "account_5d_pnl_pct": five,
        "account_intraday_pnl_pct_unknown": intraday is None,
        "account_loss_kill_switch_triggered": level is not None,
        "account_loss_kill_switch_level": level,
        "account_pnl_source": s.get("account_intraday_pnl_source") or s.get("account_pnl_source") or ("snapshot_fraction" if s else "missing"),
        "account_5d_pnl_source": s.get("account_5d_pnl_source") or "missing",
    }
