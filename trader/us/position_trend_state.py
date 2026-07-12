# -*- coding: utf-8 -*-
"""US holding trend state, staged trend/time exits, and add-to-existing filter."""
from __future__ import annotations

import logging, os
from datetime import datetime, timezone
from typing import Any

from trader.us.db.repos import load_latest_us_position_risk_state, load_us_position_risk_state, save_us_position_risk_state

logger = logging.getLogger(__name__)


def _f(v: Any, d: float | None = None) -> float | None:
    try:
        x = float(v)
        return x if x == x else d
    except Exception:
        return d

def _i(v: Any, d: int = 0) -> int:
    try: return int(float(v or 0))
    except Exception: return d

def _b(v: Any) -> bool: return bool(v)
def _iso(now: datetime) -> str: return (now if now.tzinfo else now.replace(tzinfo=timezone.utc)).isoformat()

def load_trend_state(symbol: str, trade_date: str) -> dict:
    risk = load_latest_us_position_risk_state(symbol, trade_date)
    return dict(((risk.get("state") or {}).get("trend") or {}))

def save_trend_state(symbol: str, trade_date: str, trend: dict) -> dict:
    risk = load_us_position_risk_state(symbol, trade_date) or {}
    state = dict(risk.get("state") or {})
    # preserve lifecycle from latest if today's missing
    if "lifecycle" not in state:
        latest = load_latest_us_position_risk_state(symbol, trade_date)
        if isinstance(latest.get("state"), dict) and latest["state"].get("lifecycle"):
            state["lifecycle"] = latest["state"]["lifecycle"]
    state["trend"] = trend
    risk["state"] = state
    save_us_position_risk_state(symbol, trade_date, risk)
    return trend


def update_us_position_trend_state(*, symbol: str, trade_date: str, now: datetime, current_price: float | None, holding_trade_days: int = 0, final30: dict | None = None, daily: dict | None = None, lifecycle_id: str | None = None) -> dict:
    prev = load_trend_state(symbol, trade_date)
    once = prev.get("last_daily_update_trade_date") != trade_date
    warning_thr = float(os.getenv("US_TREND_SCORE_WARNING_THRESHOLD", "0.45"))
    severe_thr = float(os.getenv("US_TREND_SCORE_SEVERE_THRESHOLD", "0.35"))
    final30 = final30 or {}
    daily = daily or {}
    cp = _f(current_price)
    ma20, ma50, ma150 = _f(daily.get("ma20")), _f(daily.get("ma50")), _f(daily.get("ma150"))
    rs20, rs60 = _f(daily.get("rs_20d")), _f(daily.get("rs_60d"))
    trend_score = _f(final30.get("trend_score") if final30.get("trend_score") is not None else daily.get("trend_score"))
    quality = "ok"
    valid_final30 = final30.get("trade_date") == trade_date and final30.get("score_contract_ok", True) and final30.get("available", True)
    if final30 and not valid_final30:
        quality = "missing_or_stale"
    final30_invalid = bool(final30 and not valid_final30)
    required_missing = cp is None or ma20 is None or ma50 is None or final30_invalid
    if not daily and required_missing:
        quality = "missing"
    in_final = bool(final30.get("in_final30_today")) if valid_final30 else bool(prev.get("in_final30_today"))
    absent = _i(prev.get("final30_absent_streak"))
    below20 = _i(prev.get("below_ma20_streak"))
    below50 = _i(prev.get("below_ma50_streak"))
    if once:
        if valid_final30:
            absent = 0 if in_final else absent + 1
        if cp is not None and ma20 is not None:
            below20 = below20 + 1 if cp < ma20 else 0
        if cp is not None and ma50 is not None:
            below50 = below50 + 1 if cp < ma50 else 0
    signals: list[str] = []
    if valid_final30 and not in_final: signals.append("FINAL30_ABSENT" if absent < 2 else f"FINAL30_ABSENT_{absent}D")
    if cp is not None and ma20 is not None and cp < ma20: signals.append("BELOW_MA20" if below20 < 2 else f"BELOW_MA20_{below20}D")
    if cp is not None and ma50 is not None and cp < ma50: signals.append("BELOW_MA50" if below50 < 2 else f"BELOW_MA50_{below50}D")
    if rs20 is not None and rs20 < 0: signals.append("RS20_NEGATIVE")
    if rs60 is not None and rs60 < 0: signals.append("RS60_NEGATIVE")
    if trend_score is not None and trend_score < warning_thr: signals.append("TREND_SCORE_LOW")
    alpha = _f(daily.get("stock_vs_sector_alpha"))
    if alpha is not None and alpha < 0: signals.append("STOCK_SECTOR_ALPHA_NEGATIVE")
    rotation = final30.get("rotation_regime") or daily.get("rotation_regime")
    theme = final30.get("theme_cluster") or daily.get("theme_cluster")
    if rotation == "AI_OFF_ROTATION" and str(theme or "").startswith("AI"): signals.append("AI_OFF_ROTATION_CONFLICT")
    if rotation == "RISK_OFF_ROTATION": signals.append("RISK_OFF_ROTATION_CONFLICT")
    healthy = bool(valid_final30 and in_final and cp is not None and ma20 is not None and ma50 is not None and cp >= ma20 and cp >= ma50 and (rs20 is None or rs20 >= 0) and (trend_score is None or trend_score >= warning_thr))
    state = "UNKNOWN" if required_missing else "HEALTHY" if healthy else "WARNING"
    sig_count = len(set(signals))
    rotation_conflict = any(s.endswith("ROTATION_CONFLICT") for s in signals)
    if state != "UNKNOWN":
        if below50 >= 2 or (absent >= 3 and sig_count >= 3) or (cp is not None and ma20 is not None and ma50 is not None and cp < ma20 and cp < ma50 and (rs20 or 0) < 0 and rotation_conflict):
            state = "EXIT"
        elif (absent >= 2 and sig_count >= 2) or (below20 >= 2 and sig_count >= 2) or (cp is not None and ma50 is not None and cp < ma50 and ((rs20 or 0) < 0 or (trend_score is not None and trend_score < severe_thr))):
            state = "TRIM"
    trend = {**prev, "last_daily_update_trade_date": trade_date, "in_final30_today": in_final, "final30_absent_streak": absent, "below_ma20_streak": below20, "below_ma50_streak": below50, "holding_trade_days": holding_trade_days or _i(prev.get("holding_trade_days")), "rank_final30": final30.get("rank_final30") if valid_final30 else prev.get("rank_final30"), "score_final": final30.get("score_final") if valid_final30 else prev.get("score_final"), "trend_score": trend_score, "current_price": cp, "ma20": ma20, "ma50": ma50, "ma150": ma150, "ma200": daily.get("ma200"), "ma200_slope": daily.get("ma200_slope"), "rs_20d": rs20, "rs_60d": rs60, "rs_120d": daily.get("rs_120d"), "daily_bar_count": daily.get("daily_bar_count"), "daily_metrics_as_of": daily.get("daily_metrics_as_of"), "daily_metrics_source": daily.get("daily_metrics_source"), "daily_metrics_trade_date": daily.get("daily_metrics_trade_date"), "daily_history_quality": daily.get("daily_history_quality"), "sector_relative_strength": daily.get("sector_relative_strength"), "stock_vs_sector_alpha": alpha, "theme_cluster": theme, "rotation_regime": rotation, "trend_state": state, "trend_data_quality": quality, "weakness_signals": signals, "updated_at": _iso(now)}
    if lifecycle_id and prev.get("lifecycle_id") and prev.get("lifecycle_id") != lifecycle_id:
        for k in list(trend):
            if k.endswith("_pending") or k.endswith("_done") or k.endswith("_order_key") or k.endswith("_trade_date") or k.endswith("_at"):
                trend.pop(k, None)
    trend["lifecycle_id"] = lifecycle_id or trend.get("lifecycle_id")
    if healthy: trend["post_trim_nonrecovery_days"] = 0
    elif once and trend.get("trend_trim_done"): trend["post_trim_nonrecovery_days"] = _i(prev.get("post_trim_nonrecovery_days")) + 1
    for k in ("trend_trim_pending","trend_trim_done","trend_exit_pending","trend_exit_done","time_stop_trim_pending","time_stop_trim_done","time_stop_exit_pending","time_stop_exit_done"):
        trend.setdefault(k, False)
    save_trend_state(symbol, trade_date, trend)
    logger.info("[US_POSITION][TREND_STATE] symbol=%s state=%s final30_absent_streak=%s below_ma20_streak=%s below_ma50_streak=%s holding_trade_days=%s signals=%s action=%s", symbol, state, absent, below20, below50, trend.get("holding_trade_days"), ",".join(signals), "HOLD_BLOCK_ADD" if state in {"UNKNOWN","WARNING"} else state)
    return trend


def choose_trend_time_exit(position: dict, trend: dict, *, pnl_pct: float, orderable_qty: int) -> tuple[str, int, str] | None:
    hold = _i(position.get("holding_trade_days") or trend.get("holding_trade_days"))
    min_hold = int(os.getenv("US_TREND_EXIT_MIN_HOLD_DAYS", "2"))
    if orderable_qty <= 0 or trend.get("trend_state") == "UNKNOWN" or hold < min_hold:
        return None
    st = trend.get("trend_state")
    if st == "EXIT" and not trend.get("trend_exit_done") and not trend.get("trend_exit_pending"):
        return "trend_deterioration_exit", orderable_qty, "trend_exit"
    if st == "TRIM" and not trend.get("trend_trim_done") and not trend.get("trend_trim_pending"):
        q = max(1, min(orderable_qty, int(orderable_qty * float(os.getenv("US_TREND_TRIM_SELL_RATIO", "0.35")))))
        return "trend_deterioration_trim", q, "trend_trim"
    days = int(os.getenv("US_TIME_STOP_DAYS", "20")); grace = int(os.getenv("US_TIME_STOP_GRACE_DAYS", "5")); min_profit = float(os.getenv("US_TIME_STOP_MIN_PROFIT_PCT", "0.03"))
    protected = pnl_pct >= min_profit or (st == "HEALTHY" and _f(trend.get("current_price"), 0) >= _f(trend.get("ma20"), 10**9))
    weak = st in {"WARNING","TRIM","EXIT"} or _i(trend.get("final30_absent_streak")) >= 2
    if not protected and hold >= days + grace and (trend.get("trend_trim_done") or trend.get("time_stop_trim_done")) and st != "HEALTHY" and not trend.get("time_stop_exit_done") and not trend.get("time_stop_exit_pending"):
        return "time_stop_exit", orderable_qty, "time_stop_exit"
    if not protected and hold >= days and weak and not trend.get("time_stop_trim_done") and not trend.get("time_stop_trim_pending") and not trend.get("trend_trim_done"):
        q = max(1, min(orderable_qty, int(orderable_qty * float(os.getenv("US_TIME_STOP_FIRST_SELL_RATIO", "0.50")))))
        return "time_stop_trim", q, "time_stop_trim"
    return None


def filter_add_to_existing_by_trend_state(entry_intents: list[dict], current_positions: list[dict]) -> tuple[list[dict], list[dict]]:
    pos = {str(p.get("symbol") or "").upper(): p for p in current_positions or []}
    kept=[]; blocked=[]
    reason_by_state = {
        "UNKNOWN": "TREND_UNKNOWN",
        "WARNING": "TREND_WARNING",
        "TRIM": "TREND_TRIM",
        "EXIT": "TREND_EXIT",
    }
    for it in entry_intents or []:
        sym = str(it.get("symbol") or "").upper()
        p = pos.get(sym)
        trend_state = (p or {}).get("trend_state") or ((p or {}).get("trend") or {}).get("trend_state")
        if str(it.get("side", "BUY")).upper() == "BUY" and p and trend_state != "HEALTHY":
            reason = reason_by_state.get(str(trend_state or "UNKNOWN"), "TREND_UNKNOWN")
            b = {**it, "block_reason": reason}
            blocked.append(b)
            logger.info("[US_ENTRY][TREND_BLOCK] symbol=%s trend_state=%s signals=%s reason=%s action=BLOCK_ADD_TO_EXISTING", sym, trend_state, ",".join(((p or {}).get("weakness_signals") or [])), reason)
        else:
            kept.append(it)
    return kept, blocked
