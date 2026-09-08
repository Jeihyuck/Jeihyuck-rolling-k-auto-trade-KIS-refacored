from __future__ import annotations

import json
from datetime import date
from typing import Any

import pandas as pd

from trader.position_age import to_kst_date


def calendar_days_held(entry_ts: Any, trade_date: date) -> int:
    entry_date = to_kst_date(entry_ts)
    if entry_date is None:
        return 0
    return max(0, (trade_date - entry_date).days)


def classify_trade_horizon(features: dict[str, Any]) -> str:
    entry_style = str(
        features.get("entry_style_selected") or features.get("entry_reason") or ""
    ).upper()
    score_final = float(features.get("score_final") or features.get("score") or 0)
    atr_pct = float(features.get("atr_pct") or 0)
    breakout = bool(features.get("breakout_signal") or features.get("pivot_breakout"))
    vcp_score = float(features.get("vcp_score") or 0)
    trend_ok = bool(features.get("trend_template_ok") or features.get("minervini_ok"))

    if entry_style in {"ENTRY_BREAKOUT", "ENTRY_MOMENTUM", "ENTRY_OPEN_PUSH"}:
        return "DAY_PROTECT"
    if breakout and atr_pct >= 5.0:
        return "DAY_PROTECT"
    if score_final >= 85 and trend_ok and atr_pct <= 4.0:
        return "CORE_CARRY"
    if entry_style in {"ENTRY_PULLBACK", "ENTRY_VCP", "ENTRY_MINERVINI"}:
        return "SWING_CARRY"
    if trend_ok and vcp_score >= 45:
        return "SWING_CARRY"
    return "SWING_CARRY"


def horizon_to_exit_family(horizon: str) -> str:
    return {
        "DAY_PROTECT": "INTRADAY_PROFIT_PROTECT",
        "SWING_CARRY": "SWING_STAGED_EXIT",
        "CORE_CARRY": "CORE_TREND_FOLLOW",
    }.get(horizon, "SWING_STAGED_EXIT")


def resolve_position_horizon(pos: dict[str, Any], now_kst_date: Any | None = None) -> str:
    meta = pos.get("position_meta") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except Exception:
            meta = {}
    horizon = str(meta.get("trade_horizon") or "").strip()
    if horizon in {"DAY_PROTECT", "SWING_CARRY", "CORE_CARRY"}:
        return horizon

    entry_meta = pos.get("entry_meta_json") or {}
    if isinstance(entry_meta, str):
        try:
            entry_meta = json.loads(entry_meta)
        except Exception:
            entry_meta = {}
    horizon = str(entry_meta.get("trade_horizon") or "").strip()
    if horizon in {"DAY_PROTECT", "SWING_CARRY", "CORE_CARRY"}:
        return horizon

    if now_kst_date is not None:
        entry_date_raw = pos.get("entry_date") or pos.get("last_fill_at") or pos.get("entry_ts")
        if entry_date_raw:
            try:
                entry_d = pd.Timestamp(entry_date_raw).date()
                if entry_d == now_kst_date:
                    return "DAY_PROTECT"
            except Exception:
                pass

    return "SWING_CARRY"


def resolve_position_book(pos: dict[str, Any]) -> str:
    entry_meta = pos.get("entry_meta_json") or {}
    if isinstance(entry_meta, str):
        try:
            entry_meta = json.loads(entry_meta)
        except Exception:
            entry_meta = {}
    book = str(entry_meta.get("book") or "").strip()
    if book in {"SWING_BOOK", "DAY_BOOK", "CORE_BOOK"}:
        return book

    meta = pos.get("position_meta") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except Exception:
            meta = {}
    book = str(meta.get("book") or "").strip()
    if book in {"SWING_BOOK", "DAY_BOOK", "CORE_BOOK"}:
        return book

    horizon = str(entry_meta.get("trade_horizon") or meta.get("trade_horizon") or "").strip()
    if horizon == "DAY_PROTECT":
        return "DAY_BOOK"
    if horizon == "CORE_CARRY":
        return "CORE_BOOK"
    if horizon == "SWING_CARRY":
        return "SWING_BOOK"
    return "SWING_BOOK"


def calculate_exit_qty(holding_qty: int, orderable_qty: int, sell_pct: float | None) -> int:
    orderable = max(0, int(orderable_qty or 0))
    if sell_pct is None:
        return orderable
    qty = int(orderable * float(sell_pct))
    if qty < 1 and orderable > 0:
        qty = 1
    return min(qty, orderable)
