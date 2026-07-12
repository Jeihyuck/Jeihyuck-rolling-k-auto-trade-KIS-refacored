# -*- coding: utf-8 -*-
"""Persistent US position lifecycle and high-watermark state."""
from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from typing import Any

from trader.us.db.repos import (
    load_latest_open_us_position_lifecycles,
    load_latest_us_position_risk_state,
    load_us_position_risk_state,
    save_us_position_risk_state,
)

logger = logging.getLogger(__name__)


def _iso(now: datetime) -> str:
    return (now if now.tzinfo else now.replace(tzinfo=timezone.utc)).isoformat()


def _sym(v: Any) -> str:
    return str(v or "").strip().upper()


def _f(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        return x if x == x else default
    except Exception:
        return default


def _i(v: Any, default: int = 0) -> int:
    try:
        return int(float(v or 0))
    except Exception:
        return default


def _new_lifecycle_id(symbol: str, trade_date: str, opened_at: str, entry_price: float, qty: int) -> str:
    raw = f"{symbol}|{trade_date}|{opened_at}|{entry_price:.6f}|{qty}"
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


def _position_entry_price(pos: dict) -> float:
    for k in ("entry_price", "avg_price_usd", "avg_cost", "average_price", "avg_buy_price"):
        v = _f(pos.get(k))
        if v > 0:
            return v
    qty = _i(pos.get("qty"))
    buy_amount = _f(pos.get("buy_amount_usd"))
    return buy_amount / qty if qty > 0 and buy_amount > 0 else 0.0


def _save_lifecycle(symbol: str, trade_date: str, lifecycle: dict, *, base: dict | None = None) -> dict:
    risk = dict(base or load_us_position_risk_state(symbol, trade_date) or {})
    state = dict(risk.get("state") or {})
    state["lifecycle"] = lifecycle
    risk["state"] = state
    save_us_position_risk_state(symbol, trade_date, risk)
    return lifecycle


def reconcile_us_position_lifecycles(*, positions: list[dict], trade_date: str, now: datetime, authoritative: bool) -> dict[str, dict]:
    """Carry open lifecycle state forward and close only on authoritative zero."""
    now_iso = _iso(now)
    current: dict[str, dict] = {_sym(p.get("symbol")): p for p in positions or [] if _sym(p.get("symbol")) and _i(p.get("qty")) > 0}
    out: dict[str, dict] = {}
    open_latest = load_latest_open_us_position_lifecycles(trade_date)

    for symbol, pos in current.items():
        qty = _i(pos.get("qty"))
        entry = _position_entry_price(pos)
        latest = load_latest_us_position_risk_state(symbol, trade_date)
        lifecycle = dict(((latest.get("state") or {}).get("lifecycle") or {}))
        if not lifecycle or lifecycle.get("is_open") is False:
            lifecycle = {
                "lifecycle_id": _new_lifecycle_id(symbol, trade_date, now_iso, entry, qty),
                "is_open": True,
                "opened_trade_date": trade_date,
                "opened_at": now_iso,
                "closed_trade_date": None,
                "closed_at": None,
                "entry_price": entry,
                "first_seen_qty": qty,
                "last_seen_qty": qty,
                "last_seen_trade_date": trade_date,
                "holding_trade_days": 1,
                "last_holding_day_counted": trade_date,
                "high_watermark": max(entry, _f(pos.get("current_price_usd") or pos.get("current_price") or pos.get("current_px"))),
                "high_watermark_at": now_iso,
                "high_watermark_source": "us_position_risk_state",
            }
        else:
            lifecycle["last_seen_qty"] = qty
            lifecycle["last_seen_trade_date"] = trade_date
            if lifecycle.get("last_holding_day_counted") != trade_date:
                lifecycle["holding_trade_days"] = _i(lifecycle.get("holding_trade_days"), 1) + 1
                lifecycle["last_holding_day_counted"] = trade_date
        _save_lifecycle(symbol, trade_date, lifecycle)
        pos.update({
            "position_lifecycle_id": lifecycle.get("lifecycle_id"),
            "opened_trade_date": lifecycle.get("opened_trade_date"),
            "holding_trade_days": lifecycle.get("holding_trade_days"),
            "lifecycle_state_source": "us_position_risk_state",
        })
        out[symbol] = lifecycle
        logger.info("[US_POSITION][LIFECYCLE] symbol=%s lifecycle_id=%s is_open=%d opened_trade_date=%s holding_trade_days=%s", symbol, lifecycle.get("lifecycle_id"), 1, lifecycle.get("opened_trade_date"), lifecycle.get("holding_trade_days"))

    if authoritative:
        for symbol, lifecycle in (open_latest or {}).items():
            if symbol in current:
                continue
            closed = dict(lifecycle)
            closed.update({"is_open": False, "closed_trade_date": trade_date, "closed_at": now_iso, "last_seen_qty": 0})
            _save_lifecycle(symbol, trade_date, closed)
            out[symbol] = closed
            logger.info("[US_POSITION][LIFECYCLE] symbol=%s lifecycle_id=%s is_open=0 opened_trade_date=%s holding_trade_days=%s", symbol, closed.get("lifecycle_id"), closed.get("opened_trade_date"), closed.get("holding_trade_days"))
    return out


def update_us_position_high_watermark(*, symbol: str, trade_date: str, lifecycle_id: str, current_price: float, entry_price: float, now: datetime) -> dict:
    symbol = _sym(symbol)
    latest = load_latest_us_position_risk_state(symbol, trade_date)
    lifecycle = dict(((latest.get("state") or {}).get("lifecycle") or {}))
    now_iso = _iso(now)
    same = lifecycle.get("lifecycle_id") == lifecycle_id
    previous = _f(lifecycle.get("high_watermark")) if same else 0.0
    new_high = max(previous, _f(current_price), _f(entry_price)) if same else max(_f(current_price), _f(entry_price))
    lifecycle.update({"lifecycle_id": lifecycle_id, "high_watermark": new_high, "high_watermark_at": now_iso if new_high != previous else lifecycle.get("high_watermark_at") or now_iso, "high_watermark_source": "us_position_risk_state"})
    if "is_open" not in lifecycle:
        lifecycle["is_open"] = True
    _save_lifecycle(symbol, trade_date, lifecycle)
    logger.info("[US_POSITION][HIGH_WATERMARK] symbol=%s lifecycle_id=%s previous=%.4f current=%.4f saved=%.4f source=us_position_risk_state", symbol, lifecycle_id, previous, current_price, new_high)
    return lifecycle
