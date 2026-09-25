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

_ENTRY_POLICY_FIELDS = (
    "book", "horizon", "exit_policy", "entry_strategy",
    "entry_signal_type", "partial_exit_allowed",
    "entry_exit_contract", "entry_exit_contract_sha256", "entry_exit_contract_version",
    "entry_reason", "entry_style_selected", "entry_style_raw", "entry_component",
    "reasons", "filters_passed", "score_breakdown", "explanation_quality",
)


def _entry_policy(pos: dict) -> dict:
    meta = pos.get("meta") if isinstance(pos.get("meta"), dict) else {}
    return {
        field: pos.get(field) if pos.get(field) is not None else meta.get(field)
        for field in _ENTRY_POLICY_FIELDS
        if (pos.get(field) if pos.get(field) is not None else meta.get(field)) is not None
    }


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


def _confirmed_buy_opened_at(
    fills: list[dict] | None,
    symbol: str,
    *,
    after: str | datetime | None = None,
) -> str | None:
    """Return earliest confirmed BUY fill for the new lifecycle.

    When a prior lifecycle closed on the same trade date, ignore fills from the
    old cycle by requiring the BUY timestamp to be strictly after closed_at.
    """
    lower_bound: datetime | None = None
    if after:
        try:
            if isinstance(after, datetime):
                lower_bound = after if after.tzinfo else after.replace(tzinfo=timezone.utc)
            else:
                lower_bound = datetime.fromisoformat(str(after).replace("Z", "+00:00"))
                if lower_bound.tzinfo is None:
                    lower_bound = lower_bound.replace(tzinfo=timezone.utc)
            lower_bound = lower_bound.astimezone(timezone.utc)
        except Exception:
            lower_bound = None

    candidates: list[tuple[datetime, str]] = []
    for fill in fills or []:
        if _sym(fill.get("symbol")) != _sym(symbol) or str(fill.get("side") or "").upper() != "BUY":
            continue
        meta = fill.get("meta") if isinstance(fill.get("meta"), dict) else {}
        raw = fill.get("filled_at") or fill.get("execution_timestamp") or meta.get("observed_at")
        if not raw:
            continue
        try:
            if isinstance(raw, datetime):
                dt = raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
                text_value = dt.isoformat()
            else:
                text_value = str(raw)
                dt = datetime.fromisoformat(text_value.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
            dt_utc = dt.astimezone(timezone.utc)
            if lower_bound is not None and dt_utc <= lower_bound:
                continue
            candidates.append((dt_utc, text_value))
        except Exception:
            continue
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    return candidates[0][1]


def _save_lifecycle(symbol: str, trade_date: str, lifecycle: dict, *, base: dict | None = None) -> dict:
    risk = dict(base or load_us_position_risk_state(symbol, trade_date) or {})
    state = dict(risk.get("state") or {})
    state["lifecycle"] = lifecycle
    risk["state"] = state
    save_us_position_risk_state(symbol, trade_date, risk)
    return lifecycle


def reconcile_us_position_lifecycles(*, positions: list[dict], trade_date: str, now: datetime, authoritative: bool, fills: list[dict] | None = None) -> dict[str, dict]:
    """Carry open lifecycle state forward and close only on authoritative zero."""
    now_iso = _iso(now)
    current: dict[str, dict] = {_sym(p.get("symbol")): p for p in positions or [] if _sym(p.get("symbol")) and _i(p.get("qty")) > 0}
    out: dict[str, dict] = {}
    open_latest = load_latest_open_us_position_lifecycles(trade_date)

    for symbol, pos in current.items():
        qty = _i(pos.get("qty"))
        entry = _position_entry_price(pos)
        position_policy = _entry_policy(pos)
        latest = load_latest_us_position_risk_state(symbol, trade_date)
        lifecycle = dict(((latest.get("state") or {}).get("lifecycle") or {}))
        if not lifecycle or lifecycle.get("is_open") is False:
            confirmed_opened_at = _confirmed_buy_opened_at(
                fills, symbol, after=lifecycle.get("closed_at") if lifecycle else None,
            )
            opened_at = confirmed_opened_at or now_iso
            lifecycle = {
                "lifecycle_id": _new_lifecycle_id(symbol, trade_date, opened_at, entry, qty),
                "is_open": True,
                "opened_trade_date": trade_date,
                "opened_at": opened_at,
                "opened_at_source": "confirmed_buy_fill" if confirmed_opened_at else "first_authoritative_position_observation",
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
                "entry_policy": position_policy,
            }
        else:
            lifecycle["last_seen_qty"] = qty
            lifecycle["last_seen_trade_date"] = trade_date
            if lifecycle.get("last_holding_day_counted") != trade_date:
                lifecycle["holding_trade_days"] = _i(lifecycle.get("holding_trade_days"), 1) + 1
                lifecycle["last_holding_day_counted"] = trade_date
            if not lifecycle.get("entry_policy") and position_policy:
                lifecycle["entry_policy"] = position_policy
        carried_policy = dict(lifecycle.get("entry_policy") or {})
        _save_lifecycle(symbol, trade_date, lifecycle)
        pos.update({
            "position_lifecycle_id": lifecycle.get("lifecycle_id"),
            "opened_trade_date": lifecycle.get("opened_trade_date"),
            "opened_at": lifecycle.get("opened_at"),
            "opened_at_source": lifecycle.get("opened_at_source"),
            "holding_trade_days": lifecycle.get("holding_trade_days"),
            "lifecycle_state_source": "us_position_risk_state",
            **carried_policy,
        })
        pos_meta = dict(pos.get("meta") or {})
        pos_meta.update(carried_policy)
        pos["meta"] = pos_meta
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
