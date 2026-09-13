# -*- coding: utf-8 -*-
"""Strict persistent-state lookups for BUY-side US risk gates.

Unlike the general repository helpers, these functions never fall back to
in-memory/empty state.  They are intentionally used only where an unknown DB
state must fail closed before a new BUY can be routed.
"""
from __future__ import annotations

from datetime import date

from sqlalchemy import text

from trader.us.db import repos


DEFAULT_PENDING_STATUSES = {
    "ACK",
    "SUBMITTED",
    "PENDING",
    "PARTIALLY_FILLED",
    "RECONCILE_PENDING",
    "ACK_DB_FAILED",
}


def _persistent_engine():
    engine = repos._get_engine_or_none()
    if engine is None:
        raise RuntimeError("us_persistent_db_unavailable")
    return engine


def load_today_symbols_sold_strict(trade_date: str | None = None) -> set[str]:
    """Return confirmed same-day SELL symbols or raise when DB truth is unknown."""
    td = trade_date or date.today().isoformat()
    engine = _persistent_engine()
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """SELECT DISTINCT f.symbol FROM us_fills f LEFT JOIN us_orders o
                    ON o.trade_date=f.trade_date AND o.client_order_key=f.client_order_key
                    WHERE f.trade_date=:td AND f.side='SELL' AND (o.id IS NULL OR o.status='FILLED')"""
            ),
            {"td": td},
        )
        return {r[0] for r in rows}


def has_pending_order_for_symbol_side_strict(
    symbol: str,
    side: str,
    trade_date: str | None = None,
    include_statuses: set[str] | None = None,
) -> bool:
    """Return persistent pending-order truth or raise when it cannot be proven."""
    statuses = include_statuses or DEFAULT_PENDING_STATUSES
    td = trade_date or date.today().isoformat()
    sym = str(symbol or "").strip().upper()
    side_u = str(side or "").strip().upper()
    engine = _persistent_engine()
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT 1 FROM us_orders
                WHERE symbol=:symbol AND side=:side AND trade_date=:td
                  AND status = ANY(:statuses)
                  AND dry_run = FALSE
                LIMIT 1
                """
            ),
            {"symbol": sym, "side": side_u, "td": td, "statuses": list(statuses)},
        ).first()
        return row is not None
