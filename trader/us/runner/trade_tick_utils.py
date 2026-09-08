# -*- coding: utf-8 -*-
"""Small pure helpers for the US trade tick runner."""
from __future__ import annotations

import json
from typing import Any


_TRANSIENT_WATCHLIST_DB_ERROR_PATTERNS = (
    "edbhandlerexited",
    "connection to database closed",
    "server closed the connection",
    "statement timeout",
    "canceling statement due to statement timeout",
    "operationalerror",
    "internalerror",
    "connection already closed",
    "ssl syscall error",
    "terminating connection",
)


def safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def normalize_kis_endpoint_name(name: str) -> str:
    text = str(name or "").strip()
    if text == "GET_inquire_balance":
        return "GET_inquire-balance"
    return text


def fill_is_synthetic(fill: dict) -> bool:
    meta = fill.get("meta") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except Exception:
            meta = {}
    evidence = str(meta.get("fill_evidence_type") or fill.get("fill_evidence_type") or "")
    source = str(fill.get("source") or fill.get("reconcile_source") or meta.get("source") or "").lower()
    return bool(
        meta.get("is_synthetic")
        or meta.get("synthetic")
        or meta.get("synthetic_fill")
        or evidence in {"BALANCE_DELTA_SYNTHETIC", "LEGACY_SYNTHETIC"}
        or "balance_reconcile" in source
        or "synthetic" in source
    )


def fill_notional_usd(fill: dict) -> float:
    meta = fill.get("meta") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except Exception:
            meta = {}
    direct = (
        fill.get("notional_usd")
        or fill.get("fill_notional_usd")
        or meta.get("notional_usd")
        or meta.get("fill_notional_usd")
        or meta.get("actual_fill_notional_usd")
    )
    if direct not in (None, ""):
        return safe_float(direct)
    qty = safe_float(
        fill.get("qty")
        or fill.get("filled_qty")
        or fill.get("cumulative_filled_qty")
        or fill.get("ft_ccld_qty")
        or meta.get("qty")
    )
    price = safe_float(
        fill.get("fill_price")
        or fill.get("avg_price_usd")
        or fill.get("avg_price")
        or fill.get("ft_ccld_unpr3")
        or meta.get("fill_price")
        or meta.get("avg_price_usd")
    )
    return qty * price if qty > 0 and price > 0 else 0.0


def aggregate_fill_notionals(fills: list[dict]) -> tuple[float, float]:
    buy_total = 0.0
    sell_total = 0.0
    for fill in fills or []:
        if fill_is_synthetic(fill):
            continue
        side = str(fill.get("side") or "").upper()
        notional = fill_notional_usd(fill)
        if side == "BUY":
            buy_total += notional
        elif side == "SELL":
            sell_total += notional
    return buy_total, sell_total


def is_transient_watchlist_db_error(exc: BaseException) -> bool:
    text = f"{type(exc).__name__}: {exc}".lower()
    return any(pattern in text for pattern in _TRANSIENT_WATCHLIST_DB_ERROR_PATTERNS)
