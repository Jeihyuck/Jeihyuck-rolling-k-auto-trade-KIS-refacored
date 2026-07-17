"""Fail-closed identity primitives shared by every US order path."""
from __future__ import annotations

import hashlib
import re
from datetime import date
from typing import Any


class InvalidOrderIdentity(ValueError):
    code = "INVALID_ORDER_IDENTITY"


class OrderIdentityCollision(RuntimeError):
    code = "ORDER_IDENTITY_COLLISION"


_BLANKS = {"", "none", "null"}


def valid_identity(value: Any) -> bool:
    return value is not None and str(value).strip().lower() not in _BLANKS


def require_identity(value: Any, field: str = "client_order_key") -> str:
    if not valid_identity(value):
        raise InvalidOrderIdentity(f"{field} must be a non-blank durable identity")
    return str(value).strip()


def _token(value: Any, fallback: str = "NA") -> str:
    text = re.sub(r"[^A-Z0-9_.-]+", "-", str(value or fallback).upper().strip())
    return text or fallback


def deterministic_order_key(intent: dict, context: Any | None = None) -> str:
    """Build an idempotent key from position state, never wall clock/tick number."""
    meta = intent.get("meta") if isinstance(intent.get("meta"), dict) else {}
    td = intent.get("trade_date") or getattr(context, "trade_date", None) or date.today().isoformat()
    symbol = _token(intent.get("symbol"))
    side = _token(intent.get("side"))
    lifecycle = _token(intent.get("position_lifecycle_id") or meta.get("position_lifecycle_id"))
    pre_qty = intent.get("pre_order_position_qty", meta.get("pre_order_position_qty", meta.get("pre_qty", "NA")))
    qty = intent.get("qty", intent.get("quantity", 0))
    reason = _token(intent.get("reason") or meta.get("reason") or intent.get("strategy"))
    state = _token(intent.get("market_state") or meta.get("market_state"))
    prefix = "US_DEF" if reason.startswith("DEFENSE_") else "US_ORD"
    raw = f"{prefix}_{td}_{symbol}_{lifecycle}_{state}_{pre_qty}_{qty}_{side}_{reason}"
    # Keep readable keys while bounding DB/index sizes deterministically.
    if len(raw) <= 180:
        return raw
    return raw[:139] + "_" + hashlib.sha256(raw.encode()).hexdigest()[:40]


def normalize_and_validate_order_identity(intent: dict, context: Any | None = None) -> dict:
    normalized = dict(intent or {})
    meta = dict(normalized.get("meta") or {})
    key = normalized.get("client_order_key") or normalized.get("order_key")
    if not valid_identity(key):
        key = deterministic_order_key(normalized, context)
    normalized["client_order_key"] = require_identity(key)
    normalized["trade_date"] = str(normalized.get("trade_date") or getattr(context, "trade_date", "") or date.today().isoformat())
    normalized["symbol"] = require_identity(normalized.get("symbol"), "symbol").upper()
    side = require_identity(normalized.get("side"), "side").upper()
    if side not in {"BUY", "SELL"}:
        raise InvalidOrderIdentity("side must be BUY or SELL")
    normalized["side"] = side
    normalized["qty"] = int(normalized.get("qty", normalized.get("quantity", 0)) or 0)
    for field in ("trade_date", "session", "session_run_id", "session_generation", "tick_id", "prep_run_id",
                  "position_lifecycle_id", "pre_order_position_qty"):
        value = normalized.get(field, meta.get(field))
        if value is None and context is not None:
            value = getattr(context, field, None)
        if value is not None:
            normalized[field] = value
            meta[field] = value
    normalized["meta"] = meta
    return normalized


def assert_same_identity(existing: dict, incoming: dict) -> None:
    for field in ("trade_date", "symbol", "side", "exchange", "position_lifecycle_id"):
        old = existing.get(field) or (existing.get("meta") or {}).get(field)
        new = incoming.get(field) or (incoming.get("meta") or {}).get(field)
        if old not in (None, "") and new not in (None, "") and str(old).upper() != str(new).upper():
            raise OrderIdentityCollision(f"{field} collision: {old!r} != {new!r}")
