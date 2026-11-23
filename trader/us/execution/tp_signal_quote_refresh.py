"""Bounded, source-timestamped refresh for US_STANDARD TP *decisions*.

The PB1 exit snapshot may be tens of seconds old by the time TP evaluates.
Refresh only a price decision input; do not mutate BUY contracts, stop state,
pending stages, router authority, or KIS order/fill records.  A second fresh
native broker quote remains mandatory in the canonical order router.
"""
from __future__ import annotations

import logging
import math
import os
import time
from datetime import datetime, timezone
from typing import Any

from trader.us.symbols import normalize_us_exchange

logger = logging.getLogger(__name__)

SOURCE_TTL_SEC = 5.0


def _source_epoch(raw: Any) -> float | None:
    if isinstance(raw, bool) or raw in (None, ""):
        return None
    try:
        if isinstance(raw, (int, float)):
            value = float(raw)
        else:
            value = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            if value.tzinfo is None or value.utcoffset() is None:
                return None
            value = value.astimezone(timezone.utc).timestamp()
    except (TypeError, ValueError, OSError, OverflowError):
        return None
    return value if math.isfinite(value) and value > 0 else None


def _quote_epoch(position: dict) -> float | None:
    proof = position.get("exit_quote_evidence")
    if not isinstance(proof, dict):
        proof = {}
    for raw in (
        position.get("resolved_quote_asof"),
        position.get("quote_source_at"),
        proof.get("asof_epoch"),
        proof.get("asof"),
    ):
        value = _source_epoch(raw)
        if value is not None:
            return value
    return None


def refresh_tp_source_quotes(
    positions: list[dict], provider: Any, *,
    budget_sec: float = 3.0, max_reads: int = 4,
    exclude_symbols: set[str] | None = None,
) -> list[dict]:
    """Return copies with a verified fresh source quote, or original rows.

    Only the practice isolated read-only KIS provider with a shared limiter
    may refresh; an unsafe provider or missing source timestamp cannot turn
    a stale quote into a fresh one. The original snapshot and stop HWM are
    never touched. A bounded overall deadline is shared across the batch.
    """
    rows = list(positions or [])
    reader = getattr(provider, "get_isolated_exit_quote", None)
    if (
        os.getenv("KIS_ENV", "practice").lower() != "practice"
        or os.getenv("US_SHARED_KIS_LIMITER_ENABLED", "0") != "1"
        or not callable(reader)
    ):
        return rows
    try:
        budget = max(0.0, min(float(budget_sec), 5.0))
        reads_allowed = max(0, min(int(max_reads), 12))
    except (ValueError, TypeError):
        return rows
    if budget <= 0 or reads_allowed <= 0:
        return rows
    deadline = time.monotonic() + budget
    excluded = {str(s).upper() for s in (exclude_symbols or set())}
    refreshed: list[dict] = []
    reads = 0
    for row in rows:
        if not isinstance(row, dict):
            refreshed.append(row)
            continue
        owner = str(
            row.get("strategy_owner") or row.get("owner_strategy")
            or row.get("sleeve_id") or ""
        ).upper()
        symbol = str(row.get("symbol") or "").strip().upper()
        try:
            held_qty = int(row.get("qty") or row.get("holding_qty") or 0)
        except (ValueError, TypeError, OverflowError):
            held_qty = 0
        if (owner != "US_STANDARD" or symbol == "TQQQ" or not symbol
                or held_qty <= 0 or symbol in excluded):
            refreshed.append(row)
            continue

        existing_at = _quote_epoch(row)
        if existing_at is not None and 0 <= time.time() - existing_at <= SOURCE_TTL_SEC:
            refreshed.append(row)
            continue
        if reads >= reads_allowed or time.monotonic() >= deadline:
            refreshed.append(row)
            continue
        try:
            exchange = normalize_us_exchange(row.get("exchange") or "NASDAQ")
        except ValueError:
            refreshed.append(row)
            continue
        reads += 1
        try:
            price_quote = reader(symbol, exchange, stage_deadline=deadline)
        except Exception as exc:
            logger.warning(
                "[US_TP][SOURCE_REFRESH_FAILED] symbol=%s type=%s action=retain_stale_block",
                symbol, type(exc).__name__,
            )
            refreshed.append(row)
            continue
        if not isinstance(price_quote, dict):
            refreshed.append(row)
            continue
        source_at = _source_epoch(
            price_quote.get("_quote_asof_epoch")
            or price_quote.get("asof_epoch")
            or price_quote.get("asof")
        )
        try:
            price = float(price_quote.get("last") or price_quote.get("price") or 0)
        except (TypeError, ValueError, OverflowError):
            price = 0.0
        now_epoch = time.time()
        source = str(price_quote.get("_quote_source") or price_quote.get("source") or "")
        quality = str(price_quote.get("_quote_quality") or price_quote.get("quality") or "").upper()
        if not (
            source_at is not None
            and 0 <= now_epoch - source_at <= SOURCE_TTL_SEC
            and math.isfinite(price) and price > 0
            and quality == "FRESH"
            and not price_quote.get("stale")
            and not price_quote.get("suspect")
            and not price_quote.get("_stale_date")
            and source not in {"", "UNKNOWN"}
        ):
            logger.warning(
                "[US_TP][SOURCE_REFRESH_UNVERIFIED] symbol=%s source=%s quality=%s action=retain_stale_block",
                symbol, source or "UNKNOWN", quality or "UNKNOWN",
            )
            refreshed.append(row)
            continue
        updated = dict(row)
        updated.update({
            "current_price": price,
            "current_price_usd": price,
            "resolved_current_price": price,
            "resolved_quote_source": source,
            "resolved_quote_asof": source_at,
            "resolved_quote_received_at": datetime.now(timezone.utc).isoformat(),
            "exit_quote_evidence": {
                "price": price, "source": source, "asof": None,
                "asof_epoch": source_at,
                "received_at_utc": datetime.now(timezone.utc).isoformat(),
                "quality": "FRESH",
            },
        })
        refreshed.append(updated)
        logger.info(
            "[US_TP][SOURCE_REFRESH_OK] symbol=%s source=%s age_sec=%.3f action=tp_recheck_only",
            symbol, source, now_epoch - source_at,
        )
    return refreshed
