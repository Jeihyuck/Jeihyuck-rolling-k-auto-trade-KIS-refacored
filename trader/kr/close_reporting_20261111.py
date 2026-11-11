"""Read-only KR CLOSE reporting helpers; never usable as an order permit.

A recent broker-originated, same-session balance already saved during CLOSE can
satisfy a *post-execution reporting* consistency check when this runner sent
zero orders.  It must not be reused for a later BUY/SELL decision and cannot
replace a broker refresh after any potentially submitted order.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

_KST = ZoneInfo("Asia/Seoul")


def close_economic_warning(pb1_result: Mapping[str, Any] | None) -> str | None:
    """Economic settlement is independent of the close runner's exit code."""
    result = pb1_result or {}
    if result.get("economic_settlement_state") == "ECONOMIC_SETTLEMENT_PENDING":
        return "ECONOMIC_SETTLEMENT_PENDING"
    if result.get("economic_settlement_pending") is True or result.get("economic_settlement_pending") == 1:
        return "ECONOMIC_SETTLEMENT_PENDING"
    try:
        gaps = result.get("native_priced_sell_gap_count")
        if gaps is not None and not isinstance(gaps, bool) and int(gaps) > 0:
            return "ECONOMIC_SETTLEMENT_PENDING"
    except (TypeError, ValueError):
        # Unknown native pricing evidence must not certify economic completion.
        return "ECONOMIC_SETTLEMENT_UNVERIFIED"
    return None


def reuse_close_balance_for_reporting(
    path: str | Path,
    *,
    new_orders_submitted: int,
    now: datetime | None = None,
    max_age_sec: float = 120.0,
) -> dict[str, Any] | None:
    """Use a checked raw KIS snapshot for read-only close reporting only.

    Zero submitted orders must be known, not assumed from a missing result.
    This snapshot is *not* a KIS orderable-quantity or real-time risk permit.
    """
    if type(new_orders_submitted) is not int or new_orders_submitted != 0:
        return None
    try:
        record = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(record, dict):
            return None
        if (
            record.get("state") != "OK"
            or record.get("source") != "KIS"
            or record.get("raw_snapshot_available") is not True
        ):
            return None
        checked_at = datetime.fromisoformat(str(record["checked_at_kst"]))
        if checked_at.tzinfo is None:
            checked_at = checked_at.replace(tzinfo=_KST)
        current = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            return None
        age = (current.astimezone(timezone.utc) - checked_at.astimezone(timezone.utc)).total_seconds()
        if not (0 <= age <= max_age_sec):
            return None
        raw = record.get("raw_snapshot")
        if not isinstance(raw, dict) or not isinstance(raw.get("output1"), list) or not isinstance(raw.get("output2"), dict):
            return None
        for holding in raw["output1"]:
            if not isinstance(holding, dict):
                return None
            code = str(holding.get("pdno") or "")
            qty = str(holding.get("hldg_qty") or "")
            if len(code) != 6 or not code.isdecimal() or not qty.isdecimal():
                return None
        return raw
    except (OSError, ValueError, KeyError, TypeError):
        return None
