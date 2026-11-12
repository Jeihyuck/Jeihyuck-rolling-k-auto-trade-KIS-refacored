"""Read-only KIS native economic-fill evidence, order-scoped and source-aware.

Does not equate ACK, RECONCILED status, submitted quantity or an order-time
proxy with an individual execution. The writer persists KIS cumulative snapshots
under one stable KR_KIS_CUM identity; individual IDs must come from the broker.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping
import json

NATIVE_CUMULATIVE = "KIS_ORDER_CUMULATIVE_EXECUTION"
NATIVE_INDIVIDUAL = "KIS_INDIVIDUAL_EXECUTION"
SUPPORTED = frozenset((NATIVE_CUMULATIVE, NATIVE_INDIVIDUAL))


def _obj(value: Any) -> dict:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            item = json.loads(value)
            return item if isinstance(item, dict) else {}
        except (ValueError, TypeError):
            pass
    return {}


def _positive_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        number = Decimal(str(value))
        if not number.is_finite() or number <= 0 or number != number.to_integral_value():
            return 0
        return int(number)
    except (InvalidOperation, ValueError, TypeError):
        return 0


def _price(value: Any) -> bool:
    try:
        number = Decimal(str(value))
        return number.is_finite() and number > 0
    except (InvalidOperation, ValueError, TypeError):
        return False


def _odno(value: Any) -> str:
    s = str(value or "").strip()
    return s.lstrip("0") if s.isascii() and s.isdecimal() and int(s) > 0 else ""


def _scope_missing(order: Mapping[str, Any]) -> list[str]:
    needed = ("order_id", "env", "strategy", "market", "code",
              "trading_epoch_id", "portfolio_epoch_id", "position_cycle_id")
    missing = [key for key in needed if not str(order.get(key) or "").strip()]
    if not _odno(order.get("kis_odno")):
        missing.append("kis_odno")
    return missing


def summarize_native_order(order: Mapping[str, Any], rows: list[dict]) -> dict:
    """Prove quantity independently for ONE KIS order; never sum order qty.

    Quantity of an order-cumulative row is the latest observed watermark;
    individual trade rows are deduplicated by exact native trade id. Mixed
    sources and identity collisions remain unproven rather than double-counted.
    """
    issues: set[str] = set()
    missing = _scope_missing(order)
    if missing:
        issues.add("ORDER_SCOPE_INCOMPLETE:" + ",".join(sorted(missing)))
    if not str(order.get("side") or "") in ("BUY", "SELL"):
        issues.add("ORDER_SIDE_INVALID")
    requested = _positive_int(order.get("qty"))
    if not requested:
        issues.add("ORDER_REQUEST_QTY_INVALID")
    related = [row for row in rows
               if str(row.get("order_id") or "") == str(order.get("order_id") or "")
               and str(row.get("order_id") or "").strip()]
    cum: dict[str, list[dict]] = {}
    indiv: dict[str, dict] = {}
    time_quality = "NO_FILL"
    fill_notional = Decimal("0")
    for row in related:
        bad = []
        for key in ("env", "code", "market", "side", "trading_epoch_id",
                    "portfolio_epoch_id", "position_cycle_id"):
            if not str(row.get(key) or "").strip():
                bad.append(key + ":MISSING")
            elif str(row.get(key)) != str(order.get(key)):
                bad.append(key + ":MISMATCH")
        if not _odno(row.get("kis_odno")) or _odno(row.get("kis_odno")) != _odno(order.get("kis_odno")):
            bad.append("kis_odno:MISMATCH_OR_MISSING")
        if bad:
            issues.add("FILL_IDENTITY_INVALID:" + ",".join(sorted(bad)))
            continue
        meta = _obj(row.get("fill_meta_json"))
        kind = str(meta.get("evidence_kind") or "")
        if kind not in SUPPORTED:
            issues.add("FILL_NATIVE_EVIDENCE_KIND_UNPROVEN")
            continue
        qty = _positive_int(row.get("qty"))
        if not qty or not _price(row.get("price")):
            issues.add("FILL_PRICE_OR_QTY_INVALID")
            continue
        if not row.get("filled_at"):
            issues.add("FILL_EXECUTION_TIME_MISSING")
            continue
        try:
            fill_notional += Decimal(str(row.get("price"))) * Decimal(qty)
        except (ValueError, TypeError, InvalidOperation):
            issues.add("FILL_NOTIONAL_INVALID")
            continue
        source = str(meta.get("filled_at_source") or meta.get("filled_at_confidence") or "")
        if source == "KIS_EXECUTION_TIMESTAMP":
            time_quality = "NATIVE_EXECUTION_TIMESTAMP"
        else:
            time_quality = "ORDER_TIME_PROXY_OR_UNKNOWN"
        if kind == NATIVE_CUMULATIVE:
            proof_id = str(row.get("broker_fill_id") or row.get("trade_id") or "")
            if not proof_id or not proof_id.startswith("KR_KIS_CUM:"):
                issues.add("CUMULATIVE_WATERMARK_KEY_MISSING")
                continue
            cum.setdefault(proof_id, []).append(row)
        else:
            proof_id = str(row.get("broker_fill_id") or row.get("trade_id") or "")
            if not proof_id or proof_id.startswith("KR_KIS_CUM:"):
                issues.add("NATIVE_INDIVIDUAL_TRADE_ID_MISSING")
                continue
            old = indiv.get(proof_id)
            if old and (_positive_int(old.get("qty")) != qty
                        or str(old.get("price")) != str(row.get("price"))
                        or str(old.get("filled_at")) != str(row.get("filled_at"))):
                issues.add("CONFLICTING_BROKER_TRADE_ID")
                continue
            indiv[proof_id] = row
    if len(cum) > 1:
        issues.add("MULTIPLE_CUMULATIVE_PROOF_IDS_SAME_ORDER")
    if cum and indiv:
        issues.add("MIXED_CUMULATIVE_AND_INDIVIDUAL_FILL_PROOF")
    watermark = 0
    for proof_rows in cum.values():
        valid_qty = [_positive_int(x.get("qty")) for x in proof_rows]
        watermark = max(watermark, max(valid_qty, default=0))
        # A single cumulative proof identity can have watermark revisions.
        # A duplicated row is not a second execution. If two disagree on
        # the same recorded_at, evidence ordering must be investigated.
        by_time: dict[str, set[int]] = {}
        for row in proof_rows:
            ts = str(row.get("created_at") or row.get("updated_at") or "")
            if ts:
                by_time.setdefault(ts, set()).add(_positive_int(row.get("qty")))
        if any(len(v) > 1 for v in by_time.values()):
            issues.add("CONFLICTING_CUMULATIVE_WATERMARK_TIMESTAMP")
    cum_qty = watermark if len(cum) == 1 else 0
    individual_qty = sum(_positive_int(row.get("qty")) for row in indiv.values())
    qty = (cum_qty if cum else individual_qty) if not (cum and indiv) else None
    if any(x.startswith("ORDER_SCOPE_INCOMPLETE") or x.startswith("FILL_IDENTITY_INVALID") for x in issues):
        qty = None
    if ("CONFLICTING_BROKER_TRADE_ID" in issues or "MULTIPLE_CUMULATIVE_PROOF_IDS_SAME_ORDER" in issues):
        qty = None
    if qty is not None and requested and qty > requested:
        issues.add("KIS_FILL_EXCEEDS_ORDER_REQUEST")
        qty = None
    if qty is None:
        evidence_status = "CONFLICT"
    elif qty == 0:
        evidence_status = "UNPROVEN"
        issues.add("ORIGINAL_NATIVE_PRICED_FILL_NOT_FOUND")
    elif time_quality != "NATIVE_EXECUTION_TIMESTAMP":
        evidence_status = "ECONOMIC_PROOF_TIMESTAMP_PROXY"
    else:
        evidence_status = "ECONOMIC_AND_TIMESTAMP_PROVEN"
    return {
        "order_id": order.get("order_id"), "side": order.get("side"),
        "submitted_qty": requested, "proven_filled_qty": qty,
        "remaining_unfilled_qty": (requested - qty if qty is not None else None),
        "evidence_status": evidence_status, "time_quality": time_quality,
        "cumulative_watermark": cum_qty, "individual_unique_qty": individual_qty,
        "native_fill_rows": len(related),
        "issues": sorted(issues), "source": "STORED_KIS_EVIDENCE_NOT_FRESH_BROKER_CALL",
    }
