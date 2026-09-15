from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

from .models import PositionSnapshot

logger = logging.getLogger(__name__)


def order_meta(order: dict) -> dict:
    value = order.get("meta") or {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _patch_order_meta(repository: Any, order: dict, patch: dict) -> None:
    """Patch only the exact TQQQ order row; never change broker/order status here."""
    key = str(order.get("client_order_key") or "")
    trade_date = str(order.get("trade_date") or "")
    if not key or not trade_date:
        raise ValueError("TQQQ TTL metadata patch requires trade_date/client_order_key")

    current = order_meta(order)
    current.update(patch)
    order["meta"] = current

    engine = getattr(repository, "engine", None)
    if engine is None:
        return
    payload = json.dumps(patch, ensure_ascii=False, default=str)
    with engine.begin() as conn:
        result = conn.execute(text("""
            UPDATE us_orders
            SET meta=COALESCE(meta, '{}'::jsonb) || CAST(:patch AS jsonb), updated_at=NOW()
            WHERE trade_date=CAST(:trade_date AS date) AND client_order_key=:key
              AND symbol='TQQQ' AND side='BUY'
        """), {"patch": payload, "trade_date": trade_date, "key": key})
        if int(getattr(result, "rowcount", 0) or 0) != 1:
            raise RuntimeError(
                f"TQQQ TTL metadata patch identity mismatch key={key} rowcount={getattr(result, 'rowcount', None)}"
            )


def mark_first_unresolved(repository: Any, order: dict, *, when: datetime, reason: str) -> str:
    """Start the liveness clock independently of cancel acknowledgement."""
    meta = order_meta(order)
    first = str(meta.get("tqqq_ttl_first_unresolved_at") or "")
    patch = {
        "tqqq_ttl_last_unresolved_at": when.astimezone(timezone.utc).isoformat(),
        "tqqq_ttl_last_unresolved_reason": str(reason or "UNKNOWN"),
    }
    if not first:
        first = when.astimezone(timezone.utc).isoformat()
        patch["tqqq_ttl_first_unresolved_at"] = first
    _patch_order_meta(repository, order, patch)
    return first


def mark_cancel_attempt(repository: Any, order: dict, *, when: datetime,
                        result: dict | None = None) -> None:
    """Persist a cancel attempt even when the broker call raised.

    `tqqq_ttl_cancel_requested_at` means the cancel API was attempted, not that
    KIS acknowledged cancellation. Cancel ACK is still non-terminal.
    """
    meta = order_meta(order)
    first = str(meta.get("tqqq_ttl_cancel_requested_at") or when.astimezone(timezone.utc).isoformat())
    _patch_order_meta(repository, order, {
        "tqqq_ttl_cancel_requested_at": first,
        "tqqq_ttl_last_cancel_attempt_at": when.astimezone(timezone.utc).isoformat(),
        "tqqq_ttl_cancel_order_no": str(order.get("order_no") or ""),
        "tqqq_ttl_cancel_result": dict(result or {}),
    })


def unresolved_age_seconds(order: dict, *, now: datetime) -> float:
    meta = order_meta(order)
    raw = meta.get("tqqq_ttl_first_unresolved_at") or meta.get("tqqq_ttl_cancel_requested_at")
    if not raw:
        return 0.0
    try:
        started = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if started.tzinfo is None:
            started = started.replace(tzinfo=timezone.utc)
        return max(0.0, (now.astimezone(timezone.utc) - started.astimezone(timezone.utc)).total_seconds())
    except Exception:
        return 0.0


def mark_manual_reconcile(repository: Any, order: dict, *, when: datetime,
                          unresolved_age_sec: float, reason: str) -> None:
    meta = order_meta(order)
    if meta.get("tqqq_ttl_unresolved_escalated_at"):
        return
    if hasattr(repository, "mark_ttl_unresolved_escalated"):
        repository.mark_ttl_unresolved_escalated(
            order, escalated_at=when, unresolved_age_sec=unresolved_age_sec,
        )
        # Existing repository method uses the legacy AFTER_CANCEL reason. Add
        # the accurate generic reason without altering terminal status.
        _patch_order_meta(repository, order, {
            "manual_reconcile_reason": reason,
            "tqqq_ttl_unresolved_trigger": reason,
        })
        return
    _patch_order_meta(repository, order, {
        "tqqq_ttl_unresolved_escalated_at": when.astimezone(timezone.utc).isoformat(),
        "tqqq_ttl_unresolved_age_sec": float(unresolved_age_sec),
        "manual_reconcile_required": True,
        "manual_reconcile_reason": reason,
        "tqqq_ttl_unresolved_trigger": reason,
    })


def immutable_kis_baseline(order: dict) -> tuple[int, int | None, float] | None:
    meta = order_meta(order)
    source = str(meta.get("pre_order_balance_source") or order.get("pre_order_balance_source") or "")
    if source != "kis_balance_authoritative":
        return None
    raw_qty = meta.get("pre_order_holding_qty", order.get("pre_order_holding_qty"))
    if raw_qty in (None, ""):
        return None
    try:
        qty = int(float(raw_qty))
        orderable_raw = meta.get("pre_order_orderable_qty", order.get("pre_order_orderable_qty"))
        orderable = int(float(orderable_raw)) if orderable_raw not in (None, "") else None
        avg = float(meta.get("pre_order_avg_price", order.get("pre_order_avg_price")) or 0.0)
    except (TypeError, ValueError):
        return None
    return qty, orderable, avg


def _derived_incremental_avg(*, pre_qty: int, pre_avg: float,
                             post_qty: int, post_avg: float, filled_qty: int) -> float:
    if filled_qty <= 0 or post_avg <= 0:
        return 0.0
    if pre_qty == 0:
        return float(post_avg)
    if pre_avg <= 0:
        return 0.0
    value = ((post_qty * post_avg) - (pre_qty * pre_avg)) / filled_qty
    return float(value) if value > 0 else 0.0


def apply_authoritative_buy_balance_delta(
    repository: Any,
    order: dict,
    *,
    broker_position: PositionSnapshot | None,
    authoritative: bool,
    observed_at: datetime,
) -> dict:
    """Use KIS quantity delta only when an immutable KIS pre-order baseline exists."""
    if not authoritative or broker_position is None:
        return {"status": "NO_PROOF", "reason": "broker_position_not_authoritative"}
    baseline = immutable_kis_baseline(order)
    if baseline is None:
        return {"status": "NO_PROOF", "reason": "immutable_kis_baseline_missing"}

    requested = int(order.get("qty_requested") or order.get("qty") or 0)
    if requested <= 0:
        return {"status": "CONFLICT", "reason": "invalid_requested_qty"}
    pre_qty, _pre_orderable, pre_avg = baseline
    post_qty = int(broker_position.qty or 0)
    delta = post_qty - pre_qty
    if delta == 0:
        return {"status": "NO_PROOF", "reason": "balance_delta_zero", "delta": 0}
    if delta < 0 or delta > requested:
        return {
            "status": "CONFLICT", "reason": "balance_delta_out_of_contract",
            "delta": delta, "pre_qty": pre_qty, "post_qty": post_qty, "requested_qty": requested,
        }

    fill_price = _derived_incremental_avg(
        pre_qty=pre_qty, pre_avg=pre_avg, post_qty=post_qty,
        post_avg=float(broker_position.average_price or 0.0), filled_qty=delta,
    )
    if fill_price <= 0:
        return {
            "status": "NO_PROOF", "reason": "balance_delta_price_unprovable",
            "delta": delta, "pre_qty": pre_qty, "post_qty": post_qty,
        }

    if hasattr(repository, "apply_ttl_balance_delta"):
        result = repository.apply_ttl_balance_delta(
            order, current_qty=post_qty, current_avg_price=float(broker_position.average_price or 0.0),
            observed_at=observed_at,
        )
    else:
        from trader.us.db.repos import mark_order_filled_by_reconcile
        result = mark_order_filled_by_reconcile(
            order_no=str(order.get("order_no") or ""),
            client_order_key=str(order.get("client_order_key") or ""),
            symbol="TQQQ", side="BUY", filled_qty=delta,
            avg_price_usd=fill_price, source="tqqq_kis_balance_delta",
            trade_date=str(order.get("trade_date") or ""), requested_qty=requested,
            cumulative_filled_qty=delta, evidence_type="BALANCE_DELTA_SYNTHETIC",
            meta={
                **order_meta(order),
                "balance_delta_evidence": True,
                "balance_delta_pre_qty": pre_qty,
                "balance_delta_post_qty": post_qty,
                "balance_delta_filled_qty": delta,
                "balance_delta_observed_at": observed_at.astimezone(timezone.utc).isoformat(),
                "balance_delta_fill_price_source": "KIS_AVG_COST_DIFFERENCE",
            },
        )
    if str((result or {}).get("status") or "").upper() != "OK":
        return {"status": "CONFLICT", "reason": "balance_delta_persist_failed", "result": result}

    status = "FILLED" if delta == requested else "PARTIALLY_FILLED"
    order["status"] = status
    order["qty_filled"] = delta
    return {
        "status": status, "filled_qty": delta, "remaining_qty": requested - delta,
        "fill_price": fill_price, "pre_qty": pre_qty, "post_qty": post_qty,
    }
