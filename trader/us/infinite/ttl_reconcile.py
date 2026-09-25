from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

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
    """Start the liveness clock independently of cancel acknowledgement.

    PR127 rows may already have only ``tqqq_ttl_cancel_requested_at``. When
    backfilling the new first-unresolved field, preserve that older timestamp
    instead of resetting the clock to ``when``.
    """
    meta = order_meta(order)
    explicit_first = str(meta.get("tqqq_ttl_first_unresolved_at") or "")
    legacy_cancel = str(meta.get("tqqq_ttl_cancel_requested_at") or "")
    first = explicit_first or legacy_cancel
    patch = {
        "tqqq_ttl_last_unresolved_at": when.astimezone(timezone.utc).isoformat(),
        "tqqq_ttl_last_unresolved_reason": str(reason or "UNKNOWN"),
    }
    if not first:
        first = when.astimezone(timezone.utc).isoformat()
        patch["tqqq_ttl_first_unresolved_at"] = first
    elif not explicit_first:
        patch["tqqq_ttl_first_unresolved_at"] = first
    _patch_order_meta(repository, order, patch)
    return first


def mark_cancel_attempt(repository: Any, order: dict, *, when: datetime,
                        result: dict | None = None) -> None:
    """Persist a cancel attempt even when the broker call raised.

    ``tqqq_ttl_cancel_requested_at`` means the cancel API was attempted, not
    that KIS acknowledged cancellation. Cancel ACK is still non-terminal.
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


def _normalized_order_no(value: Any) -> str:
    from trader.us.utils.order_no import normalize_us_order_no
    return normalize_us_order_no(str(value or ""))


def _int_value(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(str(value).replace(",", "")))
    except (TypeError, ValueError):
        return default


def _exact_zero_fill_observation(order: dict, observation: dict | None) -> bool:
    """Require exact immutable identity and an untouched requested quantity."""
    if not isinstance(observation, dict):
        return False
    requested = _int_value(order.get("qty_requested") or order.get("qty"), 0)
    if requested <= 0:
        return False
    observed_requested = _int_value(
        observation.get("requested_qty") or observation.get("qty_requested") or observation.get("qty"), 0
    )
    if observation.get("filled_qty_present") is False:
        return False
    filled_raw = observation.get("filled_qty")
    if filled_raw in (None, ""):
        filled_raw = observation.get("cumulative_filled_qty")
    if filled_raw in (None, ""):
        return False
    try:
        observed_filled_float = float(str(filled_raw).replace(",", ""))
        if observed_filled_float < 0 or not observed_filled_float.is_integer():
            return False
        observed_filled = int(observed_filled_float)
    except (TypeError, ValueError):
        return False
    remaining_raw = observation.get("remaining_qty")
    observed_remaining = (
        _int_value(remaining_raw, 0)
        if remaining_raw not in (None, "")
        else max(0, observed_requested - observed_filled)
    )
    return bool(
        _normalized_order_no(observation.get("order_no")) == _normalized_order_no(order.get("order_no"))
        and str(observation.get("symbol") or "").upper() == "TQQQ"
        and str(observation.get("side") or "").upper() == "BUY"
        and observed_requested == requested
        and observed_filled == 0
        and observed_remaining == requested
    )


def historical_zero_fill_not_live_expiry(
    order: dict,
    *,
    first_observation: dict | None,
    cancel_error: BaseException | str,
    retry_observation: dict | None,
    now: datetime,
) -> dict | None:
    """Prove a historical zero-fill order is no longer live without guessing.

    Evidence is intentionally conjunctive: both original-trade-date broker
    queries must show the exact TQQQ BUY as untouched zero-fill, and the cancel
    endpoint must explicitly say that the original order does not exist. Generic
    HTTP/transport errors never qualify. The historical boundary is the US
    trading calendar date (America/New_York), not UTC date; otherwise an order
    from the still-active US session could be misclassified after UTC midnight.
    """
    try:
        original_date = date.fromisoformat(str(order.get("trade_date") or ""))
    except ValueError:
        return None
    current_us_trade_date = now.astimezone(ZoneInfo("America/New_York")).date()
    if original_date >= current_us_trade_date:
        return None
    error_text = str(cancel_error or "").lower()
    not_live_tokens = (
        "원주문번호가 존재하지 않습니다",
        "original order number does not exist",
        "original order does not exist",
    )
    if not any(token.lower() in error_text for token in not_live_tokens):
        return None
    if not _exact_zero_fill_observation(order, first_observation):
        return None
    if not _exact_zero_fill_observation(order, retry_observation):
        return None
    requested = _int_value(order.get("qty_requested") or order.get("qty"), 0)
    return {
        "order_no": str(order.get("order_no") or ""),
        "symbol": "TQQQ",
        "side": "BUY",
        "status": "EXPIRED",
        "requested_qty": requested,
        "filled_qty": 0,
        "cumulative_filled_qty": 0,
        "remaining_qty": requested,
        "evidence_type": "TQQQ_TTL_HISTORICAL_ZERO_FILL_NOT_LIVE",
        "observed_at": now.astimezone(timezone.utc).isoformat(),
        "broker_proof": {
            "original_trade_date": str(order.get("trade_date") or ""),
            "current_us_trade_date": current_us_trade_date.isoformat(),
            "first_query_zero_fill": True,
            "cancel_original_order_not_found": True,
            "retry_query_zero_fill": True,
        },
    }


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


def fetch_fresh_tqqq_preorder_position() -> dict:
    """Fetch a dedicated fresh KIS balance snapshot immediately before TQQQ BUY.

    A zero TQQQ holding is authoritative only when the *whole* fresh balance
    contract is proven authoritative. This deliberately uses a new provider
    without tick cache so balance-reconcile skip ticks cannot reuse DB/cached
    positions as the immutable BUY baseline.
    """
    observed_at = datetime.now(timezone.utc).isoformat()
    try:
        from trader.us.data_provider import USDataProvider
        balance = USDataProvider(offline=False).get_balance(force_refresh=True)
    except Exception as exc:
        return {"authoritative": False, "reason": "fresh_kis_balance_fetch_failed", "error": str(exc)}
    if not isinstance(balance, dict):
        return {"authoritative": False, "reason": "fresh_kis_balance_not_dict"}
    if str(balance.get("balance_parse_status") or "").upper() != "OK":
        return {"authoritative": False, "reason": "fresh_kis_balance_parse_not_ok"}
    if balance.get("balance_authoritative") is not True or balance.get("balance_complete") is not True:
        return {"authoritative": False, "reason": "fresh_kis_balance_not_authoritative"}
    positions = balance.get("positions") or []
    if not isinstance(positions, list):
        return {"authoritative": False, "reason": "fresh_kis_positions_not_list"}
    row = next(
        (p for p in positions if isinstance(p, dict) and str(p.get("symbol") or "").upper() == "TQQQ"),
        None,
    )
    if row is None:
        position = PositionSnapshot(
            qty=0, orderable_qty=0, average_price=0.0, price=0.0, exchange="NASDAQ"
        )
    else:
        try:
            qty = int(float(row.get("qty") or row.get("holding_qty") or 0))
            orderable_raw = row.get("orderable_qty")
            if orderable_raw is None:
                orderable_raw = row.get("sellable_qty")
            orderable = int(float(orderable_raw)) if orderable_raw not in (None, "") else qty
            avg = float(row.get("avg_price_usd") or row.get("broker_avg_price") or row.get("avg_cost") or 0.0)
        except (TypeError, ValueError) as exc:
            return {"authoritative": False, "reason": "fresh_kis_tqqq_position_parse_failed", "error": str(exc)}
        position = PositionSnapshot(
            qty=max(0, qty), orderable_qty=max(0, orderable), average_price=max(0.0, avg),
            price=float(row.get("current_price_usd") or row.get("current_px") or 0.0),
            exchange=str(row.get("exchange") or "NASDAQ"),
        )
        observed_at = str(row.get("broker_avg_price_asof") or row.get("balance_asof") or observed_at)
    return {
        "authoritative": True,
        "reason": "ok",
        "position": position,
        "observed_at": observed_at,
        "source": "kis_balance_authoritative",
    }


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
