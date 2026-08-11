"""Fail-closed, decimal take-profit calculations shared by strategy and router."""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone
import logging

AUTHORITATIVE_AVG_SOURCES = {"kis_pchs_avg_pric", "kis_buy_amount_div_qty"}
RECONCILED_AVG_SOURCES = {"reconciled_authoritative_position"}
DB_AVG_SOURCES = {"db_position_fresh"}
BROKER_TERMINAL_EVIDENCE = {"KIS_ORDER_CUMULATIVE_ACTUAL", "KIS_EXECUTION_ACTUAL", "KIS_ORDER_STATUS_ACTUAL"}
logger = logging.getLogger(__name__)


def as_decimal(value: object, *, name: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"invalid_{name}") from exc
    if not result.is_finite() or result <= 0:
        raise ValueError(f"invalid_{name}")
    return result


def calc_return_rate(executable_price: Decimal, broker_avg_price: Decimal) -> Decimal:
    """Return a fraction (``.03`` means 3%), never a price delta."""
    if broker_avg_price <= 0:
        raise ValueError("invalid_broker_avg_price")
    if executable_price <= 0:
        raise ValueError("invalid_executable_price")
    return (executable_price - broker_avg_price) / broker_avg_price


def authoritative_broker_avg(position: dict, *, now: datetime | None = None,
                             max_age_seconds: int = 300) -> tuple[Decimal, dict]:
    """Resolve one safe cost basis: KIS balance, reconciliation, then fresh DB."""
    now = now or datetime.now(timezone.utc)
    source = str(position.get("broker_avg_price_source") or "")
    currency = str(position.get("broker_avg_price_currency") or "").upper()
    asof_raw = position.get("broker_avg_price_asof")
    value = position.get("broker_avg_price")
    # Normalize the raw KIS balance fields at this boundary.  Previously the
    # balance carried a real average but profit capture only inspected the
    # optional broker_avg_price transport field.
    is_kis = position.get("balance_source") == "kis_balance_authoritative" and position.get("authoritative_positions") is True
    if is_kis and source not in AUTHORITATIVE_AVG_SOURCES:
        for key, normalized_source in (("pchs_avg_pric", "kis_pchs_avg_pric"), ("avg_price_usd", "kis_pchs_avg_pric"), ("avg_price", "kis_pchs_avg_pric")):
            if position.get(key) not in (None, "", 0, "0"):
                value, source = position[key], normalized_source
                break
    allowed = ((is_kis and source in AUTHORITATIVE_AVG_SOURCES) or
               source in RECONCILED_AVG_SOURCES or source in DB_AVG_SOURCES)
    if not allowed or currency != "USD" or not asof_raw:
        logger.warning("[US_AVG_PRICE][MISSING] symbol=%s checked_sources=KIS_BALANCE,RECONCILED_POSITION,FRESH_DB_POSITION", position.get("symbol"))
        raise ValueError("missing_authoritative_broker_avg_price")
    try:
        asof = datetime.fromisoformat(str(asof_raw).replace("Z", "+00:00"))
        if asof.tzinfo is None:
            asof = asof.replace(tzinfo=timezone.utc)
        if abs((now - asof.astimezone(timezone.utc)).total_seconds()) > max_age_seconds:
            raise ValueError("stale_authoritative_broker_avg_price")
    except ValueError as exc:
        if str(exc) == "stale_authoritative_broker_avg_price":
            raise
        raise ValueError("invalid_broker_avg_price_asof") from exc
    if not str(position.get("position_lifecycle_id") or "").strip():
        raise ValueError("position_lifecycle_id_required")
    if position.get("orderable_qty") is None or int(position.get("qty") or 0) <= 0:
        raise ValueError("ambiguous_position_quantity")
    resolved = as_decimal(value, name="broker_avg_price")
    log_source = "KIS_BALANCE" if is_kis else ("RECONCILED_POSITION" if source in RECONCILED_AVG_SOURCES else "DB_POSITION")
    logger.info("[US_AVG_PRICE][RESOLVED] symbol=%s avg_price=%s source=%s", position.get("symbol"), resolved, log_source)
    return resolved, {
        "broker_avg_price_source": source, "broker_avg_price_currency": currency,
        "broker_avg_price_asof": str(asof_raw), "balance_source": position.get("balance_source"),
        "authoritative_positions": True,
    }


def sync_profit_capture_stage_from_order(**event) -> None:
    """Canonical, idempotent order lifecycle -> TP lifecycle transition."""
    stage = str(event.get("profit_capture_stage") or "").lower()
    lifecycle = str(event.get("position_lifecycle_id") or "").strip()
    key = str(event.get("client_order_key") or "").strip()
    if stage not in {"tp1", "tp2", "tp3"} or not lifecycle or not key:
        return
    status = str(event.get("order_status") or "").upper()
    evidence = str(event.get("evidence_type") or "").upper()
    requested = int(event.get("requested_qty") or 0)
    filled = int(event.get("filled_qty") or 0)
    if status == "FILLED" and (evidence not in BROKER_TERMINAL_EVIDENCE or requested <= 0 or filled < requested):
        status = "PARTIALLY_FILLED"
    if status in {"DRY_RUN", "SIGNAL_ONLY"}:
        return
    from trader.us.db.repos import mark_us_profit_capture_stage
    mark_us_profit_capture_stage(
        str(event.get("trade_date") or ""), str(event.get("symbol") or ""), stage,
        position_lifecycle_id=lifecycle, order_key=key,
        broker_order_no=event.get("broker_order_no"), status=status,
    )
