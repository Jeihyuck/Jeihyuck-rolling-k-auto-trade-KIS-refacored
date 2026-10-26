"""Fail-closed, read-only broker proof for handing full KR reconciliation after SELL.

This never submits/cancels or writes economic state. It is an optimization
gate, NOT a substitute for durable pre-submit order/claim/balance checks.
Any unrecognized order, partial ledger application, duplicate ID, KIS
pagination error, account/epoch ambiguity or stale holding forces the
unchanged full pre-exit reconciliation.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo
from decimal import Decimal, InvalidOperation
import math
import logging
import time as clock
from typing import Any

import sqlalchemy as sa

from trader.account_state import get_account_key
from trader.db.repos import OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.kr.reconcile_parity_fastpath import historical_repair_unnecessary
from trader.kr.runtime_integrity_20260929 import _daily_ccld_all_pages_for_date
from trader.reconcile_kis import _normalize_code, _parse_side, _parse_date_time
from trader.time_utils import now_kst

logger = logging.getLogger(__name__)


def _valid_nonnegative_integral(value: Any) -> int | None:
    """Reject fractional, NaN and infinite KIS quantities instead of rounding."""
    try:
        number = Decimal(str(value if value is not None else "0"))
        if not number.is_finite() or number < 0 or number != number.to_integral_value():
            return None
        return int(number)
    except (InvalidOperation, TypeError, ValueError, OverflowError):
        return None


def _fresh_balance_evidence(balance_snapshot: Any) -> tuple[bool, str]:
    if not isinstance(balance_snapshot, dict):
        return False, "NO_BROKER_BALANCE"
    mark = str(balance_snapshot.get("_kr_verified_fresh_at_kst") or "").strip()
    if not mark:
        return False, "KIS_BALANCE_FRESHNESS_NOT_PROVEN"
    try:
        seen = datetime.fromisoformat(mark)
    except (TypeError, ValueError):
        return False, "KIS_BALANCE_FRESHNESS_MALFORMED"
    if seen.tzinfo is None:
        return False, "KIS_BALANCE_FRESHNESS_UNZONED"
    age = (now_kst() - seen).total_seconds()
    if not math.isfinite(age) or not 0 <= age <= 20:
        return False, "STALE_KIS_BALANCE"
    if not isinstance(balance_snapshot.get("output1"), list):
        return False, "INVALID_KIS_HOLDINGS"
    return True, "FRESH_KIS_BALANCE"


def prove_protective_sell_only_preflight(
    *, engine: Any, kis: Any, env: str, strategy: str,
    trade_date: date, balance_snapshot: dict | None,
) -> tuple[bool, str]:
    """Narrow, READ-ONLY proof to offer protected SELL before full recovery.

    This never authorizes BUY or deferring post-exit reconciliation. It
    requires a fresh real broker balance, zero unresolved claims/orders, exact
    owned position-cycle quantity parity, and current account/epoch binding.
    Today's missing broker order/price history is handled AFTER exits; every
    individual SELL still passes its durable submit-time safety gates.
    """
    if env != "practice" or strategy != "pb1_pullback_close":
        return False, "SCOPE_NOT_VALIDATED"
    valid, reason = _fresh_balance_evidence(balance_snapshot)
    if not valid:
        return False, reason
    if not isinstance(balance_snapshot.get("output2"), (list, dict)) or not balance_snapshot["output2"]:
        return False, "KIS_BALANCE_SUMMARY_NOT_PROVEN"
    try:
        from trader.account_state import account_reset_mode
        if account_reset_mode():
            return False, "ACCOUNT_RESET_MODE_REQUIRES_FULL_RECONCILE"
        orders = OrdersRepo(engine)
        if orders.get_open_orders(env, include_stale=True, fail_open=False):
            return False, "OPEN_OR_UNRESOLVED_ORDER"
        health = orders.execution_claim_health(market="KR")
        if not isinstance(health, dict) or health.get("available") is not True:
            return False, "CLAIM_HEALTH_NOT_AUTHORITATIVE"
        for name in ("in_flight", "uncertain", "unresolved"):
            value = _valid_nonnegative_integral(health.get(name))
            if value is None or value:
                return False, "UNRESOLVED_OR_INVALID_CLAIM_HEALTH"
        positions = PositionsRepo(engine).list_active_positions_for_exit(env, strategy)
        parity, reason = historical_repair_unnecessary(
            holdings=balance_snapshot["output1"], positions=positions,
            reconcile_ok=True, holdings_error=None, order_or_fill_changes=0,
            unresolved=False, expected_strategy=strategy,
        )
        if not parity:
            return False, reason
        schema = schema_for_engine(engine)
        account_key = get_account_key(env=env, kis=kis)
        portfolio_ids = {
            str(pos.get("portfolio_epoch_id") or "") for pos in positions
        }
        if "" in portfolio_ids:
            return False, "POSITION_PORTFOLIO_EPOCH_UNPROVEN"
        with engine.connect() as conn:
            epochs = {
                str(row["portfolio_epoch_id"]): row
                for row in conn.execute(sa.select(schema.portfolio_epochs).where(
                    schema.portfolio_epochs.c.portfolio_epoch_id.in_(portfolio_ids)
                )).mappings().all()
            } if portfolio_ids else {}
        for pos in positions:
            epoch = epochs.get(str(pos["portfolio_epoch_id"]))
            if (
                epoch is None
                or str(epoch.get("env") or "") != env
                or str(epoch.get("strategy") or "") != strategy
                or str(epoch.get("account_id") or "") != account_key
                or str(epoch.get("status") or "").upper() != "ACTIVE"
                or not str(epoch.get("trading_epoch_id") or "")
                or str(pos.get("trading_epoch_id") or "") != str(epoch.get("trading_epoch_id"))
            ):
                return False, "POSITION_ACCOUNT_EPOCH_UNPROVEN"
        return True, "SAFE_BROKER_BALANCE_SELL_ONLY_POST_RECONCILE_REQUIRED"
    except Exception as exc:
        logger.warning(
            "[KR_RECONCILE_LANES][SELL_ONLY_PROOF_FAILED] error_type=%s "
            "action=KEEP_FULL_PRE_EXIT_RECONCILE", type(exc).__name__, exc_info=True,
        )
        return False, f"SELL_ONLY_PROOF_FAILED_{type(exc).__name__}"


def prove_safe_protective_sell_handoff(
    *, engine: Any, kis: Any, env: str, strategy: str,
    trade_date: date, balance_snapshot: dict | None,
) -> tuple[bool, str]:
    """Prove existing broker evidence/ledger parity before owner exits."""
    if env != "practice" or strategy != "pb1_pullback_close":
        return False, "SCOPE_NOT_VALIDATED"
    if not isinstance(balance_snapshot, dict):
        return False, "NO_BROKER_BALANCE"
    try:
        # In the Oct23 practice logs the prior generic ValueError hid which
        # proof input was malformed. A missing/invalid freshness mark is an
        # *unproven* balance, not a valid shortcut authorization.
        valid, freshness_reason = _fresh_balance_evidence(balance_snapshot)
        if not valid:
            return False, freshness_reason
        orders_repo = OrdersRepo(engine)
        # The global practice default can return [] on a DB timeout.
        # This proof authorizes deferring full reconciliation; an unknown
        # lookup is NEVER proof that broker-tracked orders are absent.
        if orders_repo.get_open_orders(env, include_stale=True, fail_open=False):
            return False, "OPEN_OR_UNRESOLVED_ORDER"
        health = orders_repo.execution_claim_health(market="KR")
        if not isinstance(health, dict) or health.get("available") is not True:
            return False, "UNRESOLVED_EXECUTION_CLAIM"
        if any(_valid_nonnegative_integral(health.get(k)) is None
               or _valid_nonnegative_integral(health.get(k)) != 0
               for k in ("in_flight", "uncertain", "unresolved")):
            return False, "UNRESOLVED_EXECUTION_CLAIM"
        positions = PositionsRepo(engine).list_active_positions_for_exit(env, strategy)
        parity, reason = historical_repair_unnecessary(
            holdings=balance_snapshot["output1"], positions=positions,
            reconcile_ok=True, holdings_error=None, order_or_fill_changes=0,
            unresolved=False, expected_strategy=strategy,
        )
        if not parity:
            return False, reason
        broker = _daily_ccld_all_pages_for_date(kis, trade_date)
        if broker.get("_sep29_complete") is not True or str(broker.get("rt_cd")) != "0":
            return False, "KIS_DAILY_PROOF_INCOMPLETE"
        rows = broker.get("output1")
        if not isinstance(rows, list):
            return False, "KIS_DAILY_ROWS_INVALID"
        if len(rows) > 300:
            return False, "KIS_DAILY_ORDER_COUNT_TOO_LARGE"
        kst_start = datetime.combine(trade_date, time.min, tzinfo=ZoneInfo("Asia/Seoul"))
        kst_end = kst_start + timedelta(days=1)
        schema = schema_for_engine(engine)
        with engine.connect() as conn:
            stored = conn.execute(sa.select(schema.orders).where(sa.and_(
                schema.orders.c.env == env,
                schema.orders.c.created_at >= kst_start,
                schema.orders.c.created_at < kst_end,
            ))).mappings().all()
            broker_rows: dict[tuple[str, str, str], dict] = {}
            for row in rows:
                if not isinstance(row, dict):
                    return False, "MALFORMED_KIS_ORDER"
                code = _normalize_code(row.get("pdno") or row.get("code"))
                side = _parse_side(row)
                no = str(row.get("odno") or row.get("ODNO") or "").strip()
                if not code or code == "000000" or side not in {"BUY", "SELL"} or not no:
                    return False, "KIS_ORDER_IDENTITY_MISSING"
                try:
                    parsed_broker_date = _parse_date_time(row).date()
                except (TypeError, ValueError):
                    return False, "KIS_ORDER_DATE_EVIDENCE_INVALID"
                if parsed_broker_date != trade_date:
                    return False, "KIS_ORDER_DATE_MISMATCH"
                key = (code, side, no)
                if key in broker_rows:
                    return False, "BROKER_ORDER_MULTIPLE_ROWS_REQUIRE_RECONCILE"
                broker_rows[key] = row
            db_by_key: dict[tuple[str, str, str], dict] = {}
            # orders intentionally has no account_id column: its durable
            # account/strategy scope is owned by portfolio_epochs. Query in
            # one batch, never trust a missing order field or code-only join.
            portfolio_ids = {str(item.get("portfolio_epoch_id") or "") for item in stored}
            if stored and "" in portfolio_ids:
                return False, "DURABLE_ORDER_PORTFOLIO_EPOCH_MISSING"
            epochs = {}
            if portfolio_ids:
                epoch_rows = conn.execute(sa.select(schema.portfolio_epochs).where(
                    schema.portfolio_epochs.c.portfolio_epoch_id.in_(portfolio_ids)
                )).mappings().all()
                epochs = {str(row["portfolio_epoch_id"]): row for row in epoch_rows}
            scope = get_account_key(env=env, kis=kis)
            for order in stored:
                no = str(order.get("kis_odno") or order.get("broker_order_id") or "").strip()
                epoch = epochs.get(str(order.get("portfolio_epoch_id") or ""))
                if (
                    not no or epoch is None
                    or str(epoch.get("account_id") or "") != scope
                    or str(epoch.get("env") or "") != env
                    or str(epoch.get("strategy") or "") != str(order.get("strategy") or "")
                    or (order.get("trading_epoch_id") and str(order["trading_epoch_id"]) != str(epoch.get("trading_epoch_id") or ""))
                ):
                    return False, "DURABLE_ORDER_ACCOUNT_IDENTITY_MISSING"
                key = (str(order["code"]).zfill(6), str(order["side"]).upper(), no)
                if key in db_by_key:
                    return False, "DURABLE_ORDER_AMBIGUOUS"
                db_by_key[key] = order
            if set(db_by_key) != set(broker_rows):
                return False, "BROKER_DURABLE_ORDER_SET_MISMATCH"
            # Fetch economic fills in ONE account-scoped batch. Previously
            # each broker row caused a separate DB roundtrip on the protective
            # SELL proof hot path. Do not weaken per-order exact-quantity
            # comparison: retain sum by durable order_id.
            fills_qty_by_order: dict[str, int] = {}
            if stored:
                stored_order_ids = [order["order_id"] for order in stored]
                fill_rows = conn.execute(sa.select(
                    schema.fills.c.order_id, schema.fills.c.qty,
                ).where(sa.and_(
                    schema.fills.c.env == env,
                    schema.fills.c.order_id.in_(stored_order_ids),
                ))).all()
                for order_id, fill_qty in fill_rows:
                    key_id = str(order_id)
                    fills_qty_by_order[key_id] = (
                        fills_qty_by_order.get(key_id, 0) + int(fill_qty or 0)
                    )
            for key, broker_row in broker_rows.items():
                source = db_by_key[key]
                requested = _valid_nonnegative_integral(source.get("qty"))
                observed = _valid_nonnegative_integral(broker_row.get("tot_ccld_qty"))
                order_req = _valid_nonnegative_integral(broker_row.get("ord_qty") or requested)
                if requested is None or observed is None or order_req is None:
                    return False, "KIS_ORDER_QUANTITY_EVIDENCE_INVALID"
                if requested <= 0 or order_req != requested or observed > requested:
                    return False, "BROKER_ORDER_QUANTITY_CONFLICT"
                response = source.get("response_json") or {}
                if not isinstance(response, dict):
                    return False, "DURABLE_ORDER_RESPONSE_MALFORMED"
                confirmed = response.get("confirmed_fill_qty")
                confirmed_qty = (
                    _valid_nonnegative_integral(confirmed) if confirmed is not None else None
                )
                if confirmed_qty is None or confirmed_qty != observed:
                    return False, "ORDER_CUMULATIVE_WATERMARK_UNPROVEN"
                if fills_qty_by_order.get(str(source["order_id"]), 0) != observed:
                    return False, "ECONOMIC_FILL_QUANTITY_DRIFT"
                if observed:
                    try:
                        broker_amt = Decimal(str(broker_row.get("tot_ccld_amt")))
                        durable_amt = Decimal(str(response.get("confirmed_fill_notional")))
                    except (TypeError, ValueError, InvalidOperation):
                        return False, "BROKER_PRICE_EVIDENCE_INVALID"
                    if not broker_amt.is_finite() or broker_amt <= 0 or not durable_amt.is_finite():
                        return False, "BROKER_PRICE_EVIDENCE_UNPROVEN"
                    if abs(broker_amt - durable_amt) > Decimal("0.01"):
                        return False, "ECONOMIC_NOTIONAL_WATERMARK_DRIFT"
        return True, "EXACT_BROKER_LEDGER_PRETRADE_PROOF"
    except Exception as exc:
        # The previous type-only result hid the actual Oct23 ValueError site.
        # Keep fail-closed FULL_PRE_EXIT_RECONCILE while retaining the traceback
        # for a precise fix; never treat proof exceptions as authorization.
        logger.warning(
            "[KR_RECONCILE_LANES][PROOF_READ_FAILED] exception_type=%s "
            "action=FULL_PRE_EXIT_RECONCILE",
            type(exc).__name__,
            exc_info=True,
        )
        return False, f"PROOF_READ_FAILED_{type(exc).__name__}"
