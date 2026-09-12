"""Prevent broker-quantity reconciliation from pre-empting durable fill application.

Fresh KIS holdings are canonical position truth only after every already-confirmed
broker fill has reached its exact lifecycle.  If a fill linker/apply step fails
softly and stale-position reconciliation changes DB quantity first, a later retry
can lose the immutable pre-fill basis needed for BUY metadata or SELL realized
P&L accounting.

This is deliberately narrower than the old open-order fence.  It protects only
symbols with durable fill evidence still awaiting lifecycle application.  The
real KIS snapshot is never modified; only the private balance view supplied to
stale-position reconciliation is neutralized for those symbols, so broker-truth
health can continue to report the real mismatch as RED.
"""
from __future__ import annotations

import copy
import logging
from typing import Any

import sqlalchemy as sa

from trader.db.schema import schema_for_engine
from trader.kr.broker_truth_cross_date_unowned_retry import _kst_date, _order_trade_date
import trader.reconcile_db as reconcile_db

logger = logging.getLogger(__name__)
_INSTALLED = False
_ORIGINAL_CLOSE_STALE_GUARDED = reconcile_db.close_stale_positions_guarded
_BUY_APPLIED_KEY = "broker_truth_buy_applied_orders"
_LEGACY_BUY_APPLIED_KEY = "broker_truth_applied_orders"


def _json_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _qty(value: Any) -> int:
    try:
        return int(float(value or 0))
    except Exception:
        return 0


def _baseline_qty(order: dict[str, Any]) -> int | None:
    request = _json_dict(order.get("request_json"))
    entry_meta = _json_dict(request.get("entry_meta"))
    order_entry_meta = _json_dict(order.get("entry_meta_json"))
    for source in (request, entry_meta, order_entry_meta):
        if "pre_order_holding_qty" in source:
            try:
                return int(float(source.get("pre_order_holding_qty") or 0))
            except Exception:
                return None
    return None


def _pending_fill_application_codes(*, engine, env: str, strategy: str) -> set[str]:
    """Return symbols where confirmed fills must be applied before qty reconcile."""
    schema = schema_for_engine(engine)
    pending: set[str] = set()

    with engine.connect() as conn:
        # Unowned executions fence this strategy only when a durable order with
        # the same symbol/side/ODNO exists on the same KST trade date.  KIS order
        # numbers may be reused across dates, so ODNO alone is not sufficient.
        unowned = [
            dict(row)
            for row in conn.execute(
                sa.select(
                    schema.fills.c.code,
                    schema.fills.c.side,
                    schema.fills.c.kis_odno,
                    schema.fills.c.filled_at,
                ).where(
                    sa.and_(
                        schema.fills.c.env == env,
                        schema.fills.c.order_id.is_(None),
                        schema.fills.c.kis_odno.is_not(None),
                        schema.fills.c.side.in_(["BUY", "SELL"]),
                        schema.fills.c.qty > 0,
                    )
                )
            ).mappings().all()
        ]
        for fill in unowned:
            code = str(fill.get("code") or "").lstrip("A").zfill(6)
            side = str(fill.get("side") or "").upper()
            odno = str(fill.get("kis_odno") or "").strip()
            fill_day = _kst_date(fill.get("filled_at"))
            if not code or side not in {"BUY", "SELL"} or not odno or fill_day is None:
                continue
            candidates = [
                dict(row)
                for row in conn.execute(
                    sa.select(schema.orders).where(
                        sa.and_(
                            schema.orders.c.env == env,
                            schema.orders.c.strategy == strategy,
                            schema.orders.c.code == code,
                            schema.orders.c.side == side,
                            sa.or_(
                                schema.orders.c.kis_odno == odno,
                                schema.orders.c.broker_order_id == odno,
                            ),
                        )
                    )
                ).mappings().all()
            ]
            if any(_order_trade_date(order) == fill_day for order in candidates):
                pending.add(code)

        # Only BUY orders attached to a currently OPEN exact lifecycle can be
        # overwritten by close_stale_positions.  Avoid scanning historical
        # closed orders so the safety check stays cheap on the live database.
        active_buy_order_ids = [
            str(value)
            for value in conn.execute(
                sa.select(schema.orders.c.order_id)
                .select_from(
                    schema.orders.join(
                        schema.fills,
                        schema.fills.c.order_id == schema.orders.c.order_id,
                    ).join(
                        schema.positions,
                        sa.and_(
                            schema.positions.c.env == schema.orders.c.env,
                            schema.positions.c.strategy == schema.orders.c.strategy,
                            schema.positions.c.code == schema.orders.c.code,
                            schema.positions.c.position_cycle_id == schema.orders.c.position_cycle_id,
                            schema.positions.c.portfolio_epoch_id == schema.orders.c.portfolio_epoch_id,
                        ),
                    )
                )
                .where(
                    sa.and_(
                        schema.orders.c.env == env,
                        schema.orders.c.strategy == strategy,
                        schema.orders.c.side == "BUY",
                        schema.fills.c.env == env,
                        schema.fills.c.side == "BUY",
                        schema.positions.c.status == "OPEN",
                        schema.positions.c.qty > 0,
                    )
                )
                .distinct()
            ).scalars().all()
            if value is not None
        ]
        if active_buy_order_ids:
            orders = [
                dict(row)
                for row in conn.execute(
                    sa.select(schema.orders).where(schema.orders.c.order_id.in_(active_buy_order_ids))
                ).mappings().all()
            ]
        else:
            orders = []

        for order in orders:
            order_id = order.get("order_id")
            cycle = order.get("position_cycle_id")
            epoch = order.get("portfolio_epoch_id")
            code = str(order.get("code") or "").lstrip("A").zfill(6)
            if not order_id or not cycle or not epoch or not code:
                continue
            cumulative_qty = int(
                conn.execute(
                    sa.select(sa.func.coalesce(sa.func.sum(schema.fills.c.qty), 0)).where(
                        sa.and_(
                            schema.fills.c.env == env,
                            schema.fills.c.order_id == order_id,
                            schema.fills.c.side == "BUY",
                        )
                    )
                ).scalar()
                or 0
            )
            if cumulative_qty <= 0:
                continue
            positions = [
                dict(row)
                for row in conn.execute(
                    sa.select(schema.positions).where(
                        sa.and_(
                            schema.positions.c.env == env,
                            schema.positions.c.strategy == strategy,
                            schema.positions.c.code == code,
                            schema.positions.c.position_cycle_id == cycle,
                            schema.positions.c.portfolio_epoch_id == epoch,
                            schema.positions.c.status == "OPEN",
                        )
                    )
                ).mappings().all()
            ]
            if len(positions) > 1:
                pending.add(code)
                continue
            if not positions:
                continue
            position = positions[0]
            entry_meta = _json_dict(position.get("entry_meta_json"))
            position_meta = _json_dict(position.get("position_meta"))
            applied_map = _json_dict(entry_meta.get(_BUY_APPLIED_KEY))
            if not applied_map:
                applied_map = _json_dict(position_meta.get(_LEGACY_BUY_APPLIED_KEY))
            applied = _json_dict(applied_map.get(str(order_id)))
            applied_qty = _qty(applied.get("qty"))
            if applied_qty > 0:
                if cumulative_qty > applied_qty:
                    pending.add(code)
                continue
            baseline = _baseline_qty(order)
            if baseline is not None and _qty(position.get("qty")) == baseline:
                pending.add(code)

    return pending


def _fenced_balance_for_pending(
    *,
    engine,
    env: str,
    strategy: str,
    kis_balance: dict[str, Any] | None,
    pending_codes: set[str],
) -> dict[str, Any] | None:
    if not pending_codes or not isinstance(kis_balance, dict):
        return kis_balance
    schema = schema_for_engine(engine)
    with engine.connect() as conn:
        rows = [
            dict(row)
            for row in conn.execute(
                sa.select(schema.positions).where(
                    sa.and_(
                        schema.positions.c.env == env,
                        schema.positions.c.strategy == strategy,
                        schema.positions.c.status == "OPEN",
                        schema.positions.c.qty > 0,
                    )
                )
            ).mappings().all()
        ]

    by_code: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        code = str(row.get("code") or "").lstrip("A").zfill(6)
        if code in pending_codes:
            by_code.setdefault(code, []).append(row)

    fenced = copy.deepcopy(kis_balance)
    holdings = list(fenced.get("output1") or [])
    index: dict[str, int] = {}
    for idx, raw in enumerate(holdings):
        if not isinstance(raw, dict):
            continue
        code = str(raw.get("pdno") or raw.get("code") or "").lstrip("A").zfill(6)
        if code:
            index[code] = idx

    for code in sorted(pending_codes):
        positions = by_code.get(code) or []
        if len(positions) != 1:
            continue
        position = positions[0]
        db_qty = _qty(position.get("qty"))
        db_avg = float(position.get("avg_buy_price") or 0.0)
        replacement = {
            "pdno": code,
            "hldg_qty": str(db_qty),
            "ord_psbl_qty": str(db_qty),
            "pchs_avg_pric": str(db_avg),
            "_broker_truth_pending_fill_fence": "1",
        }
        if code in index:
            original = dict(holdings[index[code]] or {})
            original.update(replacement)
            holdings[index[code]] = original
        else:
            holdings.append(replacement)
        logger.error(
            "[POSITION_RECONCILE_ADJUST][DEFER_FILL_APPLICATION] code=%s db_qty=%s action=KEEP_DB_UNTIL_FILL_APPLIED",
            code,
            db_qty,
        )
    fenced["output1"] = holdings
    return fenced


def _close_stale_positions_guarded_with_fill_fence(*args: Any, **kwargs: Any):
    engine = kwargs.get("engine")
    env = str(kwargs.get("env") or "")
    strategy = str(kwargs.get("strategy") or "")
    if engine is None or not env or not strategy:
        return _ORIGINAL_CLOSE_STALE_GUARDED(*args, **kwargs)
    try:
        pending = _pending_fill_application_codes(engine=engine, env=env, strategy=strategy)
        if pending:
            kwargs = dict(kwargs)
            kwargs["kis_balance"] = _fenced_balance_for_pending(
                engine=engine,
                env=env,
                strategy=strategy,
                kis_balance=kwargs.get("kis_balance"),
                pending_codes=pending,
            )
    except Exception as exc:
        # Never reinterpret a fence failure as KIS=0.  Skip this quantity pass;
        # the next tick can retry after durable-fill inspection recovers.
        logger.exception(
            "[POSITION_RECONCILE_ADJUST][PENDING_FILL_FENCE_FAIL] err_type=%s err=%s action=SKIP_RECONCILE_PASS",
            type(exc).__name__,
            exc,
        )
        return 0
    return _ORIGINAL_CLOSE_STALE_GUARDED(*args, **kwargs)


def install_pending_fill_application_fence() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    reconcile_db.close_stale_positions_guarded = _close_stale_positions_guarded_with_fill_fence
    _INSTALLED = True
    logger.info("[KR_BROKER_TRUTH][PENDING_FILL_FENCE][INSTALLED]")
