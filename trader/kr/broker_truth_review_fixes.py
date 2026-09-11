"""Follow-up guards for PR125 review findings.

These replacements are installed after :mod:`trader.kr.broker_truth_hardening`.
They keep the same public/runtime contract while fixing two edge cases found by
review:

* one broker BUY order may arrive as multiple daily-ccld fill rows; position
  accounting must apply the cumulative execution exactly once rather than only
  the first row;
* a fresh KIS balance that omits a DB-open symbol is broker quantity zero for
  health reporting, even though destructive zeroing still uses the existing
  multi-snapshot safety guard.
"""
from __future__ import annotations

import logging
from typing import Any

import sqlalchemy as sa

from trader.db.repos import FillsRepo, OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.time_utils import now_kst
import trader.kr.broker_truth_hardening as base

logger = logging.getLogger(__name__)
_INSTALLED = False
_APPLIED_META_KEY = "broker_truth_applied_orders"


def _link_unowned_daily_fills_fixed(*, engine, env: str, strategy: str) -> dict[str, int]:
    """Link daily fills by broker order and apply cumulative BUY deltas exactly once."""
    schema = schema_for_engine(engine)
    fills_repo = FillsRepo(engine)
    rows = [dict(row) for row in (fills_repo.list_today_fills(env) or [])]
    unowned = [row for row in rows if not row.get("order_id") and row.get("kis_odno")]
    if not unowned:
        return {"linked_fills": 0, "positions_promoted": 0}

    by_odno: dict[str, list[dict[str, Any]]] = {}
    for row in unowned:
        by_odno.setdefault(str(row.get("kis_odno") or "").strip(), []).append(row)

    linked = 0
    promoted = 0
    for kis_odno, new_rows in by_odno.items():
        if not kis_odno:
            continue
        with engine.begin() as conn:
            order_row = conn.execute(
                sa.select(schema.orders).where(
                    sa.and_(
                        schema.orders.c.env == env,
                        schema.orders.c.strategy == strategy,
                        sa.or_(
                            schema.orders.c.kis_odno == kis_odno,
                            schema.orders.c.broker_order_id == kis_odno,
                        ),
                    )
                ).order_by(schema.orders.c.created_at.desc()).limit(1)
            ).mappings().first()
            if not order_row:
                continue
            order = dict(order_row)
            order_id = order.get("order_id")
            for fill in new_rows:
                conn.execute(
                    sa.update(schema.fills)
                    .where(schema.fills.c.fill_id == fill.get("fill_id"))
                    .values(
                        order_id=order_id,
                        position_cycle_id=order.get("position_cycle_id"),
                        portfolio_epoch_id=order.get("portfolio_epoch_id"),
                    )
                )
                linked += 1

        # Re-read every execution already attributed to this durable order. This
        # makes repeated invocations and fills arriving over several ticks safe.
        with engine.connect() as conn:
            attributed = [
                dict(row)
                for row in conn.execute(
                    sa.select(schema.fills).where(schema.fills.c.order_id == order_id)
                    .order_by(schema.fills.c.filled_at.asc())
                ).mappings().all()
            ]
        cumulative_qty = sum(base._qty(row.get("qty")) for row in attributed)
        cumulative_notional = sum(base._qty(row.get("qty")) * base._px(row.get("price")) for row in attributed)
        cumulative_fee = sum(base._px(row.get("fee")) for row in attributed)
        cumulative_tax = sum(base._px(row.get("tax")) for row in attributed)
        order_qty = base._qty(order.get("qty"))
        canonical_status = "FILLED" if order_qty > 0 and cumulative_qty >= order_qty else "PARTIAL_FILLED"
        with engine.begin() as conn:
            conn.execute(
                sa.update(schema.orders)
                .where(schema.orders.c.order_id == order_id)
                .values(status=canonical_status, updated_at=sa.func.now())
            )

        if str(order.get("side") or "").upper() == "BUY" and cumulative_qty > 0 and cumulative_notional > 0:
            cycle = order.get("position_cycle_id")
            epoch = order.get("portfolio_epoch_id")
            code = base._normalize_code(order.get("code"))
            with engine.connect() as conn:
                pos = conn.execute(
                    sa.select(schema.positions).where(
                        sa.and_(
                            schema.positions.c.env == env,
                            schema.positions.c.strategy == strategy,
                            schema.positions.c.code == code,
                            schema.positions.c.position_cycle_id == cycle,
                            schema.positions.c.portfolio_epoch_id == epoch,
                            schema.positions.c.status == "OPEN",
                        )
                    ).limit(1)
                ).mappings().first()
            pos_dict = dict(pos) if pos else {}
            pos_meta = base._json_dict(pos_dict.get("position_meta"))
            applied_map = base._json_dict(pos_meta.get(_APPLIED_META_KEY))
            applied = base._json_dict(applied_map.get(str(order_id)))
            applied_qty = base._qty(applied.get("qty"))
            applied_notional = base._px(applied.get("notional"))

            # If an exact-cycle position predates this guard and has no marker,
            # do not guess whether some/all fills were already applied.
            if pos and not applied and int(pos_dict.get("qty") or 0) > 0:
                logger.error(
                    "[KR_BROKER_TRUTH][SPLIT_FILL][REVIEW_REQUIRED] code=%s order_id=%s position_qty=%s cumulative_fill_qty=%s",
                    code, order_id, pos_dict.get("qty"), cumulative_qty,
                )
            else:
                delta_qty = cumulative_qty - applied_qty
                delta_notional = cumulative_notional - applied_notional
                if delta_qty > 0 and delta_notional > 0:
                    delta_price = delta_notional / delta_qty
                    entry_meta, entry_plan = base._entry_contract(order)
                    filled_at = attributed[-1].get("filled_at") or order.get("acked_at") or order.get("submitted_at") or now_kst()
                    PositionsRepo(engine).apply_fill(
                        env=env,
                        strategy=strategy,
                        sid=int(order.get("sid") or 1),
                        mode=int(order.get("mode") or 1),
                        code=code,
                        market=order.get("market"),
                        side="BUY",
                        qty=delta_qty,
                        price=delta_price,
                        fee=max(0.0, cumulative_fee - base._px(applied.get("fee"))),
                        tax=max(0.0, cumulative_tax - base._px(applied.get("tax"))),
                        filled_at=filled_at,
                        entry_meta=entry_meta,
                        entry_exit_plan=entry_plan,
                        portfolio_epoch_id=str(epoch),
                        position_cycle_id=str(cycle),
                        order_id=str(order_id),
                    )
                    # Persist cumulative application evidence on the position so
                    # a later split row applies only the newly confirmed delta.
                    with engine.connect() as conn:
                        current = conn.execute(
                            sa.select(schema.positions).where(
                                sa.and_(
                                    schema.positions.c.env == env,
                                    schema.positions.c.strategy == strategy,
                                    schema.positions.c.code == code,
                                    schema.positions.c.position_cycle_id == cycle,
                                    schema.positions.c.portfolio_epoch_id == epoch,
                                    schema.positions.c.status == "OPEN",
                                )
                            ).limit(1)
                        ).mappings().first()
                    current_meta = base._json_dict((dict(current) if current else {}).get("position_meta"))
                    current_map = base._json_dict(current_meta.get(_APPLIED_META_KEY))
                    current_map[str(order_id)] = {
                        "qty": cumulative_qty,
                        "notional": cumulative_notional,
                        "fee": cumulative_fee,
                        "tax": cumulative_tax,
                    }
                    current_meta[_APPLIED_META_KEY] = current_map
                    PositionsRepo(engine).update_position_fields(
                        env=env,
                        strategy=strategy,
                        sid=int(order.get("sid") or 1),
                        mode=int(order.get("mode") or 1),
                        code=code,
                        fields={"position_meta": current_meta, "entry_ts": base._iso(filled_at)},
                    )
                    promoted += 1

        logger.warning(
            "[KR_BROKER_TRUTH][FILL_LINK][OK] code=%s kis_odno=%s order_id=%s linked_now=%s cumulative_qty=%s status=%s",
            base._normalize_code(order.get("code")), kis_odno, order_id, len(new_rows), cumulative_qty, canonical_status,
        )

    return {"linked_fills": linked, "positions_promoted": promoted}


def _health_after_reconcile_fixed(*, engine, env: str, strategy: str, holdings_rows: list[dict] | None) -> dict[str, Any]:
    """Treat symbol absence from a fresh KIS snapshot as broker qty zero for health only."""
    schema = schema_for_engine(engine)
    kis = base._holdings_index(holdings_rows)
    mismatches: list[dict[str, Any]] = []
    with engine.connect() as conn:
        positions = [
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
    for row in positions:
        code = base._normalize_code(row.get("code"))
        db_qty = base._qty(row.get("qty"))
        broker_qty = int((kis.get(code) or {}).get("qty") or 0)
        if db_qty != broker_qty:
            mismatches.append({"code": code, "db_qty": db_qty, "kis_qty": broker_qty})

    today = now_kst().date()
    stale_orders: list[dict[str, Any]] = []
    try:
        for order in OrdersRepo(engine).get_open_orders(env, include_stale=True) or []:
            if str(order.get("strategy") or "") != strategy:
                continue
            created_day = base._date_of(order.get("created_at"))
            if created_day is not None and created_day < today:
                stale_orders.append(
                    {
                        "order_id": str(order.get("order_id") or ""),
                        "code": base._normalize_code(order.get("code")),
                        "side": str(order.get("side") or ""),
                        "status": str(order.get("status") or ""),
                        "created_at": base._iso(order.get("created_at")),
                    }
                )
    except Exception as exc:
        logger.exception("[KR_BROKER_TRUTH][HEALTH][STALE_ORDER_SCAN_FAIL] err=%s", exc)

    if mismatches:
        logger.error("[KR_BROKER_TRUTH][HEALTH][QTY_MISMATCH] count=%s rows=%s", len(mismatches), mismatches)
    if stale_orders:
        logger.error("[KR_BROKER_TRUTH][HEALTH][STALE_OPEN_ORDER] count=%s rows=%s", len(stale_orders), stale_orders)
    return {
        "qty_mismatch_count": len(mismatches),
        "qty_mismatches": mismatches,
        "stale_open_order_count": len(stale_orders),
        "stale_open_orders": stale_orders,
    }


def install_review_feedback_guards() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    base._link_unowned_daily_fills = _link_unowned_daily_fills_fixed
    base._health_after_reconcile = _health_after_reconcile_fixed
    _INSTALLED = True
    logger.info("[KR_BROKER_TRUTH][REVIEW_FIX_GUARD][INSTALLED]")
