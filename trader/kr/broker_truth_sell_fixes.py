"""Atomic SELL-fill application guard for PR125.

A KIS daily-ccld SELL execution must not be considered reconciled merely because
its fill row was linked to an order.  The same transaction that claims the fill
also updates the exact position lifecycle, including realized P&L, and advances
the durable order state.  This removes the crash window where a linked SELL was
no longer discoverable but had never reached position accounting.
"""
from __future__ import annotations

import logging
from typing import Any

import sqlalchemy as sa

from trader.db.repos import FillsRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.time_utils import now_kst
import trader.kr.broker_truth_hardening as base
import trader.kr.broker_truth_review_fixes as review

logger = logging.getLogger(__name__)
_INSTALLED = False
_APPLIED_META_KEY = "broker_truth_applied_orders"
_SELL_APPLIED_KEY = "broker_truth_sell_applied"


def _atomic_link_and_apply_sell(
    *,
    engine,
    env: str,
    strategy: str,
    kis_odno: str,
    fill_ids: list[Any],
) -> tuple[int, int]:
    """Link one SELL order's fills and update its exact lifecycle atomically."""
    schema = schema_for_engine(engine)
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
            logger.error(
                "[KR_BROKER_TRUTH][SELL_APPLY][BLOCK] kis_odno=%s reason=ORDER_NOT_FOUND",
                kis_odno,
            )
            return 0, 0
        order = dict(order_row)
        if str(order.get("side") or "").upper() != "SELL":
            return 0, 0

        order_id = order.get("order_id")
        cycle = order.get("position_cycle_id")
        epoch = order.get("portfolio_epoch_id")
        code = base._normalize_code(order.get("code"))
        if not order_id or not cycle or not epoch or not code:
            logger.error(
                "[KR_BROKER_TRUTH][SELL_APPLY][BLOCK] code=%s kis_odno=%s reason=PROVENANCE_MISSING order_id=%s cycle=%s epoch=%s",
                code,
                kis_odno,
                order_id,
                cycle,
                epoch,
            )
            return 0, 0

        # Lock/select the one exact active lifecycle before claiming any fills.
        position_rows = [
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
        if len(position_rows) != 1:
            logger.error(
                "[KR_BROKER_TRUTH][SELL_APPLY][BLOCK] code=%s order_id=%s cycle=%s epoch=%s open_rows=%s reason=EXACT_OPEN_LIFECYCLE_NOT_UNIQUE",
                code,
                order_id,
                cycle,
                epoch,
                len(position_rows),
            )
            return 0, 0
        position = position_rows[0]

        # Claim/link only the supplied still-unowned executions.  If anything
        # races, the rowcount check fails and the transaction rolls back.
        linked_now = 0
        for fill_id in fill_ids:
            result = conn.execute(
                sa.update(schema.fills)
                .where(
                    sa.and_(
                        schema.fills.c.fill_id == fill_id,
                        schema.fills.c.order_id.is_(None),
                    )
                )
                .values(
                    order_id=order_id,
                    position_cycle_id=cycle,
                    portfolio_epoch_id=epoch,
                )
            )
            if int(result.rowcount or 0) != 1:
                raise RuntimeError(
                    f"SELL_FILL_LINK_RACE fill_id={fill_id} order_id={order_id}"
                )
            linked_now += 1

        attributed = [
            dict(row)
            for row in conn.execute(
                sa.select(schema.fills)
                .where(schema.fills.c.order_id == order_id)
                .order_by(schema.fills.c.filled_at.asc())
            ).mappings().all()
        ]
        cumulative_qty = sum(base._qty(row.get("qty")) for row in attributed)
        cumulative_notional = sum(
            base._qty(row.get("qty")) * base._px(row.get("price")) for row in attributed
        )
        cumulative_fee = sum(base._px(row.get("fee")) for row in attributed)
        cumulative_tax = sum(base._px(row.get("tax")) for row in attributed)
        order_qty = base._qty(order.get("qty"))
        canonical_status = (
            "FILLED" if order_qty > 0 and cumulative_qty >= order_qty else "PARTIAL_FILLED"
        )

        response = base._json_dict(order.get("response_json"))
        applied = base._json_dict(response.get(_SELL_APPLIED_KEY))
        applied_qty = base._qty(applied.get("qty"))
        applied_notional = base._px(applied.get("notional"))
        applied_fee = base._px(applied.get("fee"))
        applied_tax = base._px(applied.get("tax"))
        delta_qty = cumulative_qty - applied_qty
        delta_notional = cumulative_notional - applied_notional
        delta_fee = cumulative_fee - applied_fee
        delta_tax = cumulative_tax - applied_tax

        if delta_qty < 0 or delta_notional < -1e-9 or delta_fee < -1e-9 or delta_tax < -1e-9:
            raise RuntimeError(
                f"SELL_FILL_CUMULATIVE_REGRESSION order_id={order_id} cumulative_qty={cumulative_qty} applied_qty={applied_qty}"
            )

        position_applied = 0
        if delta_qty > 0:
            current_qty = int(position.get("qty") or 0)
            avg_buy_price = float(position.get("avg_buy_price") or 0.0)
            total_cost = float(position.get("total_cost") or 0.0)
            realized_pnl = float(position.get("realized_pnl") or 0.0)
            if delta_qty > current_qty:
                raise RuntimeError(
                    f"SELL_FILL_EXCEEDS_POSITION order_id={order_id} delta_qty={delta_qty} current_qty={current_qty}"
                )
            if delta_notional <= 0:
                raise RuntimeError(
                    f"SELL_FILL_NOTIONAL_MISSING order_id={order_id} delta_qty={delta_qty} delta_notional={delta_notional}"
                )

            remaining_qty = current_qty - delta_qty
            proceeds = delta_notional - max(delta_fee, 0.0) - max(delta_tax, 0.0)
            cost_basis = avg_buy_price * delta_qty
            new_realized = realized_pnl + (proceeds - cost_basis)
            new_total_cost = max(total_cost - cost_basis, 0.0)
            values: dict[str, Any] = {
                "qty": remaining_qty,
                "avg_buy_price": avg_buy_price if remaining_qty > 0 else None,
                "total_cost": new_total_cost,
                "realized_pnl": new_realized,
                "market": order.get("market"),
                "last_trade_at": attributed[-1].get("filled_at") or order.get("acked_at") or now_kst(),
                "updated_at": sa.func.now(),
            }
            if remaining_qty <= 0:
                values.update(
                    status="CLOSED",
                    closed_ts=attributed[-1].get("filled_at") or now_kst(),
                    closed_reason="FULL_SELL",
                )
            result = conn.execute(
                sa.update(schema.positions)
                .where(
                    sa.and_(
                        schema.positions.c.position_id == position.get("position_id"),
                        schema.positions.c.env == env,
                        schema.positions.c.strategy == strategy,
                        schema.positions.c.code == code,
                        schema.positions.c.position_cycle_id == cycle,
                        schema.positions.c.portfolio_epoch_id == epoch,
                        schema.positions.c.status == "OPEN",
                        schema.positions.c.qty == current_qty,
                    )
                )
                .values(**values)
            )
            if int(result.rowcount or 0) != 1:
                raise RuntimeError(
                    f"SELL_POSITION_UPDATE_RACE order_id={order_id} position_id={position.get('position_id')}"
                )
            position_applied = 1

        # Store the cumulative position-application watermark on the order in the
        # same transaction.  A retry can never double-realize a SELL.
        response[_SELL_APPLIED_KEY] = {
            "qty": cumulative_qty,
            "notional": cumulative_notional,
            "fee": cumulative_fee,
            "tax": cumulative_tax,
        }
        conn.execute(
            sa.update(schema.orders)
            .where(schema.orders.c.order_id == order_id)
            .values(
                status=canonical_status,
                response_json=response,
                updated_at=sa.func.now(),
            )
        )

        logger.warning(
            "[KR_BROKER_TRUTH][SELL_APPLY][OK] code=%s kis_odno=%s order_id=%s linked_now=%s cumulative_qty=%s delta_qty=%s status=%s realized_pnl_after=%s",
            code,
            kis_odno,
            order_id,
            linked_now,
            cumulative_qty,
            delta_qty,
            canonical_status,
            (
                float(position.get("realized_pnl") or 0.0)
                + ((delta_notional - max(delta_fee, 0.0) - max(delta_tax, 0.0)) - float(position.get("avg_buy_price") or 0.0) * delta_qty)
                if delta_qty > 0
                else float(position.get("realized_pnl") or 0.0)
            ),
        )
        return linked_now, position_applied


def _link_unowned_daily_fills_with_sell(*, engine, env: str, strategy: str) -> dict[str, int]:
    """Process SELLs atomically, then delegate remaining BUYs to reviewed logic."""
    rows = [dict(row) for row in (FillsRepo(engine).list_today_fills(env) or [])]
    unowned_sells = [
        row
        for row in rows
        if not row.get("order_id")
        and row.get("kis_odno")
        and str(row.get("side") or "").upper() == "SELL"
    ]
    linked = 0
    applied = 0
    by_odno: dict[str, list[dict[str, Any]]] = {}
    for row in unowned_sells:
        by_odno.setdefault(str(row.get("kis_odno") or "").strip(), []).append(row)

    for kis_odno, sell_rows in by_odno.items():
        if not kis_odno:
            continue
        try:
            linked_now, applied_now = _atomic_link_and_apply_sell(
                engine=engine,
                env=env,
                strategy=strategy,
                kis_odno=kis_odno,
                fill_ids=[row.get("fill_id") for row in sell_rows],
            )
            linked += linked_now
            applied += applied_now
        except Exception as exc:
            # Transaction rollback preserves the unowned fill, making the failure
            # retryable instead of silently losing realized P&L.
            logger.exception(
                "[KR_BROKER_TRUTH][SELL_APPLY][ROLLBACK] kis_odno=%s err_type=%s err=%s",
                kis_odno,
                type(exc).__name__,
                exc,
            )

    # Successfully processed SELL rows now have order_id and are invisible to
    # the BUY-oriented reviewed linker.  If a SELL rolled back, do not let the
    # older linker claim it without position application; temporarily return and
    # retry the whole reconciliation on the next tick.
    remaining = [
        dict(row)
        for row in (FillsRepo(engine).list_today_fills(env) or [])
        if not row.get("order_id") and row.get("kis_odno")
    ]
    if any(str(row.get("side") or "").upper() == "SELL" for row in remaining):
        logger.error(
            "[KR_BROKER_TRUTH][SELL_APPLY][FENCE] remaining_unapplied_sell=%s action=RETRY_NEXT_TICK",
            len([row for row in remaining if str(row.get("side") or "").upper() == "SELL"]),
        )
        return {"linked_fills": linked, "positions_promoted": applied}

    buy_result = review._link_unowned_daily_fills_fixed(
        engine=engine,
        env=env,
        strategy=strategy,
    )
    return {
        "linked_fills": linked + int(buy_result.get("linked_fills") or 0),
        "positions_promoted": applied + int(buy_result.get("positions_promoted") or 0),
    }


def install_sell_fill_guard() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    base._link_unowned_daily_fills = _link_unowned_daily_fills_with_sell
    _INSTALLED = True
    logger.info("[KR_BROKER_TRUTH][SELL_FILL_GUARD][INSTALLED]")
