"""Cross-date recovery for KIS fills persisted before durable order attribution.

`reconcile_today()` persists daily-ccld executions first and the PR125 broker-truth
linker attributes them to the durable order immediately afterwards.  A process
crash in that small gap leaves `fills.order_id` NULL.  Once KST midnight passes,
current-day linkers no longer see that fill.

This guard runs before the normal current-day linker and repairs only prior-day
unowned fills that can be matched to exactly one durable order using env,
strategy, symbol, side, broker order number, and KST trade date.  BUY fills are
linked and then flow through the existing cross-date owned-BUY retry.  SELL fills
are linked and applied to their exact lifecycle atomically so realized P&L is not
lost.  Ambiguous evidence fails closed and remains unowned for operator review.
"""
from __future__ import annotations

from datetime import datetime
import functools
import logging
from typing import Any
from zoneinfo import ZoneInfo

import sqlalchemy as sa

from trader.db.schema import schema_for_engine
from trader.time_utils import now_kst
import trader.kr.broker_truth_hardening as base

logger = logging.getLogger(__name__)
_INSTALLED = False
_KST = ZoneInfo("Asia/Seoul")
_SELL_APPLIED_KEY = "broker_truth_sell_applied"


def _kst_date(value: Any):
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.astimezone(_KST).date()
        return value.date()
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(_KST)
        return parsed.date()
    except Exception:
        return None


def _order_trade_date(order: dict[str, Any]):
    for key in ("acked_at", "submitted_at", "created_at"):
        day = _kst_date(order.get(key))
        if day is not None:
            return day
    return None


def _find_exact_order_for_unowned_fill(*, conn, schema, env: str, strategy: str, fill: dict[str, Any]):
    code = base._normalize_code(fill.get("code"))
    side = str(fill.get("side") or "").upper()
    kis_odno = str(fill.get("kis_odno") or "").strip()
    fill_day = _kst_date(fill.get("filled_at"))
    if not code or side not in {"BUY", "SELL"} or not kis_odno or fill_day is None:
        return None

    rows = [
        dict(row)
        for row in conn.execute(
            sa.select(schema.orders).where(
                sa.and_(
                    schema.orders.c.env == env,
                    schema.orders.c.strategy == strategy,
                    schema.orders.c.code == code,
                    schema.orders.c.side == side,
                    sa.or_(
                        schema.orders.c.kis_odno == kis_odno,
                        schema.orders.c.broker_order_id == kis_odno,
                    ),
                )
            )
        ).mappings().all()
    ]
    exact = [row for row in rows if _order_trade_date(row) == fill_day]
    if len(exact) != 1:
        logger.error(
            "[KR_BROKER_TRUTH][CROSS_DATE_UNOWNED][BLOCK] code=%s side=%s kis_odno=%s fill_day=%s candidates=%s exact_date_candidates=%s reason=ORDER_MATCH_NOT_UNIQUE",
            code,
            side,
            kis_odno,
            fill_day,
            len(rows),
            len(exact),
        )
        return None
    order = exact[0]
    if not order.get("order_id") or not order.get("position_cycle_id") or not order.get("portfolio_epoch_id"):
        logger.error(
            "[KR_BROKER_TRUTH][CROSS_DATE_UNOWNED][BLOCK] code=%s side=%s kis_odno=%s order_id=%s reason=ORDER_PROVENANCE_MISSING",
            code,
            side,
            kis_odno,
            order.get("order_id"),
        )
        return None
    return order


def _apply_historical_sell_exact(*, engine, env: str, strategy: str, order: dict[str, Any], fill_ids: list[Any]) -> tuple[int, int]:
    """Claim prior-day SELL fills and update the exact lifecycle in one transaction."""
    schema = schema_for_engine(engine)
    order_id = order.get("order_id")
    cycle = order.get("position_cycle_id")
    epoch = order.get("portfolio_epoch_id")
    code = base._normalize_code(order.get("code"))
    with engine.begin() as conn:
        live_order = conn.execute(
            sa.select(schema.orders).where(
                sa.and_(
                    schema.orders.c.order_id == order_id,
                    schema.orders.c.env == env,
                    schema.orders.c.strategy == strategy,
                    schema.orders.c.side == "SELL",
                )
            ).limit(1)
        ).mappings().first()
        if not live_order:
            raise RuntimeError(f"CROSS_DATE_SELL_ORDER_MISSING order_id={order_id}")
        order = dict(live_order)

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
        if len(positions) != 1:
            raise RuntimeError(
                f"CROSS_DATE_SELL_LIFECYCLE_NOT_UNIQUE order_id={order_id} rows={len(positions)}"
            )
        position = positions[0]

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
                raise RuntimeError(f"CROSS_DATE_SELL_FILL_LINK_RACE fill_id={fill_id} order_id={order_id}")
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
        cumulative_notional = sum(base._qty(row.get("qty")) * base._px(row.get("price")) for row in attributed)
        cumulative_fee = sum(base._px(row.get("fee")) for row in attributed)
        cumulative_tax = sum(base._px(row.get("tax")) for row in attributed)

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
                f"CROSS_DATE_SELL_CUMULATIVE_REGRESSION order_id={order_id} cumulative_qty={cumulative_qty} applied_qty={applied_qty}"
            )

        position_applied = 0
        if delta_qty > 0:
            current_qty = int(position.get("qty") or 0)
            avg_buy_price = float(position.get("avg_buy_price") or 0.0)
            total_cost = float(position.get("total_cost") or 0.0)
            realized_pnl = float(position.get("realized_pnl") or 0.0)
            if delta_qty > current_qty or delta_notional <= 0:
                raise RuntimeError(
                    f"CROSS_DATE_SELL_INVALID_DELTA order_id={order_id} delta_qty={delta_qty} current_qty={current_qty} delta_notional={delta_notional}"
                )
            remaining_qty = current_qty - delta_qty
            proceeds = delta_notional - max(delta_fee, 0.0) - max(delta_tax, 0.0)
            cost_basis = avg_buy_price * delta_qty
            values: dict[str, Any] = {
                "qty": remaining_qty,
                "avg_buy_price": avg_buy_price if remaining_qty > 0 else None,
                "total_cost": max(total_cost - cost_basis, 0.0),
                "realized_pnl": realized_pnl + (proceeds - cost_basis),
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
                raise RuntimeError(f"CROSS_DATE_SELL_POSITION_RACE order_id={order_id}")
            position_applied = 1

        order_qty = base._qty(order.get("qty"))
        status = "FILLED" if order_qty > 0 and cumulative_qty >= order_qty else "PARTIAL_FILLED"
        response[_SELL_APPLIED_KEY] = {
            "qty": cumulative_qty,
            "notional": cumulative_notional,
            "fee": cumulative_fee,
            "tax": cumulative_tax,
        }
        conn.execute(
            sa.update(schema.orders)
            .where(schema.orders.c.order_id == order_id)
            .values(status=status, response_json=response, updated_at=sa.func.now())
        )
        return linked_now, position_applied


def _retry_historical_unowned_fills(*, engine, env: str, strategy: str) -> dict[str, int]:
    schema = schema_for_engine(engine)
    today = now_kst().date()
    with engine.connect() as conn:
        rows = [
            dict(row)
            for row in conn.execute(
                sa.select(schema.fills).where(
                    sa.and_(
                        schema.fills.c.env == env,
                        schema.fills.c.order_id.is_(None),
                        schema.fills.c.kis_odno.is_not(None),
                        schema.fills.c.side.in_(["BUY", "SELL"]),
                    )
                ).order_by(schema.fills.c.filled_at.asc())
            ).mappings().all()
        ]

    historical_rows = [row for row in rows if (_kst_date(row.get("filled_at")) or today) < today]
    if not historical_rows:
        return {"linked_fills": 0, "positions_promoted": 0}

    buy_links: list[tuple[Any, dict[str, Any]]] = []
    sell_groups: dict[str, dict[str, Any]] = {}
    with engine.connect() as conn:
        for fill in historical_rows:
            order = _find_exact_order_for_unowned_fill(
                conn=conn,
                schema=schema,
                env=env,
                strategy=strategy,
                fill=fill,
            )
            if not order:
                continue
            if str(fill.get("side") or "").upper() == "BUY":
                buy_links.append((fill.get("fill_id"), order))
            else:
                key = str(order.get("order_id"))
                group = sell_groups.setdefault(key, {"order": order, "fill_ids": []})
                group["fill_ids"].append(fill.get("fill_id"))

    linked = 0
    applied = 0
    for fill_id, order in buy_links:
        with engine.begin() as conn:
            result = conn.execute(
                sa.update(schema.fills)
                .where(
                    sa.and_(
                        schema.fills.c.fill_id == fill_id,
                        schema.fills.c.order_id.is_(None),
                    )
                )
                .values(
                    order_id=order.get("order_id"),
                    position_cycle_id=order.get("position_cycle_id"),
                    portfolio_epoch_id=order.get("portfolio_epoch_id"),
                )
            )
        if int(result.rowcount or 0) == 1:
            linked += 1
            logger.warning(
                "[KR_BROKER_TRUTH][CROSS_DATE_UNOWNED][BUY_LINKED] code=%s order_id=%s fill_id=%s",
                base._normalize_code(order.get("code")),
                order.get("order_id"),
                fill_id,
            )

    for group in sell_groups.values():
        order = dict(group["order"])
        try:
            linked_now, applied_now = _apply_historical_sell_exact(
                engine=engine,
                env=env,
                strategy=strategy,
                order=order,
                fill_ids=list(group["fill_ids"]),
            )
            linked += linked_now
            applied += applied_now
            logger.warning(
                "[KR_BROKER_TRUTH][CROSS_DATE_UNOWNED][SELL_APPLIED] code=%s order_id=%s linked=%s applied=%s",
                base._normalize_code(order.get("code")),
                order.get("order_id"),
                linked_now,
                applied_now,
            )
        except Exception as exc:
            logger.exception(
                "[KR_BROKER_TRUTH][CROSS_DATE_UNOWNED][SELL_ROLLBACK] order_id=%s err_type=%s err=%s",
                order.get("order_id"),
                type(exc).__name__,
                exc,
            )
    return {"linked_fills": linked, "positions_promoted": applied}


def install_cross_date_unowned_retry() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    original = base._link_unowned_daily_fills

    @functools.wraps(original)
    def _with_cross_date_unowned(*, engine, env: str, strategy: str):
        historical_result = _retry_historical_unowned_fills(
            engine=engine,
            env=env,
            strategy=strategy,
        )
        current_result = dict(original(engine=engine, env=env, strategy=strategy) or {})
        current_result["linked_fills"] = int(current_result.get("linked_fills") or 0) + int(
            historical_result.get("linked_fills") or 0
        )
        current_result["positions_promoted"] = int(current_result.get("positions_promoted") or 0) + int(
            historical_result.get("positions_promoted") or 0
        )
        return current_result

    base._link_unowned_daily_fills = _with_cross_date_unowned
    _INSTALLED = True
    logger.info("[KR_BROKER_TRUTH][CROSS_DATE_UNOWNED][INSTALLED]")
