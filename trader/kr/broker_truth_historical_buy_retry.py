"""Cross-date retry for BUY fills already attributed to durable orders.

The current-day linker handles fresh KIS executions and unowned fills.  This
module closes the remaining midnight crash window: once a BUY fill has a durable
``order_id``, it is rediscovered by that order identity regardless of fill date
until the exact position lifecycle carries the canonical atomic application
watermark.
"""
from __future__ import annotations

import functools
import logging
from typing import Any

import sqlalchemy as sa

from trader.db.repos import PositionsRepo
from trader.db.schema import schema_for_engine
from trader.time_utils import now_kst
import trader.kr.broker_truth_hardening as base
import trader.kr.broker_truth_review_fixes as review
import trader.kr.broker_truth_final_review_fixes as final

logger = logging.getLogger(__name__)
_INSTALLED = False


def _owned_buy_fill_groups(*, engine, env: str, strategy: str) -> dict[str, dict[str, Any]]:
    schema = schema_for_engine(engine)
    stmt = (
        sa.select(schema.fills, schema.orders)
        .select_from(
            schema.fills.join(
                schema.orders,
                schema.orders.c.order_id == schema.fills.c.order_id,
            )
        )
        .where(
            sa.and_(
                schema.fills.c.env == env,
                schema.fills.c.side == "BUY",
                schema.fills.c.order_id.is_not(None),
                schema.orders.c.env == env,
                schema.orders.c.strategy == strategy,
                schema.orders.c.side == "BUY",
            )
        )
        .order_by(schema.fills.c.filled_at.asc())
    )
    with engine.connect() as conn:
        rows = [dict(row) for row in conn.execute(stmt).mappings().all()]

    groups: dict[str, dict[str, Any]] = {}
    # SQLAlchemy mapping keys are column names; order/fill overlap is ambiguous
    # when selecting both tables. Re-read each durable order and its fills by
    # distinct order_id to keep provenance unambiguous across KIS order-number
    # reuse on different trade dates.
    with engine.connect() as conn:
        order_ids = [
            str(value)
            for value in conn.execute(
                sa.select(schema.fills.c.order_id)
                .select_from(
                    schema.fills.join(
                        schema.orders,
                        schema.orders.c.order_id == schema.fills.c.order_id,
                    )
                )
                .where(
                    sa.and_(
                        schema.fills.c.env == env,
                        schema.fills.c.side == "BUY",
                        schema.fills.c.order_id.is_not(None),
                        schema.orders.c.env == env,
                        schema.orders.c.strategy == strategy,
                        schema.orders.c.side == "BUY",
                    )
                )
                .distinct()
            ).scalars().all()
            if value is not None
        ]
        for order_id in order_ids:
            order = conn.execute(
                sa.select(schema.orders).where(schema.orders.c.order_id == order_id).limit(1)
            ).mappings().first()
            if not order:
                continue
            fills = [
                dict(row)
                for row in conn.execute(
                    sa.select(schema.fills)
                    .where(schema.fills.c.order_id == order_id)
                    .order_by(schema.fills.c.filled_at.asc())
                ).mappings().all()
            ]
            if fills:
                groups[order_id] = {"order": dict(order), "fills": fills}
    return groups


def _mirror_exact_observability(
    *,
    engine,
    schema,
    position: dict[str, Any],
    order_id: str,
    marker: dict[str, Any],
    filled_at,
) -> None:
    legacy_meta = base._json_dict(position.get("position_meta"))
    legacy_map = base._json_dict(legacy_meta.get(review._APPLIED_META_KEY))
    legacy_map[str(order_id)] = dict(marker)
    legacy_meta[review._APPLIED_META_KEY] = legacy_map
    fields: dict[str, Any] = {"position_meta": legacy_meta}
    if not position.get("entry_ts") and filled_at is not None:
        fields["entry_ts"] = base._iso(filled_at)
    review._update_exact_open_position_fields(
        engine=engine,
        schema=schema,
        position=position,
        fields=fields,
    )


def _retry_owned_buy_fills_all_dates(*, engine, env: str, strategy: str) -> int:
    schema = schema_for_engine(engine)
    groups = _owned_buy_fill_groups(engine=engine, env=env, strategy=strategy)
    promoted = 0
    for order_id, payload in groups.items():
        order = dict(payload["order"])
        fills = list(payload["fills"])
        cycle = order.get("position_cycle_id")
        epoch = order.get("portfolio_epoch_id")
        code = base._normalize_code(order.get("code"))
        if not cycle or not epoch or not code:
            logger.error(
                "[KR_BROKER_TRUTH][HIST_BUY_RETRY][BLOCK] order_id=%s reason=ORDER_PROVENANCE_MISSING",
                order_id,
            )
            continue

        cumulative_qty = sum(base._qty(row.get("qty")) for row in fills)
        cumulative_notional = sum(
            base._qty(row.get("qty")) * base._px(row.get("price")) for row in fills
        )
        cumulative_fee = sum(base._px(row.get("fee")) for row in fills)
        cumulative_tax = sum(base._px(row.get("tax")) for row in fills)
        if cumulative_qty <= 0 or cumulative_notional <= 0:
            continue
        marker = {
            "qty": cumulative_qty,
            "notional": cumulative_notional,
            "fee": cumulative_fee,
            "tax": cumulative_tax,
        }
        filled_at = fills[-1].get("filled_at") or order.get("acked_at") or order.get("submitted_at") or now_kst()

        with engine.connect() as conn:
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
            logger.error(
                "[KR_BROKER_TRUTH][HIST_BUY_RETRY][BLOCK] code=%s order_id=%s reason=MULTIPLE_EXACT_OPEN_LIFECYCLES rows=%s",
                code,
                order_id,
                len(positions),
            )
            continue
        position = positions[0] if positions else None
        entry_json = base._json_dict((position or {}).get("entry_meta_json"))
        canonical_map = base._json_dict(entry_json.get(final._BUY_APPLIED_KEY))
        applied = base._json_dict(canonical_map.get(str(order_id)))
        applied_qty = base._qty(applied.get("qty"))
        applied_notional = base._px(applied.get("notional"))
        applied_fee = base._px(applied.get("fee"))
        applied_tax = base._px(applied.get("tax"))

        if applied_qty == cumulative_qty and abs(applied_notional - cumulative_notional) < 1e-9:
            if position:
                _mirror_exact_observability(
                    engine=engine,
                    schema=schema,
                    position=position,
                    order_id=order_id,
                    marker=marker,
                    filled_at=filled_at,
                )
            continue

        if position and not applied:
            baseline = final._baseline_qty(order)
            current_qty = int(position.get("qty") or 0)
            if baseline is None or current_qty != baseline:
                logger.error(
                    "[KR_BROKER_TRUTH][HIST_BUY_RETRY][REVIEW_REQUIRED] code=%s order_id=%s current_qty=%s baseline_qty=%s cumulative_fill_qty=%s reason=MISSING_APPLICATION_WATERMARK",
                    code,
                    order_id,
                    current_qty,
                    baseline,
                    cumulative_qty,
                )
                continue
        elif position is None:
            baseline = final._baseline_qty(order)
            if baseline not in (None, 0):
                logger.error(
                    "[KR_BROKER_TRUTH][HIST_BUY_RETRY][REVIEW_REQUIRED] code=%s order_id=%s baseline_qty=%s reason=EXISTING_POSITION_LIFECYCLE_MISSING",
                    code,
                    order_id,
                    baseline,
                )
                continue

        delta_qty = cumulative_qty - applied_qty
        delta_notional = cumulative_notional - applied_notional
        delta_fee = cumulative_fee - applied_fee
        delta_tax = cumulative_tax - applied_tax
        if delta_qty <= 0:
            if delta_qty < 0:
                logger.error(
                    "[KR_BROKER_TRUTH][HIST_BUY_RETRY][BLOCK] code=%s order_id=%s reason=CUMULATIVE_REGRESSION cumulative=%s applied=%s",
                    code,
                    order_id,
                    cumulative_qty,
                    applied_qty,
                )
            continue
        if delta_notional <= 0:
            logger.error(
                "[KR_BROKER_TRUTH][HIST_BUY_RETRY][BLOCK] code=%s order_id=%s reason=NOTIONAL_MISSING delta_qty=%s",
                code,
                order_id,
                delta_qty,
            )
            continue

        new_map = dict(canonical_map)
        new_map[str(order_id)] = marker
        entry_meta, entry_plan = base._entry_contract(order)
        entry_meta = dict(entry_meta)
        entry_meta[final._BUY_APPLIED_KEY] = new_map
        PositionsRepo(engine).apply_fill(
            env=env,
            strategy=strategy,
            sid=int(order.get("sid") or 1),
            mode=int(order.get("mode") or 1),
            code=code,
            market=order.get("market"),
            side="BUY",
            qty=delta_qty,
            price=delta_notional / delta_qty,
            fee=max(delta_fee, 0.0),
            tax=max(delta_tax, 0.0),
            filled_at=filled_at,
            entry_meta=entry_meta,
            entry_exit_plan=entry_plan,
            portfolio_epoch_id=str(epoch),
            position_cycle_id=str(cycle),
            order_id=str(order_id),
        )

        with engine.connect() as conn:
            verified = conn.execute(
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
        if not verified:
            logger.error(
                "[KR_BROKER_TRUTH][HIST_BUY_RETRY][VERIFY_FAIL] code=%s order_id=%s reason=POSITION_MISSING_AFTER_APPLY",
                code,
                order_id,
            )
            continue
        verified = dict(verified)
        verified_map = base._json_dict(base._json_dict(verified.get("entry_meta_json")).get(final._BUY_APPLIED_KEY))
        verified_marker = base._json_dict(verified_map.get(str(order_id)))
        if base._qty(verified_marker.get("qty")) != cumulative_qty:
            logger.error(
                "[KR_BROKER_TRUTH][HIST_BUY_RETRY][VERIFY_FAIL] code=%s order_id=%s expected=%s actual=%s",
                code,
                order_id,
                cumulative_qty,
                verified_marker,
            )
            continue
        _mirror_exact_observability(
            engine=engine,
            schema=schema,
            position=verified,
            order_id=order_id,
            marker=marker,
            filled_at=filled_at,
        )
        order_qty = base._qty(order.get("qty"))
        status = "FILLED" if order_qty > 0 and cumulative_qty >= order_qty else "PARTIAL_FILLED"
        with engine.begin() as conn:
            conn.execute(
                sa.update(schema.orders)
                .where(schema.orders.c.order_id == order_id)
                .values(status=status, updated_at=sa.func.now())
            )
        promoted += 1
        logger.warning(
            "[KR_BROKER_TRUTH][HIST_BUY_RETRY][OK] code=%s order_id=%s fill_last_at=%s cumulative_qty=%s delta_qty=%s",
            code,
            order_id,
            filled_at,
            cumulative_qty,
            delta_qty,
        )
    return promoted


def install_historical_buy_retry() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    original = review._link_unowned_daily_fills_fixed

    @functools.wraps(original)
    def _with_cross_date_retry(*, engine, env: str, strategy: str):
        historical_promoted = _retry_owned_buy_fills_all_dates(
            engine=engine,
            env=env,
            strategy=strategy,
        )
        result = dict(original(engine=engine, env=env, strategy=strategy) or {})
        result["positions_promoted"] = int(result.get("positions_promoted") or 0) + historical_promoted
        result.setdefault("linked_fills", 0)
        return result

    review._link_unowned_daily_fills_fixed = _with_cross_date_retry
    _INSTALLED = True
    logger.info("[KR_BROKER_TRUTH][HIST_BUY_RETRY][INSTALLED]")
