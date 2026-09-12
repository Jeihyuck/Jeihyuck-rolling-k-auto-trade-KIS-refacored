"""Retry-safe observability compatibility for PR125 BUY reconciliation.

The canonical BUY application watermark lives in ``entry_meta_json`` and is
committed atomically with quantity/cost accounting.  Older runtime checks and
operator tooling also expect ``entry_ts`` plus a mirror under
``position_meta.broker_truth_applied_orders``.  Those fields are derived from
already-committed broker truth and may safely be repaired after the canonical
transaction: if a process dies before this mirror write, the next tick repeats
it without changing quantity or cost.
"""
from __future__ import annotations

import functools
import logging

import sqlalchemy as sa

from trader.db.repos import FillsRepo
from trader.db.schema import schema_for_engine
import trader.kr.broker_truth_hardening as base
import trader.kr.broker_truth_review_fixes as review
import trader.kr.broker_truth_final_review_fixes as final

logger = logging.getLogger(__name__)
_INSTALLED = False


def _repair_buy_observability(*, engine, env: str, strategy: str) -> int:
    schema = schema_for_engine(engine)
    fills = [
        dict(row)
        for row in (FillsRepo(engine).list_today_fills(env, side="BUY") or [])
        if row.get("order_id")
    ]
    if not fills:
        return 0

    by_order: dict[str, list[dict]] = {}
    for fill in fills:
        by_order.setdefault(str(fill.get("order_id")), []).append(fill)

    repaired = 0
    for order_id, order_fills in by_order.items():
        with engine.connect() as conn:
            order_row = conn.execute(
                sa.select(schema.orders).where(
                    sa.and_(
                        schema.orders.c.order_id == order_id,
                        schema.orders.c.env == env,
                        schema.orders.c.strategy == strategy,
                        schema.orders.c.side == "BUY",
                    )
                ).limit(1)
            ).mappings().first()
        if not order_row:
            continue
        order = dict(order_row)
        cycle = order.get("position_cycle_id")
        epoch = order.get("portfolio_epoch_id")
        code = base._normalize_code(order.get("code"))
        if not cycle or not epoch or not code:
            continue

        cumulative_qty = sum(base._qty(row.get("qty")) for row in order_fills)
        cumulative_notional = sum(
            base._qty(row.get("qty")) * base._px(row.get("price")) for row in order_fills
        )
        cumulative_fee = sum(base._px(row.get("fee")) for row in order_fills)
        cumulative_tax = sum(base._px(row.get("tax")) for row in order_fills)
        if cumulative_qty <= 0:
            continue

        with engine.connect() as conn:
            rows = [
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
        if len(rows) != 1:
            if len(rows) > 1:
                logger.error(
                    "[KR_BROKER_TRUTH][OBS_COMPAT][BLOCK] code=%s order_id=%s reason=MULTIPLE_EXACT_OPEN_LIFECYCLES rows=%s",
                    code,
                    order_id,
                    len(rows),
                )
            continue
        position = rows[0]

        # Only mirror data after the atomic canonical watermark proves that the
        # cumulative BUY has already reached this exact position lifecycle.
        entry_json = base._json_dict(position.get("entry_meta_json"))
        canonical_map = base._json_dict(entry_json.get(final._BUY_APPLIED_KEY))
        canonical = base._json_dict(canonical_map.get(str(order_id)))
        if base._qty(canonical.get("qty")) != cumulative_qty:
            continue

        legacy_meta = base._json_dict(position.get("position_meta"))
        legacy_map = base._json_dict(legacy_meta.get(review._APPLIED_META_KEY))
        expected = {
            "qty": cumulative_qty,
            "notional": cumulative_notional,
            "fee": cumulative_fee,
            "tax": cumulative_tax,
        }
        current = base._json_dict(legacy_map.get(str(order_id)))
        first_fill_at = min(
            (row.get("filled_at") for row in order_fills if row.get("filled_at") is not None),
            default=None,
        )
        fields: dict = {}
        if current != expected:
            legacy_map[str(order_id)] = expected
            legacy_meta[review._APPLIED_META_KEY] = legacy_map
            fields["position_meta"] = legacy_meta
        if not position.get("entry_ts") and first_fill_at is not None:
            fields["entry_ts"] = base._iso(first_fill_at)
        if not fields:
            continue

        if review._update_exact_open_position_fields(
            engine=engine,
            schema=schema,
            position=position,
            fields=fields,
        ):
            repaired += 1
            logger.warning(
                "[KR_BROKER_TRUTH][OBS_COMPAT][REPAIRED] code=%s order_id=%s fields=%s",
                code,
                order_id,
                sorted(fields),
            )
    return repaired


def install_buy_observability_compat() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    original = review._link_unowned_daily_fills_fixed

    @functools.wraps(original)
    def _with_observability(*, engine, env: str, strategy: str):
        result = original(engine=engine, env=env, strategy=strategy)
        _repair_buy_observability(engine=engine, env=env, strategy=strategy)
        return result

    review._link_unowned_daily_fills_fixed = _with_observability
    _INSTALLED = True
    logger.info("[KR_BROKER_TRUTH][OBS_COMPAT][INSTALLED]")
