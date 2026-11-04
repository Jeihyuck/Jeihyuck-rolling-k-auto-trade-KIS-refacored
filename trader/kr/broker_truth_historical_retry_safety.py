"""Safety fence for PR125 cross-date BUY retry.

Cross-date recovery must never turn an already CLOSED historical cycle back into
an OPEN holding.  Eligible durable BUY orders are therefore limited to the
ACTIVE portfolio epoch and either:

* one exact OPEN lifecycle, or
* no lifecycle at all with an immutable pre-order holding baseline of zero
  (the precise crash-before-first-position-insert case).
"""
from __future__ import annotations

import logging
from typing import Any

import sqlalchemy as sa

from trader.db.schema import schema_for_engine
import trader.kr.broker_truth_hardening as base
import trader.kr.broker_truth_final_review_fixes as final
import trader.kr.broker_truth_historical_buy_retry as historical
from trader.kr.recovery_scope_20261104 import (
    resolve_kr_recovery_scope, scoped_portfolio_predicates,
    order_matches_recovery_scope,
)

logger = logging.getLogger(__name__)
_INSTALLED = False


def _safe_owned_buy_fill_groups(*, engine, env: str, strategy: str) -> dict[str, dict[str, Any]]:
    schema = schema_for_engine(engine)
    scope = resolve_kr_recovery_scope(engine=engine, env=env)
    with engine.connect() as conn:
        order_ids = [
            value
            for value in conn.execute(
                sa.select(schema.fills.c.order_id)
                .select_from(
                    schema.fills.join(
                        schema.orders,
                        schema.orders.c.order_id == schema.fills.c.order_id,
                    ).join(
                        schema.portfolio_epochs,
                        schema.portfolio_epochs.c.portfolio_epoch_id == schema.orders.c.portfolio_epoch_id,
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
                        schema.portfolio_epochs.c.status == "ACTIVE",
                        *(scoped_portfolio_predicates(schema, scope) if scope else ()),
                        *((schema.orders.c.trading_epoch_id == scope.trading_epoch_id,
                           schema.fills.c.trading_epoch_id == scope.trading_epoch_id)
                          if scope else ()),
                    )
                )
                .distinct()
            ).scalars().all()
            if value is not None
        ]

        groups: dict[str, dict[str, Any]] = {}
        for raw_order_id in order_ids:
            order = conn.execute(
                sa.select(schema.orders).where(schema.orders.c.order_id == raw_order_id).limit(1)
            ).mappings().first()
            if not order:
                continue
            order = dict(order)
            if not order_matches_recovery_scope(
                conn=conn, schema=schema, order=order,
                strategy=strategy, scope=scope,
            ):
                continue
            cycle = order.get("position_cycle_id")
            epoch = order.get("portfolio_epoch_id")
            code = base._normalize_code(order.get("code"))
            if not cycle or not epoch or not code:
                continue

            lifecycle_rows = [
                dict(row)
                for row in conn.execute(
                    sa.select(schema.positions).where(
                        sa.and_(
                            schema.positions.c.env == env,
                            schema.positions.c.strategy == strategy,
                            schema.positions.c.code == code,
                            schema.positions.c.position_cycle_id == cycle,
                            schema.positions.c.portfolio_epoch_id == epoch,
                            *((schema.positions.c.trading_epoch_id == scope.trading_epoch_id,)
                              if scope else ()),
                        )
                    )
                ).mappings().all()
            ]
            if len(lifecycle_rows) > 1:
                logger.error(
                    "[KR_BROKER_TRUTH][HIST_BUY_RETRY][SAFETY_BLOCK] code=%s order_id=%s reason=MULTIPLE_EXACT_LIFECYCLES rows=%s",
                    code,
                    raw_order_id,
                    len(lifecycle_rows),
                )
                continue
            if lifecycle_rows:
                if str(lifecycle_rows[0].get("status") or "").upper() != "OPEN":
                    # A closed/superseded lifecycle is historical evidence that
                    # this BUY was already consumed; never resurrect it.
                    continue
            else:
                baseline = final._baseline_qty(order)
                if baseline != 0:
                    logger.error(
                        "[KR_BROKER_TRUTH][HIST_BUY_RETRY][SAFETY_BLOCK] code=%s order_id=%s baseline_qty=%s reason=NO_LIFECYCLE_REQUIRES_ZERO_BASELINE",
                        code,
                        raw_order_id,
                        baseline,
                    )
                    continue

            fills = [
                dict(row)
                for row in conn.execute(
                    sa.select(schema.fills)
                    .where(schema.fills.c.order_id == raw_order_id)
                    .order_by(schema.fills.c.filled_at.asc())
                ).mappings().all()
            ]
            if scope and any(
                str(fill.get("trading_epoch_id") or "") != scope.trading_epoch_id
                for fill in fills
            ):
                logger.error(
                    "[KR_BROKER_TRUTH][HIST_BUY_RETRY][BLOCK] order_id=%s "
                    "reason=FILL_EPOCH_UNPROVEN", raw_order_id,
                )
                continue
            if fills:
                groups[str(raw_order_id)] = {"order": order, "fills": fills}
    return groups


def install_historical_retry_safety() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    historical._owned_buy_fill_groups = _safe_owned_buy_fill_groups
    _INSTALLED = True
    logger.info("[KR_BROKER_TRUTH][HIST_BUY_RETRY][SAFETY_INSTALLED]")
