"""Account-epoch-scoped KR broker-truth health evidence, no account-wide mutation."""
from __future__ import annotations

import logging

import sqlalchemy as sa

from trader.account_state import get_account_key
from trader.db.repos import OrdersRepo
from trader.db.schema import schema_for_engine
from trader.db.trading_epoch import active_trading_epoch_id, trading_epoch_enforced
from trader.time_utils import now_kst
import trader.kr.broker_truth_hardening as base

log = logging.getLogger(__name__)


def health_after_reconcile(*, engine, env: str, strategy: str, holdings_rows):
    schema = schema_for_engine(engine)
    kis = base._holdings_index(holdings_rows)
    with engine.connect() as conn:
        epoch = active_trading_epoch_id(
            conn, env=env, account_id=get_account_key(env=env),
            required=trading_epoch_enforced(),
        )
        if epoch is None:
            if trading_epoch_enforced():
                raise RuntimeError("KR_BROKER_HEALTH_ACTIVE_EPOCH_REQUIRED")
            # Legacy non-epoch engines keep the existing strict health checker.
            from trader.kr.broker_truth_review_fixes import _health_after_reconcile_fixed
            return _health_after_reconcile_fixed(
                engine=engine, env=env, strategy=strategy, holdings_rows=holdings_rows,
            )
        rows = [dict(x) for x in conn.execute(
            sa.select(schema.positions).where(sa.and_(
                schema.positions.c.env == env,
                schema.positions.c.strategy == strategy,
                schema.positions.c.trading_epoch_id == epoch,
                schema.positions.c.status == "OPEN",
                schema.positions.c.qty > 0,
            ))
        ).mappings().all()]
    grouped = {}
    for row in rows:
        code = base._normalize_code(row.get("code"))
        grouped.setdefault(code, []).append(row)
    mismatches, duplicates = [], []
    for code, items in sorted(grouped.items()):
        db_qty = sum(base._qty(row.get("qty")) for row in items)
        kis_qty = base._qty((kis.get(code) or {}).get("qty"))
        if len(items) > 1:
            dup = {
                "code": code, "count": len(items),
                "position_ids": [str(x.get("position_id") or "") for x in items],
                "db_qty": db_qty, "kis_qty": kis_qty,
            }
            duplicates.append(dup)
            mismatches.append({**dup, "reason": "MULTIPLE_OPEN_LIFECYCLES"})
        elif db_qty != kis_qty:
            mismatches.append({"code": code, "db_qty": db_qty, "kis_qty": kis_qty})
    stale = []
    from trader.kr.broker_truth_hardening import _date_of, _iso
    for order in OrdersRepo(engine).get_open_orders(env, include_stale=True, fail_open=False) or []:
        if str(order.get("strategy") or "") != strategy:
            continue
        if str(order.get("trading_epoch_id") or "") != str(epoch):
            continue
        created = _date_of(order.get("created_at"))
        if created is not None and created < now_kst().date():
            stale.append({
                "order_id": str(order.get("order_id") or ""),
                "code": base._normalize_code(order.get("code")),
                "side": str(order.get("side") or ""),
                "status": str(order.get("status") or ""),
                "created_at": _iso(order.get("created_at")),
            })
    if duplicates:
        log.error("[KR_BROKER_TRUTH][HEALTH][MULTIPLE_OPEN_LIFECYCLES] count=%s rows=%s", len(duplicates), duplicates)
    if mismatches:
        log.error("[KR_BROKER_TRUTH][HEALTH][QTY_MISMATCH] count=%s rows=%s", len(mismatches), mismatches)
    if stale:
        log.error("[KR_BROKER_TRUTH][HEALTH][STALE_OPEN_ORDER] count=%s rows=%s", len(stale), stale)
    return {
        "qty_mismatch_count": len(mismatches),
        "qty_mismatches": mismatches,
        "duplicate_open_lifecycle_count": len(duplicates),
        "duplicate_open_lifecycles": duplicates,
        "stale_open_order_count": len(stale),
        "stale_open_orders": stale,
    }
