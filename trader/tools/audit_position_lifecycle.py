"""Read-only audit for migration-0050 lifecycle provenance.

Usage: ``python -m trader.tools.audit_position_lifecycle --env practice``.
The command never mutates or deletes audit history.
"""
from __future__ import annotations

import argparse
from typing import Any

from sqlalchemy import and_, func, or_, select

from trader.account_state import get_account_key
from trader.db.engine import get_engine
from trader.db.schema import schema_for_engine


def audit(engine: Any, *, env: str, account_id: str | None = None) -> dict[str, int]:
    s = schema_for_engine(engine)
    account_id = account_id or get_account_key(env=env)
    legacy = or_(s.positions.c.position_cycle_id.like("legacy-cycle-%"),
                 s.positions.c.portfolio_epoch_id.like("legacy-epoch-%"))
    with engine.connect() as conn:
        legacy_count = conn.execute(select(func.count()).select_from(s.positions).where(and_(
            s.positions.c.env == env, s.positions.c.status == "OPEN", legacy))).scalar_one()
        duplicate_groups = conn.execute(
            select(s.positions.c.code).where(and_(s.positions.c.env == env, s.positions.c.status == "OPEN"))
            .group_by(s.positions.c.strategy, s.positions.c.sid, s.positions.c.mode, s.positions.c.code)
            .having(func.count() > 1)
        ).all()
        active_epochs = select(s.portfolio_epochs.c.portfolio_epoch_id).where(and_(
            s.portfolio_epochs.c.env == env, s.portfolio_epochs.c.account_id == account_id,
            s.portfolio_epochs.c.status == "ACTIVE"))
        cross_epoch = conn.execute(select(func.count()).select_from(s.positions).where(and_(
            s.positions.c.env == env, s.positions.c.status == "OPEN",
            s.positions.c.portfolio_epoch_id.not_in(active_epochs)))).scalar_one()
        orphan_fills = conn.execute(select(func.count()).select_from(
            s.fills.outerjoin(s.positions, s.positions.c.position_cycle_id == s.fills.c.position_cycle_id)
        ).where(and_(s.fills.c.env == env, s.positions.c.position_cycle_id.is_(None)))).scalar_one()
        cross_cycle_fills = conn.execute(select(func.count()).select_from(
            s.fills.join(s.orders, s.orders.c.order_id == s.fills.c.order_id)
        ).where(and_(s.fills.c.env == env,
                     or_(s.fills.c.position_cycle_id != s.orders.c.position_cycle_id,
                         s.fills.c.portfolio_epoch_id != s.orders.c.portfolio_epoch_id)))).scalar_one()
        cross_cycle_orders = conn.execute(select(func.count()).select_from(
            s.orders.join(s.positions, and_(s.positions.c.env == s.orders.c.env,
                                            s.positions.c.code == s.orders.c.code,
                                            s.positions.c.status == "OPEN"))
        ).where(and_(s.orders.c.env == env,
                     or_(s.orders.c.position_cycle_id != s.positions.c.position_cycle_id,
                         s.orders.c.portfolio_epoch_id != s.positions.c.portfolio_epoch_id)))).scalar_one()
    return {
        "LEGACY_OPEN_COUNT": int(legacy_count),
        "MULTIPLE_OPEN_CYCLE_COUNT": len(duplicate_groups),
        "ORPHAN_FILL_COUNT": int(orphan_fills),
        "CROSS_CYCLE_FILL_COUNT": int(cross_cycle_fills),
        "CROSS_EPOCH_POSITION_COUNT": int(cross_epoch),
        "CROSS_CYCLE_ORDER_COUNT": int(cross_cycle_orders),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", required=True)
    parser.add_argument("--account-id")
    args = parser.parse_args()
    for key, value in audit(get_engine(), env=args.env, account_id=args.account_id).items():
        print(f"{key}={value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
