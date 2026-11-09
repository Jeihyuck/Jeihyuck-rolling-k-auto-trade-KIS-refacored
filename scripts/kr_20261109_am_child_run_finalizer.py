"""Bounded KR AM run-ledger cleanup after the owning WSL child returns.

This is NOT a broker reconciliation, not a retry, and not a liveness probe.
It must be invoked only by the still-locked WSL wrapper after its synchronous
session child has exited. That exclusive lock proves no second KR AM child
can concurrently update the selected runs. Never call on a live session.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

import sqlalchemy as sa


def close_returned_child_runs(
    engine: sa.Engine, *, env: str, start_epoch: int, end_epoch: int,
    child_rc: int, exclusive_wrapper_lock: bool, child_returned: bool,
) -> dict:
    if env != "practice" or not exclusive_wrapper_lock or not child_returned:
        raise RuntimeError("KR_ORPHAN_CLEANUP_REQUIRES_RETURNED_PRACTICE_CHILD_EXCLUSIVE_LOCK")
    if not (0 <= int(child_rc) <= 255):
        raise ValueError("CHILD_RC_INVALID")
    start_epoch, end_epoch = int(start_epoch), int(end_epoch)
    if start_epoch <= 0 or end_epoch < start_epoch or end_epoch - start_epoch > 20000:
        raise ValueError("KR_AM_WRAPPER_WINDOW_INVALID")
    from trader.db.schema import schema_for_engine
    table = schema_for_engine(engine).runs
    start = datetime.fromtimestamp(start_epoch, tz=timezone.utc)
    end = datetime.fromtimestamp(end_epoch, tz=timezone.utc)
    status = "SESSION_CHILD_EXIT_UNVERIFIED" if child_rc else "SESSION_CHILD_RETURNED_OPEN_RUN"
    reason = "WRAPPER_CHILD_RC_" + str(child_rc) + "_RUN_NOT_TERMINAL"
    rows = []
    with engine.begin() as conn:
        query = sa.select(table.c.run_id, table.c.strategy, table.c.status).where(
            table.c.env == "practice",
            table.c.strategy.in_(("pb1_am_session_guard", "pb1_pullback_close")),
            table.c.started_at >= start,
            table.c.started_at <= end,
            table.c.finished_at.is_(None),
            table.c.status.in_((
                "STARTED", "SESSION_GUARD_STARTED", "SESSION_GUARD_RUNNING",
            )),
        )
        if engine.dialect.name == "postgresql":
            query = query.with_for_update()
        pending = conn.execute(query).mappings().all()
        for candidate in pending:
            result = conn.execute(sa.update(table).where(
                table.c.run_id == candidate["run_id"],
                table.c.finished_at.is_(None),
                table.c.status == candidate["status"],
            ).values(
                status=status, aborted_reason=reason, finished_at=sa.func.now(),
                heartbeat_at=sa.func.now(), updated_at=sa.func.now(),
            ))
            if result.rowcount == 1:
                rows.append({
                    "run_id": str(candidate["run_id"]),
                    "strategy": candidate["strategy"],
                    "previous_status": candidate["status"],
                    "new_status": status,
                })
    return {
        "status": "RUN_LEDGER_TERMINALIZED_AFTER_CHILD_RETURN",
        "child_rc": child_rc,
        "updated_count": len(rows),
        "updated_runs": rows,
        "execution_truth": "UNVERIFIED",
        "broker_orders_touched": 0,
        "note": "Only liveness rows changed. No claim/fill/position resubmission.",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="KR AM after-child bounded run finalizer")
    parser.add_argument("--env", default="practice", choices=["practice"])
    parser.add_argument("--start-epoch", type=int, required=True)
    parser.add_argument("--end-epoch", type=int, required=True)
    parser.add_argument("--child-rc", type=int, required=True)
    parser.add_argument("--exclusive-wrapper-lock", action="store_true")
    parser.add_argument("--child-returned", action="store_true")
    args = parser.parse_args(argv)
    from trader.db.engine import get_engine
    report = close_returned_child_runs(
        get_engine(), env=args.env, start_epoch=args.start_epoch,
        end_epoch=args.end_epoch, child_rc=args.child_rc,
        exclusive_wrapper_lock=args.exclusive_wrapper_lock,
        child_returned=args.child_returned,
    )
    print(json.dumps(report, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
