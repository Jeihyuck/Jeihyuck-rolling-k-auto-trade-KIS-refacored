"""KR wrapper child completion cannot leave AM guard/tick STARTED forever."""
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
import sqlalchemy as sa

from trader.db.schema import schema_for_engine
from scripts.kr_20261109_am_child_run_finalizer import close_returned_child_runs


def _fixture():
    engine = sa.create_engine("sqlite:///:memory:")
    table = schema_for_engine(engine).runs
    table.metadata.create_all(engine)
    start = datetime(2026, 11, 9, 9, 0, tzinfo=timezone.utc)
    data = [
        ("pb1_am_session_guard", "SESSION_GUARD_RUNNING", start + timedelta(seconds=20), None),
        ("pb1_pullback_close", "STARTED", start + timedelta(seconds=30), None),
        ("pb1_close_session_guard", "SESSION_GUARD_RUNNING", start + timedelta(seconds=35), None),
        ("pb1_am_session_guard", "SESSION_GUARD_RUNNING", start - timedelta(seconds=30), None),
        ("pb1_pullback_close", "OK", start + timedelta(seconds=40), start + timedelta(seconds=70)),
    ]
    ids = []
    with engine.begin() as conn:
        for owner, status, ts, finished in data:
            key = str(uuid4())
            ids.append(key)
            conn.execute(sa.insert(table).values(
                run_id=key, env="practice", strategy=owner,
                run_window="morning", phase="entry", status=status,
                started_at=ts, heartbeat_at=ts, updated_at=ts,
                finished_at=finished,
            ))
    return engine, table, ids, int(start.timestamp())


def test_am_child_returns_closes_only_unfinished_guard_and_tick():
    engine, table, ids, start = _fixture()
    report = close_returned_child_runs(
        engine, env="practice", start_epoch=start, end_epoch=start + 100,
        child_rc=137, exclusive_wrapper_lock=True, child_returned=True,
    )
    assert report["updated_count"] == 2
    assert report["broker_orders_touched"] == 0
    assert report["execution_truth"] == "UNVERIFIED"
    with engine.connect() as conn:
        rows = {str(x["run_id"]): x for x in conn.execute(
            sa.select(table)
        ).mappings()}
    assert rows[ids[0]]["status"] == "SESSION_CHILD_EXIT_UNVERIFIED"
    assert rows[ids[1]]["status"] == "SESSION_CHILD_EXIT_UNVERIFIED"
    assert rows[ids[0]]["finished_at"] is not None
    assert rows[ids[2]]["status"] == "SESSION_GUARD_RUNNING"
    assert rows[ids[3]]["status"] == "SESSION_GUARD_RUNNING"
    assert rows[ids[4]]["status"] == "OK"


def test_no_cleanup_without_exclusive_lock_and_returned_child():
    engine, _, _, start = _fixture()
    for lock, returned in ((False, True), (True, False)):
        with pytest.raises(RuntimeError, match="EXCLUSIVE_LOCK"):
            close_returned_child_runs(
                engine, env="practice", start_epoch=start, end_epoch=start + 90,
                child_rc=0, exclusive_wrapper_lock=lock, child_returned=returned,
            )


def test_cannot_rewrite_real_trading_session_or_unbounded_window():
    engine, _, _, start = _fixture()
    with pytest.raises(RuntimeError):
        close_returned_child_runs(
            engine, env="real", start_epoch=start, end_epoch=start + 90,
            child_rc=0, exclusive_wrapper_lock=True, child_returned=True,
        )
    with pytest.raises(ValueError):
        close_returned_child_runs(
            engine, env="practice", start_epoch=start, end_epoch=start + 30000,
            child_rc=0, exclusive_wrapper_lock=True, child_returned=True,
        )
