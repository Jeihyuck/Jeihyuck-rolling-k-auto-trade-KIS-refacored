"""KIS ODNO reuse across dates/epochs: offline identity + migration safety.

Do not submit broker orders or modify production Supabase rows.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
import sqlalchemy as sa

import trader.db.repos as repos_module
from trader.db.repos import OrdersRepo
from trader.db.schema import schema_for_engine
from trader.db.trading_epoch import start_new_trading_epoch


def _db(monkeypatch):
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    monkeypatch.setattr(repos_module, "get_account_key", lambda env=None, **kw: "practice:kr-incident")
    return engine, schema


def _insert_existing(schema, conn, *, code, key, odno, day, epoch=None, created=None):
    identity = str(uuid4())
    conn.execute(schema.orders.insert().values(
        order_id=identity, env="practice", strategy="pb1_pullback_close",
        sid=1, mode=1, code=code, market="KRX", side="BUY",
        ord_type="LIMIT", qty=1, client_order_key=key,
        status="EXPIRED", kis_odno=odno, broker_order_id=odno,
        request_json={}, response_json={},
        trading_epoch_id=epoch,
        submitted_at=day, acked_at=day,
        created_at=created or day,
    ))
    return identity


def _upsert(repo, *, code, key, odno, stamp, side="SELL"):
    return repo.upsert_reconciled_order(
        env="practice", run_id=None, strategy="pb1_pullback_close",
        sid=1, mode=1, code=code, market="KRX", side=side,
        ord_type="RECONCILED", qty=110, limit_price=None,
        stage="RECONCILE", client_order_key=key, kis_odno=odno,
        status="FILLED", request_json={}, response_json={"rt_cd": "0"},
        submitted_at=stamp, acked_at=stamp,
    )


def test_nov02_neosem_odno_can_recur_after_aug_legacy_epoch(monkeypatch):
    engine, schema = _db(monkeypatch)
    old_stamp = datetime(2026, 8, 26, 9, 15, tzinfo=timezone.utc)
    with engine.begin() as conn:
        old = _insert_existing(
            schema, conn, code="005830", key="aug26-legacy",
            odno="0000013020", day=old_stamp,
        )
    current = start_new_trading_epoch(
        engine, env="practice", account_id="practice:kr-incident",
        reason="REPRO_KR_NEOS",
    )
    # Simulate the actual Oct 25 active-epoch start, not the test's wall clock.
    with engine.begin() as conn:
        conn.execute(sa.update(schema.trading_epochs).where(
            schema.trading_epochs.c.trading_epoch_id == current,
        ).values(started_at=datetime(2026, 10, 25, tzinfo=timezone.utc)))
    # A new trading epoch does not allow claiming the old order row.
    repo = OrdersRepo(engine)
    stamp = datetime(2026, 10, 30, 2, 10, tzinfo=timezone.utc)
    new_id = _upsert(
        repo, code="253590", key="oct30-neosem",
        odno="0000013020", stamp=stamp,
    )
    assert new_id != old
    assert _upsert(
        repo, code="253590", key="oct30-neosem",
        odno="0000013020", stamp=stamp,
    ) == new_id
    assert repo.get_order_by_kis_odno_scoped(
        "practice", "0000013020", trade_date=date(2026, 10, 30),
        code="253590", side="SELL",
    )["order_id"] == new_id
    assert repo.get_order_by_kis_odno_scoped(
        "practice", "0000013020", trade_date=date(2026, 8, 26),
        code="005830", side="BUY",
    )["order_id"] == old
    with engine.connect() as conn:
        rows = conn.execute(
            sa.select(schema.orders).where(schema.orders.c.broker_order_id == "0000013020")
        ).mappings().all()
    assert len(rows) == 2
    assert {r["trading_epoch_id"] for r in rows} == {None, current}


def test_same_day_same_epoch_different_symbol_is_not_adopted(monkeypatch):
    engine, schema = _db(monkeypatch)
    current = start_new_trading_epoch(
        engine, env="practice", account_id="practice:kr-incident",
        reason="IDENTITY_COLLISION",
    )
    stamp = datetime(2026, 10, 30, 0, 15, tzinfo=timezone.utc)
    with engine.begin() as conn:
        _insert_existing(
            schema, conn, code="005830", key="same-day-different-code",
            odno="0000013020", day=stamp, epoch=current,
        )
    with pytest.raises(RuntimeError, match="KR_RECONCILE_ORDER_IDENTITY_MISMATCH"):
        _upsert(
            OrdersRepo(engine), code="253590", key="sell-neosem",
            odno="0000013020", stamp=stamp,
        )


def test_scoped_lookup_uses_original_broker_day_not_late_db_insertion(monkeypatch):
    engine, schema = _db(monkeypatch)
    stamp = datetime(2026, 10, 30, 0, 15, tzinfo=timezone.utc)
    later = datetime(2026, 11, 2, 0, 12, tzinfo=timezone.utc)
    with engine.begin() as conn:
        ident = _insert_existing(
            schema, conn, code="253590", key="late-insert",
            odno="0000013020", day=stamp, created=later,
        )
    found = OrdersRepo(engine).get_order_by_kis_odno_scoped(
        "practice", "0000013020", trade_date=date(2026, 10, 30),
        code="253590", side="BUY",
    )
    assert found["order_id"] == ident


def test_sqlite_unique_scope_blocks_duplicate_same_day_and_allows_new_day(monkeypatch):
    engine, schema = _db(monkeypatch)
    d1 = datetime(2026, 8, 26, 0, 15, tzinfo=timezone.utc)
    d2 = datetime(2026, 10, 30, 0, 15, tzinfo=timezone.utc)
    with engine.begin() as conn:
        _insert_existing(schema, conn, code="005830", key="first", odno="0000013020", day=d1)
        _insert_existing(schema, conn, code="253590", key="second", odno="0000013020", day=d2)
    with pytest.raises(sa.exc.IntegrityError):
        with engine.begin() as conn:
            _insert_existing(
                schema, conn, code="000660", key="same-day-collision",
                odno="0000013020", day=d2,
            )


def test_migration_preflights_and_replaces_index_without_rewriting_trades():
    sql = Path("migrations/0061_kr_order_broker_day_epoch_identity.sql").read_text()
    assert "KR_BROKER_ORDER_SCOPED_IDENTITY_DUPLICATE" in sql
    assert "Asia/Seoul" in sql
    assert "CREATE UNIQUE INDEX IF NOT EXISTS uq_orders_env_broker_order_day_epoch" in sql
    assert sql.index("CREATE UNIQUE INDEX IF NOT EXISTS") < sql.index(
        "DROP INDEX IF EXISTS public.uq_orders_env_broker_order_id"
    )
    assert "DELETE FROM" not in sql.upper()
    assert "UPDATE PUBLIC.ORDERS" not in sql.upper()



def test_reused_odno_same_date_different_trading_epoch_is_distinct(monkeypatch):
    engine, schema = _db(monkeypatch)
    day = datetime(2026, 10, 30, 0, 15, tzinfo=timezone.utc)
    with engine.begin() as conn:
        old = _insert_existing(
            schema, conn, code="005830", key="epoch-before",
            odno="0000099999", day=day, epoch="older-epoch",
        )
        new = _insert_existing(
            schema, conn, code="253590", key="epoch-after",
            odno="0000099999", day=day, epoch="newer-epoch",
        )
    assert old != new


def test_response_merge_does_not_adopt_august_broker_watermark(monkeypatch):
    from trader.kr import broker_truth_final_review_fixes as final

    engine, schema = _db(monkeypatch)
    old_stamp = datetime(2026, 8, 26, 0, 15, tzinfo=timezone.utc)
    with engine.begin() as conn:
        old = _insert_existing(
            schema, conn, code="005830", key="legacy-buy",
            odno="0000013020", day=old_stamp,
        )
        conn.execute(sa.update(schema.orders).where(
            schema.orders.c.order_id == old,
        ).values(response_json={"confirmed_fill_qty": 999, "rt_cd": "0"}))

    epoch_id = start_new_trading_epoch(
        engine, env="practice", account_id="practice:kr-incident",
        reason="TEST_RESPONSE_MERGE_DATE_FENCE",
    )
    with engine.begin() as conn:
        conn.execute(sa.update(schema.trading_epochs).where(
            schema.trading_epochs.c.trading_epoch_id == epoch_id,
        ).values(started_at=datetime(2026, 10, 25, tzinfo=timezone.utc)))

    class IsolatedOrdersRepo(OrdersRepo):
        pass

    monkeypatch.setattr(final, "OrdersRepo", IsolatedOrdersRepo)
    final._install_order_response_merge_guard()
    current_time = datetime(2026, 10, 30, 2, 10, tzinfo=timezone.utc)
    repo = IsolatedOrdersRepo(engine)
    new = repo.upsert_reconciled_order(
        env="practice", run_id=None, strategy="pb1_pullback_close",
        sid=1, mode=1, code="253590", market="KRX", side="SELL",
        ord_type="RECONCILED", qty=110, limit_price=None,
        stage="RECONCILE", client_order_key="new-epoch-neosem",
        kis_odno="0000013020", status="ACKED", request_json={},
        response_json={"rt_cd": "0", "confirmed_fill_qty": 110},
        submitted_at=current_time, acked_at=current_time,
    )
    assert new != old
    with engine.connect() as conn:
        rows = {
            x["order_id"]: x for x in conn.execute(
                sa.select(schema.orders).where(
                    schema.orders.c.broker_order_id == "0000013020",
                )
            ).mappings()
        }
    assert rows[old]["response_json"]["confirmed_fill_qty"] == 999
    assert rows[new]["response_json"]["confirmed_fill_qty"] == 110



def test_reused_odno_mark_filled_affects_only_current_day_and_epoch(monkeypatch):
    from datetime import timedelta
    engine, schema = _db(monkeypatch)
    epoch = start_new_trading_epoch(
        engine, env="practice", account_id="practice:kr-incident",
        reason="STATUS_DAY_FENCE",
    )
    now = datetime.now(timezone.utc)
    old_ts = now - timedelta(days=3)
    with engine.begin() as conn:
        old = _insert_existing(
            schema, conn, code="005830", key="filled-old", odno="0000013020",
            day=old_ts, epoch=epoch,
        )
        new = _insert_existing(
            schema, conn, code="253590", key="filled-new", odno="0000013020",
            day=now, epoch=epoch,
        )
        conn.execute(sa.update(schema.orders).where(
            schema.orders.c.order_id.in_([old, new]),
        ).values(status="ACKED"))

    OrdersRepo(engine).mark_filled("practice", kis_odno="0000013020")
    with engine.connect() as conn:
        rows = {
            x["order_id"]: x["status"] for x in conn.execute(
                sa.select(schema.orders.c.order_id, schema.orders.c.status)
            ).mappings()
        }
    assert rows[old] == "ACKED"
    assert rows[new] == "FILLED"


def test_reused_odno_mark_acked_affects_only_current_day(monkeypatch):
    from datetime import timedelta
    engine, schema = _db(monkeypatch)
    epoch = start_new_trading_epoch(
        engine, env="practice", account_id="practice:kr-incident",
        reason="ACK_DAY_FENCE",
    )
    now = datetime.now(timezone.utc)
    with engine.begin() as conn:
        old = _insert_existing(
            schema, conn, code="005830", key="ack-old", odno="0000013020",
            day=now - timedelta(days=3), epoch=epoch,
        )
        new = _insert_existing(
            schema, conn, code="253590", key="ack-new", odno="0000013020",
            day=now, epoch=epoch,
        )
        conn.execute(sa.update(schema.orders).where(
            schema.orders.c.order_id.in_([old, new])
        ).values(status="SUBMITTED"))

    OrdersRepo(engine).mark_acked("practice", kis_odno="0000013020",
                                 response_json={"rt_cd": "0"})
    with engine.connect() as conn:
        rows = {
            x["order_id"]: x["status"] for x in conn.execute(
                sa.select(schema.orders.c.order_id, schema.orders.c.status)
            ).mappings()
        }
    assert rows[old] == "SUBMITTED"
    assert rows[new] == "ACKED"
