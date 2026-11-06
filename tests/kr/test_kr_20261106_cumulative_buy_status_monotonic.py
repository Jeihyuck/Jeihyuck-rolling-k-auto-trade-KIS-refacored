"""2026-11-06 KR KIS cumulative BUY status provenance and replay monotonicity.

No KIS client, live orders or production DB: isolated SQLite/PostgreSQL.
"""
from datetime import timedelta
import os
from uuid import uuid4

import pytest
import sqlalchemy as sa

from trader.db.schema import schema_for_engine
from trader.db.repos import OrdersRepo
from trader.time_utils import now_kst
from trader import db
from trader.db import repos as repos_module


@pytest.mark.parametrize("recorded,observed,expected", [
    ("FILLED", "RECONCILED", "FILLED"),
    ("PARTIAL_FILLED", "RECONCILED", "PARTIAL_FILLED"),
    ("UNRESOLVED_ACK", "RECONCILED", "UNRESOLVED_ACK"),
    ("FILLED_QTY_CONFIRMED_PRICE_UNRESOLVED", "RECONCILED",
     "FILLED_QTY_CONFIRMED_PRICE_UNRESOLVED"),
    ("CANCELLED", "RECONCILED", "CANCELLED"),
    ("FILLED", "PARTIAL_FILLED", "FILLED"),
    ("ACKED", "RECONCILED", "ACKED"),
    ("ACCEPTED", "RECONCILED", "ACCEPTED"),
    ("SUBMITTED", "RECONCILED", "SUBMITTED"),
    ("INTENT", "RECONCILED", "INTENT"),
    ("FAILED", "RECONCILED", "FAILED"),
    ("REJECTED", "RECONCILED", "REJECTED"),
    ("CANCELLED", "PARTIAL_FILLED", "CANCELLED"),
    ("REJECTED", "PARTIAL_FILLED", "REJECTED"),
    ("FILLED", "CANCELLED", "FILLED"),
    ("FILLED", "REJECTED", "FILLED"),
    ("ACKED", "PARTIAL_FILLED", "PARTIAL_FILLED"),
    ("ACKED", "FILLED", "FILLED"),
    # Proven broker fill events must still upgrade an unresolved/open source.
    # The atomic guard restricts *unproven* RECONCILED and stale terminal
    # observations, not correctly attributed later real fill observations.
    ("RECONCILED", "PARTIAL_FILLED", "PARTIAL_FILLED"),
    ("RECONCILED", "FILLED", "FILLED"),
    ("UNRESOLVED_ACK", "FILLED", "FILLED"),
])
def test_same_source_reconciled_observation_never_downgrades_order(
    monkeypatch, recorded, observed, expected,
):
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    account = "practice:22222222:01"
    epoch, portfolio, cycle, original = [str(uuid4()) for _ in range(4)]
    monkeypatch.setattr(repos_module, "get_account_key", lambda **_kwargs: account)
    monkeypatch.setattr(repos_module, "trading_epoch_enforced", lambda: False)
    day = now_kst() - timedelta(minutes=10)
    with engine.begin() as conn:
        conn.execute(sa.insert(schema.trading_epochs).values(
            trading_epoch_id=epoch, env="practice", account_id=account,
            status="ACTIVE", reason="unit_only",
        ))
        conn.execute(sa.insert(schema.portfolio_epochs).values(
            portfolio_epoch_id=portfolio, trading_epoch_id=epoch,
            env="practice", account_id=account, sid=1, mode=1,
            strategy="pb1_pullback_close", status="ACTIVE",
        ))
        conn.execute(sa.insert(schema.orders).values(
            order_id=original, position_cycle_id=cycle,
            portfolio_epoch_id=portfolio, trading_epoch_id=epoch,
            env="practice", strategy="pb1_pullback_close", sid=1, mode=1,
            code="028050", market="KOSPI", side="BUY", ord_type="MARKET",
            qty=33, stage="PB1-AM", status=recorded,
            client_order_key="practice:pb1:20261106:028050:buy33",
            kis_odno="0000011628", broker_order_id="0000011628",
            created_at=day, submitted_at=day,
        ))
    repo = OrdersRepo(engine)
    result_id = repo.upsert_reconciled_order(
        env="practice", run_id=None, strategy="pb1_pullback_close",
        sid=1, mode=1, code="028050", market="KOSPI", side="BUY",
        ord_type="RECONCILED", qty=33, limit_price=60200,
        stage="RECONCILE",
        client_order_key="practice:pb1:20261106:028050:buy33",
        kis_odno="0000011628", status=observed,
        request_json={"kis_row": {"pdno": "028050"}},
        response_json={"confirmed_fill_qty": 33},
        submitted_at=day, acked_at=day,
    )
    with engine.connect() as conn:
        rows = list(conn.execute(sa.select(schema.orders)).mappings().all())
        fills = list(conn.execute(sa.select(schema.fills)).mappings().all())
    assert len(rows) == 1
    assert str(rows[0]["order_id"]) == str(result_id) == original
    assert rows[0]["status"] == expected
    assert rows[0]["stage"] == "PB1-AM"
    assert rows[0]["qty"] == 33
    assert fills == []


@pytest.mark.parametrize("late_observation", [
    "RECONCILED", "PARTIAL_FILLED", "CANCELLED",
])
def test_postgres_status_race_rechecks_at_update_boundary(
    monkeypatch, late_observation,
):
    """A second writer sets FILLED *after SELECT, before UPDATE*.

    Only disposable localhost PostgreSQL kr_ci is allowed. The original order,
    lifecycle and broker identity stay intact and no synthetic fill is made.
    A Python-only status guard fails this interleaving: SQL CASE/column guard
    must read the latest committed status at the actual UPDATE boundary.
    """
    url = os.getenv("PBCORE_TEST_POSTGRES_URL")
    if not url:
        pytest.skip("requires CI-local PostgreSQL")
    target = sa.engine.make_url(url)
    if target.host not in {"127.0.0.1", "localhost"} or target.database != "kr_ci":
        pytest.skip("requires disposable localhost kr_ci; never production DB")

    admin = sa.create_engine(target, isolation_level="AUTOCOMMIT")
    db_name = "kr320_status_" + uuid4().hex[:12]
    engine = None
    created = False
    try:
        with admin.connect() as conn:
            conn.exec_driver_sql(f'CREATE DATABASE "{db_name}"')
        created = True
        engine = sa.create_engine(target.set(database=db_name))
        schema = schema_for_engine(engine)
        schema.metadata.create_all(engine)
        account = "practice:22222222:01"
        epoch, portfolio, cycle, original = [str(uuid4()) for _ in range(4)]
        monkeypatch.setattr(repos_module, "get_account_key", lambda **_kwargs: account)
        monkeypatch.setattr(repos_module, "trading_epoch_enforced", lambda: False)
        day = now_kst() - timedelta(minutes=10)
        with engine.begin() as conn:
            conn.execute(sa.insert(schema.trading_epochs).values(
                trading_epoch_id=epoch, env="practice", account_id=account,
                status="ACTIVE", reason="disposable_pg",
            ))
            conn.execute(sa.insert(schema.portfolio_epochs).values(
                portfolio_epoch_id=portfolio, trading_epoch_id=epoch,
                env="practice", account_id=account, sid=1, mode=1,
                strategy="pb1_pullback_close", status="ACTIVE",
            ))
            conn.execute(sa.insert(schema.orders).values(
                order_id=original, position_cycle_id=cycle,
                portfolio_epoch_id=portfolio, trading_epoch_id=epoch,
                env="practice", strategy="pb1_pullback_close", sid=1, mode=1,
                code="028050", market="KOSPI", side="BUY", ord_type="MARKET",
                qty=33, stage="PB1-AM", status="ACKED",
                client_order_key="practice:pb1:20261106:028050:buy33",
                kis_odno="0000011628", broker_order_id="0000011628",
                created_at=day, submitted_at=day,
            ))

        injected = {"done": False}

        def promote_just_before_reconcile_update(
            _conn, _cursor, statement, _params, _context, _many,
        ):
            if injected["done"] or not statement.lstrip().upper().startswith("UPDATE ORDERS SET"):
                return
            injected["done"] = True
            # Different committed transaction promotes the original to
            # FILLED while the older observation is still being applied.
            with engine.begin() as other:
                other.execute(sa.update(schema.orders).where(
                    schema.orders.c.order_id == original,
                ).values(status="FILLED"))

        sa.event.listen(engine, "before_cursor_execute", promote_just_before_reconcile_update)
        try:
            returned = OrdersRepo(engine).upsert_reconciled_order(
                env="practice", run_id=None, strategy="pb1_pullback_close",
                sid=1, mode=1, code="028050", market="KOSPI", side="BUY",
                ord_type="RECONCILED", qty=33, limit_price=60200,
                stage="RECONCILE",
                client_order_key="practice:pb1:20261106:028050:buy33",
                kis_odno="0000011628", status=late_observation,
                request_json={"kis_row": {"pdno": "028050"}},
                response_json={"confirmed_fill_qty": 33},
                submitted_at=day, acked_at=day,
            )
        finally:
            sa.event.remove(engine, "before_cursor_execute", promote_just_before_reconcile_update)
        assert injected["done"], "did not reproduce read/write interleaving"
        with engine.connect() as conn:
            orders = list(conn.execute(sa.select(schema.orders)).mappings().all())
            fills = list(conn.execute(sa.select(schema.fills)).mappings().all())
        assert len(orders) == 1
        assert str(returned) == str(orders[0]["order_id"]) == original
        assert orders[0]["status"] == "FILLED"
        assert str(orders[0]["position_cycle_id"]) == cycle
        assert str(orders[0]["trading_epoch_id"]) == epoch
        assert fills == []
    finally:
        if engine is not None:
            engine.dispose()
        if created:
            with admin.connect() as conn:
                conn.exec_driver_sql(f'DROP DATABASE "{db_name}" WITH (FORCE)')
        admin.dispose()
