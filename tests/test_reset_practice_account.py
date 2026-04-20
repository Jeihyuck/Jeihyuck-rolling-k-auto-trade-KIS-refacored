from __future__ import annotations

from datetime import datetime
from typing import Any

import sqlalchemy as sa
import pytest
from sqlalchemy import event
from sqlalchemy.exc import IntegrityError

from scripts.reset_practice_account import build_account_key, execute_practice_account_reset
from trader.db.repos import PracticeAccountResetRepo
from trader.db.schema import schema_for_engine


def _make_engine(*, create_optional_tables: bool = True, create_strategy_tables: bool = True) -> tuple[sa.Engine, Any]:
    engine = sa.create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=sa.pool.StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_connection, _connection_record) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)

    if not create_optional_tables and not create_strategy_tables:
        return engine, schema

    optional_metadata = sa.MetaData()
    if create_optional_tables:
        sa.Table(
            "cooldowns",
            optional_metadata,
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("env", sa.String, nullable=False),
            sa.Column("code", sa.String, nullable=False),
        )
        sa.Table(
            "account_snapshot",
            optional_metadata,
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("env", sa.String, nullable=False),
            sa.Column("account_key", sa.String, nullable=False),
            sa.Column("cash_krw", sa.Integer, nullable=False),
        )
        sa.Table(
            "holdings_snapshot",
            optional_metadata,
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("env", sa.String, nullable=False),
            sa.Column("account_key", sa.String, nullable=False),
            sa.Column("payload", sa.JSON, nullable=False, default=dict),
        )
        sa.Table(
            "portfolio_state",
            optional_metadata,
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("env", sa.String, nullable=False),
            sa.Column("payload", sa.JSON, nullable=False, default=dict),
        )
        sa.Table(
            "open_orders",
            optional_metadata,
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("env", sa.String, nullable=False),
            sa.Column("payload", sa.JSON, nullable=False, default=dict),
        )
        sa.Table(
            "trade_state",
            optional_metadata,
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("env", sa.String, nullable=False),
            sa.Column("payload", sa.JSON, nullable=False, default=dict),
        )
        sa.Table(
            "entry_state",
            optional_metadata,
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("env", sa.String, nullable=False),
            sa.Column("payload", sa.JSON, nullable=False, default=dict),
        )
        sa.Table(
            "exit_state",
            optional_metadata,
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("env", sa.String, nullable=False),
            sa.Column("payload", sa.JSON, nullable=False, default=dict),
        )
    if create_strategy_tables:
        sa.Table(
            "final30",
            optional_metadata,
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("env", sa.String, nullable=False),
            sa.Column("code", sa.String, nullable=False),
        )
        sa.Table(
            "candidate_pool",
            optional_metadata,
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("env", sa.String, nullable=False),
            sa.Column("code", sa.String, nullable=False),
        )
        sa.Table(
            "watchlist_snapshot",
            optional_metadata,
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("env", sa.String, nullable=False),
            sa.Column("code", sa.String, nullable=False),
        )
    optional_metadata.create_all(engine)
    return engine, schema


def _seed_tables(engine: sa.Engine, schema: Any) -> None:
    account_key = build_account_key("practice")
    with engine.begin() as conn:
        conn.execute(
            sa.insert(schema.positions).values(
                position_id="pos-1",
                env="practice",
                strategy="pb1_pullback_close",
                sid=1,
                mode=1,
                code="005930",
                market="KOSPI",
                qty=3,
                avg_buy_price=70000.0,
                total_cost=210000.0,
                realized_pnl=0.0,
            )
        )
        conn.execute(
            sa.insert(schema.orders).values(
                order_id="ord-1",
                env="practice",
                strategy="pb1_pullback_close",
                sid=1,
                mode=1,
                code="005930",
                market="KOSPI",
                side="BUY",
                ord_type="LIMIT",
                qty=3,
                limit_price=70000.0,
                stage="PB1-CLOSE",
                client_order_key="practice-buy-1",
                status="SUBMITTED",
                request_json={},
            )
        )
        conn.execute(
            sa.insert(schema.fills).values(
                fill_id="fill-1",
                env="practice",
                order_id="ord-1",
                code="005930",
                market="KOSPI",
                side="BUY",
                qty=3,
                price=70000.0,
                fee=0.0,
                tax=0.0,
                filled_at=datetime(2026, 4, 20, 9, 0, 0),
                raw_json={},
            )
        )
        conn.execute(sa.text("INSERT INTO cooldowns (env, code) VALUES ('practice', '005930')"))
        conn.execute(
            sa.text(
                "INSERT INTO account_snapshot (env, account_key, cash_krw) VALUES (:env, :account_key, :cash_krw)"
            ),
            {"env": "practice", "account_key": account_key, "cash_krw": 100000000},
        )
        conn.execute(
            sa.text(
                "INSERT INTO holdings_snapshot (env, account_key, payload) VALUES (:env, :account_key, :payload)"
            ),
            {"env": "practice", "account_key": account_key, "payload": "{}"},
        )
        conn.execute(sa.text("INSERT INTO portfolio_state (env, payload) VALUES ('practice', '{}')"))
        conn.execute(sa.text("INSERT INTO open_orders (env, payload) VALUES ('practice', '{}')"))
        conn.execute(sa.text("INSERT INTO trade_state (env, payload) VALUES ('practice', '{}')"))
        conn.execute(sa.text("INSERT INTO entry_state (env, payload) VALUES ('practice', '{}')"))
        conn.execute(sa.text("INSERT INTO exit_state (env, payload) VALUES ('practice', '{}')"))
        conn.execute(sa.text("INSERT INTO final30 (env, code) VALUES ('practice', '005930')"))
        conn.execute(sa.text("INSERT INTO candidate_pool (env, code) VALUES ('practice', '000660')"))
        conn.execute(sa.text("INSERT INTO watchlist_snapshot (env, code) VALUES ('practice', '035420')"))


def test_reset_requires_practice_env(monkeypatch):
    engine, _schema = _make_engine()
    monkeypatch.setenv("STRATEGY_ENV", "real")
    monkeypatch.setenv("KIS_ENV", "real")
    monkeypatch.setenv("RESET_PRACTICE_ACCOUNT", "1")

    with pytest.raises(RuntimeError, match="env=practice"):
        execute_practice_account_reset(engine=engine)


def test_reset_noop_without_flag(monkeypatch, caplog):
    engine, schema = _make_engine()
    _seed_tables(engine, schema)
    repo = PracticeAccountResetRepo(engine)
    monkeypatch.setenv("STRATEGY_ENV", "practice")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("RESET_PRACTICE_ACCOUNT", "0")
    monkeypatch.setenv("PAPER_MAX_CAPITAL_KRW", "100000000")
    monkeypatch.setenv("CANO", "50160136")
    monkeypatch.setenv("ACNT_PRDT_CD", "01")

    result = execute_practice_account_reset(engine=engine)

    assert result["performed"] is False
    assert repo.count_account_state_rows(env="practice", account_key=build_account_key("practice"))["positions"] == 1
    assert "[ACCOUNT_RESET][NOOP]" in caplog.text


def test_reset_clears_account_state_but_preserves_strategy_data(monkeypatch):
    engine, schema = _make_engine()
    _seed_tables(engine, schema)
    repo = PracticeAccountResetRepo(engine)
    monkeypatch.setenv("STRATEGY_ENV", "practice")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("RESET_PRACTICE_ACCOUNT", "1")
    monkeypatch.setenv("PAPER_MAX_CAPITAL_KRW", "100000000")
    monkeypatch.setenv("CANO", "50160136")
    monkeypatch.setenv("ACNT_PRDT_CD", "01")

    result = execute_practice_account_reset(engine=engine)

    assert result["performed"] is True
    counts = repo.count_account_state_rows(env="practice", account_key=build_account_key("practice"))
    assert counts["positions"] == 0
    assert counts["orders"] == 0
    assert counts["fills"] == 0
    assert counts["cooldowns"] == 0
    with engine.begin() as conn:
        final30_rows = conn.execute(sa.text("SELECT COUNT(*) FROM final30")).scalar_one()
        candidate_pool_rows = conn.execute(sa.text("SELECT COUNT(*) FROM candidate_pool")).scalar_one()
        watchlist_snapshot_rows = conn.execute(sa.text("SELECT COUNT(*) FROM watchlist_snapshot")).scalar_one()
    assert final30_rows == 1
    assert candidate_pool_rows == 1
    assert watchlist_snapshot_rows == 1


def test_reset_deletes_fills_before_orders(monkeypatch):
    engine, schema = _make_engine()
    _seed_tables(engine, schema)
    repo = PracticeAccountResetRepo(engine)
    monkeypatch.setenv("CANO", "50160136")
    monkeypatch.setenv("ACNT_PRDT_CD", "01")

    cleared_counts = repo.clear_account_state(env="practice", account_key=build_account_key("practice"))

    assert cleared_counts["fills"] == 1
    assert cleared_counts["orders"] == 1
    assert cleared_counts["positions"] == 1
    with engine.begin() as conn:
        fills_count = conn.execute(sa.text("SELECT COUNT(*) FROM fills WHERE lower(env) = 'practice'"))
        fills_count = fills_count.scalar_one()
        orders_count = conn.execute(sa.text("SELECT COUNT(*) FROM orders WHERE lower(env) = 'practice'"))
        orders_count = orders_count.scalar_one()
        positions_count = conn.execute(sa.text("SELECT COUNT(*) FROM positions WHERE lower(env) = 'practice'"))
        positions_count = positions_count.scalar_one()
    assert fills_count == 0
    assert orders_count == 0
    assert positions_count == 0


def test_orders_cannot_be_deleted_before_fills_due_to_fk():
    engine, schema = _make_engine()
    _seed_tables(engine, schema)

    with pytest.raises(IntegrityError):
        with engine.begin() as conn:
            conn.execute(sa.delete(schema.orders).where(schema.orders.c.env == "practice"))


def test_reset_skips_missing_optional_tables(monkeypatch, caplog):
    engine, schema = _make_engine(create_optional_tables=False, create_strategy_tables=False)
    account_key = build_account_key("practice")
    with engine.begin() as conn:
        conn.execute(
            sa.insert(schema.positions).values(
                position_id="pos-1",
                env="practice",
                strategy="pb1_pullback_close",
                sid=1,
                mode=1,
                code="005930",
                market="KOSPI",
                qty=3,
                avg_buy_price=70000.0,
                total_cost=210000.0,
                realized_pnl=0.0,
            )
        )
        conn.execute(
            sa.insert(schema.orders).values(
                order_id="ord-1",
                env="practice",
                strategy="pb1_pullback_close",
                sid=1,
                mode=1,
                code="005930",
                market="KOSPI",
                side="BUY",
                ord_type="LIMIT",
                qty=3,
                limit_price=70000.0,
                stage="PB1-CLOSE",
                client_order_key="practice-buy-1",
                status="SUBMITTED",
                request_json={},
            )
        )
        conn.execute(
            sa.insert(schema.fills).values(
                fill_id="fill-1",
                env="practice",
                order_id="ord-1",
                code="005930",
                market="KOSPI",
                side="BUY",
                qty=3,
                price=70000.0,
                fee=0.0,
                tax=0.0,
                filled_at=datetime(2026, 4, 20, 9, 0, 0),
                raw_json={},
            )
        )

    repo = PracticeAccountResetRepo(engine)
    monkeypatch.setenv("CANO", "50160136")
    monkeypatch.setenv("ACNT_PRDT_CD", "01")

    cleared_counts = repo.clear_account_state(env="practice", account_key=account_key)

    assert cleared_counts["fills"] == 1
    assert cleared_counts["orders"] == 1
    assert cleared_counts["positions"] == 1
    assert cleared_counts["cooldowns"] == 0
    assert "[ACCOUNT_RESET][SKIP] table=cooldowns reason=missing" in caplog.text


def test_reset_preserves_strategy_tables(monkeypatch):
    engine, schema = _make_engine()
    _seed_tables(engine, schema)
    monkeypatch.setenv("STRATEGY_ENV", "practice")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("RESET_PRACTICE_ACCOUNT", "1")
    monkeypatch.setenv("PAPER_MAX_CAPITAL_KRW", "100000000")
    monkeypatch.setenv("CANO", "50160136")
    monkeypatch.setenv("ACNT_PRDT_CD", "01")

    execute_practice_account_reset(engine=engine)

    with engine.begin() as conn:
        final30_rows = conn.execute(sa.text("SELECT COUNT(*) FROM final30")).scalar_one()
        candidate_pool_rows = conn.execute(sa.text("SELECT COUNT(*) FROM candidate_pool")).scalar_one()
        watchlist_snapshot_rows = conn.execute(sa.text("SELECT COUNT(*) FROM watchlist_snapshot")).scalar_one()

    assert final30_rows == 1
    assert candidate_pool_rows == 1
    assert watchlist_snapshot_rows == 1


def test_reset_writes_ledger_event(monkeypatch):
    engine, schema = _make_engine()
    _seed_tables(engine, schema)
    monkeypatch.setenv("STRATEGY_ENV", "practice")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("RESET_PRACTICE_ACCOUNT", "1")
    monkeypatch.setenv("PAPER_MAX_CAPITAL_KRW", "100000000")
    monkeypatch.setenv("CANO", "50160136")
    monkeypatch.setenv("ACNT_PRDT_CD", "01")

    execute_practice_account_reset(engine=engine)

    with engine.begin() as conn:
        row = conn.execute(
            sa.select(schema.ledger_events).where(schema.ledger_events.c.event_type == "PRACTICE_ACCOUNT_RESET")
        ).mappings().first()
    assert row is not None
    payload = dict(row["payload_json"] or {})
    assert payload["status"] == "DONE"
    assert payload["capital_krw"] == 100000000