from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
import sqlalchemy as sa

import trader.db.repos as db_repos
from trader.db.repos import FillsRepo, PortfolioEpochsRepo, RunsRepo
from trader.db.schema import schema_for_engine
from trader.db.trading_epoch import start_new_trading_epoch
from trader.pb1_runner import _register_kr_run_row


def _engine():
    engine = sa.create_engine("sqlite:///:memory:", future=True)
    schema_for_engine(engine).metadata.create_all(engine)
    return engine


def test_runs_repo_upsert_run_inserts_and_reuses_same_run_id():
    engine = _engine()
    schema = schema_for_engine(engine)
    repo = RunsRepo(engine)
    run_id = str(uuid4())
    started = datetime.now(timezone.utc)

    repo.upsert_run(
        run_id=run_id,
        env="practice",
        strategy="pb1_pullback_close",
        workflow_run_id="1001",
        ts_start=started,
    )
    repo.upsert_run(
        run_id=run_id,
        env="practice",
        strategy="pb1_pullback_close",
        workflow_run_id="1002",
        ts_start=started,
    )

    with engine.connect() as conn:
        rows = conn.execute(
            sa.select(schema.runs).where(
                sa.cast(schema.runs.c.run_id, sa.String) == run_id
            )
        ).mappings().all()

    assert len(rows) == 1
    assert rows[0]["env"] == "practice"
    assert rows[0]["strategy"] == "pb1_pullback_close"
    assert str(rows[0]["workflow_run_id"]) == "1002"


def test_kr_startup_registration_uses_real_runs_repo_without_nameerror(monkeypatch):
    engine = _engine()
    schema = schema_for_engine(engine)
    run_id = str(uuid4())
    monkeypatch.setenv("TRADER_RUN_ID", run_id)
    ctx = SimpleNamespace(
        env="practice",
        strategy="pb1_pullback_close",
        gh_run_number=77,
        started_at=datetime.now(timezone.utc),
    )

    repo, actual_run_id = _register_kr_run_row(engine=engine, ctx=ctx)

    assert isinstance(repo, RunsRepo)
    assert actual_run_id == run_id
    with engine.connect() as conn:
        row = conn.execute(
            sa.select(schema.runs).where(
                sa.cast(schema.runs.c.run_id, sa.String) == run_id
            )
        ).mappings().one()
    assert row["env"] == "practice"
    assert row["strategy"] == "pb1_pullback_close"
    assert str(row["workflow_run_id"]) == "77"


def _active_epoch(engine, monkeypatch):
    account_id = "practice:test"
    monkeypatch.setattr(db_repos, "get_account_key", lambda env=None: account_id)
    top = start_new_trading_epoch(
        engine,
        env="practice",
        account_id=account_id,
        reason="TEST_PR138_FILL_COLLISION",
    )
    child = PortfolioEpochsRepo(engine).get_or_create_active(
        env="practice",
        account_id=account_id,
        sid=1,
        mode=1,
        strategy="pb1",
    )
    return top, child


def _save_fill(repo: FillsRepo, *, trade_id: str, child: str, cycle: str):
    return repo.upsert_fill(
        env="practice",
        run_id=None,
        order_id=None,
        kis_odno="1234567890",
        trade_id=trade_id,
        code="005930",
        market="KOSPI",
        side="BUY",
        qty=1,
        price=70000.0,
        fee=0.0,
        tax=0.0,
        filled_at=datetime.now(timezone.utc),
        raw_json={"source": "test"},
        position_cycle_id=cycle,
        portfolio_epoch_id=child,
    )


def test_fill_conflict_guard_is_in_fills_repo_for_position_cycle(monkeypatch):
    engine = _engine()
    _, child = _active_epoch(engine, monkeypatch)
    repo = FillsRepo(engine)
    trade_id = "same-broker-fill-cycle"

    _save_fill(repo, trade_id=trade_id, child=child, cycle="cycle-a")
    with pytest.raises(RuntimeError, match="KR_FILL_POSITION_CYCLE_COLLISION"):
        _save_fill(repo, trade_id=trade_id, child=child, cycle="cycle-b")


def test_fill_conflict_guard_is_in_fills_repo_for_portfolio_epoch(monkeypatch):
    engine = _engine()
    account_id = "practice:test"
    _, child = _active_epoch(engine, monkeypatch)
    child2 = PortfolioEpochsRepo(engine).get_or_create_active(
        env="practice",
        account_id=account_id,
        sid=2,
        mode=1,
        strategy="pb1-secondary",
    )
    repo = FillsRepo(engine)
    trade_id = "same-broker-fill-portfolio"

    _save_fill(repo, trade_id=trade_id, child=child, cycle="cycle-a")
    with pytest.raises(RuntimeError, match="KR_FILL_PORTFOLIO_EPOCH_COLLISION"):
        _save_fill(repo, trade_id=trade_id, child=child2, cycle="cycle-a")
