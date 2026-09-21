from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
import sqlalchemy as sa

import trader.db.repos as db_repos
from trader.db.schema import schema_for_engine
from trader.db.repos import FillsRepo, OrdersRepo, PortfolioEpochsRepo, PositionsRepo
from trader.db.trading_epoch import (
    active_trading_epoch_id,
    start_new_trading_epoch,
    trading_epoch_enforced,
)


def _engine():
    engine = sa.create_engine("sqlite:///:memory:", future=True)
    schema_for_engine(engine).metadata.create_all(engine)
    return engine


def test_start_new_epoch_preserves_history_and_ends_prior_children():
    engine = _engine()
    schema = schema_for_engine(engine)

    first = start_new_trading_epoch(
        engine, env="practice", account_id="practice:test",
        reason="PRACTICE_RESET_A",
    )
    assert active_trading_epoch_id(
        engine, env="practice", account_id="practice:test"
    ) == first

    child = PortfolioEpochsRepo(engine).get_or_create_active(
        env="practice", account_id="practice:test", sid=1, mode=1, strategy="pb1"
    )
    with engine.connect() as conn:
        row = conn.execute(
            sa.select(schema.portfolio_epochs).where(
                schema.portfolio_epochs.c.portfolio_epoch_id == child
            )
        ).mappings().one()
        assert str(row["trading_epoch_id"]) == first
        assert row["status"] == "ACTIVE"

    second = start_new_trading_epoch(
        engine, env="practice", account_id="practice:test",
        reason="PRACTICE_RESET_B",
    )
    assert second != first
    assert active_trading_epoch_id(
        engine, env="practice", account_id="practice:test"
    ) == second

    with engine.connect() as conn:
        epochs = conn.execute(
            sa.select(schema.trading_epochs).where(
                schema.trading_epochs.c.env == "practice",
                schema.trading_epochs.c.account_id == "practice:test",
            ).order_by(schema.trading_epochs.c.created_at)
        ).mappings().all()
        children = conn.execute(
            sa.select(schema.portfolio_epochs).where(
                schema.portfolio_epochs.c.account_id == "practice:test"
            )
        ).mappings().all()

    assert len(epochs) == 2
    assert {row["status"] for row in epochs} == {"ACTIVE", "ENDED"}
    assert len(children) == 1
    assert children[0]["status"] == "ENDED"
    assert str(children[0]["trading_epoch_id"]) == first


def test_new_kr_portfolio_child_belongs_to_current_top_epoch():
    engine = _engine()
    top = start_new_trading_epoch(
        engine, env="practice", account_id="practice:test",
        reason="PRACTICE_CLEAN_RESTART",
    )
    child = PortfolioEpochsRepo(engine).get_or_create_active(
        env="practice", account_id="practice:test", sid=1, mode=1, strategy="pb1"
    )
    schema = schema_for_engine(engine)
    with engine.connect() as conn:
        row = conn.execute(
            sa.select(schema.portfolio_epochs).where(
                schema.portfolio_epochs.c.portfolio_epoch_id == child
            )
        ).mappings().one()
    assert str(row["trading_epoch_id"]) == top


def test_0052_migration_covers_kr_us_and_infinite_state():
    sql = Path("migrations/0052_unified_trading_epoch.sql").read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS trading_epochs" in sql
    for table in (
        "portfolio_epochs", "orders", "fills", "positions",
        "us_order_intents", "us_orders", "us_fills", "us_positions",
        "us_order_events", "us_profit_capture_lifecycle",
        "us_tqqq_infinite_state", "kr_infinite_state", "kr_infinite_order_intents",
    ):
        assert f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS trading_epoch_id" in sql
    assert "PRIMARY KEY(trading_epoch_id, strategy_id, symbol)" in sql
    assert "ON kr_infinite_order_intents(trading_epoch_id, idempotency_key)" in sql


def test_canonical_preflight_requires_active_epoch():
    source = Path("scripts/wsl/deploy-preflight.sh").read_text(encoding="utf-8")
    assert "TRADING_EPOCH_ENFORCE=1" in source
    assert "scripts/verify_active_trading_epoch.py" in source
    assert "active_trading_epoch_missing" in source


def test_pr137_db_replacement_is_not_part_of_restored_design():
    # This branch is intentionally rooted at PR135. The epoch reset does not
    # depend on the PR137 fresh-database cutover path.
    assert not Path("scripts/prepare_new_practice_database.py").exists()
    assert not Path("scripts/verify_new_practice_database_cutover.py").exists()



def test_canonical_wsl_runtime_enforces_epoch_even_if_flag_is_zero(monkeypatch):
    monkeypatch.setenv("TRADING_EPOCH_ENFORCE", "0")
    monkeypatch.setenv("WSL_RUN_MARKET", "US")
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    assert trading_epoch_enforced() is True


def test_noncanonical_test_fixture_keeps_legacy_compatibility(monkeypatch):
    monkeypatch.setenv("TRADING_EPOCH_ENFORCE", "0")
    monkeypatch.delenv("WSL_RUN_MARKET", raising=False)
    assert trading_epoch_enforced() is False



def _activate_practice_epoch(engine, monkeypatch, account_id="practice:test"):
    monkeypatch.setattr(db_repos, "get_account_key", lambda env=None: account_id)
    return start_new_trading_epoch(
        engine,
        env="practice",
        account_id=account_id,
        reason="TEST_ACTIVE_EPOCH",
    )


def test_kr_buy_fill_position_chain_shares_active_trading_epoch(monkeypatch):
    engine = _engine()
    top = _activate_practice_epoch(engine, monkeypatch)
    schema = schema_for_engine(engine)
    orders = OrdersRepo(engine)
    fills = FillsRepo(engine)
    positions = PositionsRepo(engine)

    order_id, created = orders.create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1", sid=1, mode=1,
        code="005930", market="KOSPI", side="BUY", ord_type="LIMIT",
        qty=2, limit_price=70000, stage="ENTRY",
        client_order_key="epoch-kr-buy-1", request_json={},
        account_id="practice:test",
    )
    assert created is True

    fill_id = fills.upsert_fill(
        env="practice", run_id=None, order_id=order_id, kis_odno="KR-1",
        trade_id="epoch-fill-1", code="005930", market="KOSPI",
        side="BUY", qty=2, price=70000, fee=0, tax=0,
        filled_at=datetime.now(timezone.utc), raw_json={},
    )
    positions.apply_fill(
        env="practice", strategy="pb1", sid=1, mode=1,
        code="005930", market="KOSPI", side="BUY", qty=2,
        price=70000, fee=0, tax=0, filled_at=datetime.now(timezone.utc),
        account_id="practice:test", order_id=order_id,
    )

    with engine.connect() as conn:
        order = conn.execute(
            sa.select(schema.orders).where(schema.orders.c.order_id == order_id)
        ).mappings().one()
        fill = conn.execute(
            sa.select(schema.fills).where(schema.fills.c.fill_id == fill_id)
        ).mappings().one()
        position = conn.execute(
            sa.select(schema.positions).where(
                schema.positions.c.code == "005930",
                schema.positions.c.status == "OPEN",
                schema.positions.c.trading_epoch_id == top,
            )
        ).mappings().one()

    assert str(order["trading_epoch_id"]) == top
    assert str(fill["trading_epoch_id"]) == top
    assert str(position["trading_epoch_id"]) == top
    assert fill["portfolio_epoch_id"] == position["portfolio_epoch_id"] == order["portfolio_epoch_id"]
    assert fill["position_cycle_id"] == position["position_cycle_id"] == order["position_cycle_id"]
    assert [row["code"] for row in positions.list_positions("practice", "pb1")] == ["005930"]


def test_kr_kis_holding_recovery_uses_current_epoch_and_is_idempotent(monkeypatch):
    engine = _engine()
    top = _activate_practice_epoch(engine, monkeypatch)
    schema = schema_for_engine(engine)
    positions = PositionsRepo(engine)
    child = PortfolioEpochsRepo(engine).get_or_create_active(
        env="practice", account_id="practice:test", sid=1, mode=1, strategy="pb1"
    )

    # Legacy/unscoped row with the same symbol/child must not be reused.
    with engine.begin() as conn:
        conn.execute(sa.insert(schema.positions).values(
            position_id=str(uuid4()), position_cycle_id=str(uuid4()),
            portfolio_epoch_id=child, trading_epoch_id=None,
            opened_at=datetime.now(timezone.utc), position_origin="RECOVERY",
            env="practice", strategy="pb1", sid=1, mode=1, code="000660",
            market="KOSPI", qty=99, avg_buy_price=1, total_cost=99,
            realized_pnl=0, status="OPEN",
        ))

    first = positions.upsert_positions_from_kis_holdings(
        env="practice", account_key="practice:test",
        holdings=[{"pdno":"000660","hldg_qty":"3","pchs_avg_pric":"200000","pchs_amt":"600000","market":"KOSPI"}],
    )
    second = positions.upsert_positions_from_kis_holdings(
        env="practice", account_key="practice:test",
        holdings=[{"pdno":"000660","hldg_qty":"4","pchs_avg_pric":"210000","pchs_amt":"840000","market":"KOSPI"}],
    )
    assert first == {"inserted": 1, "updated": 0, "skipped": 0}
    assert second == {"inserted": 0, "updated": 1, "skipped": 0}

    with engine.connect() as conn:
        current = conn.execute(sa.select(schema.positions).where(
            schema.positions.c.code == "000660",
            schema.positions.c.trading_epoch_id == top,
            schema.positions.c.status == "OPEN",
        )).mappings().all()
        legacy = conn.execute(sa.select(schema.positions).where(
            schema.positions.c.code == "000660",
            schema.positions.c.trading_epoch_id.is_(None),
        )).mappings().all()
    assert len(current) == 1
    assert current[0]["qty"] == 4
    assert float(current[0]["avg_buy_price"]) == 210000
    assert len(legacy) == 1 and legacy[0]["qty"] == 99
    visible = positions.list_positions_by_codes(env="practice", strategy="pb1", codes=["000660"])
    assert len(visible) == 1 and str(visible[0]["trading_epoch_id"]) == top


def test_kr_reconciled_order_preserves_epoch_contract_and_dedupes(monkeypatch):
    engine = _engine()
    top = _activate_practice_epoch(engine, monkeypatch)
    schema = schema_for_engine(engine)
    orders = OrdersRepo(engine)
    observed_at = datetime.now(timezone.utc) + timedelta(seconds=1)

    kwargs = dict(
        env="practice", run_id=None, strategy="pb1", sid=1, mode=1,
        code="035420", market="KOSPI", side="BUY", ord_type="LIMIT",
        qty=2, limit_price=200000, stage="ENTRY",
        client_order_key="reconcile-current-epoch", kis_odno="KIS-RECON-1",
        status="ACKED",
        request_json={"strategy_owner":"PB1","entry_reason":"ENTRY_PULLBACK","contract_token":"IMMUTABLE"},
        response_json={"rt_cd":"0"}, submitted_at=observed_at, acked_at=observed_at,
    )
    first_id = orders.upsert_reconciled_order(**kwargs)
    second_id = orders.upsert_reconciled_order(
        **{**kwargs, "request_json": {"observed":"different"}, "status":"FILLED"}
    )
    assert first_id == second_id

    with engine.connect() as conn:
        rows = conn.execute(sa.select(schema.orders).where(
            schema.orders.c.client_order_key == "reconcile-current-epoch"
        )).mappings().all()
    assert len(rows) == 1
    row = rows[0]
    assert str(row["trading_epoch_id"]) == top
    assert row["portfolio_epoch_id"]
    assert row["request_json"]["contract_token"] == "IMMUTABLE"
    assert row["request_json"]["strategy_owner"] == "PB1"
    assert row["request_json"]["_reconcile_observation"] == {"observed":"different"}


def test_kr_fill_fallback_never_aggregates_previous_epoch(monkeypatch):
    engine = _engine()
    top = _activate_practice_epoch(engine, monkeypatch)
    schema = schema_for_engine(engine)
    now = datetime.now(timezone.utc)
    with engine.begin() as conn:
        for epoch, qty, trade_id in (
            ("old-epoch", 9, "old-fill"),
            (top, 2, "new-fill"),
        ):
            conn.execute(sa.insert(schema.fills).values(
                fill_id=str(uuid4()), env="practice", trading_epoch_id=epoch,
                trade_id=trade_id, broker_fill_id=trade_id,
                code="005930", market="KOSPI", side="BUY", qty=qty,
                price=70000, fee=0, tax=0, filled_at=now, raw_json={},
            ))

    rows = FillsRepo(engine).list_net_positions_from_fills(
        "practice", strategy=None, codes=["005930"], kr_only=True
    )
    assert len(rows) == 1
    assert rows[0]["code"] == "005930"
    assert rows[0]["qty"] == 2


def test_kr_old_order_key_cannot_be_adopted_into_new_epoch(monkeypatch):
    engine = _engine()
    schema = schema_for_engine(engine)
    monkeypatch.setattr(db_repos, "get_account_key", lambda env=None: "practice:test")
    orders = OrdersRepo(engine)

    legacy_id, _ = orders.create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1", sid=1, mode=1,
        code="068270", market="KOSPI", side="BUY", ord_type="LIMIT",
        qty=1, limit_price=100000, stage="ENTRY",
        client_order_key="same-key-across-epoch", request_json={},
        account_id="practice:test",
    )
    top = start_new_trading_epoch(
        engine, env="practice", account_id="practice:test", reason="NEW_EPOCH"
    )
    with pytest.raises(RuntimeError, match="KR_ORDER_TRADING_EPOCH_COLLISION"):
        orders.create_intent_idempotent(
            env="practice", run_id=None, strategy="pb1", sid=1, mode=1,
            code="068270", market="KOSPI", side="BUY", ord_type="LIMIT",
            qty=1, limit_price=100000, stage="ENTRY",
            client_order_key="same-key-across-epoch", request_json={},
            account_id="practice:test",
        )
    with engine.connect() as conn:
        old = conn.execute(sa.select(schema.orders).where(schema.orders.c.order_id == legacy_id)).mappings().one()
    assert old["trading_epoch_id"] is None
    assert top



def test_kr_fill_dedupe_cannot_adopt_previous_epoch(monkeypatch):
    engine = _engine()
    top = _activate_practice_epoch(engine, monkeypatch)
    schema = schema_for_engine(engine)
    now = datetime.now(timezone.utc)

    with engine.begin() as conn:
        conn.execute(sa.insert(schema.fills).values(
            fill_id=str(uuid4()), env="practice", trading_epoch_id="old-epoch",
            trade_id="BROKER-FILL-REUSED", broker_fill_id="BROKER-FILL-REUSED",
            code="005930", market="KOSPI", side="BUY", qty=1,
            price=70000, fee=0, tax=0, filled_at=now, raw_json={"old": True},
        ))

    with pytest.raises(RuntimeError, match="KR_FILL_TRADING_EPOCH_COLLISION"):
        FillsRepo(engine).upsert_fill(
            env="practice", run_id=None, order_id=None, kis_odno="KR-REUSED",
            trade_id="BROKER-FILL-REUSED", code="005930", market="KOSPI",
            side="BUY", qty=1, price=71000, fee=0, tax=0,
            filled_at=now, raw_json={"new": True},
        )

    with engine.connect() as conn:
        row = conn.execute(sa.select(schema.fills).where(
            schema.fills.c.broker_fill_id == "BROKER-FILL-REUSED"
        )).mappings().one()
    assert row["trading_epoch_id"] == "old-epoch"
    assert row["raw_json"] == {"old": True}
    assert top != "old-epoch"



def test_kr_position_mutators_never_touch_previous_trading_epoch(monkeypatch):
    engine = _engine()
    top = _activate_practice_epoch(engine, monkeypatch)
    schema = schema_for_engine(engine)
    positions = PositionsRepo(engine)
    current_child = PortfolioEpochsRepo(engine).get_or_create_active(
        env="practice", account_id="practice:test", sid=1, mode=1, strategy="pb1"
    )
    old_child = str(uuid4())

    with engine.begin() as conn:
        conn.execute(sa.insert(schema.portfolio_epochs).values(
            portfolio_epoch_id=old_child,
            trading_epoch_id="old-epoch",
            env="practice", account_id="practice:test",
            sid=1, mode=1, strategy="pb1",
            status="ENDED", reason="HISTORICAL",
        ))
        conn.execute(sa.insert(schema.positions).values(
            position_id=str(uuid4()), position_cycle_id=str(uuid4()),
            portfolio_epoch_id=old_child, trading_epoch_id="old-epoch",
            opened_at=datetime.now(timezone.utc), position_origin="SYSTEM",
            env="practice", strategy="pb1", sid=1, mode=1,
            code="035420", market="KOSPI", qty=7, avg_buy_price=100,
            total_cost=700, realized_pnl=0, max_price=111, status="OPEN",
        ))
        conn.execute(sa.insert(schema.positions).values(
            position_id=str(uuid4()), position_cycle_id=str(uuid4()),
            portfolio_epoch_id=current_child, trading_epoch_id=top,
            opened_at=datetime.now(timezone.utc), position_origin="SYSTEM",
            env="practice", strategy="pb1", sid=1, mode=1,
            code="035420", market="KOSPI", qty=3, avg_buy_price=200,
            total_cost=600, realized_pnl=0, max_price=222, status="OPEN",
        ))

    positions.update_position_fields(
        env="practice", strategy="pb1", sid=1, mode=1,
        code="035420", fields={"max_price": 333},
    )
    assert positions.close_positions(
        env="practice", strategy="pb1", codes=["035420"]
    ) == 1

    with engine.connect() as conn:
        rows = conn.execute(sa.select(schema.positions).where(
            schema.positions.c.code == "035420"
        )).mappings().all()
    by_epoch = {str(row["trading_epoch_id"]): row for row in rows}
    assert by_epoch[top]["status"] == "CLOSED"
    assert by_epoch[top]["qty"] == 0
    assert float(by_epoch[top]["max_price"]) == 333
    assert by_epoch["old-epoch"]["status"] == "OPEN"
    assert by_epoch["old-epoch"]["qty"] == 7
    assert float(by_epoch["old-epoch"]["max_price"]) == 111
