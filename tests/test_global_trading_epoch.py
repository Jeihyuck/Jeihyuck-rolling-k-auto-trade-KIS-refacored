from __future__ import annotations

from datetime import datetime, timezone

import sqlalchemy as sa

from trader.db.repos import FillsRepo, OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.trading_epoch import (
    active_epoch_snapshot,
    get_active_trading_epoch_id,
    start_new_trading_epoch,
)


def _engine():
    engine = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(engine).metadata.create_all(engine)
    return engine


def test_global_epoch_preserves_old_history_and_new_kr_orders_use_new_epoch():
    engine = _engine()
    schema = schema_for_engine(engine)
    account = "practice:test:01"

    old_epoch = get_active_trading_epoch_id(
        engine, env="practice", account_id=account, reason="INITIAL"
    )
    orders = OrdersRepo(engine)
    positions = PositionsRepo(engine)
    fills = FillsRepo(engine)

    old_order_id, created = orders.create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close", sid=1, mode=1,
        code="005930", market="KOSPI", side="BUY", ord_type="LIMIT", qty=1,
        limit_price=10000, stage="ENTRY", client_order_key="old-epoch-buy",
        request_json={}, account_id=account,
    )
    assert created
    fills.upsert_fill(
        env="practice", run_id=None, order_id=old_order_id, kis_odno="old-1",
        trade_id="old-trade", code="005930", market="KOSPI", side="BUY",
        qty=1, price=10000, fee=0, tax=0, filled_at=datetime.now(timezone.utc),
        raw_json={},
    )
    positions.apply_fill(
        env="practice", strategy="pb1_pullback_close", sid=1, mode=1,
        code="005930", market="KOSPI", side="BUY", qty=1, price=10000,
        fee=0, tax=0, filled_at=datetime.now(timezone.utc), account_id=account,
        order_id=old_order_id,
    )

    with engine.connect() as conn:
        old_order = conn.execute(
            sa.select(schema.orders).where(schema.orders.c.order_id == old_order_id)
        ).mappings().one()
        old_fill = conn.execute(sa.select(schema.fills)).mappings().one()
        old_position = conn.execute(sa.select(schema.positions)).mappings().one()
    assert old_order["trading_epoch_id"] == old_epoch
    assert old_fill["trading_epoch_id"] == old_epoch
    assert old_position["trading_epoch_id"] == old_epoch

    new_epoch = start_new_trading_epoch(
        engine, env="practice", account_id=account, reason="KIS_PRACTICE_ACCOUNT_RESET"
    )
    assert new_epoch != old_epoch

    with engine.connect() as conn:
        old_epoch_row = conn.execute(
            sa.select(schema.trading_epochs).where(
                schema.trading_epochs.c.trading_epoch_id == old_epoch
            )
        ).mappings().one()
        old_position_after = conn.execute(
            sa.select(schema.positions).where(
                schema.positions.c.position_id == old_position["position_id"]
            )
        ).mappings().one()
    assert old_epoch_row["status"] == "ENDED"
    assert old_position_after["status"] == "CLOSED"
    assert old_position_after["closed_reason"] == "TRADING_EPOCH_ENDED"

    new_order_id, created = orders.create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close", sid=1, mode=1,
        code="005930", market="KOSPI", side="BUY", ord_type="LIMIT", qty=1,
        limit_price=11000, stage="ENTRY", client_order_key="new-epoch-buy",
        request_json={}, account_id=account,
    )
    assert created
    with engine.connect() as conn:
        new_order = conn.execute(
            sa.select(schema.orders).where(schema.orders.c.order_id == new_order_id)
        ).mappings().one()
        all_orders = list(conn.execute(sa.select(schema.orders)).mappings())
    assert new_order["trading_epoch_id"] == new_epoch
    assert len(all_orders) == 2
    assert {row["trading_epoch_id"] for row in all_orders} == {old_epoch, new_epoch}
    assert active_epoch_snapshot(engine, env="practice", account_id=account)["trading_epoch_id"] == new_epoch


def test_new_epoch_command_requires_flat_kr_and_us_balances(monkeypatch):
    import scripts.start_new_practice_trading_epoch as cmd

    engine = _engine()
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("START_NEW_PRACTICE_EPOCH", "1")
    monkeypatch.setenv("EPOCH_RESET_CONFIRM", "YES")
    monkeypatch.setattr(cmd, "run_migrations", lambda _engine: None)

    class KR:
        CANO = "12345678"
        ACNT_PRDT_CD = "01"
        def get_balance_cached(self, force=False):
            assert force is True
            return {"output1": []}

    class US:
        def get_balance(self, force_refresh=False):
            assert force_refresh is True
            return {"positions": [], "balance_parse_status": "OK"}

    result = cmd.start_new_practice_epoch(
        engine=engine, kis=KR(), us_provider=US(), reason="TEST_RESET"
    )
    assert result["status"] == "OK"
    assert result["kr_holdings"] == 0
    assert result["us_holdings"] == 0
    assert result["history_deleted"] is False
    assert result["new_trading_epoch_id"]
