"""2026-11-06 KR KIS cumulative BUY status provenance and replay monotonicity.

No KIS client, orders or production DB: disposable SQLite only.
"""
from datetime import timedelta
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
    ("ACKED", "FILLED", "FILLED"),
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
