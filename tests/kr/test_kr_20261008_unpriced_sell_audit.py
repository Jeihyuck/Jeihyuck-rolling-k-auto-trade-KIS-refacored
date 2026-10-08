from __future__ import annotations

from datetime import date, datetime
from uuid import uuid4
from zoneinfo import ZoneInfo

import sqlalchemy as sa

from trader.db.schema import schema_for_engine
from trader.kr.unpriced_sell_audit import audit_kr_unpriced_sell_orders

STRATEGY = "pb1_pullback_close"


def test_kst_unpriced_sells_never_count_missing_execution_price_as_zero_pnl():
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    created = datetime(2026, 10, 8, 13, 17, tzinfo=ZoneInfo("Asia/Seoul"))
    with engine.begin() as conn:
        for code, qty, order_no, response in [
            ("010120", 7, "0000006722", {"confirmed_fill_qty": 7, "confirmed_fill_price": None}),
            ("000660", 1, "0000027169", {"confirmed_fill_qty": 1, "confirmed_fill_price": None}),
            ("066570", 5, "practice:semantic:retry2", {
                "rt_cd": "UNRESOLVED_ACK", "confirmed_fill_qty": 5, "confirmed_fill_price": None,
            }),
        ]:
            conn.execute(sa.insert(schema.orders).values(
                order_id=str(uuid4()), env="practice", strategy=STRATEGY,
                sid=1, mode=1, code=code, side="SELL", stage="FULL_EXIT", ord_type="LIMIT",
                status="FILLED_QTY_CONFIRMED_PRICE_UNRESOLVED", qty=qty,
                client_order_key=f"test:{code}", kis_odno=order_no,
                request_json={}, response_json=response, created_at=created,
            ))
    a = audit_kr_unpriced_sell_orders(engine, env="practice", trade_date=date(2026, 10, 8))
    assert a["unpriced_sell_count"] == 3
    assert a["unproven_broker_order_count"] == 1
    assert {x["code"] for x in a["orders"]} == {"010120", "000660", "066570"}
    assert all(x["realized_pnl_status"] == "UNRESOLVED_NOT_ZERO" for x in a["orders"])
    assert audit_kr_unpriced_sell_orders(
        engine, env="practice", trade_date=date(2026, 10, 9),
    )["unpriced_sell_count"] == 0


def test_audit_is_fail_closed_for_unspecified_scope():
    engine = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(engine).metadata.create_all(engine)
    import pytest
    with pytest.raises(ValueError):
        audit_kr_unpriced_sell_orders(engine, env="unknown", trade_date=date(2026, 10, 8))


def test_operator_cli_reports_explicit_empty_date_and_zero_exit(monkeypatch, capsys):
    from trader.kr.unpriced_sell_audit import main

    engine = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(engine).metadata.create_all(engine)
    monkeypatch.setattr("trader.db.engine.get_engine", lambda: engine)
    assert main(["--env", "practice", "--trade-date", "2026-10-08"]) == 0
    import json
    data = json.loads(capsys.readouterr().out)
    assert data["env"] == "practice"
    assert data["market"] == "KR"
    assert data["unpriced_sell_count"] == 0


def test_sqlite_kst_evening_unpriced_sell_remains_on_original_trade_date():
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(sa.insert(schema.orders).values(
            order_id=str(uuid4()), env="practice", strategy=STRATEGY,
            sid=1, mode=1, code="010120", side="SELL",
            ord_type="LIMIT", stage="FULL_EXIT", qty=1,
            client_order_key="kst-evening-20261008", kis_odno="0000006722",
            status="FILLED_QTY_CONFIRMED_PRICE_UNRESOLVED",
            request_json={}, response_json={
                "confirmed_fill_qty": 1, "confirmed_fill_price": None,
            },
            created_at=datetime(2026, 10, 8, 20, 17, tzinfo=ZoneInfo("Asia/Seoul")),
        ))
    today = audit_kr_unpriced_sell_orders(engine, env="practice", trade_date=date(2026, 10, 8))
    tomorrow = audit_kr_unpriced_sell_orders(engine, env="practice", trade_date=date(2026, 10, 9))
    assert today["unpriced_sell_count"] == 1
    assert tomorrow["unpriced_sell_count"] == 0
