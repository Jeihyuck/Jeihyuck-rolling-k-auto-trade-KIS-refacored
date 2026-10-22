"""Read-only broker replay fixtures for the 2026-10-21/22 KRX defects.

No network, live KIS client, or production DB is used.
"""
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from trader.reconcile_kis import (
    _parse_date_time,
    _parse_execution_time_with_source,
    _verified_kr_cumulative_buy_evidence,
    _verified_kr_cumulative_sell_evidence,
)


def _source(side, quantity, odno):
    return {"code": "251970", "side": side, "qty": quantity, "kis_odno": odno,
            "order_id": "replay-order"}


def _row(side, qty, total, odno, *, trade_day="20261021"):
    return {"pdno": "251970", "sll_buy_dvsn_cd": "01" if side == "SELL" else "02",
            "odno": odno, "ord_dt": trade_day, "ord_tmd": "151620",
            "tot_ccld_qty": str(qty), "tot_ccld_amt": str(total),
            "avg_prvs": str(total / qty)}


def test_kis_daily_trade_time_is_kst_not_naive_utc():
    morning = _parse_date_time({"ord_dt": "20261021", "ord_tmd": "093900"})
    closing = _parse_date_time({"ord_dt": "20261021", "ord_tmd": "151620"})
    assert morning.utcoffset().total_seconds() == 9 * 3600
    assert morning.isoformat() == "2026-10-21T09:39:00+09:00"
    assert closing.astimezone(ZoneInfo("UTC")).isoformat() == "2026-10-21T06:16:20+00:00"
    assert closing.date() == date(2026, 10, 21)


@pytest.mark.parametrize("qty,total,requested,odno", [
    (10, 685000, 17, "0000040226"),   # 251970 day-one partial, unfilled remainder 7
    (52, 836180, 79, "0000040083"),  # 253590 day-two partial, unfilled remainder 27
    (81, 1091430, 81, "0000040070"), # 144960 whole SELL, regression
])
def test_priced_cumulative_sell_accepts_broker_partial_not_quote(qty,total,requested,odno):
    row = _row("SELL", qty, total, odno)
    source = _source("SELL", requested, odno)
    source["code"] = row["pdno"]
    evidence = _verified_kr_cumulative_sell_evidence(
        row, source_order=source, side="SELL", trade_date=date(2026,10,21),
        kis_odno=odno, env="practice",
    )
    assert evidence["qty"] == qty
    assert evidence["notional"] == float(total)
    assert evidence["kind"] == "KIS_ORDER_CUMULATIVE_EXECUTION"
    row["tot_ccld_amt"] = "0"
    assert _verified_kr_cumulative_sell_evidence(
        row, source_order=source, side="SELL", trade_date=date(2026,10,21),
        kis_odno=odno, env="practice",
    ) is None


@pytest.mark.parametrize("first_qty,first_amt,next_qty,next_amt,odno", [
    (15, 1026000, 17, 1162800, "0000013196"),
    (22, 539000, 48, 1176000, "0000013221"),
])
def test_cumulative_buy_has_one_stable_evidence_id(first_qty,first_amt,next_qty,next_amt,odno):
    source = _source("BUY", next_qty, odno)
    first = _verified_kr_cumulative_buy_evidence(
        _row("BUY", first_qty, first_amt, odno), source_order=source, side="BUY",
        trade_date=date(2026, 10, 21), kis_odno=odno, env="practice",
    )
    second = _verified_kr_cumulative_buy_evidence(
        _row("BUY", next_qty, next_amt, odno), source_order=source, side="BUY",
        trade_date=date(2026, 10, 21), kis_odno=odno, env="practice",
    )
    assert first["evidence_id"] == second["evidence_id"]
    assert next_qty - first_qty in (2, 26)
    assert second["notional"] - first["notional"] == next_amt - first_amt


def test_no_cumulative_without_exact_durable_source_or_broker_notional():
    row = _row("BUY", 17, 1162800, "0000013196")
    assert _verified_kr_cumulative_buy_evidence(
        row, source_order=None, side="BUY", trade_date=date(2026,10,21),
        kis_odno="0000013196", env="practice",
    ) is None
    row["tot_ccld_amt"] = "0"
    assert _verified_kr_cumulative_buy_evidence(
        row, source_order=_source("BUY",17,"0000013196"), side="BUY",
        trade_date=date(2026,10,21), kis_odno="0000013196", env="practice",
    ) is None


@pytest.mark.parametrize("sequence", [
    ((15, 68400.0), (17, 68400.0), (15, 68400.0), (17, 68400.0)),
    ((10, 68500.0), (52, 16080.384615384615), (10, 68500.0), (52, 16080.384615384615)),
])
def test_order_cumulative_fill_snapshot_is_monotonic_and_one_row(sequence):
    """SQLite storage regression; PostgreSQL CI runs the independent cross-path tests."""
    import uuid

    import sqlalchemy as sa

    from trader.db.repos import FillsRepo
    from trader.db.schema import schema_for_engine
    from tests.kr.execution_claim_fixtures import create_schema_with_active_test_epoch

    engine = sa.create_engine("sqlite:///:memory:")
    create_schema_with_active_test_epoch(engine)
    repo = FillsRepo(engine)
    order_id = str(uuid.uuid4())
    for qty, price in sequence:
        repo.upsert_fill(
            env="practice", run_id=None, order_id=order_id,
            kis_odno="0000013196", trade_id="KR_KIS_CUM:fixture:20261021:0000013196:251970:BUY",
            code="251970", market="KOSPI", side="BUY", qty=qty, price=price,
            fee=0.0, tax=0.0,
            filled_at=datetime(2026, 10, 21, 9, 39, tzinfo=ZoneInfo("Asia/Seoul")),
            raw_json={"tot_ccld_qty": qty},
            fill_meta_json={"evidence_kind": "KIS_ORDER_CUMULATIVE_EXECUTION",
                            "fill_source": "daily_ccld_order_cumulative"},
        )
    schema = schema_for_engine(engine)
    with engine.connect() as conn:
        rows = conn.execute(sa.select(schema.fills).where(
            schema.fills.c.order_id == order_id
        )).mappings().all()
    assert len(rows) == 1
    assert rows[0]["qty"] == max(q for q, _ in sequence)


@pytest.mark.parametrize("code,entry_qty,entry_price,sell_qty,sell_notional,order_id", [
    ("251970", 17, 68400.0, 10, 685000.0, "oct21-251970-close-10of17"),
    ("253590", 79, 16130.0, 52, 836180.0, "oct22-253590-close-52of79"),
    ("144960", 81, 14045.0, 81, 1091430.0, "oct22-144960-full-81"),
])
def test_priced_partial_sell_converges_economic_position_once(
    code, entry_qty, entry_price, sell_qty, sell_notional, order_id,
):
    """Exercise the *same* PnL/position writer used by reconciliation."""
    from trader.db.repos import PositionsRepo
    from tests.kr.test_kr_20261006_execution_convergence import _db, _open_position

    engine = _db()
    positions = PositionsRepo(engine)
    position = _open_position(engine, code=code, qty=entry_qty, avg=entry_price)
    cycle = str(position["position_cycle_id"])
    epoch = str(position["portfolio_epoch_id"])
    fill_time = datetime(2026, 10, 22, 15, 18, tzinfo=ZoneInfo("Asia/Seoul"))
    expected_pnl = sell_notional - entry_price * sell_qty
    for attempt in range(100):
        result = positions.reconcile_sell_execution(
            env="practice", strategy="pb1_pullback_close", sid=1, mode=1,
            code=code, market="J", confirmed_cumulative_qty=sell_qty,
            fill_price=sell_notional / sell_qty, filled_at=fill_time,
            position_cycle_id=cycle, portfolio_epoch_id=epoch,
            order_id=order_id, pre_order_holding_qty=entry_qty,
        )
        assert result["qty_applied"] == (sell_qty if attempt == 0 else 0)
        assert result["pnl_qty_applied"] == (sell_qty if attempt == 0 else 0)
        assert result["remaining_qty"] == entry_qty - sell_qty
    from trader.db.schema import schema_for_engine
    import sqlalchemy as sa
    schema = schema_for_engine(engine)
    with engine.connect() as conn:
        final = conn.execute(sa.select(schema.positions).where(
            schema.positions.c.position_cycle_id == cycle
        )).mappings().one()
    assert final["qty"] == entry_qty - sell_qty
    assert float(final["realized_pnl"]) == pytest.approx(expected_pnl, abs=0.001)
    assert final["status"] == ("CLOSED" if entry_qty == sell_qty else "OPEN")
    assert final["position_meta"]["realized_pnl_status"] == "CONFIRMED"


def test_partial_sell_cumulative_increase_and_stale_snapshot_never_double_applies():
    from trader.db.repos import PositionsRepo
    from tests.kr.test_kr_20261006_execution_convergence import _db, _open_position

    engine = _db()
    repo = PositionsRepo(engine)
    p = _open_position(engine, code="253590", qty=79, avg=16130.0)
    fields = dict(
        env="practice", strategy="pb1_pullback_close", sid=1, mode=1,
        code="253590", market="J", position_cycle_id=str(p["position_cycle_id"]),
        portfolio_epoch_id=str(p["portfolio_epoch_id"]), order_id="test-stable-odno-253590",
        filled_at=datetime(2026, 10, 22, 15, 18, tzinfo=ZoneInfo("Asia/Seoul")),
        pre_order_holding_qty=79,
    )
    repo.reconcile_sell_execution(
        **fields, confirmed_cumulative_qty=10, fill_price=16100.0,
    )
    repo.reconcile_sell_execution(
        **fields, confirmed_cumulative_qty=52, fill_price=836180.0 / 52,
    )
    assert repo.get_position(
        env="practice", strategy="pb1_pullback_close", sid=1, mode=1,
        code="253590"
    )["qty"] == 27
    with pytest.raises(RuntimeError, match="KR_SELL_RECONCILE_CUMULATIVE_REGRESSION"):
        repo.reconcile_sell_execution(
            **fields, confirmed_cumulative_qty=10, fill_price=16100.0,
        )


def test_cumulative_same_quantity_with_changed_notional_fails_closed():
    import uuid
    import sqlalchemy as sa
    from trader.db.repos import FillsRepo
    from trader.db.schema import schema_for_engine
    from tests.kr.execution_claim_fixtures import create_schema_with_active_test_epoch

    db = sa.create_engine("sqlite:///:memory:")
    create_schema_with_active_test_epoch(db)
    repo = FillsRepo(db)
    args = dict(
        env="practice", run_id=None, order_id=str(uuid.uuid4()),
        kis_odno="0000013196", trade_id="KR_KIS_CUM:fixture:20261021:0000013196:251970:BUY",
        code="251970", market="KOSPI", side="BUY",
        qty=17, price=68400.0, fee=0.0, tax=0.0,
        filled_at=datetime(2026, 10, 21, 9, 39, tzinfo=ZoneInfo("Asia/Seoul")),
        raw_json={"tot_ccld_qty": 17, "tot_ccld_amt": 1162800},
        fill_meta_json={"evidence_kind": "KIS_ORDER_CUMULATIVE_EXECUTION"},
    )
    repo.upsert_fill(**args)
    with pytest.raises(RuntimeError, match="KR_CUMULATIVE_FILL_SAME_QTY_NOTIONAL_CONFLICT"):
        repo.upsert_fill(**{**args, "price": 69000.0})
    with db.connect() as conn:
        stored = conn.execute(sa.select(schema_for_engine(db).fills).where(
            schema_for_engine(db).fills.c.kis_odno == "0000013196"
        )).mappings().all()
    assert len(stored) == 1
    assert stored[0]["qty"] == 17
    assert stored[0]["price"] == 68400.0


def test_execution_timestamp_preferred_over_order_timestamp_with_provenance():
    broker_row = {
        "ord_dt": "20261022", "ord_tmd": "151810",
        "ccld_dt": "20261022", "ccld_tmd": "151815",
    }
    execution, evidence_source = _parse_execution_time_with_source(broker_row)
    assert execution.isoformat() == "2026-10-22T15:18:15+09:00"
    assert evidence_source == "KIS_EXECUTION_TIMESTAMP"
    order_proxy, proxy_source = _parse_execution_time_with_source(
        {"ord_dt": "20261022", "ord_tmd": "151810"}
    )
    assert order_proxy.isoformat() == "2026-10-22T15:18:10+09:00"
    assert proxy_source == "ORDER_TIME_PROXY"


def test_two_independent_cumulative_writers_and_restart_keep_one_fill(tmp_path):
    """SQLite WAL file is a separate-connection concurrency stress fixture.

    Real Postgres concurrent isolation and broker account authorization are
    separate release gates; this test specifically guards fill identity.
    """
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    import uuid
    import sqlalchemy as sa
    from trader.db.repos import FillsRepo
    from trader.db.schema import schema_for_engine
    from tests.kr.execution_claim_fixtures import create_schema_with_active_test_epoch

    db_file = tmp_path / "kr249_fills.sqlite3"
    engine = sa.create_engine(
        "sqlite:///" + str(db_file),
        connect_args={"check_same_thread": False, "timeout": 20},
        poolclass=sa.pool.NullPool,
    )
    schema = create_schema_with_active_test_epoch(engine)
    order_id = str(uuid.uuid4())
    barrier = Barrier(2)

    def write(qty, price):
        barrier.wait(timeout=5)
        return FillsRepo(engine).upsert_fill(
            env="practice", run_id=None, order_id=order_id,
            kis_odno="0000040226",
            trade_id="KR_KIS_CUM:practice:20261021:0000040226:251970:SELL",
            code="251970", market="KOSPI", side="SELL", qty=qty, price=price,
            fee=0.0, tax=0.0,
            filled_at=datetime(2026, 10, 21, 15, 18, tzinfo=ZoneInfo("Asia/Seoul")),
            raw_json={"broker_snapshot_qty": qty},
            fill_meta_json={"evidence_kind": "KIS_ORDER_CUMULATIVE_EXECUTION"},
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(write, 10, 68500.0), pool.submit(write, 17, 68500.0)]
        for future in futures:
            assert future.result(timeout=25)
    engine.dispose()
    # Reopening a new engine simulates cold process restart and replay.
    restarted = sa.create_engine("sqlite:///" + str(db_file))
    FillsRepo(restarted).upsert_fill(
        env="practice", run_id=None, order_id=order_id,
        kis_odno="0000040226",
        trade_id="KR_KIS_CUM:practice:20261021:0000040226:251970:SELL",
        code="251970", market="KOSPI", side="SELL", qty=17, price=68500.0,
        fee=0, tax=0,
        filled_at=datetime(2026, 10, 21, 15, 18, tzinfo=ZoneInfo("Asia/Seoul")),
        raw_json={"broker_snapshot_qty": 17, "post_restart": True},
        fill_meta_json={"evidence_kind": "KIS_ORDER_CUMULATIVE_EXECUTION"},
    )
    with restarted.connect() as conn:
        records = conn.execute(sa.select(schema_for_engine(restarted).fills)).mappings().all()
    assert len(records) == 1
    assert records[0]["qty"] == 17
    restarted.dispose()
