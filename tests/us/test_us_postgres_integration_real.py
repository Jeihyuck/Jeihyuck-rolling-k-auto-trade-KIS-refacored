import os
import pytest
from datetime import datetime
from zoneinfo import ZoneInfo

# PR68 final accounting integration tests use the real PostgreSQL service in CI.
pytestmark = pytest.mark.skipif(not os.getenv("PBCORE_TEST_POSTGRES_URL"), reason="real PostgreSQL integration URL not configured")

DAYTIME_ET = datetime(2026, 8, 24, 10, 30, tzinfo=ZoneInfo("America/New_York"))

@pytest.fixture()
def pg_engine(monkeypatch):
    from sqlalchemy import create_engine, text
    import trader.us.db.repos as repos
    engine = create_engine(os.environ["PBCORE_TEST_POSTGRES_URL"], future=True)
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS us_fills CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS us_orders CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS us_order_intents CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS us_order_events CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS us_profit_capture_lifecycle CASCADE"))
        # Exercise the same production migrations rather than a test-only schema.
        conn.exec_driver_sql(open("migrations/0038_us_agent_tables.sql", encoding="utf-8").read())
        conn.exec_driver_sql(open("migrations/0043_us_fills_idempotency_and_order_reconcile_fix.sql", encoding="utf-8").read())
        conn.exec_driver_sql(open("migrations/0046_us_orders_committed_notional.sql", encoding="utf-8").read())
        conn.exec_driver_sql(open("migrations/0047_us_order_events_profit_lifecycle.sql", encoding="utf-8").read())
        # Production has migration 0052's unified trading-epoch columns.  This
        # focused US fixture does not create the KR/state tables required to
        # execute the entire 0052 script, so mirror the US ALTERs that current
        # repository code reads.
        for table in (
            "us_order_intents", "us_orders", "us_fills", "us_positions",
            "us_order_events", "us_profit_capture_lifecycle",
        ):
            conn.exec_driver_sql(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT"
            )
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: engine)
    yield engine
    engine.dispose()

def _seed_order(engine, *, key, order_no, qty_requested=10, qty_filled=3):
    from sqlalchemy import text
    import json
    with engine.begin() as conn:
        conn.execute(text("""INSERT INTO us_orders
            (trade_date,client_order_key,symbol,exchange,side,qty_requested,qty_filled,avg_price_usd,order_no,status,meta)
            VALUES ('2026-07-16',:key,'AMD','NASDAQ','SELL',:requested,:filled,100,:order_no,'PARTIALLY_FILLED','{}'::jsonb)"""),
            {"key":key,"order_no":order_no,"requested":qty_requested,"filled":qty_filled})
        conn.execute(text("""INSERT INTO us_fills
            (trade_date,symbol,exchange,side,qty,price_usd,order_no,client_order_key,filled_at,meta,fill_idempotency_key)
            VALUES ('2026-07-16','AMD','NASDAQ','SELL',:qty,100,:order_no,:key,now(),CAST(:meta AS jsonb),:idem)"""),
            {"qty":qty_filled,"order_no":order_no,"key":key,"idem":f"synthetic-{order_no}","meta":json.dumps({"is_synthetic":True,"fill_evidence_type":"BALANCE_DELTA_SYNTHETIC","cumulative_filled_qty":qty_filled,"accounting_active":True})})


def test_real_postgres_durable_ledger_and_lifecycle_isolation(pg_engine):
    from trader.us.db import repos

    event = {
        "event_id": "2d71827d-31c8-4f50-818a-b27881db6fa6",
        "trade_date": "2026-08-04", "event_type": "BROKER_SUBMIT_STARTED",
        "event_timestamp": "2026-08-04T13:30:00+00:00", "session": "am",
        "session_run_id": "R1", "tick_id": "T1", "client_order_key": "L1K",
        "submit_attempt_id": "A1", "symbol": "JPM", "side": "SELL",
        "requested_qty": 2, "raw_broker_order_no": None,
        "canonical_broker_order_no": None, "position_lifecycle_id": "L1",
        "profit_capture_stage": "tp1", "cumulative_filled_qty": 0,
        "payload": {"strategy_reason": "TAKE_PROFIT_TP1"}, "idempotency_key": "idem-A1",
    }
    assert repos.append_us_order_event(event)
    assert repos.append_us_order_event(event)
    assert len(repos.load_us_order_events("2026-08-04")) == 1

    repos.mark_us_profit_capture_stage("2026-08-04", "JPM", "tp1", status="FILLED",
                                       position_lifecycle_id="L1", order_key="L1K")
    repos.mark_us_profit_capture_stage("2026-08-04", "JPM", "tp1", status="PENDING",
                                       position_lifecycle_id="L2", order_key="L2K")
    l1 = repos.load_us_profit_capture_state("2026-08-04", ["JPM"], {"JPM": "L1"})["JPM"]
    l2 = repos.load_us_profit_capture_state("2026-08-04", ["JPM"], {"JPM": "L2"})["JPM"]
    assert l1["tp1_done"] and not l1["tp1_pending"]
    assert l2["tp1_pending"] and not l2["tp1_done"]


def test_real_postgres_reconcile_updates_actual_fill_with_typed_jsonb_binds(pg_engine):
    """Regression: psycopg3 must type the 2026-07-21 KIS JSONB arguments."""
    from sqlalchemy import text
    import json
    import trader.us.db.repos as repos

    with pg_engine.begin() as conn:
        conn.execute(text("""INSERT INTO us_orders
            (trade_date,client_order_key,symbol,exchange,side,qty_requested,qty_filled,avg_price_usd,order_no,status,meta)
            VALUES ('2026-07-21','e741f4c726b86941c78ed3f0','AMD','NASDAQ','SELL',1,0,NULL,'0000037900','ACK','{}'::jsonb)"""))
        conn.execute(text("""INSERT INTO us_fills
            (trade_date,symbol,exchange,side,qty,price_usd,order_no,client_order_key,filled_at,meta,fill_idempotency_key)
            VALUES ('2026-07-21','AMD','NASDAQ','SELL',1,0,'0000037900','e741f4c726b86941c78ed3f0',now(),CAST(:meta AS jsonb),'actual-amd-0000037900')"""),
            {"meta": json.dumps({"is_synthetic": False, "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL"})})

    result = repos.mark_order_filled_by_reconcile(
        order_no="0000037900", client_order_key="e741f4c726b86941c78ed3f0", symbol="AMD", side="SELL",
        filled_qty=1, requested_qty=1, cumulative_filled_qty=1, avg_price_usd=529.245,
        trade_date="2026-07-21", evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL", source="fills_by_order_no",
    )

    assert result["status"] == "OK"
    with pg_engine.begin() as conn:
        row = conn.execute(text("SELECT qty,price_usd,meta FROM us_fills WHERE order_no='0000037900'")).mappings().one()
    assert row["qty"] == 1
    assert float(row["price_usd"]) == 529.245
    assert row["meta"]["cumulative_filled_qty"] == 1
    assert row["meta"]["remaining_qty"] == 0
    assert row["meta"]["requested_qty"] == 1
    assert row["meta"]["observed_at"]


def test_tqqq_load_open_orders_side_filter_has_no_postgres_ambiguous_parameter(pg_engine):
    """TQQQ's side-filtered us_orders path must bind a concrete side type."""
    from sqlalchemy import text
    from trader.us.infinite.repository import InfiniteRepository

    with pg_engine.begin() as conn:
        conn.execute(text("""INSERT INTO us_orders
            (trade_date,client_order_key,symbol,exchange,side,qty_requested,qty_filled,order_no,status,meta)
            VALUES ('2026-09-04','TQQQ_INF_V3:cycle-1:2026-09-04:BUY',
                    'TQQQ','NASDAQ','BUY',1,0,'tqqq-open','OPEN','{}'::jsonb)"""))

    repo = InfiniteRepository(pg_engine)
    assert [order["order_no"] for order in repo.load_open_orders(
        symbol="TQQQ", cycle_id="cycle-1", side="BUY",
    )] == ["tqqq-open"]
    assert repo.load_open_orders(symbol="TQQQ", cycle_id="cycle-1", side="SELL") == []


def test_real_postgres_promotion_regression_and_rollback(pg_engine):
    from sqlalchemy import text
    import trader.us.db.repos as repos
    _seed_order(pg_engine,key="K1",order_no="O1")
    result = repos.mark_order_filled_by_reconcile(order_no="O1",client_order_key="K1",symbol="AMD",side="SELL",filled_qty=6,requested_qty=10,cumulative_filled_qty=6,avg_price_usd=101,trade_date="2026-07-16",evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",source="fills_by_order_no")
    assert result["status"] == "OK"
    with pg_engine.begin() as conn:
        assert conn.execute(text("SELECT qty_filled FROM us_orders WHERE order_no='O1'")).scalar_one() == 6
        rows=conn.execute(text("SELECT qty,meta FROM us_fills WHERE order_no='O1' ORDER BY id")).mappings().all()
        assert rows[0]["meta"]["accounting_active"] is False
        assert rows[1]["qty"] == 6
    regression = repos.mark_order_filled_by_reconcile(order_no="O1",client_order_key="K1",symbol="AMD",side="SELL",filled_qty=4,requested_qty=10,cumulative_filled_qty=4,avg_price_usd=99,trade_date="2026-07-16",evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",source="fills_by_order_no")
    assert regression["status"] == "EVIDENCE_QUANTITY_REGRESSION"
    with pg_engine.begin() as conn:
        assert conn.execute(text("SELECT qty_filled FROM us_orders WHERE order_no='O1'")).scalar_one() == 6
    _seed_order(pg_engine,key="K2",order_no="ROLL")
    with pg_engine.begin() as conn:
        conn.execute(text("""CREATE OR REPLACE FUNCTION fail_roll_fill() RETURNS trigger AS $$
        BEGIN IF NEW.order_no='ROLL' AND COALESCE(NEW.meta->>'fill_evidence_type','')='KIS_ORDER_CUMULATIVE_ACTUAL' THEN RAISE EXCEPTION 'forced insert failure'; END IF; RETURN NEW; END; $$ LANGUAGE plpgsql"""))
        conn.execute(text("CREATE TRIGGER fail_roll_fill_trigger BEFORE INSERT ON us_fills FOR EACH ROW EXECUTE FUNCTION fail_roll_fill()"))
    failed = repos.mark_order_filled_by_reconcile(order_no="ROLL",client_order_key="K2",symbol="AMD",side="SELL",filled_qty=6,requested_qty=10,cumulative_filled_qty=6,avg_price_usd=101,trade_date="2026-07-16",evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",source="fills_by_order_no")
    assert failed["status"] == "RECONCILE_UPDATE_FAILED"
    with pg_engine.begin() as conn:
        assert conn.execute(text("SELECT qty_filled FROM us_orders WHERE order_no='ROLL'")).scalar_one() == 3
        assert conn.execute(text("SELECT COALESCE((meta->>'accounting_active')::boolean,true) FROM us_fills WHERE order_no='ROLL'")).scalar_one() is True

def test_real_postgres_save_fills_actual_conflict_and_overflow_are_atomic(pg_engine):
    from sqlalchemy import text
    import trader.us.db.repos as repos

    _seed_order(pg_engine, key="K3", order_no="C1", qty_requested=10, qty_filled=7)
    conflict = repos.save_fills_with_result([{
        "symbol": "AMD", "exchange": "NASDAQ", "side": "SELL", "qty": 3,
        "price_usd": 99, "order_no": "C1", "client_order_key": "K3",
        "cumulative_filled_qty": 3, "requested_qty": 10,
        "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
        "meta": {"fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL", "cumulative_filled_qty": 3, "requested_qty": 10, "is_synthetic": False},
    }], trade_date="2026-07-16")
    assert conflict["status"] == "EVIDENCE_QUANTITY_CONFLICT"
    with pg_engine.begin() as conn:
        assert conn.execute(text("SELECT qty_filled FROM us_orders WHERE order_no='C1'")).scalar_one() == 7
        assert conn.execute(text("SELECT count(*) FROM us_fills WHERE order_no='C1' AND NOT COALESCE((meta->>'is_synthetic')::boolean,false)")).scalar_one() == 0
        assert conn.execute(text("SELECT COALESCE((meta->>'accounting_active')::boolean,true) FROM us_fills WHERE order_no='C1'")).scalar_one() is True

    _seed_order(pg_engine, key="K4", order_no="OFL", qty_requested=10, qty_filled=0)
    overflow = repos.save_fills_with_result([{
        "symbol": "AMD", "exchange": "NASDAQ", "side": "SELL", "qty": 11,
        "price_usd": 101, "order_no": "OFL", "client_order_key": "K4",
        "cumulative_filled_qty": 11, "requested_qty": 10,
        "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
        "meta": {"fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL", "cumulative_filled_qty": 11, "requested_qty": 10, "is_synthetic": False},
    }], trade_date="2026-07-16")
    assert overflow["status"] == "EVIDENCE_QUANTITY_OVERFLOW"
    with pg_engine.begin() as conn:
        assert conn.execute(text("SELECT qty_filled FROM us_orders WHERE order_no='OFL'")).scalar_one() == 0
        assert conn.execute(text("SELECT count(*) FROM us_fills WHERE order_no='OFL' AND NOT COALESCE((meta->>'is_synthetic')::boolean,false)")).scalar_one() == 0


def test_real_postgres_strict_committed_buy_notional_distinguishes_zero_rows_and_query_failure(pg_engine, monkeypatch):
    """Risk loader must query PostgreSQL directly, dedupe keys, and fail closed."""
    from sqlalchemy import text
    import trader.us.db.repos as repos

    assert repos.save_order_ack({
        "client_order_key": "ack-key", "symbol": "AAPL", "exchange": "NASDAQ",
        "side": "BUY", "qty_requested": 6, "qty_filled": 0, "order_no": "ack-order",
        "status": "ACK", "committed_notional_usd": 600, "env": "practice",
    }, trade_date="2026-07-31")
    # The real reconcile path transitions ACK to FILLED while retaining the
    # original requested commitment used by the daily limit.
    filled = repos.mark_order_filled_by_reconcile(
        order_no="ack-order", client_order_key="ack-key", symbol="AAPL", side="BUY",
        filled_qty=6, requested_qty=6, cumulative_filled_qty=6, avg_price_usd=101,
        trade_date="2026-07-31", evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        source="fills_by_order_no",
    )
    assert filled["status"] == "OK"
    with pg_engine.begin() as conn:
        assert conn.execute(text("SELECT status FROM us_orders WHERE client_order_key='ack-key'")).scalar_one() == "FILLED"
    assert repos.save_dry_run_order({
        "client_order_key": "dry-key", "symbol": "MSFT", "exchange": "NASDAQ",
        "side": "BUY", "qty": 1, "limit_price_usd": 100, "notional_usd": 100,
        "env": "practice", "meta": {},
    }, trade_date="2026-07-31")
    assert repos.save_order_ack({
        "client_order_key": "other-env", "symbol": "NVDA", "exchange": "NASDAQ",
        "side": "BUY", "qty_requested": 1, "qty_filled": 0, "order_no": "prod-order",
        "status": "PENDING", "committed_notional_usd": 999, "env": "prod",
    }, trade_date="2026-07-31")
    for index, status in enumerate(("PENDING", "SUBMITTED", "RECONCILE_PENDING", "ACK_DB_FAILED")):
        assert repos.save_order_ack({
            "client_order_key": f"status-{index}", "symbol": "GOOGL", "exchange": "NASDAQ",
            "side": "BUY", "qty_requested": 1, "qty_filled": 0,
            "order_no": f"status-order-{index}", "status": status,
            "committed_notional_usd": 10, "env": "practice",
        }, trade_date="2026-07-31")
    assert repos.save_order_ack({
        "client_order_key": "sell-key", "symbol": "AAPL", "exchange": "NASDAQ",
        "side": "SELL", "qty_requested": 1, "qty_filled": 0, "order_no": "sell-order",
        "status": "ACK", "committed_notional_usd": 999, "env": "practice",
    }, trade_date="2026-07-31")

    # Legacy row: canonical columns null, recovered through the persisted intent.
    with pg_engine.begin() as conn:
        conn.execute(text("""INSERT INTO us_order_intents
            (trade_date,client_order_key,symbol,exchange,side,qty,limit_price_usd,notional_usd,status,meta)
            VALUES ('2026-07-31','legacy-key','AMZN','NASDAQ','BUY',3,100,300,'SENT',
                    '{"env":"practice"}'::jsonb)"""))
        conn.execute(text("""INSERT INTO us_orders
            (trade_date,client_order_key,symbol,exchange,side,qty_requested,qty_filled,
             order_no,status,dry_run,meta,committed_notional_usd,env)
            VALUES ('2026-07-31','legacy-key','AMZN','NASDAQ','BUY',3,0,
                    'legacy-order','SUBMITTED',false,'{}'::jsonb,NULL,'unknown')"""))

    committed = repos.load_today_committed_buy_notional_result("2026-07-31", env="practice")
    assert committed.available is True
    assert committed.notional_usd == 1040.0
    assert committed.row_count == 9

    # A later tick starts from the actually persisted first-tick total.  A
    # candidate larger than the remaining daily allowance is skipped while a
    # smaller candidate can still backfill.
    from trader.us.execution.order_router import select_preflight_buy_candidates
    monkeypatch.setenv("US_MAX_DAILY_NOTIONAL_USD", "1500")
    monkeypatch.setenv("US_MAX_ORDER_USD", "1000")
    state = {
            "available_cash_usd": 10000, "daily_notional_usd": committed.notional_usd,
            "position_count": 0, "portfolio_usd": 100000, "order_keys": set(),
            "now": DAYTIME_ET,
    }
    candidates = [
            {"symbol": "COST", "exchange": "NASDAQ", "side": "BUY", "qty": 1,
             "limit_price": 500, "notional_usd": 500, "client_order_key": "tick2-large",
             "position_state": "NOT_HELD", "position_action": "NEW_POSITION_BUY",
             "theme_cluster": "CONSUMER_STAPLES", "meta": {"position_state": "NOT_HELD",
             "position_action": "NEW_POSITION_BUY", "theme_cluster": "CONSUMER_STAPLES"}},
            {"symbol": "KO", "exchange": "NYSE", "side": "BUY", "qty": 1,
             "limit_price": 400, "notional_usd": 400, "client_order_key": "tick2-small",
             "position_state": "NOT_HELD", "position_action": "NEW_POSITION_BUY",
             "theme_cluster": "CONSUMER_STAPLES", "meta": {"position_state": "NOT_HELD",
             "position_action": "NEW_POSITION_BUY", "theme_cluster": "CONSUMER_STAPLES"}},
    ]
    accepted, rejected, _ = select_preflight_buy_candidates(
        candidates, target_accept_count=1, projected_state=state,
        allowed_symbols={"COST", "KO"}, current_positions=[],
    )
    assert [item["symbol"] for item in accepted] == ["KO"]
    assert rejected[0]["reason"] == "daily_notional_exceeded"

    empty = repos.load_today_committed_buy_notional_result("2026-08-01", env="practice")
    assert empty.available is True
    assert empty.notional_usd == 0.0
    assert empty.row_count == 0

    with pg_engine.begin() as conn:
        conn.execute(text("""INSERT INTO us_orders
            (trade_date,client_order_key,symbol,exchange,side,qty_requested,status,dry_run,meta,
             committed_notional_usd,env)
            VALUES ('2026-08-02','unsafe-legacy','META','NASDAQ','BUY',1,'ACK',false,
                    '{}'::jsonb,NULL,'unknown')"""))
    unsafe = repos.load_today_committed_buy_notional_result("2026-08-02", env="practice")
    assert unsafe.available is False
    assert "environment_unavailable" in (unsafe.error or "")

    with pg_engine.begin() as conn:
        conn.execute(text("DROP TABLE us_orders"))
    unavailable = repos.load_today_committed_buy_notional_result("2026-07-31", env="practice")
    assert unavailable.available is False
    assert unavailable.notional_usd == 0.0
    assert unavailable.error

def test_real_postgres_cancelled_partial_fill_normalize_persist_close_chain(pg_engine):
    from sqlalchemy import text
    from trader.us.data_provider import normalize_us_order_status_row
    import trader.us.db.repos as repos
    from trader.us.runner.daily_report_runner import _explicit_order_filled_qty

    with pg_engine.begin() as conn:
        conn.execute(text("""INSERT INTO us_orders
            (trade_date,client_order_key,symbol,exchange,side,qty_requested,qty_filled,
             avg_price_usd,order_no,status,meta)
            VALUES ('2026-09-24','TQQQ-PARTIAL-CANCEL','TQQQ','NASDAQ','BUY',
                    2,0,NULL,'PARTIAL-CANCEL-1','ACK','{}'::jsonb)"""))

    observation = normalize_us_order_status_row({
        "odno": "PARTIAL-CANCEL-1",
        "pdno": "TQQQ",
        "sll_buy_dvsn_cd": "02",
        "ord_qty": "2",
        "ft_ccld_qty": "1",
        "ft_ccld_unpr3": "77.25",
        "nccs_qty": "0",
        "status": "CANCELLED",
    })
    assert observation["filled_qty_present"] is True
    assert observation["filled_qty"] == 1

    applied = repos.apply_broker_order_observation(
        trade_date="2026-09-24",
        client_order_key="TQQQ-PARTIAL-CANCEL",
        raw_order_no="PARTIAL-CANCEL-1",
        canonical_order_no="PARTIAL-CANCEL-1",
        symbol="TQQQ",
        side="BUY",
        requested_qty=2,
        filled_qty=observation["filled_qty"],
        remaining_qty=observation["remaining_qty"],
        broker_status=observation["status"],
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        raw_row=observation,
    )
    assert applied["status"] == "OK"

    with pg_engine.begin() as conn:
        row = dict(conn.execute(text("""
            SELECT status,qty_filled,meta
            FROM us_orders
            WHERE client_order_key='TQQQ-PARTIAL-CANCEL'
        """)).mappings().one())
        fill_row = conn.execute(text("""
            SELECT COALESCE(SUM(qty),0) AS qty,
                   COALESCE(MAX(price_usd),0) AS price
            FROM us_fills
            WHERE client_order_key='TQQQ-PARTIAL-CANCEL'
              AND COALESCE((meta->>'accounting_active')::boolean,true)
        """)).mappings().one()

    assert row["status"] == "CANCELLED"
    assert row["qty_filled"] == 1
    assert fill_row["qty"] == 1
    assert float(fill_row["price"]) == 77.25
    assert _explicit_order_filled_qty(row) == 1


def test_real_postgres_tqqq_cancel_reprices_cumulative_actual_fill(pg_engine):
    from sqlalchemy import text
    import trader.us.db.repos as repos
    from trader.us.data_provider import normalize_us_order_status_row
    from trader.us.infinite.repository import InfiniteRepository

    with pg_engine.begin() as conn:
        conn.execute(text("""INSERT INTO us_orders
            (trade_date,client_order_key,symbol,exchange,side,qty_requested,qty_filled,
             avg_price_usd,order_no,status,meta)
            VALUES ('2026-09-24','TQQQ-CUMULATIVE-CANCEL','TQQQ','NASDAQ','BUY',
                    3,0,NULL,'CUM-CANCEL-1','ACK','{}'::jsonb)"""))

    first = repos.mark_order_filled_by_reconcile(
        order_no="CUM-CANCEL-1",
        client_order_key="TQQQ-CUMULATIVE-CANCEL",
        symbol="TQQQ",
        side="BUY",
        filled_qty=1,
        requested_qty=3,
        cumulative_filled_qty=1,
        avg_price_usd=70.0,
        source="fills_by_order_no",
        trade_date="2026-09-24",
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
    )
    assert first["status"] == "OK"

    observation = normalize_us_order_status_row({
        "odno": "CUM-CANCEL-1",
        "pdno": "TQQQ",
        "sll_buy_dvsn_cd": "02",
        "ord_qty": "3",
        "ft_ccld_qty": "2",
        "ft_ccld_unpr3": "75.0",
        "nccs_qty": "0",
        "status": "CANCELLED",
    })
    repo = InfiniteRepository(pg_engine)
    applied = repo.apply_ttl_terminal_observation(
        {
            "trade_date": "2026-09-24",
            "client_order_key": "TQQQ-CUMULATIVE-CANCEL",
            "order_no": "CUM-CANCEL-1",
            "symbol": "TQQQ",
            "side": "BUY",
            "qty_requested": 3,
        },
        observation,
    )
    assert applied["status"] == "OK"

    with pg_engine.begin() as conn:
        order = conn.execute(text("""
            SELECT status,qty_filled,avg_price_usd
            FROM us_orders
            WHERE client_order_key='TQQQ-CUMULATIVE-CANCEL'
        """)).mappings().one()
        fills = conn.execute(text("""
            SELECT qty,price_usd,meta
            FROM us_fills
            WHERE client_order_key='TQQQ-CUMULATIVE-CANCEL'
              AND COALESCE((meta->>'accounting_active')::boolean,true)
            ORDER BY id
        """)).mappings().all()

    assert order["status"] == "CANCELLED"
    assert order["qty_filled"] == 2
    assert float(order["avg_price_usd"]) == 75.0
    assert sum(int(row["qty"]) for row in fills) == 2
    assert sum(float(row["qty"]) * float(row["price_usd"]) for row in fills) == 150.0
    assert any(
        str((row["meta"] or {}).get("fill_evidence_type") or "") == "KIS_ORDER_CUMULATIVE_ACTUAL"
        for row in fills
    )


def test_real_postgres_partial_fill_cancel_without_price_stays_unresolved(pg_engine):
    from sqlalchemy import text
    from trader.us.data_provider import normalize_us_order_status_row
    import trader.us.db.repos as repos

    with pg_engine.begin() as conn:
        conn.execute(text("""INSERT INTO us_orders
            (trade_date,client_order_key,symbol,exchange,side,qty_requested,qty_filled,
             avg_price_usd,order_no,status,meta)
            VALUES ('2026-09-24','TQQQ-PARTIAL-NO-PRICE','TQQQ','NASDAQ','BUY',
                    2,0,NULL,'PARTIAL-NO-PRICE-1','ACK','{}'::jsonb)"""))

    observation = normalize_us_order_status_row({
        "odno": "PARTIAL-NO-PRICE-1",
        "pdno": "TQQQ",
        "sll_buy_dvsn_cd": "02",
        "ord_qty": "2",
        "ft_ccld_qty": "1",
        "nccs_qty": "0",
        "status": "CANCELLED",
    })
    assert observation["filled_qty"] == 1
    assert observation["avg_price"] == 0

    applied = repos.apply_broker_order_observation(
        trade_date="2026-09-24",
        client_order_key="TQQQ-PARTIAL-NO-PRICE",
        raw_order_no="PARTIAL-NO-PRICE-1",
        canonical_order_no="PARTIAL-NO-PRICE-1",
        symbol="TQQQ",
        side="BUY",
        requested_qty=2,
        filled_qty=observation["filled_qty"],
        remaining_qty=observation["remaining_qty"],
        broker_status=observation["status"],
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        raw_row=observation,
    )
    assert applied["status"] == "PENDING"
    assert applied["reason"] == "cancel_partial_fill_price_missing"

    with pg_engine.begin() as conn:
        row = conn.execute(text("""
            SELECT status,qty_filled,avg_price_usd
            FROM us_orders
            WHERE client_order_key='TQQQ-PARTIAL-NO-PRICE'
        """)).mappings().one()
        fill_count = conn.execute(text("""
            SELECT COUNT(*)
            FROM us_fills
            WHERE client_order_key='TQQQ-PARTIAL-NO-PRICE'
        """)).scalar_one()

    assert row["status"] == "ACK"
    assert row["qty_filled"] == 0
    assert row["avg_price_usd"] is None
    assert fill_count == 0


@pytest.mark.parametrize("bad_price", ["NaN", "Infinity"])
def test_real_postgres_nonfinite_partial_cancel_price_stays_unresolved(pg_engine, bad_price):
    from sqlalchemy import text
    from trader.us.data_provider import normalize_us_order_status_row
    import trader.us.db.repos as repos

    suffix = "NAN" if bad_price == "NaN" else "INF"
    key = f"TQQQ-NONFINITE-{suffix}"
    order_no = f"NONFINITE-{suffix}-1"
    with pg_engine.begin() as conn:
        conn.execute(text("""INSERT INTO us_orders
            (trade_date,client_order_key,symbol,exchange,side,qty_requested,qty_filled,
             avg_price_usd,order_no,status,meta)
            VALUES ('2026-09-24',:key,'TQQQ','NASDAQ','BUY',
                    2,0,NULL,:order_no,'ACK','{}'::jsonb)"""),
            {"key": key, "order_no": order_no})

    observation = normalize_us_order_status_row({
        "odno": order_no,
        "pdno": "TQQQ",
        "sll_buy_dvsn_cd": "02",
        "ord_qty": "2",
        "ft_ccld_qty": "1",
        "ft_ccld_unpr3": bad_price,
        "nccs_qty": "0",
        "status": "CANCELLED",
    })
    applied = repos.apply_broker_order_observation(
        trade_date="2026-09-24",
        client_order_key=key,
        raw_order_no=order_no,
        canonical_order_no=order_no,
        symbol="TQQQ",
        side="BUY",
        requested_qty=2,
        filled_qty=1,
        remaining_qty=0,
        broker_status="CANCELLED",
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        raw_row=observation,
    )
    assert applied["status"] == "PENDING"
    assert applied["reason"] == "cancel_partial_fill_price_missing"

    with pg_engine.begin() as conn:
        order = conn.execute(text("""
            SELECT status,qty_filled,avg_price_usd
            FROM us_orders WHERE client_order_key=:key
        """), {"key": key}).mappings().one()
        fill_count = conn.execute(text("""
            SELECT COUNT(*) FROM us_fills WHERE client_order_key=:key
        """), {"key": key}).scalar_one()
    assert order["status"] == "ACK"
    assert order["qty_filled"] == 0
    assert order["avg_price_usd"] is None
    assert fill_count == 0


def test_real_postgres_missing_fill_cancel_is_not_terminalized(pg_engine):
    from sqlalchemy import text
    from trader.us.data_provider import normalize_us_order_status_row
    from trader.us.infinite.repository import InfiniteRepository

    with pg_engine.begin() as conn:
        conn.execute(text("""INSERT INTO us_orders
            (trade_date,client_order_key,symbol,exchange,side,qty_requested,qty_filled,
             avg_price_usd,order_no,status,meta)
            VALUES ('2026-09-24','TQQQ-MISSING-FILL','TQQQ','NASDAQ','BUY',
                    2,0,NULL,'MISSING-FILL-1','ACK','{}'::jsonb)"""))

    observation = normalize_us_order_status_row({
        "odno": "MISSING-FILL-1",
        "pdno": "TQQQ",
        "sll_buy_dvsn_cd": "02",
        "ord_qty": "2",
        "nccs_qty": "0",
        "status": "CANCELLED",
    })
    assert observation["filled_qty"] is None
    assert observation["filled_qty_present"] is False

    repo = InfiniteRepository(pg_engine)
    result = repo.apply_ttl_terminal_observation(
        {
            "trade_date": "2026-09-24",
            "client_order_key": "TQQQ-MISSING-FILL",
            "order_no": "MISSING-FILL-1",
            "symbol": "TQQQ",
            "side": "BUY",
            "qty_requested": 2,
        },
        observation,
    )
    assert result["status"] == "PENDING"

    with pg_engine.begin() as conn:
        row = conn.execute(text("""
            SELECT status,qty_filled
            FROM us_orders
            WHERE client_order_key='TQQQ-MISSING-FILL'
        """)).mappings().one()
    assert row["status"] == "ACK"
    assert row["qty_filled"] == 0

@pytest.mark.parametrize("bad_price", ["NaN", "Infinity", "-Infinity"])
def test_real_postgres_partial_cancel_rejects_nonfinite_fill_price(pg_engine, bad_price):
    from sqlalchemy import text
    from trader.us.data_provider import normalize_us_order_status_row
    import trader.us.db.repos as repos

    key = "TQQQ-NONFINITE-" + bad_price.replace("-", "NEG").replace("Infinity", "INF").replace("NaN", "NAN")
    order_no = key + "-ORDER"

    with pg_engine.begin() as conn:
        conn.execute(text("""INSERT INTO us_orders
            (trade_date,client_order_key,symbol,exchange,side,qty_requested,qty_filled,
             avg_price_usd,order_no,status,meta)
            VALUES ('2026-09-24',:key,'TQQQ','NASDAQ','BUY',
                    2,0,NULL,:order_no,'ACK','{}'::jsonb)"""),
            {"key": key, "order_no": order_no})

    observation = normalize_us_order_status_row({
        "odno": order_no,
        "pdno": "TQQQ",
        "sll_buy_dvsn_cd": "02",
        "ord_qty": "2",
        "ft_ccld_qty": "1",
        "ft_ccld_unpr3": bad_price,
        "nccs_qty": "0",
        "status": "CANCELLED",
    })
    assert observation["filled_qty"] == 1

    applied = repos.apply_broker_order_observation(
        trade_date="2026-09-24",
        client_order_key=key,
        raw_order_no=order_no,
        canonical_order_no=order_no,
        symbol="TQQQ",
        side="BUY",
        requested_qty=2,
        filled_qty=observation["filled_qty"],
        remaining_qty=observation["remaining_qty"],
        broker_status=observation["status"],
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        raw_row=observation,
    )
    assert applied["status"] == "PENDING"
    assert applied["reason"] == "cancel_partial_fill_price_missing"

    with pg_engine.begin() as conn:
        row = conn.execute(text("""
            SELECT status,qty_filled,avg_price_usd
            FROM us_orders
            WHERE client_order_key=:key
        """), {"key": key}).mappings().one()
        fill_count = conn.execute(text("""
            SELECT COUNT(*)
            FROM us_fills
            WHERE client_order_key=:key
        """), {"key": key}).scalar_one()

    assert row["status"] == "ACK"
    assert row["qty_filled"] == 0
    assert row["avg_price_usd"] is None
    assert fill_count == 0

@pytest.mark.parametrize("bad_price", ["NaN", "Infinity", "-Infinity"])
def test_real_postgres_full_fill_rejects_nonfinite_fill_price(pg_engine, bad_price):
    from sqlalchemy import text
    import trader.us.db.repos as repos

    key = "FULL-NONFINITE-" + bad_price.replace("-", "NEG").replace("Infinity", "INF").replace("NaN", "NAN")
    order_no = key + "-ORDER"
    with pg_engine.begin() as conn:
        conn.execute(text("""INSERT INTO us_orders
            (trade_date,client_order_key,symbol,exchange,side,qty_requested,qty_filled,
             avg_price_usd,order_no,status,meta)
            VALUES ('2026-09-24',:key,'AAPL','NASDAQ','BUY',
                    1,0,NULL,:order_no,'ACK','{}'::jsonb)"""),
            {"key": key, "order_no": order_no})

    result = repos.apply_broker_order_observation(
        trade_date="2026-09-24",
        client_order_key=key,
        raw_order_no=order_no,
        canonical_order_no=order_no,
        symbol="AAPL",
        side="BUY",
        requested_qty=1,
        filled_qty=1,
        remaining_qty=0,
        broker_status="FILLED",
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        raw_row={"avg_price": bad_price},
    )
    assert result["status"] == "PENDING"
    assert result["reason"] == "broker_fill_price_missing"

    with pg_engine.begin() as conn:
        row = conn.execute(text("""
            SELECT status,qty_filled,avg_price_usd
            FROM us_orders WHERE client_order_key=:key
        """), {"key": key}).mappings().one()
        fill_count = conn.execute(text("""
            SELECT COUNT(*) FROM us_fills WHERE client_order_key=:key
        """), {"key": key}).scalar_one()

    assert row["status"] == "ACK"
    assert row["qty_filled"] == 0
    assert row["avg_price_usd"] is None
    assert fill_count == 0

