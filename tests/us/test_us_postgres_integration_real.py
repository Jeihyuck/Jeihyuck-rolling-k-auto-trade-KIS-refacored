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
        conn.execute(text("DROP TABLE IF EXISTS us_execution_attempts CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS us_execution_claims CASCADE"))
        # Exercise the same production migrations rather than a test-only schema.
        conn.exec_driver_sql(open("migrations/0038_us_agent_tables.sql", encoding="utf-8").read())
        conn.exec_driver_sql(open("migrations/0043_us_fills_idempotency_and_order_reconcile_fix.sql", encoding="utf-8").read())
        conn.exec_driver_sql(open("migrations/0046_us_orders_committed_notional.sql", encoding="utf-8").read())
        conn.exec_driver_sql(open("migrations/0047_us_order_events_profit_lifecycle.sql", encoding="utf-8").read())
        conn.exec_driver_sql(open("migrations/0053_execution_state_claims.sql", encoding="utf-8").read())
        conn.exec_driver_sql(open("migrations/0054_semantic_action_instances.sql", encoding="utf-8").read())
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
                                       position_lifecycle_id="L1", order_key="L1K",
                                       qty=2, filled_qty=2, evidence_type="KIS_EXECUTION_ACTUAL")
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


def test_real_postgres_buy_fill_history_query_binds_trade_date(pg_engine):
    from sqlalchemy import text
    from trader.us.db import repos

    with pg_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO us_fills
                (trade_date, symbol, exchange, side, qty, price_usd, filled_at, meta)
            VALUES
                ('2026-08-24', 'HIST', 'NASDAQ', 'BUY', 2, 100,
                 '2026-08-24T14:00:00+00:00', '{}'::jsonb)
        """))

    fills = repos.load_us_buy_fill_history_candidates(
        "HIST", trade_date="2026-08-24", lookback_days=30,
    )

    assert len(fills) == 1
    assert fills[0]["symbol"] == "HIST"
    assert fills[0]["trade_date"].isoformat() == "2026-08-24"


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


def test_real_postgres_rebounds_unique_submit_journal_candidate(pg_engine, monkeypatch, tmp_path):
    from datetime import date
    from sqlalchemy import text
    import trader.us.db.repos as repos
    from trader.execution_claims import DurableExecutionClaimRepo
    from trader.execution_state import SemanticActionIdentity
    from trader.us.db.execution_claim_schema import (
        us_execution_attempts,
        us_execution_claims,
    )
    from trader.us.execution.order_journal import append_order_event

    monkeypatch.setenv("US_ORDER_JOURNAL_DIR", str(tmp_path / "journal"))
    trade_date = "2026-10-01"
    client_key = "recovery-persisted-submit"
    attempt_id = "recovery-persisted-attempt"
    lifecycle = "recovery-persisted-cycle"
    action = "DEFENSE_RISK_OFF_TRIM"
    identity = SemanticActionIdentity(
        env="practice",
        account_id="recovery-postgres-account",
        market="US",
        trading_epoch_id="recovery-postgres-epoch",
        strategy_owner="US_STANDARD",
        lifecycle_id=lifecycle,
        action=action,
        trade_date=date.fromisoformat(trade_date),
    )
    claim_repo = DurableExecutionClaimRepo(
        pg_engine, us_execution_claims, us_execution_attempts,
    )
    assert claim_repo.acquire(
        identity, attempt_id=attempt_id, requested_qty=5,
        client_order_key=client_key,
    ).acquired
    claim_repo.record_observation(
        identity, attempt_id=attempt_id, state="UNRESOLVED",
        cumulative_filled_qty=None, authoritative=False,
    )
    monkeypatch.setattr(repos, "_execution_claim_repo", lambda: claim_repo)

    meta = {
        "strategy_owner": "US_STANDARD",
        "position_lifecycle_id": lifecycle,
        "submit_attempt_id": attempt_id,
        "execution_action_key": identity.action_key,
        "semantic_action": action,
        "avg_cost": 42.5,
        "entry_price": 42.5,
        "submitted_at_utc": "2026-10-01T15:00:00+00:00",
    }
    intent = {
        "trade_date": trade_date,
        "client_order_key": client_key,
        "symbol": "XYZ",
        "exchange": "NASDAQ",
        "side": "SELL",
        "qty": 5,
        "strategy": "us_pb1",
        "strategy_owner": "US_STANDARD",
        "position_lifecycle_id": lifecycle,
        "submit_attempt_id": attempt_id,
        "meta": meta,
    }
    assert repos.save_order_intent(intent, trade_date=trade_date)
    assert repos.save_order_ack(
        {
            **intent,
            "qty_requested": 5,
            "qty_filled": 0,
            "status": "PENDING",
            "order_no": "",
            "meta": meta,
        },
        trade_date=trade_date,
    )
    append_order_event("BROKER_SUBMIT_STARTED", intent)

    fill = {
        "trade_date": trade_date,
        "symbol": "XYZ",
        "exchange": "NASDAQ",
        "side": "SELL",
        "qty": 3,
        "cumulative_filled_qty": 3,
        "requested_qty": 5,
        "broker_open_qty": 2,
        "price_usd": 44.0,
        "order_no": "broker-recovery-1",
        "filled_at": "2026-10-01T15:00:00+00:00",
        "order_timestamp": "2026-10-02T00:00:00",
        "order_timestamp_utc": "2026-10-01T15:00:00+00:00",
        "order_timestamp_source": "KIS_INQUIRE_CCNL_ORD_DT_ORD_TMD",
        "order_timestamp_source_timezone": "Asia/Seoul",
        "meta": {
            "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
            "cumulative_filled_qty": 3,
            "requested_qty": 5,
            "broker_open_qty": 2,
            "order_timestamp": "2026-10-02T00:00:00",
            "order_timestamp_utc": "2026-10-01T15:00:00+00:00",
            "order_timestamp_source": "KIS_INQUIRE_CCNL_ORD_DT_ORD_TMD",
            "order_timestamp_source_timezone": "Asia/Seoul",
            "avg_price_usd": 44.0,
        },
    }
    assert repos.save_fills([fill], trade_date=trade_date) == 1

    with pg_engine.begin() as conn:
        order = conn.execute(
            text("SELECT client_order_key,qty_filled,meta FROM us_orders WHERE order_no=:order_no"),
            {"order_no": "broker-recovery-1"},
        ).mappings().one()
        actual_fills = conn.execute(
            text("SELECT qty,meta FROM us_fills WHERE order_no=:order_no AND meta->>'is_synthetic'='false'"),
            {"order_no": "broker-recovery-1"},
        ).mappings().all()
    assert order["client_order_key"] == client_key
    assert order["qty_filled"] == 3
    assert order["meta"]["strategy_owner"] == "US_STANDARD"
    assert order["meta"]["position_lifecycle_id"] == lifecycle
    assert order["meta"]["avg_cost"] == 42.5
    assert order["meta"]["semantic_action"] == action
    assert len(actual_fills) == 1
    assert actual_fills[0]["qty"] == 3
    assert actual_fills[0]["meta"]["strategy_owner"] == "US_STANDARD"
    assert actual_fills[0]["meta"]["position_lifecycle_id"] == lifecycle
    assert actual_fills[0]["meta"]["semantic_action"] == action
    assert actual_fills[0]["meta"]["cost_basis_price_usd"] == 42.5
    assert actual_fills[0]["meta"]["realized_pnl_usd"] == 4.5


def test_real_postgres_nvda_route_journal_recovery_fences_and_rebounds_after_restart(
    pg_engine, monkeypatch, tmp_path,
):
    from sqlalchemy import text
    from trader.us.execution import order_router
    from trader.us.execution.order_journal import (
        load_order_events,
        replay_order_journal,
    )
    from trader.us.execution.order_router import route_order
    from trader.execution_claims import DurableExecutionClaimRepo
    from trader.us.db.execution_claim_schema import (
        us_execution_attempts,
        us_execution_claims,
    )
    import trader.us.db.repos as repos

    trade_date = "2026-10-01"
    journal_dir = tmp_path / "journal"
    monkeypatch.setenv("US_ORDER_JOURNAL_DIR", str(journal_dir))
    monkeypatch.setattr(repos, "_active_us_epoch", lambda *_args, **_kwargs: "incident-epoch")
    monkeypatch.setattr(order_router, "same_day_semantic_sell_exists", lambda _intent: False)
    monkeypatch.setattr(
        "trader.us.execution.order_identity.normalize_and_validate_order_identity",
        lambda intent, _context: intent,
    )
    monkeypatch.setattr(order_router, "canonical_order_risk_check", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(order_router, "resolve_dry_run_for_us_order", lambda: False)
    monkeypatch.setattr(order_router, "assert_order_allowed", lambda *_args, **_kwargs: None)

    claim_repo = DurableExecutionClaimRepo(
        pg_engine, us_execution_claims, us_execution_attempts,
    )
    monkeypatch.setattr(repos, "_execution_claim_repo", lambda: claim_repo)
    monkeypatch.setattr(repos, "claim_execution_action", claim_repo.acquire)
    monkeypatch.setattr(repos, "record_execution_action_observation", claim_repo.record_observation)
    monkeypatch.setattr(repos, "release_execution_action_before_submit", claim_repo.release_before_submit)

    class Broker:
        calls = 0

        def place_us_sell_order(self, *_args):
            self.calls += 1
            if self.calls == 1:
                raise TimeoutError("response lost after broker acceptance")
            return {"ok": True, "order_no": f"nvda-order-{self.calls}"}

    broker = Broker()
    intent = {
        "trade_date": trade_date,
        "client_order_key": "nvda-postgres-ambiguous",
        "symbol": "NVDA",
        "exchange": "NASDAQ",
        "side": "SELL",
        "qty": 3,
        "limit_price": 90.0,
        "notional_usd": 270.0,
        "strategy_owner": "US_STANDARD",
        "position_lifecycle_id": "nvda-postgres-lifecycle",
        "reason": "DEFENSE_RISK_OFF_TRIM",
        "semantic_action": "DEFENSE_RISK_OFF_TRIM",
        "submitted_at_utc": "2026-10-01T14:00:00+00:00",
        "holding_qty": 10,
        "orderable_qty": 10,
        "sellable_qty": 10,
        "available_qty": 10,
        "meta": {
            "strategy_owner": "US_STANDARD",
            "position_lifecycle_id": "nvda-postgres-lifecycle",
            "semantic_action": "DEFENSE_RISK_OFF_TRIM",
            "reason": "DEFENSE_RISK_OFF_TRIM",
            "avg_cost": 80.0,
        },
    }

    first = route_order(intent, kis_client=broker)
    assert first["status"] == "BROKER_SUBMIT_RESULT_UNKNOWN"
    assert broker.calls == 1
    fenced = route_order(
        {**intent, "trade_date": "2026-10-05", "client_order_key": "nvda-postgres-before-truth"},
        kis_client=broker,
    )
    assert fenced["status"] == "ORDER_FENCED_UNRESOLVED_ACTION"
    assert broker.calls == 1
    submit = next(
        event for event in load_order_events(trade_date)
        if event["event_type"] == "BROKER_SUBMIT_STARTED"
    )

    class BrokerTruth:
        _offline = False
        _tick_context = None

        def get_balance(self, **_kwargs):
            return {"positions": [{"symbol": "NVDA", "qty": 7}]}

        def get_today_orders(self, trade_date):
            assert trade_date in {"2026-10-01", "2026-10-05"}
            broker_order_no = (
                "nvda-broker-order" if trade_date == "2026-10-01"
                else "nvda-order-2"
            )
            return [{
                "trade_date": trade_date,
                "order_no": broker_order_no,
                "symbol": "NVDA",
                "exchange": "NASDAQ",
                "side": "SELL",
                "requested_qty": 3,
                "filled_qty": 3,
                "remaining_qty": 0,
                "limit_price": 90.0,
                "submitted_at_utc": submit["submitted_at_utc"],
                "status": "FILLED",
                "avg_price": 90.0,
            }]

        def get_fills_by_order_no(self, **_kwargs):
            return {
                "order_no": (
                    "nvda-broker-order" if _kwargs.get("order_no") == "nvda-broker-order"
                    else "nvda-order-2"
                ),
                "symbol": "NVDA",
                "side": "SELL",
                "requested_qty": 3,
                "filled_qty": 3,
                "remaining_qty": 0,
                "status": "FILLED",
                "avg_price": 90.0,
            }

        def _get_client(self):
            return self

        def get_us_fills_today(self, **_kwargs):
            return []

    provider = BrokerTruth()
    recovery = replay_order_journal(
        "2026-10-02", provider=provider, include_active_claims=True,
    )
    assert recovery["broker_full_fill_count"] == 1
    with pg_engine.begin() as conn:
        order = conn.execute(
            text("""SELECT client_order_key,order_no,qty_filled,status,meta
                FROM us_orders WHERE order_no='nvda-broker-order'"""),
        ).mappings().one()
        fill_count = conn.execute(
            text("""SELECT COUNT(*) FROM us_fills
                WHERE order_no='nvda-broker-order' AND meta->>'is_synthetic'='false'"""),
        ).scalar_one()
        fill = conn.execute(
            text("""SELECT meta FROM us_fills
                WHERE order_no='nvda-broker-order' AND meta->>'is_synthetic'='false'"""),
        ).mappings().one()
    assert order["client_order_key"] == intent["client_order_key"]
    assert order["qty_filled"] == 3
    assert order["status"] == "FILLED"
    assert order["meta"]["strategy_owner"] == "US_STANDARD"
    assert order["meta"]["position_lifecycle_id"] == "nvda-postgres-lifecycle"
    assert fill_count == 1
    assert fill["meta"]["strategy_owner"] == "US_STANDARD"
    assert fill["meta"]["position_lifecycle_id"] == "nvda-postgres-lifecycle"
    assert fill["meta"]["cost_basis_price_usd"] == 80.0
    assert fill["meta"]["realized_pnl_usd"] == 30.0

    restarted_repo = DurableExecutionClaimRepo(
        pg_engine, us_execution_claims, us_execution_attempts,
    )
    monkeypatch.setattr(repos, "_execution_claim_repo", lambda: restarted_repo)
    monkeypatch.setattr(repos, "claim_execution_action", restarted_repo.acquire)
    prior_action_key = submit["meta"]["execution_action_key"]
    assert restarted_repo.get(prior_action_key).action_state == "SATISFIED"
    repeated_recovery = replay_order_journal(
        trade_date, provider=provider, include_active_claims=True,
    )
    assert repeated_recovery["broker_full_fill_count"] == 1
    with pg_engine.begin() as conn:
        repeated_fill_count = conn.execute(
            text("""SELECT COUNT(*) FROM us_fills
                WHERE order_no='nvda-broker-order' AND meta->>'is_synthetic'='false'"""),
        ).scalar_one()
    assert repeated_fill_count == 1

    with pg_engine.begin() as conn:
        conn.execute(text("""CREATE OR REPLACE FUNCTION fail_nvda_ack_insert()
            RETURNS trigger AS $$ BEGIN
                IF NEW.client_order_key = 'nvda-postgres-later-date' THEN
                    RAISE EXCEPTION 'forced ACK persistence failure';
                END IF;
                RETURN NEW;
            END; $$ LANGUAGE plpgsql"""))
        conn.execute(text("""CREATE TRIGGER fail_nvda_ack_insert_trigger
            BEFORE INSERT ON us_orders FOR EACH ROW EXECUTE FUNCTION fail_nvda_ack_insert()"""))
    later = route_order(
        {**intent, "trade_date": "2026-10-05", "client_order_key": "nvda-postgres-later-date"},
        kis_client=broker,
    )
    assert later["status"] == "ACK_DB_FAILED"
    assert broker.calls == 2
    duplicate = route_order(
        {**intent, "trade_date": "2026-10-05", "client_order_key": "nvda-postgres-same-day-duplicate"},
        kis_client=broker,
    )
    assert duplicate["status"] == "ORDER_FENCED_UNRESOLVED_ACTION"
    assert broker.calls == 2
    with pg_engine.begin() as conn:
        conn.execute(text("DROP TRIGGER fail_nvda_ack_insert_trigger ON us_orders"))
        conn.execute(text("DROP FUNCTION fail_nvda_ack_insert()"))
    ack_recovery = replay_order_journal(
        "2026-10-05", provider=provider, include_active_claims=True,
    )
    assert ack_recovery["broker_full_fill_count"] == 1
    later_action_key = later["intent"]["meta"]["execution_action_key"]
    assert restarted_repo.get(later_action_key).action_state == "SATISFIED"
    with pg_engine.begin() as conn:
        ack_recovered_order = conn.execute(
            text("""SELECT client_order_key,order_no,qty_filled,status
                FROM us_orders WHERE client_order_key='nvda-postgres-later-date'"""),
        ).mappings().one()
    assert ack_recovered_order["order_no"] == "nvda-order-2"
    assert ack_recovered_order["qty_filled"] == 3
    assert ack_recovered_order["status"] == "FILLED"


def test_real_postgres_ambiguous_candidates_persist_unattributed_fill(pg_engine, monkeypatch, tmp_path):
    from datetime import date
    from trader.execution_claims import DurableExecutionClaimRepo
    from trader.execution_state import SemanticActionIdentity
    from trader.us.db.execution_claim_schema import (
        us_execution_attempts,
        us_execution_claims,
    )
    from trader.us.execution.order_journal import append_order_event
    import trader.us.db.repos as repos

    monkeypatch.setenv("US_ORDER_JOURNAL_DIR", str(tmp_path / "journal"))
    trade_date = "2026-10-01"
    claim_repo = DurableExecutionClaimRepo(
        pg_engine, us_execution_claims, us_execution_attempts,
    )
    monkeypatch.setattr(repos, "_execution_claim_repo", lambda: claim_repo)

    for owner, lifecycle, suffix in (
        ("US_STANDARD", "pb1-recovery-cycle", "pb1"),
        ("TQQQ_INFINITE", "infinite-recovery-cycle", "infinite"),
    ):
        client_key = f"recovery-candidate-{suffix}"
        attempt_id = f"recovery-attempt-{suffix}"
        identity = SemanticActionIdentity(
            env="practice",
            account_id="recovery-postgres-account",
            market="US",
            trading_epoch_id="recovery-postgres-epoch",
            strategy_owner=owner,
            lifecycle_id=lifecycle,
            action="DEFENSE_RISK_OFF_TRIM",
            trade_date=date.fromisoformat(trade_date),
        )
        assert claim_repo.acquire(
            identity, attempt_id=attempt_id, requested_qty=5,
            client_order_key=client_key,
        ).acquired
        claim_repo.record_observation(
            identity, attempt_id=attempt_id, state="UNRESOLVED",
            cumulative_filled_qty=None, authoritative=False,
        )
        meta = {
            "strategy_owner": owner,
            "position_lifecycle_id": lifecycle,
            "submit_attempt_id": attempt_id,
            "execution_action_key": identity.action_key,
            "semantic_action": "DEFENSE_RISK_OFF_TRIM",
            "submitted_at_utc": "2026-10-01T15:00:00+00:00",
        }
        intent = {
            "trade_date": trade_date,
            "client_order_key": client_key,
            "symbol": "XYZ",
            "exchange": "NASDAQ",
            "side": "SELL",
            "qty": 5,
            "strategy": "us_pb1" if owner == "US_STANDARD" else "tqqq_infinite",
            "strategy_owner": owner,
            "position_lifecycle_id": lifecycle,
            "submit_attempt_id": attempt_id,
            "meta": meta,
        }
        assert repos.save_order_intent(intent, trade_date=trade_date)
        assert repos.save_order_ack(
            {
                **intent,
                "qty_requested": 5,
                "qty_filled": 0,
                "status": "PENDING",
                "order_no": "",
            },
            trade_date=trade_date,
        )
        append_order_event("BROKER_SUBMIT_STARTED", intent)

    fill = {
        "trade_date": trade_date,
        "symbol": "XYZ",
        "exchange": "NASDAQ",
        "side": "SELL",
        "qty": 3,
        "cumulative_filled_qty": 3,
        "requested_qty": 5,
        "broker_open_qty": 2,
        "price_usd": 44.0,
        "order_no": "ambiguous-broker-order",
        "filled_at": "2026-10-01T15:00:00+00:00",
        "order_timestamp": "2026-10-02T00:00:00",
        "order_timestamp_utc": "2026-10-01T15:00:00+00:00",
        "order_timestamp_source": "KIS_INQUIRE_CCNL_ORD_DT_ORD_TMD",
        "order_timestamp_source_timezone": "Asia/Seoul",
        "meta": {
            "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
            "cumulative_filled_qty": 3,
            "requested_qty": 5,
            "broker_open_qty": 2,
            "order_timestamp": "2026-10-02T00:00:00",
            "order_timestamp_utc": "2026-10-01T15:00:00+00:00",
            "order_timestamp_source": "KIS_INQUIRE_CCNL_ORD_DT_ORD_TMD",
            "order_timestamp_source_timezone": "Asia/Seoul",
            "avg_price_usd": 44.0,
        },
    }
    repos.save_fills([fill], trade_date=trade_date)

    from sqlalchemy import text
    with pg_engine.begin() as conn:
        fills = conn.execute(
            text("SELECT client_order_key,meta FROM us_fills WHERE order_no=:order_no"),
            {"order_no": "ambiguous-broker-order"},
        ).mappings().all()
        imported_orders = conn.execute(
            text("SELECT COUNT(*) FROM us_orders WHERE client_order_key LIKE 'KIS_IMPORTED_%'"),
        ).scalar_one()
        claims = conn.execute(
            text("""SELECT strategy_owner,lifecycle_id,action_state,active_attempt_id
                FROM us_execution_claims ORDER BY strategy_owner"""),
        ).mappings().all()
    assert len(fills) == 1
    assert fills[0]["meta"]["attribution_status"] == "UNATTRIBUTED"
    assert fills[0]["meta"]["candidate_count"] == 2
    assert "strategy_owner" not in fills[0]["meta"]
    assert imported_orders == 0
    assert [(row["strategy_owner"], row["lifecycle_id"]) for row in claims] == [
        ("TQQQ_INFINITE", "infinite-recovery-cycle"),
        ("US_STANDARD", "pb1-recovery-cycle"),
    ]
    assert all(
        row["action_state"] == "UNCERTAIN" and row["active_attempt_id"]
        for row in claims
    )


def test_real_postgres_imports_only_after_no_internal_candidate(pg_engine, tmp_path):
    from sqlalchemy import text
    import trader.us.db.repos as repos

    trade_date = "2026-10-02"
    fill = {
        "trade_date": trade_date,
        "symbol": "ABC",
        "exchange": "NASDAQ",
        "side": "SELL",
        "qty": 2,
        "cumulative_filled_qty": 2,
        "requested_qty": 2,
        "broker_open_qty": 0,
        "price_usd": 51.0,
        "order_no": "unmatched-broker-order",
        "filled_at": "2026-10-02T15:00:00+00:00",
        "meta": {
            "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
            "cumulative_filled_qty": 2,
            "requested_qty": 2,
            "broker_open_qty": 0,
            "avg_price_usd": 51.0,
        },
    }
    repos.save_fills([fill], trade_date=trade_date)

    with pg_engine.begin() as conn:
        order = conn.execute(
            text("SELECT client_order_key,meta FROM us_orders WHERE order_no=:order_no"),
            {"order_no": "unmatched-broker-order"},
        ).mappings().one()
        actual_fill_count = conn.execute(
            text("SELECT COUNT(*) FROM us_fills WHERE order_no=:order_no"),
            {"order_no": "unmatched-broker-order"},
        ).scalar_one()
    assert order["client_order_key"].startswith("KIS_IMPORTED_")
    assert order["meta"]["order_origin"] == "broker_actual_without_local_order"
    assert actual_fill_count == 1


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


def test_real_postgres_active_claim_recovery_is_scoped_to_current_generation(
    pg_engine, monkeypatch,
):
    import hashlib
    from datetime import date
    import trader.us.db.repos as repos
    from trader.account_state import get_account_key
    from trader.execution_claims import DurableExecutionClaimRepo
    from trader.execution_state import SemanticActionIdentity
    from trader.us.db.execution_claim_schema import (
        us_execution_attempts,
        us_execution_claims,
    )

    monkeypatch.setenv("STRATEGY_ENV", "practice")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("CANO", "12345678")
    monkeypatch.setenv("ACNT_PRDT_CD", "01")
    monkeypatch.setattr(repos, "_us_state_epoch_id", lambda *args, **kwargs: "current-epoch")
    claim_repo = DurableExecutionClaimRepo(
        pg_engine, us_execution_claims, us_execution_attempts,
    )
    monkeypatch.setattr(repos, "_execution_claim_repo", lambda: claim_repo)

    account_id = hashlib.sha256(
        get_account_key(env="practice").encode("utf-8")
    ).hexdigest()

    def unresolved_claim(
        *,
        env,
        epoch,
        attempt_id,
        client_key,
        claim_account_id=account_id,
        trade_date=date(2026, 10, 1),
    ):
        identity = SemanticActionIdentity(
            env=env,
            account_id=claim_account_id,
            market="US",
            trading_epoch_id=epoch,
            strategy_owner="US_STANDARD",
            lifecycle_id="same-lifecycle",
            action=f"ACTION:{attempt_id}",
            trade_date=trade_date,
        )
        assert claim_repo.acquire(
            identity,
            attempt_id=attempt_id,
            requested_qty=1,
            client_order_key=client_key,
        ).acquired
        claim_repo.record_observation(
            identity,
            attempt_id=attempt_id,
            state="UNRESOLVED",
            cumulative_filled_qty=None,
            authoritative=False,
        )
        return identity

    unresolved_claim(
        env="live", epoch="current-epoch",
        attempt_id="foreign-live", client_key="foreign-live-key",
    )
    unresolved_claim(
        env="practice", epoch="prior-epoch",
        attempt_id="prior-generation", client_key="prior-generation-key",
    )
    unresolved_claim(
        env="practice", epoch="current-epoch",
        attempt_id="foreign-account", client_key="foreign-account-key",
        claim_account_id="foreign-account-id",
    )
    current_identity = unresolved_claim(
        env="practice", epoch="current-epoch",
        attempt_id="current-generation", client_key="current-generation-key",
        trade_date=date(2026, 9, 30),
    )

    same_generation = SemanticActionIdentity(
        env="practice",
        account_id=account_id,
        market="US",
        trading_epoch_id="current-epoch",
        strategy_owner="US_STANDARD",
        lifecycle_id="same-lifecycle",
        action="ACTION:SAME-GENERATION-RETRY",
        trade_date=date(2026, 10, 1),
    )
    blocked = claim_repo.acquire(
        same_generation,
        attempt_id="same-generation-retry",
        requested_qty=1,
        client_order_key="same-generation-retry-key",
    )

    active_claims = repos.load_active_execution_claim_attempts()
    health = repos.load_execution_claim_health()

    assert not blocked.acquired
    assert blocked.reason == "unresolved_lifecycle_action"
    assert [claim["active_attempt_id"] for claim in active_claims] == [
        "current-generation",
    ]
    assert health["unresolved_execution_actions"] == 1
    assert claim_repo.get(current_identity).action_state == "UNCERTAIN"
    assert {
        claim["active_attempt_id"] for claim in claim_repo.active_attempts()
    } == {
        "foreign-live",
        "prior-generation",
        "foreign-account",
        "current-generation",
    }



def test_same_cumulative_kis_price_correction_is_not_skipped(pg_engine):
    """Qty unchanged is not equivalent to unchanged evidence/economics.

    A previously synthetic cumulative fill must promote to actual KIS evidence,
    and a later corrected average price must update the SAME execution row.
    """
    from sqlalchemy import text
    import trader.us.db.repos as repos

    _seed_order(pg_engine, key="K-EQUAL-EVIDENCE", order_no="EQUAL-3",
                qty_requested=10, qty_filled=3)
    params = {
        "order_no": "EQUAL-3", "client_order_key": "K-EQUAL-EVIDENCE",
        "symbol": "AMD", "side": "SELL",
        "filled_qty": 3, "requested_qty": 10,
        "cumulative_filled_qty": 3,
        "trade_date": "2026-07-16",
        "evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
        "source": "fills_by_order_no",
    }
    first = repos.mark_order_filled_by_reconcile(
        **params, avg_price_usd=105.0,
    )
    assert first["status"] == "OK", first
    corrected = repos.mark_order_filled_by_reconcile(
        **params, avg_price_usd=107.25,
    )
    assert corrected["status"] == "OK", corrected
    with pg_engine.begin() as conn:
        actual = conn.execute(text("""
            SELECT qty, price_usd, meta FROM us_fills
            WHERE order_no = 'EQUAL-3'
              AND NOT COALESCE((meta->>'is_synthetic')::boolean, false)
            ORDER BY id
        """)).mappings().all()
        synthetic = conn.execute(text("""
            SELECT COALESCE((meta->>'accounting_active')::boolean, true)
            FROM us_fills
            WHERE order_no = 'EQUAL-3'
              AND COALESCE((meta->>'is_synthetic')::boolean, false)
        """)).scalars().all()
        order = conn.execute(text("""
            SELECT qty_filled, avg_price_usd, status
            FROM us_orders WHERE order_no='EQUAL-3'
        """)).mappings().one()
    assert len(actual) == 1, actual
    assert actual[0]["qty"] == 3
    assert float(actual[0]["price_usd"]) == 107.25
    assert actual[0]["meta"]["fill_evidence_type"] == "KIS_ORDER_CUMULATIVE_ACTUAL"
    assert synthetic == [False]
    assert order["qty_filled"] == 3
    assert float(order["avg_price_usd"]) == 107.25
    assert order["status"] == "PARTIALLY_FILLED"


def test_real_postgres_synthetic_price_is_not_actual_and_claim_proof_survives_restart(
        pg_engine, monkeypatch, tmp_path):
    from datetime import date
    import json
    import sqlalchemy as sa
    from trader.execution_claims import DurableExecutionClaimRepo
    from trader.execution_state import SemanticActionIdentity
    from trader.us.db import repos
    from trader.us.db.execution_claim_schema import us_execution_claims, us_execution_attempts

    monkeypatch.setenv('US_ORDER_JOURNAL_DIR', str(tmp_path))
    identity = SemanticActionIdentity(env='practice', account_id='test-account',
        market='US', trading_epoch_id='test-epoch', strategy_owner='US_STANDARD',
        lifecycle_id='pg-life', action='TP1', trade_date=date(2026,7,16))
    claims = DurableExecutionClaimRepo(pg_engine, us_execution_claims, us_execution_attempts)
    monkeypatch.setattr(repos, '_execution_claim_repo', lambda: claims)
    claims.acquire(identity, attempt_id='pg-proof-attempt', requested_qty=10, client_order_key='PG-PROOF')
    claims.record_observation(identity, attempt_id='pg-proof-attempt', state='ACKED',
                              cumulative_filled_qty=None, authoritative=False)
    from scripts.us_order_evidence_dry_run import ACTIVE_CLAIM_SQL
    with pg_engine.connect() as conn:
        inventory=conn.execute(sa.text(ACTIVE_CLAIM_SQL),{
            'env':'practice','account_id':'test-account','market':'US',
            'trading_epoch_id':'test-epoch','start_date':date(2026,7,1),
            'end_date':date(2026,7,31)}).mappings().all()
    assert len(inventory)==1  # The active attempt is visible even without us_orders.
    _seed_order(pg_engine, key='PG-PROOF', order_no='PG-PROOF-NO')
    with pg_engine.begin() as conn:
        conn.execute(sa.text('UPDATE us_orders SET meta=CAST(:meta AS jsonb) WHERE client_order_key=:key'),
            {'key':'PG-PROOF','meta':json.dumps({'execution_action_key':identity.action_key,
                'submit_attempt_id':'pg-proof-attempt','strategy_owner':'US_STANDARD',
                'position_lifecycle_id':'pg-life'})})
    observation = dict(trade_date='2026-07-16', client_order_key='PG-PROOF',
        raw_order_no='PG-PROOF-NO', canonical_order_no='PG-PROOF-NO', symbol='AMD',
        side='SELL', requested_qty=10, filled_qty=3, remaining_qty=7,
        broker_status='PARTIALLY_FILLED', evidence_type='KIS_ORDER_CUMULATIVE_ACTUAL')
    missing = repos.apply_broker_order_observation(**observation,
        raw_row={'filled_qty':3,'requested_qty':10})
    assert missing.get('requires_reconcile') is True
    assert claims.get(identity).active_attempt_id
    for qty in (3,10,10):
        result = repos.apply_broker_order_observation(**{**observation,'filled_qty':qty,
            'remaining_qty':10-qty,'broker_status':'FILLED' if qty==10 else 'PARTIALLY_FILLED'},
            raw_row={'filled_qty':qty,'requested_qty':10,'avg_price':105})
        assert result['status']=='OK' and not result.get('requires_reconcile'), result
    restarted = DurableExecutionClaimRepo(pg_engine, us_execution_claims, us_execution_attempts)
    assert restarted.get(identity).action_state=='SATISFIED'
    assert restarted.get(identity).active_attempt_id is None
    with pg_engine.connect() as conn:
        attempt=conn.execute(sa.select(us_execution_attempts)).mappings().one()
        actual=conn.execute(sa.text("SELECT qty,price_usd FROM us_fills WHERE client_order_key='PG-PROOF' AND NOT COALESCE((meta->>'is_synthetic')::boolean,false)")).mappings().all()
    assert attempt['authoritative'] is True
    assert len(actual)==1 and actual[0]['qty']==10 and float(actual[0]['price_usd'])==105


def test_real_postgres_multi_broker_observation_persists_verified_evidence_and_is_idempotent(
        pg_engine, monkeypatch, tmp_path):
    """The real us_order_events JSONB and UNIQUE idem preserve broker truth."""
    from trader.us.execution.order_journal import append_order_event, load_order_events
    from trader.us.db import repos

    monkeypatch.setenv("US_ORDER_JOURNAL_DIR", str(tmp_path / "verified-orders"))
    trade_date = "2026-10-13"
    intent = {
        "trade_date": trade_date, "client_order_key": "collision-pg-proof",
        "submit_attempt_id": "old-attempt-pg", "symbol": "AMD",
        "side": "SELL", "exchange": "NASDAQ", "qty": 2, "session": "am",
        "session_run_id": "historical-session",
    }
    base_proof = {
        "broker_requested_qty": 2,
        "cumulative_filled_qty": 1,
        "broker_remaining_qty": 1,
        "broker_avg_price": 23.75,
        "broker_observed_at": "2026-10-14T01:31:00Z",
        "broker_lookup_scope": {
            "source_trade_date": trade_date,
            "market": "US", "verified": True,
            "provider_source": "kis_authenticated_order_book",
            "account_id": "sha256-account-id",
            "account_product_code": "01", "env": "practice",
        },
    }
    first = append_order_event(
        "JOURNAL_REPLAY_MULTI_ATTEMPT_BROKER_EVIDENCE",
        intent, broker_order_no="0000010201",
        broker_status="PARTIALLY_FILLED", raw_response=base_proof,
    )
    second = append_order_event(
        "JOURNAL_REPLAY_MULTI_ATTEMPT_BROKER_EVIDENCE",
        intent, broker_order_no="0000010201",
        broker_status="PARTIALLY_FILLED",
        raw_response={**base_proof, "broker_observed_at": "2026-10-14T01:35:00Z"},
    )
    assert first["event_id"] != second["event_id"]
    assert first["idempotency_key"] == second["idempotency_key"]
    rows = [
        r for r in repos.load_us_order_events(trade_date)
        if r["event_type"] == "JOURNAL_REPLAY_MULTI_ATTEMPT_BROKER_EVIDENCE"
        and r["client_order_key"] == intent["client_order_key"]
    ]
    assert len(rows) == 1, rows
    saved = rows[0]
    assert saved["submit_attempt_id"] == "old-attempt-pg"
    assert saved["broker_order_no"] == "0000010201"
    assert saved["cumulative_filled_qty"] == 1
    assert saved["broker_evidence"]["requested_qty"] == 2
    assert saved["broker_evidence"]["remaining_qty"] == 1
    assert saved["broker_evidence"]["avg_price"] == 23.75
    assert saved["broker_evidence"]["observed_at"] == "2026-10-14T01:31:00Z"
    assert saved["broker_evidence"]["lookup_scope"]["source_trade_date"] == trade_date
    assert saved["broker_evidence"]["lookup_scope"]["account_id"] == "sha256-account-id"
    assert saved["raw_response_hash"]
    assert "broker_account_no" not in str(saved)
    # These are evidence-only, never a second filled us_orders row.
    from sqlalchemy import text
    with pg_engine.connect() as conn:
        assert conn.execute(text(
            "SELECT COUNT(*) FROM us_orders WHERE client_order_key = :key"
        ), {"key": intent["client_order_key"]}).scalar_one() == 0
        assert conn.execute(text(
            "SELECT COUNT(*) FROM us_fills WHERE client_order_key = :key"
        ), {"key": intent["client_order_key"]}).scalar_one() == 0
    assert len([
        r for r in load_order_events(trade_date)
        if r["event_type"] == "JOURNAL_REPLAY_MULTI_ATTEMPT_BROKER_EVIDENCE"
        and r["client_order_key"] == intent["client_order_key"]
    ]) == 1


def test_equal_qty_price_synthetic_broker_promotion_preserves_unique_tuple(pg_engine):
    """Oct 2026 GILD/PG: original synthetic is exactly same qty AND price.

    Schema uq_us_fills_idempotent ignores evidence_type, so a separate real
    fill row with that tuple would violate UNIQUE and leave a false claim.
    Promote only the scoped superseded synthetic with an immutable provenance
    breadcrumb; the second actual replay is idempotent.
    """
    import trader.us.db.repos as repos
    from sqlalchemy import text
    _seed_order(pg_engine, key="SAME-TUPLE-REPAIR", order_no="0000900123",
                qty_requested=4, qty_filled=4)
    opts = dict(
        trade_date="2026-07-16",order_no="0000900123",
        client_order_key="SAME-TUPLE-REPAIR",
        symbol="AMD",side="SELL",filled_qty=4,
        requested_qty=4,cumulative_filled_qty=4,
        avg_price_usd=100,evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        source="broker_order_observation",
    )
    first = repos.mark_order_filled_by_reconcile(**opts)
    second = repos.mark_order_filled_by_reconcile(**opts)
    assert first["status"] == "OK",first
    assert second["status"] == "OK",second
    with pg_engine.begin() as conn:
        fills=conn.execute(text("""
            SELECT qty,price_usd,meta FROM us_fills
            WHERE client_order_key='SAME-TUPLE-REPAIR'
        """)).mappings().all()
        order=conn.execute(text("""
            SELECT status,qty_filled,avg_price_usd FROM us_orders
            WHERE client_order_key='SAME-TUPLE-REPAIR'
        """)).mappings().one()
    assert len(fills)==1
    assert fills[0]["qty"]==4
    assert float(fills[0]["price_usd"])==100.0
    assert fills[0]["meta"]["fill_evidence_type"]=="KIS_ORDER_CUMULATIVE_ACTUAL"
    assert fills[0]["meta"]["is_synthetic"] is False
    assert fills[0]["meta"]["accounting_active"] is True
    assert fills[0]["meta"]["synthetic_actual_same_tuple_promoted"] is True
    assert fills[0]["meta"]["original_synthetic_evidence_type"]=="BALANCE_DELTA_SYNTHETIC"
    assert order["status"]=="FILLED"
    assert order["qty_filled"]==4


def test_oct20_cancelled_tqqq_uncertain_claim_recovers_with_proven_semantic_qty(
    pg_engine, monkeypatch, tmp_path,
):
    """Real PostgreSQL path: broker-open=0, action remaining=3, Claim RETRYABLE.

    All test data are in the disposable CI PostgreSQL service; no real KIS or
    operating database is touched.  This repros the mismatched Oct20 lifecycle.
    """
    import json
    from datetime import date
    from sqlalchemy import text
    from trader.execution_claims import DurableExecutionClaimRepo
    from trader.execution_state import SemanticActionIdentity
    from trader.us.db.execution_claim_schema import (
        us_execution_attempts, us_execution_claims,
    )
    from trader.us.infinite.repository import InfiniteRepository
    from trader.us.db import repos

    monkeypatch.setenv("US_ORDER_JOURNAL_DIR", str(tmp_path / "us-journal"))
    monkeypatch.setattr(repos, "_active_us_epoch", lambda *_a, **_k: "pg-tqqq-epoch")

    identity = SemanticActionIdentity(
        env="practice", account_id="pg-tqqq-account", market="US",
        trading_epoch_id="pg-tqqq-epoch",
        strategy_owner="TQQQ_INFINITE",
        lifecycle_id="pg-tqqq-cycle",
        action="TQQQ_INFINITE_BUY",
        trade_date=date(2026, 10, 20),
    )
    claim_repo = DurableExecutionClaimRepo(
        pg_engine, us_execution_claims, us_execution_attempts,
    )
    monkeypatch.setattr(repos, "_execution_claim_repo", lambda: claim_repo)
    key = "TQQQ_INF_V3:pg-tqqq-cycle:2026-10-20:BUY"
    attempt_id = "pg-tqqq-attempt-38776"
    assert claim_repo.acquire(
        identity, attempt_id=attempt_id, requested_qty=3, client_order_key=key,
    ).acquired
    claim_repo.record_observation(
        identity, attempt_id=attempt_id, state="UNRESOLVED",
        cumulative_filled_qty=None, authoritative=False,
    )
    meta = {
        "execution_action_key": identity.action_key,
        "submit_attempt_id": attempt_id,
        "tqqq_ttl_cancel_result": {
            "rt_cd": "0", "msg_cd": "40630000",
            "output": {"ODNO": "0000038961"},
        },
    }
    with pg_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO us_orders(
                trade_date, client_order_key, symbol, exchange, side,
                qty_requested, qty_filled, order_no, status, env,
                trading_epoch_id, meta
            ) VALUES (
                '2026-10-20', :key, 'TQQQ', 'NASDAQ', 'BUY',
                3, 0, '0000038776', 'CANCELLED', 'practice',
                'pg-tqqq-epoch', CAST(:meta AS jsonb)
            )
        """), {"key": key, "meta": json.dumps(meta)})

    repo = InfiniteRepository(pg_engine)
    rows = repo.load_cancelled_uncertain_buy_orders(symbol="TQQQ")
    assert len(rows) == 1
    assert claim_repo.get(identity).action_state == "UNCERTAIN"

    observed = {
        "order_no": "0000038776", "symbol": "TQQQ", "side": "BUY",
        "status": "CANCELLED", "requested_qty": 3,
        "requested_qty_present": True, "filled_qty": 0,
        "filled_qty_present": True, "remaining_qty": 0,
        "remaining_qty_present": True, "broker_open_qty": 0,
        "broker_open_qty_present": True,
        "evidence_type": "TQQQ_TTL_CANCEL_ACK_ZERO_REMAINING",
        "observed_at": "2026-10-20T14:17:00+00:00",
    }
    updated = repo.apply_ttl_terminal_observation(rows[0], observed)
    assert updated["status"] == "OK"
    assert updated["authoritative"] is True
    assert updated["semantic_action_remaining_qty"] == 3
    assert updated["broker_open_qty"] == 0
    snapshot = claim_repo.get(identity)
    assert snapshot.action_state == "RETRYABLE"
    assert snapshot.remaining_target_qty == 3
    assert snapshot.active_attempt_id is None
    with pg_engine.connect() as conn:
        db_row = conn.execute(text("""
            SELECT status, qty_filled, meta FROM us_orders
            WHERE trade_date='2026-10-20' AND client_order_key=:key
        """), {"key": key}).mappings().one()
    assert db_row["status"] == "CANCELLED"
    assert db_row["qty_filled"] == 0
    assert db_row["meta"]["broker_open_qty"] == 0
    assert db_row["meta"]["semantic_action_remaining_qty"] == 3
    assert repo.load_cancelled_uncertain_buy_orders(symbol="TQQQ") == []


def test_oct20_zero_fill_cancel_and_claim_update_are_one_pg_transaction(pg_engine, monkeypatch, tmp_path):
    """One failure cannot create CANCELLED local order with UNCERTAIN Claim.

    Real PostgreSQL fixture is disposable CI only. KIS is never called.
    """
    import json
    from datetime import date
    from sqlalchemy import text
    from trader.execution_claims import DurableExecutionClaimRepo
    from trader.execution_state import SemanticActionIdentity
    from trader.us.db.execution_claim_schema import us_execution_attempts, us_execution_claims
    from trader.us.db import repos

    monkeypatch.setenv("US_ORDER_JOURNAL_DIR", str(tmp_path / "journal"))
    identity = SemanticActionIdentity(
        env="practice", account_id="atomic-tqqq-account",
        market="US", trading_epoch_id="atomic-tqqq-epoch",
        strategy_owner="TQQQ_INFINITE", lifecycle_id="atomic-cycle",
        action="BUY", trade_date=date(2026, 10, 20),
    )
    claim_repo = DurableExecutionClaimRepo(
        pg_engine, us_execution_claims, us_execution_attempts,
    )
    monkeypatch.setattr(repos, "_execution_claim_repo", lambda: claim_repo)
    key = "TQQQ_INF_V3:atomic-cycle:2026-10-20:BUY"
    attempt = "attempt-atomic-38776"
    assert claim_repo.acquire(
        identity, attempt_id=attempt, requested_qty=3, client_order_key=key,
    ).acquired
    claim_repo.record_observation(
        identity, attempt_id=attempt, state="UNRESOLVED",
        cumulative_filled_qty=None, authoritative=False,
    )
    with pg_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO us_orders
                (trade_date, client_order_key, symbol, exchange, side,
                 qty_requested, qty_filled, order_no, status, env,
                 trading_epoch_id, meta)
            VALUES ('2026-10-20', :key, 'TQQQ', 'NASDAQ', 'BUY',
                    3, 0, '0000038776', 'ACK', 'practice',
                    'atomic-tqqq-epoch', CAST(:meta AS jsonb))
        """), {
            "key": key, "meta": json.dumps({
                "execution_action_key": identity.action_key,
                "submit_attempt_id": attempt,
                "strategy_owner": "TQQQ_INFINITE",
            }),
        })

    evidence = {
        "trade_date": "2026-10-20", "client_order_key": key,
        "raw_order_no": "0000038776", "canonical_order_no": "38776",
        "symbol": "TQQQ", "side": "BUY", "requested_qty": 3,
        "filled_qty": 0, "remaining_qty": 3, "broker_open_qty": 0,
        "broker_status": "CANCELLED",
        "evidence_type": "TQQQ_TTL_CANCEL_ACK_ZERO_REMAINING",
        "raw_row": {
            "order_no": "0000038776", "symbol": "TQQQ", "side": "BUY",
            "status": "CANCELLED", "requested_qty": 3,
            "requested_qty_present": True,
            "filled_qty": 0, "filled_qty_present": True,
            "remaining_qty": 0, "remaining_qty_present": True,
            "broker_open_qty": 0, "broker_open_qty_present": True,
        },
    }

    actual = repos.record_execution_action_observation_by_key
    def failed_claim(*args, **kwargs):
        assert kwargs.get("_conn") is not None
        raise RuntimeError("injected_claim_db_failure")
    monkeypatch.setattr(repos, "record_execution_action_observation_by_key", failed_claim)
    failed = repos.apply_broker_order_observation(**evidence)
    assert failed["status"] == "RECONCILE_UPDATE_FAILED"
    assert failed["retry_order"] is False
    assert failed["entry_fence"] is True
    with pg_engine.connect() as conn:
        state = conn.execute(text(
            "SELECT status FROM us_orders WHERE client_order_key=:key"
        ), {"key": key}).scalar_one()
    assert state == "ACK", "order status must rollback with failed Claim write"
    assert claim_repo.get(identity).action_state == "UNCERTAIN"

    monkeypatch.setattr(repos, "record_execution_action_observation_by_key", actual)
    success = repos.apply_broker_order_observation(**evidence)
    assert success["status"] == "OK"
    assert success["authoritative"] is True
    assert success["order_status"] == "CANCELLED"
    assert claim_repo.get(identity).action_state == "RETRYABLE"
    with pg_engine.connect() as conn:
        row = conn.execute(text(
            "SELECT status, meta FROM us_orders WHERE client_order_key=:key"
        ), {"key": key}).mappings().one()
    assert row["status"] == "CANCELLED"
    assert row["meta"]["broker_open_qty"] == 0
    assert row["meta"]["semantic_action_remaining_qty"] == 3


def test_oct20_actual_fill_claim_failure_rolls_back_pg_order_and_fill(
    pg_engine, monkeypatch,
):
    """A failed Claim update must not commit KIS economic truth independently.

    Covers both the normal engine.begin() call and a caller-owned transaction
    in disposable PostgreSQL.  Never contacts KIS/operational Supabase.
    """
    import json
    import pytest
    from sqlalchemy import text
    from trader.us.db import repos

    key = "tqqq-pg-claim-atomic-fill-20261020"
    broker_no = "0000039009"
    with pg_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO us_orders(
                trade_date, client_order_key, symbol, exchange, side,
                qty_requested, qty_filled, order_no, status, env, meta
            )
            VALUES (
                '2026-10-20', :key, 'TQQQ', 'NASDAQ', 'BUY',
                3, 0, :order_no, 'ACK', 'practice', CAST(:meta AS jsonb)
            )
        """), {
            "key": key, "order_no": broker_no,
            "meta": json.dumps({
                "execution_action_key": "claim-key-test",
                "submit_attempt_id": "attempt-key-test",
                "strategy_owner": "TQQQ_INFINITE",
            }),
        })

    def injected_failure(*args, **kwargs):
        assert kwargs.get("_conn") is not None
        raise RuntimeError("injected_claim_failure_after_fill_write")

    monkeypatch.setattr(
        repos, "record_execution_action_observation_by_key", injected_failure
    )
    observation = dict(
        order_no=broker_no, client_order_key=key, symbol="TQQQ", side="BUY",
        filled_qty=1, requested_qty=3, cumulative_filled_qty=1,
        avg_price_usd=70.5, trade_date="2026-10-20",
        source="fills_by_order_no", evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
    )

    # The repository owns the connection: failure is structured, never
    # retried automatically, and writes from its transaction must roll back.
    first = repos.mark_order_filled_by_reconcile(**observation)
    assert first["status"] == "RECONCILE_UPDATE_FAILED"
    assert first["entry_fence"] is True
    assert first["retry_order"] is False
    assert "US_ATOMIC_CLAIM_FILL_APPLY_FAILED" in first["error"]

    def assert_no_economic_commit():
        with pg_engine.connect() as conn:
            order = conn.execute(text("""
                SELECT status, qty_filled FROM us_orders
                WHERE client_order_key=:key
            """), {"key": key}).mappings().one()
            fills = conn.execute(text("""
                SELECT count(*) FROM us_fills
                WHERE client_order_key=:key
            """), {"key": key}).scalar_one()
        assert order["status"] == "ACK"
        assert order["qty_filled"] == 0
        assert fills == 0

    assert_no_economic_commit()

    # A higher-level reconciler can supply its own transaction.  The nested
    # function must RAISE so that the outer unit of work is also rolled back.
    with pytest.raises(RuntimeError, match="US_ATOMIC_CLAIM_FILL_APPLY_FAILED"):
        with pg_engine.begin() as conn:
            repos.mark_order_filled_by_reconcile(**observation, _conn=conn)

    assert_no_economic_commit()


def test_oct20_tqqq_partial_cancel_releases_exact_attempt(pg_engine, monkeypatch):
    """One of three TQQQ shares filled before cancel: preserve two as semantic
    remainder, broker-open zero, and terminalize the immutable Attempt.
    """
    import json
    from datetime import date
    from sqlalchemy import text
    from trader.execution_claims import DurableExecutionClaimRepo
    from trader.execution_state import SemanticActionIdentity
    from trader.us.db.execution_claim_schema import us_execution_claims, us_execution_attempts
    from trader.us.db import repos

    identity = SemanticActionIdentity(
        env="practice", account_id="partial-cancel-test-account",
        market="US", trading_epoch_id="partial-cancel-epoch",
        strategy_owner="TQQQ_INFINITE", lifecycle_id="partial-cycle",
        action="TQQQ_INFINITE_BUY", trade_date=date(2026, 10, 20),
    )
    ledger = DurableExecutionClaimRepo(
        pg_engine, us_execution_claims, us_execution_attempts
    )
    monkeypatch.setattr(repos, "_execution_claim_repo", lambda: ledger)
    key = "TQQQ_INF_V3:partial-cycle:2026-10-20:BUY"
    attempt_id = "partial-cancel-38776"
    assert ledger.acquire(
        identity, attempt_id=attempt_id, requested_qty=3, client_order_key=key
    ).acquired
    ledger.record_observation(
        identity, attempt_id=attempt_id, state="UNRESOLVED",
        cumulative_filled_qty=None, authoritative=False,
    )
    with pg_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO us_orders(
                trade_date,client_order_key,symbol,exchange,side,
                qty_requested,qty_filled,order_no,status,env,trading_epoch_id,meta
            ) VALUES (
                '2026-10-20',:key,'TQQQ','NASDAQ','BUY',
                3,0,'0000038776','ACK','practice','partial-cancel-epoch',
                CAST(:meta AS jsonb)
            )
        """), {"key": key, "meta": json.dumps({
            "execution_action_key": identity.action_key,
            "submit_attempt_id": attempt_id,
            "strategy_owner": "TQQQ_INFINITE",
        })})

    row = {
        "order_no": "0000038776", "symbol": "TQQQ", "side": "BUY",
        "status": "CANCELLED", "requested_qty": 3,
        "requested_qty_present": True,
        "filled_qty": 1, "filled_qty_present": True,
        "remaining_qty": 0, "remaining_qty_present": True,
        "broker_open_qty": 0, "broker_open_qty_present": True,
        "avg_price": 70.5,
    }
    result = repos.apply_broker_order_observation(
        trade_date="2026-10-20", client_order_key=key,
        raw_order_no="0000038776", canonical_order_no="38776",
        symbol="TQQQ", side="BUY", requested_qty=3, filled_qty=1,
        remaining_qty=2, broker_status="CANCELLED",
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL", raw_row=row,
    )
    assert result["status"] == "OK", result
    assert result["order_status"] == "CANCELLED"
    snap = ledger.get(identity)
    assert snap.action_state == "PARTIALLY_SATISFIED"
    assert snap.remaining_target_qty == 2
    assert snap.cumulative_filled_qty == 1
    assert snap.active_attempt_id is None
    with pg_engine.connect() as conn:
        order = conn.execute(text(
            "SELECT status,qty_filled,meta FROM us_orders WHERE client_order_key=:key"
        ), {"key": key}).mappings().one()
        fills = conn.execute(text(
            "SELECT COALESCE(SUM(qty),0) FROM us_fills WHERE client_order_key=:key"
        ), {"key": key}).scalar_one()
    assert order["status"] == "CANCELLED"
    assert order["qty_filled"] == 1
    assert order["meta"]["semantic_action_remaining_qty"] == 2
    assert order["meta"]["broker_open_qty"] == 0
    assert fills == 1


def test_claim_pre_submit_outbox_atomic_rollback(pg_engine):
    """Crash during DB outbox insert rolls back the exact execution Claim.

    Disposable PostgreSQL only. No broker HTTP or production Supabase.
    """
    from datetime import date, datetime, timezone
    from uuid import uuid4
    from sqlalchemy import text
    from trader.execution_state import SemanticActionIdentity
    from trader.us.db import repos
    from trader.execution_claims import DurableExecutionClaimRepo
    from trader.us.db.execution_claim_schema import (
        us_execution_claims, us_execution_attempts,
    )

    identity = SemanticActionIdentity(
        env="practice", account_id="pg-outbox-test-account",
        market="US", trading_epoch_id="pg-outbox-test-epoch",
        strategy_owner="TQQQ_INFINITE", lifecycle_id="pg-outbox-test-lifecycle",
        action="BUY", trade_date=date(2026, 10, 20),
    )
    attempt = "pg-outbox-test-attempt"
    key = "TQQQ_INF_V3:pg-outbox-test-lifecycle:2026-10-20:BUY"
    claim_repo = DurableExecutionClaimRepo(
        pg_engine, us_execution_claims, us_execution_attempts,
    )
    event = {
        "event_id": str(uuid4()), "trade_date": "2026-10-20",
        "event_type": "BROKER_CLAIM_PREPARED",
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "session": "am", "session_run_id": "unit-run",
        "tick_id": "unit-tick", "client_order_key": key,
        "submit_attempt_id": attempt, "symbol": "TQQQ",
        "side": "BUY", "requested_qty": 3,
        "raw_broker_order_no": None, "canonical_broker_order_no": None,
        "position_lifecycle_id": "pg-outbox-test-lifecycle",
        "profit_capture_stage": "", "cumulative_filled_qty": 0,
        "payload": {"submission_status": "NOT_YET_AT_HTTP_BOUNDARY"},
        "idempotency_key": "us-claim-prepared:" + attempt,
    }
    with pytest.raises(RuntimeError, match="injected_outbox_failure"):
        with pg_engine.begin() as conn:
            acquired = claim_repo.acquire(
                identity, attempt_id=attempt, requested_qty=3,
                client_order_key=key, _conn=conn,
            )
            assert acquired.acquired
            assert repos.append_us_order_event(event, _conn=conn)
            raise RuntimeError("injected_outbox_failure")

    with pg_engine.connect() as conn:
        claim_count = conn.execute(text("""
            SELECT count(*) FROM us_execution_claims WHERE action_key=:key
        """), {"key": identity.action_key}).scalar_one()
        event_count = conn.execute(text("""
            SELECT count(*) FROM us_order_events WHERE idempotency_key=:key
        """), {"key": event["idempotency_key"]}).scalar_one()
    assert claim_count == event_count == 0

    with pg_engine.begin() as conn:
        acquired = claim_repo.acquire(
            identity, attempt_id=attempt, requested_qty=3,
            client_order_key=key, _conn=conn,
        )
        assert acquired.acquired
        assert repos.append_us_order_event(event, _conn=conn)
    with pg_engine.connect() as conn:
        counts = (
            conn.execute(text("""
                SELECT count(*) FROM us_execution_claims WHERE action_key=:key
            """), {"key": identity.action_key}).scalar_one(),
            conn.execute(text("""
                SELECT count(*) FROM us_order_events WHERE idempotency_key=:key
            """), {"key": event["idempotency_key"]}).scalar_one(),
        )
    assert counts == (1, 1)


def test_claim_reservation_outbox_triplet_rolls_back_as_one(pg_engine):
    """Full Stage 1+3 DB preparation is one atomic write on PostgreSQL.

    Three durable rows (Claim / account reservation / immutable pre-submit
    outbox) are never partially visible after an exception, without KIS IO.
    """
    from datetime import date, datetime, timezone
    import hashlib
    from pathlib import Path
    from uuid import uuid4
    from sqlalchemy import text
    from trader.execution_state import SemanticActionIdentity
    from trader.execution_claims import DurableExecutionClaimRepo
    from trader.us.db import repos
    from trader.us.db.execution_claim_schema import (
        us_execution_claims, us_execution_attempts,
    )
    from trader.us.execution.account_authority import AccountExecutionAuthority

    with pg_engine.begin() as conn:
        conn.exec_driver_sql(Path(
            "migrations/0056_us_execution_authority_reservations.sql"
        ).read_text())
    fingerprint = hashlib.sha256(b"stage-123-synthetic-test").hexdigest()
    authority = AccountExecutionAuthority(
        pg_engine, env="practice", account_fingerprint=fingerprint,
        trading_epoch_id="stage123-epoch",
    )
    ledger = DurableExecutionClaimRepo(
        pg_engine, us_execution_claims, us_execution_attempts,
    )
    identity = SemanticActionIdentity(
        env="practice", account_id=fingerprint,
        market="US", trading_epoch_id="stage123-epoch",
        strategy_owner="TQQQ_INFINITE", lifecycle_id="stage123-cycle",
        action="BUY", trade_date=date(2026, 10, 20),
    )
    key = "TQQQ_INF_V3:stage123-cycle:2026-10-20:BUY"
    attempt = "stage123-reserved-attempt"
    event = {
        "event_id": str(uuid4()), "trade_date": "2026-10-20",
        "event_type": "BROKER_CLAIM_PREPARED",
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "session": "am", "session_run_id": "synthetic",
        "tick_id": "synthetic", "client_order_key": key,
        "submit_attempt_id": attempt, "symbol": "TQQQ", "side": "BUY",
        "requested_qty": 3, "raw_broker_order_no": None,
        "canonical_broker_order_no": None,
        "position_lifecycle_id": "stage123-cycle",
        "profit_capture_stage": "", "cumulative_filled_qty": 0,
        "payload": {"submission_status": "NOT_YET_AT_HTTP_BOUNDARY"},
        "idempotency_key": "stage123:" + attempt,
    }
    lease = authority.acquire_lease(holder_id="stage123-router")
    def write_triplet(conn):
        assert ledger.acquire(
            identity, attempt_id=attempt, requested_qty=3,
            client_order_key=key, _conn=conn,
        ).acquired
        authority.reserve(
            lease=lease, action_key=attempt,
            strategy_owner="TQQQ_INFINITE",
            symbol="TQQQ", side="BUY", qty=3,
            price_usd="100", broker_cash_usd="500",
            _conn=conn,
        )
        assert repos.append_us_order_event(event, _conn=conn)

    with pytest.raises(RuntimeError, match="synthetic_crash"):
        with pg_engine.begin() as conn:
            write_triplet(conn)
            raise RuntimeError("synthetic_crash")

    def counts():
        with pg_engine.connect() as conn:
            return (
                conn.execute(text(
                    "SELECT count(*) FROM us_execution_claims WHERE action_key=:key"
                ), {"key": identity.action_key}).scalar_one(),
                conn.execute(text(
                    "SELECT count(*) FROM us_execution_account_reservations WHERE action_key=:key"
                ), {"key": attempt}).scalar_one(),
                conn.execute(text(
                    "SELECT count(*) FROM us_order_events WHERE idempotency_key=:key"
                ), {"key": event["idempotency_key"]}).scalar_one(),
            )
    assert counts() == (0, 0, 0)
    with pg_engine.begin() as conn:
        write_triplet(conn)
    assert counts() == (1, 1, 1)
    authority.assert_reserved_submit_authority(
        attempt_id=attempt, client_order_key=key,
        strategy_owner="TQQQ_INFINITE", symbol="TQQQ", side="BUY", qty=3,
    )
    from trader.us.execution.account_authority import AuthorityUnavailable
    with pytest.raises(AuthorityUnavailable, match="STALE"):
        authority.assert_reserved_submit_authority(
            attempt_id=attempt, client_order_key=key,
            strategy_owner="US_STANDARD", symbol="TQQQ", side="BUY", qty=3,
        )
    ledger.release_before_submit(identity, attempt_id=attempt)
    with pytest.raises(AuthorityUnavailable, match="STALE"):
        authority.assert_reserved_submit_authority(
            attempt_id=attempt, client_order_key=key,
            strategy_owner="TQQQ_INFINITE", symbol="TQQQ", side="BUY", qty=3,
        )
    authority.release_lease(lease=lease)


def test_account_reservation_only_terminalizes_after_exact_tqqq_broker_proof(
    pg_engine, monkeypatch,
):
    """No cancellation request or ACK can release a reserved 3-share BUY."""
    import hashlib
    import json
    from pathlib import Path
    from datetime import date
    from sqlalchemy import text
    from trader.execution_claims import DurableExecutionClaimRepo
    from trader.execution_state import SemanticActionIdentity
    from trader.us.db.execution_claim_schema import us_execution_claims, us_execution_attempts
    from trader.us.db import repos
    from trader.us.execution.account_authority import (
        AccountExecutionAuthority, ReservationConflict,
    )
    with pg_engine.begin() as conn:
        conn.exec_driver_sql(Path(
            "migrations/0056_us_execution_authority_reservations.sql"
        ).read_text())
    account = hashlib.sha256(b"terminal-proof-stage123").hexdigest()
    owner = AccountExecutionAuthority(
        pg_engine, env="practice", account_fingerprint=account,
        trading_epoch_id="test-tqqq-epoch",
    )
    lease = owner.acquire_lease(holder_id="worker-test")
    identity = SemanticActionIdentity(
        env="practice", account_id=account, market="US",
        trading_epoch_id="test-tqqq-epoch",
        strategy_owner="TQQQ_INFINITE", lifecycle_id="test-tqqq-cycle",
        action="BUY", trade_date=date(2026, 10, 20),
    )
    ledger = DurableExecutionClaimRepo(
        pg_engine, us_execution_claims, us_execution_attempts,
    )
    monkeypatch.setattr(repos, "_execution_claim_repo", lambda: ledger)
    key = "TQQQ_INF_V3:test-tqqq-cycle:2026-10-20:BUY"
    attempt = "verified-ttl-attempt"
    assert ledger.acquire(
        identity, attempt_id=attempt,
        requested_qty=3, client_order_key=key,
    ).acquired
    ledger.record_observation(
        identity, attempt_id=attempt, state="UNRESOLVED",
        cumulative_filled_qty=None, authoritative=False,
    )
    owner.reserve(
        lease=lease, action_key=attempt, strategy_owner="TQQQ_INFINITE",
        symbol="TQQQ", side="BUY", qty=3, price_usd="100",
        broker_cash_usd="500",
    )
    with pg_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO us_orders(
              trade_date,client_order_key,symbol,exchange,side,
              qty_requested,qty_filled,order_no,status,env,trading_epoch_id,meta
            ) VALUES (
              '2026-10-20',:key,'TQQQ','NASDAQ','BUY',
              3,0,'0000038776','ACK','practice','test-tqqq-epoch',
              CAST(:meta AS jsonb)
            )
        """), {"key": key, "meta": json.dumps({
            "execution_action_key": identity.action_key,
            "submit_attempt_id": attempt,
            "strategy_owner": "TQQQ_INFINITE",
        })})
    with pytest.raises(ReservationConflict, match="terminal_attempt_proof_unverified"):
        owner.terminalize_verified_attempt(
            lease=lease, attempt_id=attempt,
            client_order_key=key, trade_date="2026-10-20",
        )
    original_broker_row = {
        "order_no": "0000038776", "symbol": "TQQQ", "side": "BUY",
        "status": "CANCELLED", "requested_qty": 3,
        "requested_qty_present": True,
        "filled_qty": 0, "filled_qty_present": True,
        "remaining_qty": 0, "remaining_qty_present": True,
        "broker_open_qty": 0, "broker_open_qty_present": True,
    }
    proof = repos.apply_broker_order_observation(
        trade_date="2026-10-20", client_order_key=key,
        raw_order_no="0000038776", canonical_order_no="38776",
        symbol="TQQQ", side="BUY", requested_qty=3, filled_qty=0,
        remaining_qty=3, broker_status="CANCELLED",
        evidence_type="TQQQ_TTL_CANCEL_ACK_ZERO_REMAINING",
        observed_at="2026-10-20T14:17:00+00:00",
        raw_row=original_broker_row,
    )
    assert proof["status"] == "OK", proof
    assert ledger.get(identity).action_state == "RETRYABLE"
    owner.release_lease(lease=lease)
    # The standalone fenced worker may release this reservation only AFTER
    # the exact original KIS cancel + durable Claim transition is stored.
    monkeypatch.setenv("US_ACCOUNT_RESERVATION_MODE", "ENFORCE")
    monkeypatch.setattr(
        repos, "_us_execution_claim_scope",
        lambda: {
            "env": "practice", "account_id": account,
            "market": "US", "trading_epoch_id": "test-tqqq-epoch",
        },
    )
    from trader.us.execution.reservation_reconcile_worker import (
        reconcile_proven_reservations_once,
    )
    worker_result = reconcile_proven_reservations_once(
        env="practice", trade_date="2026-10-20",
    )
    assert worker_result["attempted"] == 1, worker_result
    assert worker_result["released"] == 1, worker_result
    assert worker_result["unresolved"] == 0, worker_result
    next_lease = owner.acquire_lease(holder_id="verify-after-worker")
    replay = owner.terminalize_verified_attempt(
        lease=next_lease, attempt_id=attempt,
        client_order_key=key, trade_date="2026-10-20",
    )
    assert replay["state"] == "RELEASED"
    assert replay["idempotent"]
    owner.release_lease(lease=next_lease)


def test_fenced_reservation_worker_releases_only_proven_pre_http_aborts(
    pg_engine, monkeypatch,
):
    """No stuck cash holds after an exact durable ABANDONED_PRE_SUBMIT."""
    import hashlib
    from pathlib import Path
    from datetime import date
    from trader.execution_claims import DurableExecutionClaimRepo
    from trader.execution_state import SemanticActionIdentity
    from trader.us.db.execution_claim_schema import us_execution_claims, us_execution_attempts
    from trader.us.execution.account_authority import (
        AccountExecutionAuthority, ReservationConflict,
    )
    from trader.us.execution.reservation_reconcile_worker import (
        reconcile_proven_reservations_once,
    )
    from trader.us.db import repos

    with pg_engine.begin() as conn:
        conn.exec_driver_sql(Path(
            "migrations/0056_us_execution_authority_reservations.sql"
        ).read_text())
    account = hashlib.sha256(b"pre-http-abort-isolation").hexdigest()
    auth = AccountExecutionAuthority(
        pg_engine, env="practice", account_fingerprint=account,
        trading_epoch_id="pre-http-epoch",
    )
    ledger = DurableExecutionClaimRepo(
        pg_engine, us_execution_claims, us_execution_attempts,
    )
    identity = SemanticActionIdentity(
        env="practice", account_id=account, market="US",
        trading_epoch_id="pre-http-epoch",
        strategy_owner="US_STANDARD", lifecycle_id="pre-http-cycle",
        action="BUY", trade_date=date(2026, 10, 20),
    )
    attempt = "pre-http-cancel-before-boundary"
    cok = "US_STANDARD:pre-http-cycle:2026-10-20:BUY"
    lease = auth.acquire_lease(holder_id="original-router")
    with pg_engine.begin() as conn:
        assert ledger.acquire(
            identity, attempt_id=attempt, requested_qty=2,
            client_order_key=cok, _conn=conn,
        ).acquired
        auth.reserve(
            lease=lease, action_key=attempt, strategy_owner="US_STANDARD",
            symbol="AMD", side="BUY", qty=2, price_usd="100",
            broker_cash_usd="400", _conn=conn,
        )
    with pytest.raises(ReservationConflict, match="not_authoritatively_proven"):
        auth.release_proven_pre_submit_abort(
            lease=lease, attempt_id=attempt, client_order_key=cok,
        )
    ledger.release_before_submit(identity, attempt_id=attempt)
    auth.release_lease(lease=lease)
    monkeypatch.setenv("US_ACCOUNT_RESERVATION_MODE", "ENFORCE")
    monkeypatch.setattr(
        repos, "_us_execution_claim_scope",
        lambda: {
            "env": "practice", "account_id": account,
            "market": "US", "trading_epoch_id": "next-afternoon-epoch",
        },
    )
    sweep = reconcile_proven_reservations_once(
        env="practice", trade_date="2026-10-20",
    )
    assert sweep["released"] == 1 and sweep["unresolved"] == 0, sweep
    with pg_engine.connect() as conn:
        from sqlalchemy import text
        state = conn.execute(text("""
            SELECT state,terminal_evidence
            FROM us_execution_account_reservations WHERE action_key=:id
        """), {"id": attempt}).mappings().one()
    assert state["state"] == "RELEASED"
    assert state["terminal_evidence"] == "DURABLE_ABANDONED_PRE_SUBMIT"
