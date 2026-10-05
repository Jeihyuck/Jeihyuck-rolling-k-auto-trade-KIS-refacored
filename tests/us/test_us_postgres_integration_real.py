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
