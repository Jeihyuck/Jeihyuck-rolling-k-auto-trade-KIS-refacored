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
        # PR311: shared execution claims must have the same scope columns as
        # production migration 0062 before any claim/observation code runs.
        # This fixture runs in a disposable PostgreSQL service only.
        for column in ("portfolio_epoch_id", "position_cycle_id"):
            conn.exec_driver_sql(
                f"ALTER TABLE us_execution_claims ADD COLUMN IF NOT EXISTS {column} TEXT"
            )
        # PR311: the US fixture is deliberately lean (no KR portfolio tables).
        # Mirror only the additive US claim/order lineage columns of migration
        # 0062. The canonical migration SQL is separately audited/tested.
        for table in (
            "us_orders", "us_order_intents", "us_order_events", "us_fills",
            "us_positions", "us_execution_claims",
        ):
            for column in ("portfolio_epoch_id", "position_cycle_id"):
                conn.exec_driver_sql(
                    f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} TEXT"
                )
        for table in (
            "us_orders", "us_order_intents", "us_order_events", "us_fills",
            "us_positions",
        ):
            conn.exec_driver_sql(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS account_id TEXT"
            )
        # Migration 0052 rekeys snapshots by trading epoch. The isolated
        # fixture cannot run all KR/epoch DDL, so mirror precisely the
        # *production* scoped uniqueness before testing original BUY writes.
        # Without it repos.save_position_snapshot fails on ON CONFLICT rather
        # than exercising the lineage assertions.
        conn.exec_driver_sql(
            "ALTER TABLE us_positions "
            "DROP CONSTRAINT IF EXISTS us_positions_as_of_symbol_exchange_key"
        )
        conn.exec_driver_sql(
            "DROP INDEX IF EXISTS uq_us_positions_epoch_asof_symbol_exchange"
        )
        conn.exec_driver_sql(
            "CREATE UNIQUE INDEX uq_us_positions_epoch_asof_symbol_exchange "
            "ON us_positions(trading_epoch_id, as_of, symbol, exchange)"
        )
    # Fail the fixture immediately on missing 0062 columns, rather than
    # reporting misleading downstream KIS pre-submit/claim failures.
    with engine.connect() as conn:
        for table in ("us_orders", "us_fills", "us_execution_claims"):
            columns = {
                row[0] for row in conn.execute(text("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = current_schema() AND table_name=:table
                """), {"table": table}).all()
            }
            assert {"portfolio_epoch_id", "position_cycle_id"} <= columns, (
                f"PR311 fixture missing 0062 scope columns: {table}"
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
    # Exercise the practice-order preflight, not the CI DRY_RUN=1 guard.
    # This selection-only test never submits a broker order.
    monkeypatch.setenv("DRY_RUN", "0")
    monkeypatch.setenv("KIS_ENV", "practice")
    # Permission gates remain enabled in production; isolate the risk-budget
    # selector with a fully authorized *mock-only* practice context.
    monkeypatch.setenv("DISABLE_LIVE_TRADING", "0")
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "1")
    monkeypatch.setenv("US_LIVE_TRADING_ENABLED", "1")
    monkeypatch.setenv("US_ORDER_ARMED", "1")
    monkeypatch.setenv("SIGNAL_ONLY", "0")
    monkeypatch.setenv("RUN_MODE", "TRADE")
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
    assert [item["symbol"] for item in accepted] == ["KO"], {"rejected": rejected, "projected_state": state}
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


@pytest.mark.parametrize("owner,symbol", [
    ("TQQQ_INFINITE", "TQQQ"),
    ("US_STANDARD", "AMGN"),
])
def test_claim_reservation_outbox_triplet_rolls_back_as_one(pg_engine, owner, symbol):
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
    # Both parametrized owners share this real PostgreSQL database. Keep
    # their account-reservation leases distinct to avoid a cross-owner
    # reservation idempotency collision that is unrelated to rollback.
    fingerprint = hashlib.sha256(
        ("stage-123-synthetic-test:" + owner).encode("utf-8")
    ).hexdigest()
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
        strategy_owner=owner, lifecycle_id="stage123-cycle",
        action="BUY", trade_date=date(2026, 10, 20),
    )
    key = f"{owner}:stage123-cycle:2026-10-20:BUY"
    attempt = "stage123-reserved-attempt:" + owner
    event = {
        "event_id": str(uuid4()), "trade_date": "2026-10-20",
        "event_type": "BROKER_CLAIM_PREPARED",
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "session": "am", "session_run_id": "synthetic",
        "tick_id": "synthetic", "client_order_key": key,
        "submit_attempt_id": attempt, "symbol": symbol, "side": "BUY",
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
            strategy_owner=owner,
            symbol=symbol, side="BUY", qty=3,
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
        strategy_owner=owner, symbol=symbol, side="BUY", qty=3,
    )
    from trader.us.execution.account_authority import AuthorityUnavailable
    with pytest.raises(AuthorityUnavailable, match="STALE"):
        authority.assert_reserved_submit_authority(
            attempt_id=attempt, client_order_key=key,
            strategy_owner=("US_STANDARD" if owner == "TQQQ_INFINITE" else "TQQQ_INFINITE"),
            symbol=symbol, side="BUY", qty=3,
        )
    # A lost HTTP response must NOT cause a second identical broker POST
    # from the same Claim/Attempt. The initial boundary transitions Attempt
    # to SUBMITTED; a second attempt must fail even while reservation remains.
    ledger.record_observation(
        identity, attempt_id=attempt, state="SUBMITTED",
        cumulative_filled_qty=None, authoritative=False,
    )
    with pytest.raises(AuthorityUnavailable, match="STALE"):
        authority.assert_reserved_submit_authority(
            attempt_id=attempt, client_order_key=key,
            strategy_owner=owner, symbol=symbol, side="BUY", qty=3,
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



def test_stage123_default_on_schema_preflight_requires_actual_rls_migration(pg_engine):
    """Production activation must stop before orders on missing DDL or RLS.

    All DDL here runs ONLY in disposable PBCORE_TEST_POSTGRES_URL.
    """
    from pathlib import Path
    from sqlalchemy import text
    from scripts.us_stage123_paper_preflight import validate_stage123_prerequisites

    env = {
        "KIS_ENV": "practice", "US_PAPER_TRADING_ENABLED": "1",
        "DRY_RUN": "0", "US_OFFLINE": "0", "US_SIGNAL_ONLY": "0",
        "US_STAGE123_PAPER_PROFILE": "ON",
        "US_ATOMIC_CLAIM_OUTBOX_ENABLED": "1",
        "US_ACCOUNT_RESERVATION_MODE": "ENFORCE",
        "US_RECONCILE_WORKER_MODE": "RESERVATION_ONLY",
        "US_SHARED_KIS_LIMITER_ENABLED": "1",
        "US_EXIT_QUOTE_PREFETCH_ENABLED": "1",
        "US_EARLY_HARD_STOP_PILOT_ENABLED": "1",
        "US_EARLY_MULTI_HARD_STOP_ENABLED": "1",
        "US_EARLY_SOFT_STOP_PILOT_ENABLED": "1",
    }
    scope = {"env": "practice", "account_id": "a" * 64,
             "trading_epoch_id": "verified-us-epoch"}
    with pg_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS us_execution_account_reservations"))
        conn.execute(text("DROP TABLE IF EXISTS us_execution_authority_leases"))
    with pytest.raises(RuntimeError, match="migration_0056_missing"):
        validate_stage123_prerequisites(engine=pg_engine, scope=scope, env=env)
    with pg_engine.begin() as conn:
        conn.exec_driver_sql(Path(
            "migrations/0056_us_execution_authority_reservations.sql"
        ).read_text())
    assert validate_stage123_prerequisites(
        engine=pg_engine, scope=scope, env=env,
    )["status"] == "PASS"
    with pg_engine.begin() as conn:
        conn.execute(text(
            "ALTER TABLE us_execution_account_reservations DISABLE ROW LEVEL SECURITY"
        ))
    with pytest.raises(RuntimeError, match="migration_0056_missing_or_rls_off"):
        validate_stage123_prerequisites(engine=pg_engine, scope=scope, env=env)
    with pg_engine.begin() as conn:
        conn.execute(text(
            "ALTER TABLE us_execution_account_reservations ENABLE ROW LEVEL SECURITY"
        ))


def test_p0_proven_unsent_buy_intent_postgres_terminalization(pg_engine, monkeypatch):
    """#341: close only exact authoritative pre-HTTP attempts; never broker-possible ones."""
    import json
    from sqlalchemy import text
    from trader.us.db import repos

    monkeypatch.setattr(repos, "_active_us_epoch", lambda conn, required=True: "p0-epoch")

    def seed(key, *, positive=False, active=False, unverified=False, order=False):
        attempt = f"{key}-attempt"
        with pg_engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO us_execution_claims
                  (action_key,env,account_id,market,trading_epoch_id,strategy_owner,
                   lifecycle_id,action,target_qty,action_state,active_attempt_id,trade_date)
                VALUES (:action,'practice','p0-account','US','p0-epoch','US_STANDARD',
                        :action,'NEW_POSITION_BUY',1,:state,:active,'2026-11-09')
            """), {"action": key, "state": "IN_FLIGHT" if active else "OPEN",
                   "active": attempt if active else None})
            conn.execute(text("""
                INSERT INTO us_execution_attempts
                  (attempt_id,action_key,attempt_no,requested_qty,client_order_key,
                   attempt_state,authoritative)
                VALUES (:attempt,:action,1,1,:key,'ABANDONED_PRE_SUBMIT',:verified)
            """), {"attempt": attempt, "action": key, "key": key,
                   "verified": not unverified})
            conn.execute(text("""
                INSERT INTO us_order_intents
                  (trade_date,client_order_key,symbol,exchange,side,qty,status,
                   trading_epoch_id,meta)
                VALUES ('2026-11-09',:key,'AMGN','NASDAQ','BUY',1,'PENDING',
                        'p0-epoch',CAST(:meta AS jsonb))
            """), {"key": key, "meta": json.dumps({"submit_attempt_id": attempt,
                      "strategy_owner": "US_STANDARD", "position_action": "NEW_POSITION_BUY"})})
            for kind in (("BROKER_SUBMIT_ABORTED_PRE_IO", "BROKER_ACK_RECEIVED")
                         if positive else ("BROKER_SUBMIT_ABORTED_PRE_IO",)):
                conn.execute(text("""
                    INSERT INTO us_order_events
                      (event_id,trade_date,event_type,event_timestamp,client_order_key,
                       submit_attempt_id,symbol,side,requested_qty,idempotency_key,trading_epoch_id)
                    VALUES (gen_random_uuid(),'2026-11-09',:kind,now(),:key,:attempt,
                            'AMGN','BUY',1,:idem,'p0-epoch')
                """), {"kind": kind, "key": key, "attempt": attempt,
                       "idem": f"{key}:{kind}"})
            if order:
                conn.execute(text("""
                    INSERT INTO us_orders
                      (trade_date,client_order_key,symbol,exchange,side,qty_requested,
                       status,dry_run,trading_epoch_id)
                    VALUES ('2026-11-09',:key,'AMGN','NASDAQ','BUY',1,'ACK',false,'p0-epoch')
                """), {"key": key})
        return attempt

    seed("p0-safe")
    seed("p0-broker-ack", positive=True)
    seed("p0-active-claim", active=True)
    seed("p0-unverified-attempt", unverified=True)
    seed("p0-existing-order", order=True)

    assert repos.close_proven_unsent_us_buy_intent(
        client_order_key="p0-safe", attempt_id="p0-safe-attempt",
        trade_date="2026-11-09", failure_code="US_ENTRY_TIMING_BINDING_INVALID",
    )
    # Exact same attempt is idempotent; a changed attempt is not.
    assert repos.close_proven_unsent_us_buy_intent(
        client_order_key="p0-safe", attempt_id="p0-safe-attempt",
        trade_date="2026-11-09", failure_code="US_ENTRY_TIMING_BINDING_INVALID",
    )
    assert not repos.close_proven_unsent_us_buy_intent(
        client_order_key="p0-safe", attempt_id="p0-forged-attempt",
        trade_date="2026-11-09", failure_code="US_ENTRY_TIMING_BINDING_INVALID",
    )
    for key in ("p0-broker-ack", "p0-active-claim", "p0-unverified-attempt", "p0-existing-order"):
        assert not repos.close_proven_unsent_us_buy_intent(
            client_order_key=key, attempt_id=f"{key}-attempt",
            trade_date="2026-11-09", failure_code="US_ENTRY_TIMING_BINDING_INVALID",
        ), key
    with pg_engine.connect() as conn:
        rows = dict(conn.execute(text(
            "SELECT client_order_key,status FROM us_order_intents "
            "WHERE client_order_key LIKE 'p0-%'"
        )).all())
        safe_meta = conn.execute(text(
            "SELECT meta FROM us_order_intents WHERE client_order_key='p0-safe'"
        )).scalar_one()
    assert rows["p0-safe"] == "BLOCKED"
    assert all(rows[key] == "PENDING" for key in (
        "p0-broker-ack", "p0-active-claim", "p0-unverified-attempt", "p0-existing-order"))
    assert safe_meta["pre_io_abort_stage"] == "BEFORE_KIS_HTTP"
    assert safe_meta["pre_io_abort_attempt_id"] == "p0-safe-attempt"


@pytest.fixture()
def amgn_pg_engine(monkeypatch):
    """Isolated REAL PostgreSQL CI DB; never poison other suite fixtures.

    The original fixture uses a shared us_ci database and resets tables per
    test. This AMGN original lineage test requires migration 0052 / 0062
    scope and an active trading_epochs record; a shared database corrupted
    unrelated tests after the focused test step. Use a dedicated disposable
    localhost ci database, and drop it with FORCE after verification.
    """
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import make_url
    import trader.us.db.repos as repos

    source=make_url(os.environ["PBCORE_TEST_POSTGRES_URL"])
    # Never CREATE/DROP a nonlocal or non-ci database.
    if (source.host not in ("localhost","127.0.0.1")
            or source.database != "us_ci"
            or not source.drivername.startswith("postgresql")):
        pytest.skip("AMGN isolated original BUY test requires localhost us_ci")
    db_name="us_ci_amgn_original_buy_proof"
    admin=create_engine(
        source.set(database="postgres"),
        isolation_level="AUTOCOMMIT",future=True,
    )
    with admin.connect() as conn:
        conn.execute(text('DROP DATABASE IF EXISTS "'+db_name+'" WITH (FORCE)'))
        conn.execute(text('CREATE DATABASE "'+db_name+'"'))
    engine=create_engine(source.set(database=db_name),future=True)
    try:
        with engine.begin() as conn:
            for path in (
                "migrations/0038_us_agent_tables.sql",
                "migrations/0043_us_fills_idempotency_and_order_reconcile_fix.sql",
                "migrations/0046_us_orders_committed_notional.sql",
                "migrations/0047_us_order_events_profit_lifecycle.sql",
                "migrations/0053_execution_state_claims.sql",
                "migrations/0054_semantic_action_instances.sql",
            ):
                conn.exec_driver_sql(open(path,encoding="utf-8").read())
            for table in (
                "us_order_intents","us_orders","us_fills","us_positions",
                "us_order_events","us_profit_capture_lifecycle",
            ):
                conn.exec_driver_sql(
                    f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT"
                )
            for table in (
                "us_orders","us_order_intents","us_order_events",
                "us_fills","us_positions","us_execution_claims",
            ):
                for column in ("portfolio_epoch_id","position_cycle_id"):
                    conn.exec_driver_sql(
                        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} TEXT"
                    )
            for table in (
                "us_orders","us_order_intents","us_order_events",
                "us_fills","us_positions",
            ):
                conn.exec_driver_sql(
                    f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS account_id TEXT"
                )
            conn.exec_driver_sql(
                "ALTER TABLE us_positions DROP CONSTRAINT IF EXISTS "
                "us_positions_as_of_symbol_exchange_key"
            )
            conn.exec_driver_sql(
                "CREATE UNIQUE INDEX uq_us_positions_epoch_asof_symbol_exchange "
                "ON us_positions(trading_epoch_id,as_of,symbol,exchange)"
            )
        monkeypatch.setattr(repos,"_get_engine_or_none",lambda:engine)
        yield engine
    finally:
        engine.dispose()
        with admin.connect() as conn:
            conn.execute(text('DROP DATABASE IF EXISTS "'+db_name+'" WITH (FORCE)'))
        admin.dispose()


def test_amgn_new_buy_original_scope_is_durable_through_fill_and_snapshot(
    amgn_pg_engine, monkeypatch,
):
    pg_engine = amgn_pg_engine

    """No network: real PostgreSQL order -> cumulative KIS evidence -> balance."""
    import hashlib
    import json
    from sqlalchemy import text
    from trader.us.db import repos
    from trader.us.entry_exit_contract import build_us_entry_exit_contract

    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("US_SWING_EXIT_MODE", "LEGACY_V2")
    account = "practice:ci:01"
    broker_hash = hashlib.sha256(account.encode()).hexdigest()
    epoch = "epoch-ci-amgn"
    portfolio = "portfolio-ci-amgn"
    cycle = "cycle-ci-amgn"
    key = "original-buy-amgn-ci"
    order_no = "0000039784"
    contract = build_us_entry_exit_contract({
        "symbol": "AMGN", "strategy_owner": "US_STANDARD",
        "position_action": "NEW_POSITION_BUY", "book": "SWING_BOOK",
        "horizon": "SWING_CARRY", "entry_reason": "CI_BUY",
    })
    meta = {
        "env": "practice", "strategy_owner": "US_STANDARD", "sleeve_id": "US_STANDARD",
        "broker_account_hash": broker_hash, "trading_epoch_id": epoch,
        "portfolio_epoch_id": portfolio, "position_cycle_id": cycle,
        "position_lifecycle_id": cycle,
        "entry_exit_contract": contract,
        "entry_exit_contract_sha256": contract["sha256"],
        "entry_exit_contract_version": contract["version"],
        "pre_order_holding_qty": 0, "book": "SWING_BOOK",
        "horizon": "SWING_CARRY", "exit_policy": "US_SWING_DEFAULT",
    }
    with pg_engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS trading_epochs(
                trading_epoch_id TEXT PRIMARY KEY,
                env TEXT NOT NULL,
                account_id TEXT NOT NULL,
                status TEXT NOT NULL,
                reason TEXT NOT NULL DEFAULT 'CI_ORIGINAL_BUY_SCOPE',
                started_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        conn.execute(text("""
            INSERT INTO trading_epochs(trading_epoch_id,env,account_id,status,reason)
            VALUES (:epoch,'practice',:account,'ACTIVE','CI_ORIGINAL_BUY_SCOPE')
            ON CONFLICT(trading_epoch_id) DO UPDATE SET
                account_id=EXCLUDED.account_id,status='ACTIVE'
        """), {"epoch": epoch, "account": account})
        conn.execute(text("""
            INSERT INTO us_order_intents
                (trade_date,client_order_key,symbol,exchange,side,qty,
                 strategy,status,trading_epoch_id,account_id,portfolio_epoch_id,
                 position_cycle_id,meta)
            VALUES ('2026-11-10',:key,'AMGN','NASDAQ','BUY',6,
                    'us_pb1','SENT',:epoch,:account,:portfolio,:cycle,
                    CAST(:meta AS jsonb))
        """), {"key": key, "epoch": epoch, "account": broker_hash,
               "portfolio": portfolio, "cycle": cycle, "meta": json.dumps(meta)})
        conn.execute(text("""
            INSERT INTO us_orders
                (trade_date,client_order_key,symbol,exchange,side,
                 qty_requested,qty_filled,order_no,status,trading_epoch_id,
                 account_id,portfolio_epoch_id,position_cycle_id,meta)
            VALUES ('2026-11-10',:key,'AMGN','NASDAQ','BUY',6,0,
                    :order_no,'ACK',:epoch,:account,:portfolio,:cycle,
                    CAST(:meta AS jsonb))
        """), {"key": key, "order_no": order_no, "epoch": epoch,
               "account": broker_hash, "portfolio": portfolio,
               "cycle": cycle, "meta": json.dumps(meta)})
    monkeypatch.setattr(repos, "_active_us_epoch", lambda conn=None: epoch)
    monkeypatch.setattr(repos, "_us_state_epoch_id", lambda conn=None: epoch)

    # Native-shaped BUY partial fill must remain in the ORIGINAL order and
    # cycle; a later cumulative fill cannot become a new position cycle.
    partial_evidence = {
        **meta, "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
        "cumulative_filled_qty": 2,
    }
    first_partial = repos.mark_order_filled_by_reconcile(
        order_no=order_no, client_order_key=key, symbol="AMGN", side="BUY",
        filled_qty=2, requested_qty=6, cumulative_filled_qty=2,
        avg_price_usd=461.8, trade_date="2026-11-10",
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        source="fills_by_order_no", meta=partial_evidence,
    )
    assert first_partial["status"] == "OK", first_partial
    with pg_engine.connect() as conn:
        before_late_fill = conn.execute(text("""
            SELECT status,qty_filled,position_cycle_id
              FROM us_orders WHERE order_no=:order_no
        """), {"order_no": order_no}).mappings().one()
        before_qty = conn.execute(text("""
            SELECT qty FROM us_fills WHERE order_no=:order_no
              AND side='BUY' AND symbol='AMGN'
        """), {"order_no":order_no}).scalar_one()
    assert (before_late_fill["status"],before_late_fill["qty_filled"],
            before_late_fill["position_cycle_id"]) == (
        "PARTIALLY_FILLED",2,cycle,
    )
    assert before_qty == 2

    evidence = {
        **meta, "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
        "cumulative_filled_qty": 6,
    }
    result = repos.mark_order_filled_by_reconcile(
        order_no=order_no, client_order_key=key, symbol="AMGN", side="BUY",
        filled_qty=6, requested_qty=6, cumulative_filled_qty=6,
        avg_price_usd=461.8, trade_date="2026-11-10",
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        source="fills_by_order_no", meta=evidence,
    )
    assert result["status"] == "OK", result
    with pg_engine.connect() as conn:
        fill = conn.execute(text("""
            SELECT qty,account_id,portfolio_epoch_id,position_cycle_id,meta
            FROM us_fills WHERE trade_date='2026-11-10'
              AND symbol='AMGN' AND order_no=:order_no
        """), {"order_no": order_no}).mappings().one()
        assert fill["qty"] == 6
        assert (fill["account_id"],fill["portfolio_epoch_id"],fill["position_cycle_id"]) == (
            broker_hash, portfolio, cycle,
        )
        assert fill["meta"]["entry_exit_contract_sha256"] == contract["sha256"]

    # A source fill row with mismatched top-level economic scope is not
    # valid original BUY evidence even if signed JSON meta still looks valid.
    from trader.us.original_buy_lineage import load_verified_us_original_buy_scope
    identity = {"env": "practice", "account_id": account,
                "trading_epoch_id": epoch, "strategy_owner": "US_STANDARD"}
    def original_proof():
        return load_verified_us_original_buy_scope(
            symbol="AMGN", trade_date="2026-11-10", qty=6,
            identity=identity,
        )
    assert original_proof() is not None
    with pg_engine.begin() as conn:
        conn.execute(text("""
            UPDATE us_fills SET position_cycle_id='foreign-cycle'
            WHERE trade_date='2026-11-10' AND symbol='AMGN'
              AND order_no=:order_no
        """), {"order_no": order_no})
    assert original_proof() is None
    with pg_engine.begin() as conn:
        conn.execute(text("""
            UPDATE us_fills SET position_cycle_id=:cycle
            WHERE trade_date='2026-11-10' AND symbol='AMGN'
              AND order_no=:order_no
        """), {"cycle": cycle, "order_no": order_no})
    assert original_proof() is not None

    saved = repos.save_position_snapshot(
        [{
            "symbol": "AMGN", "exchange": "NASDAQ", "qty": 6,
            "avg_cost": 461.8, "current_px": 465.875,
            "balance_source": "kis_balance_authoritative", "meta": {},
        }],
        trade_date="2026-11-10",
        balance_fetch_status="OK", balance_parse_status="OK",
        authoritative_positions=True,
        preserve_previous_positions=False,
    )
    assert saved == 1
    with pg_engine.connect() as conn:
        position = conn.execute(text("""
            SELECT qty,account_id,portfolio_epoch_id,position_cycle_id,meta
            FROM us_positions WHERE as_of='2026-11-10' AND symbol='AMGN'
              AND trading_epoch_id=:epoch
        """), {"epoch": epoch}).mappings().one()
    assert position["qty"] == 6
    assert (position["account_id"],position["portfolio_epoch_id"],
            position["position_cycle_id"]) == (broker_hash,portfolio,cycle)
    assert position["meta"]["position_lifecycle_id"] == cycle
    assert position["meta"]["strategy_owner"] == "US_STANDARD"
    assert position["meta"]["entry_exit_contract_sha256"] == contract["sha256"]
    assert position["meta"]["lineage_identity_status"] == "VERIFIED_ORIGINAL_KIS_BUY"

    # The following US session must not erase the just-established owner,
    # cycle, signed BUY contract or original provenance on a fresh KIS balance.
    first_lifecycle = {
        "lifecycle_id": cycle, "position_cycle_id": cycle,
        "portfolio_epoch_id": portfolio, "trading_epoch_id": epoch,
        "account_id": account, "strategy_owner": "US_STANDARD",
        "env": "practice", "is_open": True,
        "opened_trade_date": "2026-11-10",
        "opened_at": "2026-11-10T16:26:14+00:00",
        "opened_at_source": "confirmed_buy_fill",
        "high_watermark": 470.0,
        "high_watermark_source": "us_position_risk_state",
        "high_watermark_at": "2026-11-10T20:00:00+00:00",
        "holding_trade_days": 1,
        "entry_policy": {"entry_exit_contract_sha256": contract["sha256"]},
    }
    with pg_engine.begin() as conn:
        conn.execute(text("""
            -- The PostgreSQL service is shared by focused + complete US
            -- suites; a four-column shortcut table poisons later tests.
            -- Match migration 0052's actual risk-state schema.
            CREATE TABLE IF NOT EXISTS us_position_risk_state(
                trade_date DATE NOT NULL,
                symbol TEXT NOT NULL,
                soft_stop_breach_count INTEGER NOT NULL DEFAULT 0,
                first_soft_stop_seen_at TIMESTAMPTZ,
                last_soft_stop_seen_at TIMESTAMPTZ,
                lowest_price_since_breach NUMERIC,
                last_price NUMERIC,
                last_pnl_pct NUMERIC,
                state JSONB NOT NULL DEFAULT '{}'::jsonb,
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                trading_epoch_id TEXT NOT NULL
            )
        """))
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS
            uq_us_position_risk_state_epoch_date_symbol
            ON us_position_risk_state(trading_epoch_id, trade_date, symbol)
        """))
        conn.execute(text("""
            INSERT INTO us_position_risk_state(trading_epoch_id,trade_date,symbol,state)
            VALUES (:epoch,'2026-11-10','AMGN',CAST(:state AS jsonb))
            ON CONFLICT(trading_epoch_id,trade_date,symbol)
            DO UPDATE SET state=EXCLUDED.state
        """), {"epoch": epoch, "state": json.dumps({"lifecycle": first_lifecycle})})

    assert repos.save_position_snapshot(
        [{"symbol": "AMGN", "exchange": "NASDAQ", "qty": 6,
          "avg_cost": 461.8, "current_px": 469.0,
          "balance_source": "kis_balance_authoritative", "meta": {}}],
        trade_date="2026-11-11",
        balance_fetch_status="OK", balance_parse_status="OK",
        authoritative_positions=True, preserve_previous_positions=False,
    ) == 1
    with pg_engine.connect() as conn:
        tomorrow = conn.execute(text("""
            SELECT qty, account_id, portfolio_epoch_id, position_cycle_id, meta
            FROM us_positions WHERE as_of='2026-11-11' AND symbol='AMGN'
              AND trading_epoch_id=:epoch
        """), {"epoch": epoch}).mappings().one()
    assert tomorrow["qty"] == 6
    assert (tomorrow["account_id"], tomorrow["portfolio_epoch_id"],
            tomorrow["position_cycle_id"]) == (broker_hash, portfolio, cycle)
    assert tomorrow["meta"]["position_lifecycle_id"] == cycle
    assert tomorrow["meta"]["lineage_identity_status"] == "VERIFIED_CURRENT_EPOCH_CONTINUITY"
    assert tomorrow["meta"]["entry_exit_contract_sha256"] == contract["sha256"]

    # An actual PostgreSQL / repo-reconciled partial SELL in the SAME scoped
    # cycle must not orphan the original BUY V2/V3 on the next KIS balance day.
    # This is an offline native-evidence fixture; never a real KIS HTTP call.
    # The original V2 TP1 stage is reserved on the BUY cycle before SELL HTTP.
    tp1_key = "partial-sell-amgn-ci"
    repos.mark_us_profit_capture_stage(
        "2026-11-12", "AMGN", "tp1", position_lifecycle_id=cycle,
        order_key=tp1_key, qty=2, status="PENDING",
    )
    pending = repos.load_us_profit_capture_state(
        "2026-11-12", ["AMGN"], {"AMGN": cycle},
    )["AMGN"]
    assert pending["tp1_pending"] is True and pending["tp1_done"] is False
    with pg_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO us_position_risk_state(trading_epoch_id,trade_date,symbol,state)
            VALUES (:epoch,'2026-11-11','AMGN',CAST(:state AS jsonb))
            ON CONFLICT(trading_epoch_id,trade_date,symbol)
            DO UPDATE SET state=EXCLUDED.state
        """), {"epoch": epoch, "state": json.dumps({"lifecycle": first_lifecycle})})
        conn.execute(text("""
            INSERT INTO us_orders(
                trade_date,client_order_key,symbol,exchange,side,
                qty_requested,qty_filled,order_no,status,trading_epoch_id,
                account_id,portfolio_epoch_id,position_cycle_id,meta)
            VALUES ('2026-11-12','partial-sell-amgn-ci','AMGN','NASDAQ','SELL',
                    2,0,'0000039785','ACK',:epoch,:account,:portfolio,:cycle,
                    CAST(:meta AS jsonb))
        """), {"epoch": epoch, "account": broker_hash,
                 "portfolio": portfolio, "cycle": cycle,
                 "meta": json.dumps({**meta, "pre_order_holding_qty": 6,
                                      "profit_capture_stage": "tp1"})})

    actual_sell = repos.mark_order_filled_by_reconcile(
        order_no="0000039785", client_order_key="partial-sell-amgn-ci",
        symbol="AMGN", side="SELL", filled_qty=2, requested_qty=2,
        cumulative_filled_qty=2, avg_price_usd=475.0,
        trade_date="2026-11-12", evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        source="fills_by_order_no",
        meta={**meta, "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
              "cumulative_filled_qty": 2},
    )
    assert actual_sell["status"] == "OK", actual_sell
    tp1 = repos.load_us_profit_capture_state(
        "2026-11-12", ["AMGN"], {"AMGN": cycle},
    )["AMGN"]
    assert tp1["tp1_done"] is True and tp1["tp1_pending"] is False
    assert int(tp1["meta"]["tp1_filled_qty"]) == 2
    assert tp1["tp2_done"] is False
    # Replaying the same cumulative KIS order evidence must not count 4 SOLD.
    again = repos.mark_order_filled_by_reconcile(
        order_no="0000039785", client_order_key=tp1_key,
        symbol="AMGN", side="SELL", filled_qty=2, requested_qty=2,
        cumulative_filled_qty=2, avg_price_usd=475.0,
        trade_date="2026-11-12", evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        source="fills_by_order_no",
        meta={**meta, "profit_capture_stage": "tp1"},
    )
    assert again["status"] == "OK", again
    replay = repos.load_us_profit_capture_state(
        "2026-11-13", ["AMGN"], {"AMGN": cycle},
    )["AMGN"]
    assert replay["tp1_done"] is True and int(replay["meta"]["tp1_filled_qty"]) == 2

    with pg_engine.connect() as conn:
        sell = conn.execute(text("""
            SELECT o.qty_filled AS order_filled, o.status, o.position_cycle_id,
                   f.qty AS fill_qty, f.position_cycle_id AS fill_cycle,
                   f.meta AS fill_meta
              FROM us_orders o JOIN us_fills f
                ON f.order_no=o.order_no AND f.client_order_key=o.client_order_key
               AND f.trade_date=o.trade_date AND f.account_id=o.account_id
             WHERE o.client_order_key='partial-sell-amgn-ci'
        """)).mappings().one()
    assert sell["status"] == "FILLED"
    assert (sell["order_filled"], sell["fill_qty"]) == (2, 2)
    assert (sell["position_cycle_id"], sell["fill_cycle"]) == (cycle, cycle)
    assert sell["fill_meta"]["fill_evidence_type"] == "KIS_ORDER_CUMULATIVE_ACTUAL"

    assert repos.save_position_snapshot(
        [{"symbol": "AMGN", "exchange": "NASDAQ", "qty": 4,
          "avg_cost": 461.8, "current_px": 474.0,
          "balance_source": "kis_balance_authoritative", "meta": {}}],
        trade_date="2026-11-13",
        balance_fetch_status="OK", balance_parse_status="OK",
        authoritative_positions=True, preserve_previous_positions=False,
    ) == 1
    with pg_engine.connect() as conn:
        carried = conn.execute(text("""
            SELECT qty,account_id,portfolio_epoch_id,position_cycle_id,meta
              FROM us_positions WHERE as_of='2026-11-13' AND symbol='AMGN'
                AND trading_epoch_id=:epoch
        """), {"epoch": epoch}).mappings().one()
    assert carried["qty"] == 4
    assert (carried["account_id"], carried["portfolio_epoch_id"],
            carried["position_cycle_id"]) == (broker_hash, portfolio, cycle)
    assert carried["meta"]["entry_exit_contract"] == contract
    assert carried["meta"]["entry_exit_contract_sha256"] == contract["sha256"]
    assert carried["meta"]["lineage_identity_status"] == "VERIFIED_CURRENT_EPOCH_CONTINUITY"
    assert carried["meta"]["lineage_verified_partial_sell_shares"] == 2

    # TP1 is already DONE: only the original signed TP2 may emit another
    # intent. This is the actual policy builder + PG stage ledger, with a
    # simulated fresh authoritative KIS average (not an authenticated GET).
    from trader.us.market_state_overlay import build_profit_capture_intents
    stage_inputs = {
        "symbol": "AMGN", "qty": 4, "orderable_qty": 4,
        "current_price": 490.0, "position_lifecycle_id": cycle,
        "entry_exit_contract": contract,
        "broker_avg_price": 461.8, "broker_avg_price_currency": "USD",
        "broker_avg_price_source": "kis_pchs_avg_pric",
        "broker_avg_price_asof": "2026-11-13T16:00:00+00:00",
        "balance_source": "kis_balance_authoritative",
        "authoritative_positions": True,
        "meta": {**carried["meta"], "entry_exit_contract": contract},
    }
    from datetime import datetime as dt, timezone as tz
    next_tp = build_profit_capture_intents(
        [stage_inputs], {"profit_capture_enabled": True, "market_state": "NORMAL"},
        now=dt(2026, 11, 13, 16, tzinfo=tz.utc), trade_date="2026-11-13",
    )
    assert len(next_tp) == 1, next_tp
    assert next_tp[0]["reason"] == "TAKE_PROFIT_TP2"
    assert next_tp[0]["meta"]["source_entry_contract_sha256"] == contract["sha256"]
    assert next_tp[0]["position_lifecycle_id"] == cycle
    assert not any(t.get("reason") == "TAKE_PROFIT_TP1" for t in next_tp)

    # Continue the real original-BUY -> actual BUY fill -> carry -> original
    # TP1 fill -> next-day original TP2 evaluation THROUGH THE ORDER ROUTER.
    # Only the broker API boundary is simulated; no real KIS calls occur.
    from trader.us.runner.trade_tick_runner import route_exit_orders_immediately
    from trader.us.execution import order_router
    from trader.execution_claims import DurableExecutionClaimRepo
    from trader.us.db.execution_claim_schema import (
        us_execution_attempts, us_execution_claims,
    )
    monkeypatch.setattr(repos, "_active_us_epoch",
                        lambda _conn=None, **_kw: epoch)
    monkeypatch.setattr(repos, "_us_state_epoch_id",
                        lambda _conn=None, **_kw: epoch)
    monkeypatch.setenv("WSL_RUN_MARKET", "")
    monkeypatch.setenv("US_ACCOUNT_RESERVATION_MODE", "OFF")
    monkeypatch.setenv("US_ATOMIC_CLAIM_OUTBOX_ENABLED", "0")
    monkeypatch.setattr(order_router, "resolve_dry_run_for_us_order", lambda: False)
    claim_repo = DurableExecutionClaimRepo(
        pg_engine, us_execution_claims, us_execution_attempts,
    )
    monkeypatch.setattr(repos, "_execution_claim_repo", lambda: claim_repo)
    monkeypatch.setattr(repos, "claim_execution_action", claim_repo.acquire)
    monkeypatch.setattr(
        repos, "record_execution_action_observation", claim_repo.record_observation,
    )
    monkeypatch.setattr(
        repos, "release_execution_action_before_submit", claim_repo.release_before_submit,
    )

    class TP2TestBroker:
        def __init__(self):
            self.posts = []

        def get_us_price(self, symbol, exchange):
            # Native-shaped *offline* quote; strict age/source/threshold gates
            # remain active. This is never authenticated KIS price evidence.
            import time
            assert symbol == "AMGN" and exchange == "NASDAQ"
            return {
                "_quote_quality": "FRESH",
                "_quote_source": "KIS_LIVE",
                "_quote_asof_epoch": time.time(),
                "output": {"last": 490.0},
            }

        def get_balance(self, **_kw):
            return {"positions": [{
                "symbol": "AMGN", "exchange": "NASDAQ",
                "qty": 4, "holding_qty": 4, "orderable_qty": 4,
                "avg_price": 461.8, "broker_avg_price_source": "kis_pchs_avg_pric",
                "currency": "USD",
            }]}

        def place_us_sell_order(self, symbol, exchange, qty, price):
            assert symbol == "AMGN" and exchange == "NASDAQ"
            assert qty > 0
            self.posts.append((symbol, qty, price))
            return {"ok": True, "order_no": "CI-AMGN-TP2-SELL"}

    broker = TP2TestBroker()
    tp2_intent = next_tp[0]
    # The future-session 2026-11-13 scenario runs on today's real CI wall
    # clock. At its *simulated pre-HTTP tick* refresh the decision timestamp,
    # not the frozen contract, TP quantity, or TP threshold; keep the strict
    # 30-second pre-HTTP freshness fence exercised by the actual router.
    from datetime import datetime as _wall_datetime, timezone as _wall_timezone
    tp2_intent.setdefault("meta", {})["last_profit_capture_at"] = (
        _wall_datetime.now(_wall_timezone.utc).isoformat()
    )
    tp2_intent["trade_date"] = "2026-11-13"
    tp2_intent.setdefault("strategy_owner", "US_STANDARD")
    tp2_intent.setdefault("sleeve_id", "US_STANDARD")
    tp2_intent.setdefault("holding_qty", 4)
    tp2_intent.setdefault("orderable_qty", 4)
    tp2_intent.setdefault("sellable_qty", 4)
    tp2_intent.setdefault("available_qty", 4)
    routed_tp2 = route_exit_orders_immediately(
        [tp2_intent], buy_daily_notional=0, position_count=1,
        effective_budget=10000, signal_only=False, kis_order_allowed=True,
        current_position_symbols={"AMGN"}, kis_client=broker,
    )
    assert len(broker.posts) == 1, routed_tp2
    assert routed_tp2["orders"][0]["status"] == "ACK", routed_tp2
    tp2_qty = int(broker.posts[0][1])
    # ACK is not TP2 completion; only actual native-format evidence can do so.
    pending_tp2 = repos.load_us_profit_capture_state(
        "2026-11-13", ["AMGN"], {"AMGN": cycle},
    )["AMGN"]
    assert pending_tp2["tp2_done"] is False
    tp2_fill = repos.mark_order_filled_by_reconcile(
        order_no="CI-AMGN-TP2-SELL",
        client_order_key=tp2_intent["client_order_key"],
        symbol="AMGN", side="SELL",
        filled_qty=tp2_qty, requested_qty=tp2_qty,
        cumulative_filled_qty=tp2_qty, avg_price_usd=490.0,
        trade_date="2026-11-13",
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        source="fills_by_order_no",
        meta={**meta, "profit_capture_stage": "tp2",
              "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
              "cumulative_filled_qty": tp2_qty},
    )
    assert tp2_fill["status"] == "OK", tp2_fill
    done_tp2 = repos.load_us_profit_capture_state(
        "2026-11-13", ["AMGN"], {"AMGN": cycle},
    )["AMGN"]
    assert done_tp2["tp1_done"] is True
    assert done_tp2["tp2_done"] is True and not done_tp2["tp2_pending"]
    assert int(done_tp2["meta"]["tp2_filled_qty"]) == tp2_qty
    repeated_tp2 = repos.mark_order_filled_by_reconcile(
        order_no="CI-AMGN-TP2-SELL",
        client_order_key=tp2_intent["client_order_key"],
        symbol="AMGN", side="SELL",
        filled_qty=tp2_qty, requested_qty=tp2_qty,
        cumulative_filled_qty=tp2_qty, avg_price_usd=490.0,
        trade_date="2026-11-13",
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        source="fills_by_order_no",
        meta={**meta, "profit_capture_stage": "tp2",
              "cumulative_filled_qty": tp2_qty},
    )
    assert repeated_tp2["status"] == "OK", repeated_tp2
    reloaded_tp2 = repos.load_us_profit_capture_state(
        "2026-11-13", ["AMGN"], {"AMGN": cycle},
    )["AMGN"]
    assert int(reloaded_tp2["meta"]["tp2_filled_qty"]) == tp2_qty
    with pg_engine.connect() as conn:
        persisted = conn.execute(text("""
            SELECT o.meta AS order_meta, f.meta AS fill_meta, f.qty AS fill_qty
            FROM us_orders o JOIN us_fills f
              ON f.order_no=o.order_no AND f.client_order_key=o.client_order_key
            WHERE o.order_no='CI-AMGN-TP2-SELL'
        """)).mappings().one()
    assert persisted["fill_qty"] == tp2_qty
    assert persisted["order_meta"]["source_entry_contract_sha256"] == contract["sha256"]
    assert persisted["fill_meta"]["position_lifecycle_id"] == cycle


def test_pg_original_signed_buy_contract_survives_raw_snapshot_upsert(pg_engine, monkeypatch):
    """Actual PG UPSERT must preserve V2/V3 and cycle after unproven broker copy."""
    import json
    from sqlalchemy import text
    from trader.us.db import repos
    from trader.us.entry_exit_contract import build_us_entry_exit_contract

    monkeypatch.setattr(repos, "_active_us_epoch",
                        lambda bind=None, **_kw: "fixture-original-scope")
    source = {
        "symbol": "LIFE", "strategy_owner": "US_STANDARD",
        "entry_style_selected": "ENTRY_PULLBACK",
        "entry_strategy": "us_pb1", "strategy": "us_pb1",
        "book": "SWING_BOOK", "horizon": "SWING_CARRY",
        "exit_policy": "US_SWING_DEFAULT",
        "position_action": "NEW_POSITION_BUY",
    }
    frozen = build_us_entry_exit_contract(source)
    changed = build_us_entry_exit_contract({
        **source, "entry_style_selected": "ENTRY_BREAKOUT",
    })
    assert frozen["sha256"] != changed["sha256"]
    original_meta = {
        "strategy_owner": "US_STANDARD", "entry_style_selected": "ENTRY_PULLBACK",
        "position_lifecycle_id": "cycle-original",
        "position_cycle_id": "cycle-original",
        "portfolio_epoch_id": "portfolio-fixture",
        "entry_exit_contract": frozen,
        "entry_exit_contract_sha256": frozen["sha256"],
        "entry_exit_contract_version": frozen["version"],
        "opened_trade_date": "2026-11-10",
        "lineage_identity_status": "VERIFIED_ORIGINAL_KIS_BUY",
    }
    with pg_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO us_positions
              (as_of,symbol,exchange,qty,avg_cost,current_px,unrealized_pnl_usd,
               trading_epoch_id,account_id,portfolio_epoch_id,position_cycle_id,meta)
            VALUES ('2026-11-11','LIFE','NASDAQ',6,60,62,12,
                    'fixture-original-scope','account-fingerprint','portfolio-fixture',
                    'cycle-original',CAST(:meta AS jsonb))
        """), {"meta": json.dumps(original_meta)})
    result = repos.save_position_snapshot([{
        "symbol": "LIFE", "exchange": "NASDAQ", "qty": 6,
        "avg_cost": 60, "current_px": 63,
        "meta": {
            "entry_style_selected": "ENTRY_BREAKOUT", "strategy_owner": "US_STANDARD",
            "book": "SWING_BOOK", "horizon": "SWING_CARRY",
            "exit_policy": "US_SWING_DEFAULT",
            "entry_exit_contract": changed,
            "entry_exit_contract_sha256": changed["sha256"],
            "entry_exit_contract_version": changed["version"],
        },
    }], trade_date="2026-11-11", authoritative_positions=False)
    assert result == 1
    with pg_engine.connect() as conn:
        row = conn.execute(text("""
          SELECT qty,current_px,account_id,portfolio_epoch_id,position_cycle_id,meta
          FROM us_positions WHERE symbol='LIFE' AND as_of='2026-11-11'
          AND trading_epoch_id='fixture-original-scope'
        """)).mappings().one()
    assert row["qty"] == 6 and float(row["current_px"]) == 63
    assert row["position_cycle_id"] == "cycle-original"
    assert row["portfolio_epoch_id"] == "portfolio-fixture"
    assert row["account_id"] == "account-fingerprint"
    assert row["meta"]["entry_exit_contract"] == frozen
    assert row["meta"]["entry_exit_contract_sha256"] == frozen["sha256"]
    assert row["meta"]["entry_style_selected"] == "ENTRY_PULLBACK"
    assert row["meta"]["lineage_identity_status"] == "VERIFIED_ORIGINAL_KIS_BUY"


def test_pg_tqqq_cycle_version_cas_active_and_new_cycle(pg_engine, monkeypatch):
    """Real PostgreSQL CAS: stale READY cannot overwrite an ACTIVE cycle."""
    from dataclasses import replace
    from datetime import date
    from sqlalchemy import text
    from trader.us.infinite.models import InfiniteState, Status
    from trader.us.infinite.repository import InfiniteRepository, StateCorruptionError
    from trader.us.infinite.policy_state import reserve_new_cycle

    # Run the real pre-0052 TQQQ migration on a disposable CI schema; mirror
    # the exact epoch composite PK implemented by the deployed 0052 migration.
    with pg_engine.begin() as conn:
        conn.exec_driver_sql("DROP TABLE IF EXISTS us_tqqq_infinite_state CASCADE")
        conn.exec_driver_sql(open("migrations/0048_add_us_tqqq_infinite_state.sql",
                                  encoding="utf-8").read())
        conn.exec_driver_sql(
            "ALTER TABLE us_tqqq_infinite_state ADD COLUMN trading_epoch_id TEXT"
        )
        conn.exec_driver_sql(
            "ALTER TABLE us_tqqq_infinite_state "
            "DROP CONSTRAINT us_tqqq_infinite_state_pkey"
        )
        conn.exec_driver_sql(
            "ALTER TABLE us_tqqq_infinite_state "
            "ADD PRIMARY KEY (trading_epoch_id,strategy_id,symbol)"
        )
    repo = InfiniteRepository(pg_engine)
    monkeypatch.setattr(repo, "_epoch_id",
                        lambda _conn, **_kw: "fixture-current-epoch")
    ready = InfiniteState(
        status=Status.READY, cycle_id="original-cycle",
        cycle_start_date=date(2026, 11, 11), version=1,
    )
    repo.save_state(ready)
    assert ready.version == 1
    active = replace(ready, status=Status.ACTIVE)
    repo.save_state(active)
    assert active.version == 2
    with pytest.raises(StateCorruptionError, match="STALE_VERSION_OR_CYCLE"):
        repo.save_state(ready)
    assert repo.load_state().status == Status.ACTIVE
    pending = replace(active, status=Status.EXIT_PENDING)
    repo.save_state(pending)
    completed = replace(pending, status=Status.COMPLETE,
                        cycle_complete_date=date(2026, 11, 12))
    repo.save_state(completed)
    next_ready = reserve_new_cycle(
        completed, date(2026, 11, 13), cycle_id="distinct-next-cycle",
    )
    repo.save_state(next_ready)
    current = repo.load_state()
    assert current.status == Status.READY
    assert current.cycle_id == "distinct-next-cycle"
    assert current.version == 5


def test_pg_risk_cash_resized_buy_intent_finalization_atomic(pg_engine, monkeypatch):
    """Execution qty changes before claim/POST, never the signed sell contract."""
    from sqlalchemy import text
    from trader.us.db import repos
    from trader.us.entry_exit_contract import build_us_entry_exit_contract

    monkeypatch.setattr(repos, "_active_us_epoch",
                        lambda bind=None, **kwargs: "fixture-resize-epoch")
    initial = {
        "client_order_key": "f" * 24, "symbol": "LIFY",
        "exchange": "NASDAQ", "side": "BUY", "qty": 7,
        "limit_price": 50, "notional_usd": 350,
        "strategy_owner": "US_STANDARD", "sleeve_id": "US_STANDARD",
        "strategy_name": "us_pb1", "strategy": "us_pb1",
        "strategy_version": "fixture-v42",
        "entry_style_selected": "ENTRY_MOMENTUM",
        "position_action": "NEW_POSITION_BUY",
        "book": "SWING_BOOK", "horizon": "SWING_CARRY",
    }
    signed = build_us_entry_exit_contract(initial)
    initial["meta"] = {
        "strategy_owner": "US_STANDARD", "strategy_name": "us_pb1",
        "strategy_version": "fixture-v42", "sleeve_id": "US_STANDARD",
        "entry_exit_contract": signed,
        "entry_exit_contract_sha256": signed["sha256"],
        "entry_exit_contract_version": signed["version"],
        "entry_style_selected": "ENTRY_MOMENTUM",
    }
    assert repos.save_order_intent(initial, trade_date="2026-11-12")
    resized = {**initial, "qty": 4, "notional_usd": 200}
    assert repos.finalize_us_buy_intent_pre_submit(resized, trade_date="2026-11-12")
    with pg_engine.connect() as conn:
        row = conn.execute(text("""
            SELECT qty,notional_usd,meta FROM us_order_intents
            WHERE client_order_key=:key
        """), {"key": initial["client_order_key"]}).mappings().one()
    assert row["qty"] == 4
    assert float(row["notional_usd"]) == 200
    assert row["meta"]["entry_exit_contract"] == signed
    assert row["meta"]["entry_exit_contract_sha256"] == signed["sha256"]
    assert row["meta"]["execution_qty_finalization"]["original_qty"] == 7

    # Once the same client key has an execution record, a different resize
    # must fail closed and leave its original contract/qty untouched.
    with pg_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO us_orders(
                trade_date,client_order_key,symbol,exchange,side,
                qty_requested,qty_filled,status,meta)
            VALUES ('2026-11-12',:key,'LIFY','NASDAQ','BUY',
                    4,0,'ACK','{}'::jsonb)
        """), {"key": initial["client_order_key"]})
    assert not repos.finalize_us_buy_intent_pre_submit(
        {**initial, "qty": 3, "notional_usd": 150},
        trade_date="2026-11-12",
    )
    with pg_engine.connect() as conn:
        persisted = conn.execute(text("""
            SELECT qty,meta FROM us_order_intents
            WHERE client_order_key=:key
        """), {"key": initial["client_order_key"]}).mappings().one()
    assert persisted["qty"] == 4
    assert persisted["meta"]["entry_exit_contract"] == signed


def test_pg_closed_latest_snapshot_never_resurrects_old_buy_parent(pg_engine, monkeypatch):
    """Real PG: a later zero-share balance disqualifies the old ADD parent."""
    import json
    from sqlalchemy import text
    from trader.us.db import repos
    from trader.us.entry_exit_contract import build_us_entry_exit_contract

    epoch = "fixture-closed-cycle-epoch"
    monkeypatch.setattr(repos, "_active_us_epoch", lambda _conn=None: epoch)
    frozen = build_us_entry_exit_contract({
        "symbol": "AMGN", "strategy_owner": "US_STANDARD",
        "position_action": "NEW_POSITION_BUY",
        "entry_style_selected": "ENTRY_PULLBACK",
        "book": "SWING_BOOK", "horizon": "SWING_CARRY",
    })
    with pg_engine.begin() as conn:
        for dt, qty, cycle in (
            ("2026-11-10", 6, "original-closed-cycle"),
            ("2026-11-11", 0, "original-closed-cycle"),
        ):
            meta = {
                "strategy_owner": "US_STANDARD",
                "position_lifecycle_id": cycle,
                "position_cycle_id": cycle,
                "entry_exit_contract": frozen,
                "entry_exit_contract_sha256": frozen["sha256"],
            }
            conn.execute(text("""
                INSERT INTO us_positions
                    (as_of,symbol,exchange,qty,avg_cost,trading_epoch_id,
                     account_id,portfolio_epoch_id,position_cycle_id,meta)
                VALUES (:dt,'AMGN','NASDAQ',:qty,100,:epoch,'hash-account',
                        'portfolio-old',:cycle,CAST(:meta AS jsonb))
            """), {"dt":dt,"qty":qty,"epoch":epoch,"cycle":cycle,
                   "meta":json.dumps(meta)})
    prior = repos.load_us_positions_by_symbols(["AMGN"], as_of="2026-11-10")
    assert prior["AMGN"]["position_cycle_id"] == "original-closed-cycle"
    assert prior["AMGN"]["meta"]["entry_exit_contract_sha256"] == frozen["sha256"]
    # Reading 11/11 or later may not 'revive' a 11/10 positive holding.
    assert repos.load_us_positions_by_symbols(["AMGN"], as_of="2026-11-11") == {}
    assert repos.load_us_positions_by_symbols(["AMGN"], as_of="2026-11-12") == {}
    assert repos.load_us_positions_by_symbols(["AMGN"]) == {}


@pytest.mark.parametrize("paper_profile", [False, True], ids=["legacy-off", "paper-on-enforce"])
@pytest.mark.parametrize("multiple_unresolved", [False, True], ids=["ko-only", "ko-kr-tmo"])
def test_pg_ten_holdings_unresolved_peer_does_not_suppress_other_exit_decisions(
    pg_engine, monkeypatch, paper_profile, multiple_unresolved,
):
    """Real PG -> exit resolver -> signed original V2/V3 -> actual order
    router and fake broker ACK; neither a real KIS request nor a KIS fill.
    """
    import json
    from datetime import datetime, timezone
    from sqlalchemy import text
    from trader.us.db import repos
    from trader.us.entry_exit_contract import build_us_entry_exit_contract
    from trader.us.pb1.us_exit_position_resolver import enrich_us_positions_for_exit
    from trader.us.pb1.us_exit_engine import generate_exit_intents, should_skip_exit_due_to_pending_sell
    from trader.us.runner.trade_tick_runner import route_exit_orders_immediately

    epoch = (
        "fixture-ten-symbol-exit-epoch-on"
        if paper_profile else "fixture-ten-symbol-exit-epoch"
    )
    symbols = ["ABBV", "AMGN", "DHR", "HCA", "KO",
               "KR", "MCK", "PFE", "PG", "TMO"]
    blocked = {"KO", "KR", "TMO"} if multiple_unresolved else {"KO"}
    monkeypatch.setattr(repos, "_active_us_epoch", lambda _conn=None, **_kw: epoch)
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setattr(
        "trader.us.protective_stop_obligation.record_confirmed_stop",
        lambda *_args, **_kw: None,
    )
    with pg_engine.begin() as conn:
        for idx, sym in enumerate(symbols):
            # Four actual US_STANDARD entry selections retain distinct signed
            # originals; they share the existing book/horizon exit philosophy.
            selected_style = (
                "ENTRY_PULLBACK", "ENTRY_MOMENTUM",
                "ENTRY_BREAKOUT", "ENTRY_MINERVINI",
            )[idx % 4]
            # Alternate the existing V2 and approved practice-only V3
            # contract builders at BUY-time, then keep both frozen while
            # downstream engine/router run with the common runtime mode.
            use_v3 = bool(idx % 2)
            monkeypatch.setenv(
                "US_SWING_EXIT_MODE",
                "V3_ACTIVE_PRACTICE" if use_v3 else "LEGACY_V2",
            )
            monkeypatch.setenv(
                "US_SWING_V3_DEPLOY_APPROVED", "1" if use_v3 else "0",
            )
            signed = build_us_entry_exit_contract({
                "symbol": sym, "strategy_owner": "US_STANDARD",
                "position_action": "NEW_POSITION_BUY",
                "entry_style_selected": selected_style,
                "book": "SWING_BOOK", "horizon": "SWING_CARRY",
            })
            assert signed["version"] == (
                "us_entry_exit_contract_v3" if use_v3
                else "us_entry_exit_contract_v2"
            )
            meta = {
                "strategy_owner": "US_STANDARD",
                "sleeve_id": "US_STANDARD",
                "position_lifecycle_id": f"fixture-cycle-{sym}",
                "position_cycle_id": f"fixture-cycle-{sym}",
                "entry_exit_contract": signed,
                "entry_exit_contract_sha256": signed["sha256"],
                "entry_time": "2026-11-10T16:00:00+00:00",
                "orderable_qty": 4,
            }
            if sym == "ABBV":
                # Real missing-contract row: a profitable/flat holding cannot
                # inherit a different symbol's original exit agreement.
                meta.pop("entry_exit_contract")
                meta.pop("entry_exit_contract_sha256")
                meta.pop("position_lifecycle_id")
                meta.pop("position_cycle_id")
            conn.execute(text("""
                INSERT INTO us_positions(
                    as_of,symbol,exchange,qty,avg_cost,current_px,trading_epoch_id,
                    account_id,portfolio_epoch_id,position_cycle_id,meta)
                VALUES ('2026-11-12',:sym,'NASDAQ',4,100,:price,:epoch,
                        'hash-fixture','portfolio-fixture',:cycle,CAST(:meta AS jsonb))
            """), {
                "sym": sym, "price": 100 if sym == "ABBV" else 80,
                "epoch": epoch, "cycle": None if sym == "ABBV" else f"fixture-cycle-{sym}",
                "meta": json.dumps(meta),
            })
        for blocked_symbol in blocked:
            conn.execute(text("""
            INSERT INTO us_orders(
                trade_date,client_order_key,symbol,exchange,side,qty_requested,
                qty_filled,order_no,status,dry_run,trading_epoch_id,meta)
            VALUES ('2026-11-12',:key,:sym,'NYSE','SELL',
                    1,0,:order_no,'ACK',false,:epoch,'{}'::jsonb)
        """), {"epoch": epoch, "sym": blocked_symbol,
                "key": "fixture-" + blocked_symbol + "-unresolved",
                "order_no": blocked_symbol + "-UNRESOLVED"})

    # Deliberately change the runtime defaults after BUY creation. Decisions
    # must continue to use each held position's signed original V2/V3.
    monkeypatch.setenv("US_SWING_EXIT_MODE", "LEGACY_V2")
    monkeypatch.setenv("US_SWING_V3_DEPLOY_APPROVED", "0")
    positions = repos.load_us_positions_by_symbols(symbols, as_of="2026-11-12")
    assert set(positions) == set(symbols)
    for blocked_symbol in blocked:
        assert should_skip_exit_due_to_pending_sell(blocked_symbol, "2026-11-12")[0] is True
    resolved, resolution = enrich_us_positions_for_exit(
        list(positions.values()), trade_date="2026-11-12", env="practice",
    )
    assert len(resolved) == 10
    for row in resolved:
        row["resolved_current_price"] = float(row.get("current_px") or 0)
        # Real PG snapshots do not own a fresh broker orderable-qty column.
        # The normal tick merges KIS balance before exit evaluation: inject
        # the explicit 4-share test-broker snapshot here, not a guessed DB
        # value. V3's strict orderable proof gate is kept intact.
        row["holding_qty"] = 4
        row["orderable_qty"] = 4
        row["sellable_qty"] = 4
        row["orderable_qty_source"] = "TEST_BROKER_FRESH_BALANCE"
        # Preserve the actual signed original from the stored DB JSON.
        row_meta = row.get("meta") or {}
        if row["symbol"] != "ABBV":
            assert row_meta["entry_exit_contract_sha256"] == row_meta["entry_exit_contract"]["sha256"]
            row["entry_exit_contract"] = row_meta["entry_exit_contract"]
    now = datetime(2026, 11, 12, 15, 0, tzinfo=timezone.utc)
    signals = generate_exit_intents(
        [], prepared_snapshots=resolved, now=now, include_trend_time=False,
    )
    found = {intent["symbol"]: intent for intent in signals if intent.get("side") == "SELL"}
    assert len(found) == len(symbols) - len(blocked) - 1, found
    assert not (blocked | {"ABBV"}).intersection(found)
    for sym, intent in found.items():
        assert intent["position_lifecycle_id"] == f"fixture-cycle-{sym}"
        assert intent["source_entry_contract_sha256"] == (
            positions[sym]["meta"]["entry_exit_contract_sha256"]
        )
    if paper_profile:
        # The legacy-off parameter ran earlier against the same PG fixture.
        # Distinct test sessions must not reuse its date-scoped SELL client
        # keys; otherwise the operational router correctly deduplicates all
        # eight, but this test mistakes fixture pollution for failed isolation.
        for candidate in signals:
            if candidate.get("side") == "SELL":
                candidate["client_order_key"] = (
                    "ci-paper-on-" + str(candidate["client_order_key"])
                )
    routed = route_exit_orders_immediately(
        signals, buy_daily_notional=0, position_count=10,
        effective_budget=10000, signal_only=True, kis_order_allowed=True,
        current_position_symbols=set(symbols),
    )
    assert len(routed["orders"]) == len(found)
    assert {order.get("symbol") for order in routed["orders"]} == set(found)
    assert all(order["status"] == "SIGNAL_ONLY" for order in routed["orders"])

    # Continue the *same* real SQL-loaded resolver -> engine signals through
    # the unmocked order router, durable PG intent/claim/ACK repositories, and
    # a deterministic test broker HTTP boundary (NEVER authenticated KIS).
    # SIGNAL_ONLY above is intentionally not credited as a submitted SELL.
    from trader.us.execution import order_router
    from trader.execution_claims import DurableExecutionClaimRepo
    from trader.us.db.execution_claim_schema import (
        us_execution_attempts, us_execution_claims,
    )
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("WSL_RUN_MARKET", "")
    monkeypatch.setenv("US_ATOMIC_CLAIM_OUTBOX_ENABLED", "1" if paper_profile else "0")
    monkeypatch.setenv("US_ACCOUNT_RESERVATION_MODE", "ENFORCE" if paper_profile else "OFF")
    monkeypatch.setenv("US_RECONCILE_WORKER_MODE", "RESERVATION_ONLY" if paper_profile else "LEGACY")
    monkeypatch.setattr(order_router, "resolve_dry_run_for_us_order", lambda: False)
    if paper_profile:
        import hashlib
        from pathlib import Path
        import trader.account_state as account_state
        from trader.us import config as us_cfg
        from trader.us.execution import reservation_reconcile_worker
        with pg_engine.begin() as conn:
            conn.exec_driver_sql(Path(
                "migrations/0056_us_execution_authority_reservations.sql"
            ).read_text())
        raw_account = "practice:ten-symbol-concurrency-integration"
        account_fingerprint = hashlib.sha256(raw_account.encode()).hexdigest()
        monkeypatch.setattr(
            account_state, "get_account_key", lambda **_kw: raw_account,
        )
        monkeypatch.setattr(repos, "_us_execution_claim_scope", lambda: {
            "env": "practice",
            "account_id": account_fingerprint,
            "trading_epoch_id": epoch,
        })
        monkeypatch.setattr(us_cfg, "assert_us_paper_order_allowed", lambda: None)
        monkeypatch.setattr(
            reservation_reconcile_worker,
            "assert_fresh_reservation_worker", lambda **_kw: True,
        )
    claim_repo = DurableExecutionClaimRepo(
        pg_engine, us_execution_claims, us_execution_attempts,
    )
    monkeypatch.setattr(repos, "_execution_claim_repo", lambda: claim_repo)
    monkeypatch.setattr(repos, "claim_execution_action", claim_repo.acquire)
    monkeypatch.setattr(
        repos, "record_execution_action_observation",
        claim_repo.record_observation,
    )
    monkeypatch.setattr(
        repos, "release_execution_action_before_submit",
        claim_repo.release_before_submit,
    )

    class TestBroker:
        def __init__(self):
            self.sells = []

        def get_balance(self, **_kwargs):
            return {"positions": [
                {"symbol": sym, "exchange": "NASDAQ",
                 "qty": 4, "holding_qty": 4, "orderable_qty": 4,
                 "avg_price": 100.0, "currency": "USD"}
                for sym in symbols
            ]}

        def place_us_sell_order(self, symbol, exchange, qty, price):
            assert symbol not in blocked | {"ABBV"}
            assert exchange == "NASDAQ" and qty > 0
            self.sells.append((symbol, qty, price))
            return {"ok": True, "order_no": "CI-SELL-" + symbol}

    if paper_profile:
        monkeypatch.setattr(order_router, "NativeKisUSClient", TestBroker)
    broker = TestBroker()
    for intent in signals:
        intent["trade_date"] = "2026-11-12"
        intent.setdefault("limit_price", 80.0)
        intent.setdefault("strategy_owner", "US_STANDARD")
        intent.setdefault("sleeve_id", "US_STANDARD")
        intent.setdefault("holding_qty", 4)
        intent.setdefault("orderable_qty", 4)
        intent.setdefault("sellable_qty", 4)
        intent.setdefault("available_qty", 4)
    actual_routes = route_exit_orders_immediately(
        signals, buy_daily_notional=0, position_count=10,
        effective_budget=10000, signal_only=False, kis_order_allowed=True,
        current_position_symbols=set(symbols), kis_client=broker,
    )
    # A broker HTTP adapter call proves actual router traversal. The DB claims
    # and ACKs prove local write order, not live brokerage acceptance.
    assert len(broker.sells) == len(found), actual_routes
    assert {sym for sym, *_ in broker.sells} == set(found)
    assert all(row.get("status") == "ACK" for row in actual_routes["orders"]), actual_routes
    with pg_engine.connect() as conn:
        pg_sell_rows = conn.execute(text("""
            SELECT symbol, status, client_order_key, order_no, meta
            FROM us_orders
            WHERE trade_date='2026-11-12' AND side='SELL'
              AND order_no LIKE 'CI-SELL-%'
        """)).mappings().all()
    assert len(pg_sell_rows) == len(found)
    assert {r["symbol"] for r in pg_sell_rows} == set(found)
    if paper_profile:
        with pg_engine.connect() as conn:
            reservations = conn.execute(text("""
                SELECT symbol,side,state,requested_qty
                  FROM us_execution_account_reservations
                 WHERE env='practice' AND trading_epoch_id=:epoch
            """), {"epoch": epoch}).mappings().all()
            outbox = conn.execute(text("""
                SELECT symbol,side,COUNT(*) AS n
                  FROM us_order_events
                 WHERE trade_date='2026-11-12'
                   AND event_type='BROKER_CLAIM_PREPARED'
                 GROUP BY symbol,side
            """)).mappings().all()
            attempts = conn.execute(text("""
                SELECT COUNT(*) FROM us_execution_attempts a
                  JOIN us_execution_claims c ON c.action_key=a.action_key
                 WHERE c.trading_epoch_id=:epoch
            """), {"epoch": epoch}).scalar_one()
        assert {r["symbol"] for r in reservations} == set(found)
        assert all(r["side"] == "SELL" and r["requested_qty"] > 0
                   for r in reservations)
        assert {r["symbol"] for r in outbox} == set(found)
        assert all(r["n"] == 1 for r in outbox)
        assert attempts == len(found)
    assert {
        positions[row["symbol"]]["meta"]["entry_exit_contract"]["version"]
        for row in pg_sell_rows
    } == {"us_entry_exit_contract_v2", "us_entry_exit_contract_v3"}
    assert {
        positions[row["symbol"]]["meta"]["entry_exit_contract"]
        ["entry_strategy_detail"]["selected_style"]
        for row in pg_sell_rows
    } == {
        "ENTRY_PULLBACK", "ENTRY_MOMENTUM",
        "ENTRY_BREAKOUT", "ENTRY_MINERVINI",
    }
    for record in pg_sell_rows:
        assert record["status"] == "ACK"
        # SELL carries the immutable BUY as a *source* lineage reference;
        # it must not pretend to be the BUY that originally signed the policy.
        assert record["meta"]["source_entry_contract_sha256"] == (
            positions[record["symbol"]]["meta"]["entry_exit_contract_sha256"]
        )



@pytest.mark.parametrize(
    "selected_style,contract_mode,paper_profile,paper_failure",
    [
        (style, mode, False, None)
        for mode in ("LEGACY_V2", "V3_ACTIVE_PRACTICE")
        for style in ("ENTRY_PULLBACK", "ENTRY_MOMENTUM", "ENTRY_BREAKOUT", "ENTRY_MINERVINI")
    ] + [
        pytest.param("ENTRY_PULLBACK", "V3_ACTIVE_PRACTICE", True, None,
                     id="real-pg-paper-on-enforce-native-router"),
        pytest.param("ENTRY_PULLBACK", "V3_ACTIVE_PRACTICE", True, "unknown_ack",
                     id="real-pg-paper-on-enforce-post-unknown-ack"),
        pytest.param("ENTRY_PULLBACK", "V3_ACTIVE_PRACTICE", True, "ack_db_failure",
                     id="real-pg-paper-on-enforce-post-ack-db-failure"),
    ],
)
def test_pg_eight_families_actual_buy_route_to_overnight_protective_sell(
    amgn_pg_engine, monkeypatch, tmp_path, selected_style, contract_mode,
    paper_profile, paper_failure,
):
    """Eight explicit production-router paths; only KIS HTTP is a test broker.

    Do not seed BUY order/intent, contract, position, or the eventual SELL by
    fixture. The actual BUY router must create/persist a signed original, then
    real repository cumulative-fill, snapshot, resolver, engine and SELL router
    must independently carry it through an overnight process restart.
    """
    import hashlib
    from datetime import datetime, timezone
    from sqlalchemy import text
    from trader.us.db import repos
    from trader.us.execution import order_router
    from trader.us.execution.order_router import route_order
    from trader.us.entry_exit_contract import verify_us_entry_exit_contract
    from trader.us.pb1.us_exit_position_resolver import enrich_us_positions_for_exit
    from trader.us.pb1.us_exit_engine import generate_exit_intents
    from trader.execution_claims import DurableExecutionClaimRepo
    from trader.us.db.execution_claim_schema import (
        us_execution_claims, us_execution_attempts,
    )
    import trader.account_state as account_state

    engine = amgn_pg_engine
    symbol = "AMGN"
    # Use UUID-shaped IDs compatible with the actual portfolio-epoch schema.
    epoch = "e3d79991-73aa-425d-971b-cbb80c7e9c10"
    portfolio = "f6f8b13e-10d9-481f-8f0d-8987be0946c0"
    raw_account = "practice:offline-eight-combinations"
    account_hash = hashlib.sha256(raw_account.encode()).hexdigest()
    is_v3 = contract_mode == "V3_ACTIVE_PRACTICE"
    expected_version = (
        "us_entry_exit_contract_v3" if is_v3 else "us_entry_exit_contract_v2"
    )
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("WSL_RUN_MARKET", "US")
    monkeypatch.setenv("US_SWING_EXIT_MODE", contract_mode)
    monkeypatch.setenv("US_SWING_V3_DEPLOY_APPROVED", "1" if is_v3 else "0")
    monkeypatch.setenv("US_ACCOUNT_RESERVATION_MODE", "ENFORCE" if paper_profile else "OFF")
    monkeypatch.setenv("US_ATOMIC_CLAIM_OUTBOX_ENABLED", "1" if paper_profile else "0")
    monkeypatch.setenv("US_RECONCILE_WORKER_MODE", "RESERVATION_ONLY" if paper_profile else "LEGACY")
    # The shared CI suite intentionally applies a $100 test risk cap for
    # unrelated rejection cases. Restore the repository production DEFAULT
    # $2,500 maximum within this one 6x$100 BUY scenario; do not disable risk.
    monkeypatch.setenv("US_MAX_ORDER_USD", "2500")
    monkeypatch.setenv("US_ORDER_JOURNAL_DIR", str(tmp_path / "journal"))
    monkeypatch.setattr(
        repos, "_active_us_epoch", lambda _conn=None, **_kw: epoch,
    )
    monkeypatch.setattr(
        repos, "_us_state_epoch_id", lambda _conn=None, **_kw: epoch,
    )
    monkeypatch.setattr(
        account_state, "get_account_key", lambda **_kw: raw_account,
    )
    monkeypatch.setattr(
        order_router, "resolve_dry_run_for_us_order", lambda: False,
    )
    # Parametrized CI cases represent separate fresh processes and separate
    # disposable databases, unlike the module-level live-process dedupe cache.
    monkeypatch.setattr(order_router, "_SENT_ORDER_KEYS", set())
    monkeypatch.setattr(order_router, "_BLOCKED_INTENT_KEYS", set())
    claim_repo = DurableExecutionClaimRepo(
        engine, us_execution_claims, us_execution_attempts,
    )
    monkeypatch.setattr(repos, "_execution_claim_repo", lambda: claim_repo)
    monkeypatch.setattr(repos, "claim_execution_action", claim_repo.acquire)
    monkeypatch.setattr(
        repos, "record_execution_action_observation",
        claim_repo.record_observation,
    )
    monkeypatch.setattr(
        repos, "release_execution_action_before_submit",
        claim_repo.release_before_submit,
    )
    # Both epoch tables are the actual production SQLAlchemy schema, not
    # a shortened CI DDL stub. The BUY router must perform its normal
    # account -> portfolio -> deterministic cycle binding in PostgreSQL.
    from trader.db.schema import schema_for_engine
    epoch_schema = schema_for_engine(engine)
    epoch_schema.trading_epochs.create(engine, checkfirst=True)
    epoch_schema.portfolio_epochs.create(engine, checkfirst=True)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO trading_epochs(
                trading_epoch_id,env,account_id,status,reason)
            VALUES (:epoch,'practice',:account,'ACTIVE','CI_EIGHT')
        """), {"epoch":epoch,"account":raw_account})
        conn.execute(text("""
            INSERT INTO portfolio_epochs(
                portfolio_epoch_id,trading_epoch_id,env,account_id,
                sid,mode,strategy,status,reason)
            VALUES (:portfolio,:epoch,'practice',:account,
                    0,0,'US_STANDARD','ACTIVE','CI_EIGHT')
        """), {"portfolio":portfolio,"epoch":epoch,"account":raw_account})
    cycle = hashlib.sha256("|".join((
        account_hash, epoch, portfolio, "US_STANDARD", symbol,
        "2026-11-10", "NEW_POSITION_BUY",
    )).encode("utf-8")).hexdigest()[:32]

    class TestBroker:
        def __init__(self):
            self.qty = 0
            self.posts = []

        def get_orderable_cash(self, **_kw):
            return 100000.0

        def get_us_balance(self, *, force_refresh=False):
            # Native KIS shape used by the real pre-HTTP BUY baseline reader.
            assert force_refresh is True
            assert self.qty == 0
            return {
                "rt_cd": "0", "output1": [], "output2": {},
                "balance_complete": True, "balance_authoritative": True,
            }

        def get_balance(self, **_kw):
            return {"positions": [{
                "symbol": symbol, "exchange": "NASDAQ", "qty": self.qty,
                "holding_qty": self.qty, "orderable_qty": self.qty,
                "avg_price": 10.0,
            }]} if self.qty else {"positions": []}

        def place_us_buy_order(self, sym, exchange, qty, price):
            assert (sym, exchange, qty, price) == (symbol, "NASDAQ", 6, 10.0)
            self.posts.append(("BUY", sym, qty))
            if paper_failure == "unknown_ack":
                # The fake broker may have received the economic order; this
                # transport error MUST NOT authorize a second native POST.
                raise TimeoutError("synthetic_unknown_ack_after_post")
            return {"ok": True, "order_no": "CI-BUY-EIGHT"}

        def place_us_sell_order(self, sym, exchange, qty, price):
            assert sym == symbol and exchange == "NASDAQ"
            assert 0 < qty <= self.qty
            self.posts.append(("SELL", sym, qty))
            return {"ok": True, "order_no": "CI-SELL-EIGHT"}

    if paper_profile:
        # Keep the actual router, PostgreSQL authority and Claim+Outbox path.
        # Only the transport is a test broker; no KIS credential or HTTP.
        from pathlib import Path
        from trader.us import config as us_cfg
        from trader.us.execution import reservation_reconcile_worker
        with engine.begin() as conn:
            conn.exec_driver_sql(Path(
                "migrations/0056_us_execution_authority_reservations.sql"
            ).read_text())
        monkeypatch.setattr(order_router, "NativeKisUSClient", TestBroker)
        monkeypatch.setattr(us_cfg, "assert_us_paper_order_allowed", lambda: None)
        monkeypatch.setattr(
            reservation_reconcile_worker,
            "assert_fresh_reservation_worker", lambda **_kw: True,
        )
    broker = TestBroker()
    original_intent = {
        "trade_date": "2026-11-10",
        "client_order_key": "ci-eight-original-buy",
        "symbol": symbol, "exchange": "NASDAQ", "side": "BUY",
        "qty": 6, "limit_price": 10.0, "notional_usd": 60.0,
        "strategy_owner": "US_STANDARD", "sleeve_id": "US_STANDARD",
        "strategy": "us_pb1", "strategy_name": "us_pb1",
        "strategy_version": "offline-eight-v1",
        "entry_style_selected": selected_style,
        "position_action": "NEW_POSITION_BUY",
        "entry_reason": selected_style,
        "book": "SWING_BOOK", "horizon": "SWING_CARRY",
        # Supply the exact classification provenance required by the
        # production canonical risk gate; do not monkeypatch away risk checks.
        "theme_cluster": "OTHER",
        "classification_source": "CI_FINAL30_OFFLINE",
        "position_state": "NEW",
        "position_lifecycle_id": cycle, "position_cycle_id": cycle,
        "portfolio_epoch_id": portfolio, "trading_epoch_id": epoch,
        "broker_account_hash": account_hash,
        "meta": {
            "broker_account_hash": account_hash,
            "trading_epoch_id": epoch, "portfolio_epoch_id": portfolio,
            "position_cycle_id": cycle, "position_lifecycle_id": cycle,
            "entry_style_selected": selected_style,
            "theme_cluster": "OTHER",
            "classification_source": "CI_FINAL30_OFFLINE",
            "position_state": "NEW",
            "position_action": "NEW_POSITION_BUY",
        },
    }
    # Non-native test adapters deliberately bypass the native-only baseline
    # hook in route_order. Exercise that *real operating function* explicitly
    # with native-shaped GET evidence rather than hand-writing a fake
    # pre_order_holding_qty into order metadata.
    order_router._capture_native_buy_baseline(original_intent, broker, None)
    assert original_intent["meta"]["pre_order_holding_qty"] == 0
    assert original_intent["meta"]["pre_order_position_source"] == "kis_balance_authoritative"
    if paper_failure == "ack_db_failure":
        def synthetic_ack_failure(*_args, **_kw):
            raise RuntimeError("synthetic_ack_db_failure")
        monkeypatch.setattr(repos, "save_order_ack", synthetic_ack_failure)
    buy = route_order(
        original_intent, kis_client=broker,
        allowed_symbols={symbol}, current_position_symbols=set(),
        total_portfolio_usd=20000, available_cash_usd=10000,
        now=datetime(2026, 11, 10, 16, 0, tzinfo=timezone.utc),
    )
    if paper_failure:
        expected = ("BROKER_SUBMIT_RESULT_UNKNOWN" if paper_failure == "unknown_ack"
                    else "ACK_DB_FAILED")
        assert buy["status"] == expected, (paper_failure, buy)
        assert buy["retry_order"] is False
        assert buy["requires_reconcile"] is True
        assert broker.posts == [("BUY", symbol, 6)]
        with engine.connect() as conn:
            original = conn.execute(text("""
                SELECT meta FROM us_order_intents
                 WHERE client_order_key='ci-eight-original-buy'
            """)).mappings().one()
            assert verify_us_entry_exit_contract(
                original["meta"]["entry_exit_contract"]
            )
            assert conn.execute(text("""
                SELECT count(*) FROM us_execution_account_reservations
                 WHERE symbol='AMGN' AND side='BUY'
            """)).scalar_one() == 1
            assert conn.execute(text("""
                SELECT count(*) FROM us_order_events
                 WHERE client_order_key='ci-eight-original-buy'
                   AND event_type='BROKER_CLAIM_PREPARED'
            """)).scalar_one() == 1
            assert conn.execute(text("""
                SELECT count(*) FROM us_orders
                 WHERE client_order_key='ci-eight-original-buy'
            """)).scalar_one() == 0
        retried = route_order(
            {**original_intent, "client_order_key": "ci-eight-unsafely-retried-buy"},
            kis_client=broker, allowed_symbols={symbol},
            current_position_symbols=set(),
            total_portfolio_usd=20000, available_cash_usd=10000,
            now=datetime(2026, 11, 10, 16, 1, tzinfo=timezone.utc),
        )
        assert retried["status"] != "ACK", retried
        assert broker.posts == [("BUY", symbol, 6)]
        return
    assert buy["status"] == "ACK", (selected_style, contract_mode, buy)
    assert broker.posts == [("BUY", symbol, 6)]
    with engine.connect() as conn:
        original = conn.execute(text("""
            SELECT meta,position_cycle_id,portfolio_epoch_id
            FROM us_order_intents WHERE client_order_key='ci-eight-original-buy'
        """)).mappings().one()
        order = conn.execute(text("""
            SELECT status,order_no,meta
            FROM us_orders WHERE client_order_key='ci-eight-original-buy'
        """)).mappings().one()
    signed = original["meta"]["entry_exit_contract"]
    assert verify_us_entry_exit_contract(signed)
    assert signed["version"] == expected_version
    assert signed["entry_strategy_detail"]["selected_style"] == selected_style
    assert original["meta"]["entry_exit_contract_sha256"] == signed["sha256"]
    assert order["status"] == "ACK" and order["order_no"] == "CI-BUY-EIGHT"
    if paper_profile:
        with engine.connect() as conn:
            prepared = conn.execute(text("""
                SELECT count(*) FROM us_order_events
                 WHERE client_order_key='ci-eight-original-buy'
                   AND event_type='BROKER_CLAIM_PREPARED'
            """)).scalar_one()
            reserved = conn.execute(text("""
                SELECT count(*) FROM us_execution_account_reservations
                 WHERE symbol='AMGN' AND side='BUY'
                   AND strategy_owner='US_STANDARD'
            """)).scalar_one()
            claimed = conn.execute(text("""
                SELECT count(*) FROM us_execution_claims
                 WHERE trading_epoch_id=:epoch
            """), {"epoch": epoch}).scalar_one()
        assert (prepared, reserved, claimed) == (1, 1, 1)
    # The native WSL binder must scope the *real BUY ledger*, not merely
    # attach signed JSON to an otherwise anonymous client key.
    with engine.connect() as conn:
        durable_buy_scope = conn.execute(text("""
            SELECT i.account_id AS i_account,i.portfolio_epoch_id AS i_portfolio,
                   i.position_cycle_id AS i_cycle,i.trading_epoch_id AS i_epoch,
                   o.account_id AS o_account,o.portfolio_epoch_id AS o_portfolio,
                   o.position_cycle_id AS o_cycle,o.trading_epoch_id AS o_epoch
              FROM us_order_intents i
              JOIN us_orders o ON o.client_order_key=i.client_order_key
             WHERE i.client_order_key='ci-eight-original-buy'
        """)).mappings().one()
    assert (
        durable_buy_scope["i_account"],durable_buy_scope["i_portfolio"],
        durable_buy_scope["i_cycle"],durable_buy_scope["i_epoch"],
        durable_buy_scope["o_account"],durable_buy_scope["o_portfolio"],
        durable_buy_scope["o_cycle"],durable_buy_scope["o_epoch"],
    ) == (account_hash,portfolio,cycle,epoch,account_hash,portfolio,cycle,epoch), durable_buy_scope

    fill_meta = {
        **original["meta"],
        "broker_account_hash": account_hash,
        "trading_epoch_id": epoch,
        "portfolio_epoch_id": portfolio,
        "position_cycle_id": cycle,
        "position_lifecycle_id": cycle,
        "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
        "cumulative_filled_qty": 6,
    }
    if paper_profile:
        partial = repos.mark_order_filled_by_reconcile(
            order_no="CI-BUY-EIGHT", client_order_key="ci-eight-original-buy",
            symbol=symbol, side="BUY", filled_qty=2, requested_qty=6,
            cumulative_filled_qty=2, avg_price_usd=10.0,
            trade_date="2026-11-10",
            evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
            source="fills_by_order_no",
            meta={**fill_meta, "cumulative_filled_qty": 2},
        )
        assert partial["status"] == "OK", partial
        with engine.connect() as conn:
            assert conn.execute(text("""
                SELECT qty_filled FROM us_orders
                 WHERE client_order_key='ci-eight-original-buy'
            """)).scalar_one() == 2
        # A newly launched OS process must independently read durable
        # PostgreSQL state. This is a real process boundary, distinct from
        # constructing a second repository object in the same interpreter.
        import json as _json
        import subprocess as _subprocess
        import sys as _sys
        import os as _os
        _read_across_process = """
import json, os
from sqlalchemy import create_engine, text
engine = create_engine(os.environ['US353_PG_TEST_URL'], future=True)
with engine.connect() as conn:
    row = conn.execute(text('''
        SELECT i.position_cycle_id AS cycle,
               i.meta->>'entry_exit_contract_sha256' AS digest,
               o.qty_filled AS filled,
               (SELECT count(*) FROM us_execution_attempts
                 WHERE client_order_key='ci-eight-original-buy') AS attempts,
               (SELECT count(*) FROM us_execution_account_reservations
                 WHERE symbol='AMGN' AND side='BUY') AS reservations,
               (SELECT count(*) FROM us_order_events
                 WHERE client_order_key='ci-eight-original-buy'
                   AND event_type='BROKER_CLAIM_PREPARED') AS outbox
        FROM us_order_intents i
        JOIN us_orders o USING (client_order_key)
        WHERE i.client_order_key='ci-eight-original-buy'
    ''')).mappings().one()
    print(json.dumps(dict(row)))
engine.dispose()
"""
        def _child_read(expected_qty):
            child_env = _os.environ.copy()
            child_env["US353_PG_TEST_URL"] = engine.url.render_as_string(
                hide_password=False
            )
            result = _subprocess.run(
                [_sys.executable, "-c", _read_across_process],
                env=child_env, capture_output=True, text=True,
                timeout=15,
            )
            # Never expose DB URL, process environment, or full stderr.
            assert result.returncode == 0, "US353_CHILD_PG_READ_FAILED"
            observed = _json.loads(result.stdout.strip())
            assert observed["cycle"] == cycle, observed
            assert observed["digest"] == signed["sha256"], observed
            assert observed["filled"] == expected_qty, observed
            assert observed["attempts"] == 1, observed
            assert observed["reservations"] == 1, observed
            assert observed["outbox"] == 1, observed
        _child_read(2)
        # A separate durable-repository instance sees the same original
        # execution evidence before the delayed final cumulative fill.
        fresh_partial_repo = DurableExecutionClaimRepo(
            engine, us_execution_claims, us_execution_attempts,
        )
        monkeypatch.setattr(repos, "_execution_claim_repo", lambda: fresh_partial_repo)
        monkeypatch.setattr(repos, "claim_execution_action", fresh_partial_repo.acquire)
        monkeypatch.setattr(
            repos, "record_execution_action_observation",
            fresh_partial_repo.record_observation,
        )
    verified_buy = repos.mark_order_filled_by_reconcile(
        order_no="CI-BUY-EIGHT", client_order_key="ci-eight-original-buy",
        symbol=symbol, side="BUY", filled_qty=6, requested_qty=6,
        cumulative_filled_qty=6, avg_price_usd=10.0,
        trade_date="2026-11-10",
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        source="fills_by_order_no", meta=fill_meta,
    )
    assert verified_buy["status"] == "OK", verified_buy
    if paper_profile:
        _child_read(6)
    from trader.us.original_buy_lineage import load_verified_us_original_buy_scope
    identity = {"env": "practice", "account_id": raw_account,
                "trading_epoch_id": epoch, "strategy_owner": "US_STANDARD"}
    original_buy_proof = load_verified_us_original_buy_scope(
        symbol=symbol, trade_date="2026-11-10", qty=6, identity=identity,
    )
    assert original_buy_proof is not None, (
        selected_style, contract_mode, durable_buy_scope,
        "ORIGINAL_BUY_SCOPE_MISSING_AFTER_KIS_FORMAT_FILL",
    )
    assert original_buy_proof["position_cycle_id"] == cycle
    broker.qty = 6
    # First actual BUY snapshot: the repo proves account + epoch + original
    # contract against its persistent native-shaped BUY order and fill.
    assert repos.save_position_snapshot(
        [{"symbol":symbol,"exchange":"NASDAQ","qty":6,
          "avg_cost":10.0,"current_px":8.0,
          "balance_source":"kis_balance_authoritative","meta":{}}],
        trade_date="2026-11-10", balance_fetch_status="OK",
        balance_parse_status="OK", authoritative_positions=True,
        preserve_previous_positions=False,
    ) == 1
    # Real session lifecycle writer: this state does not come from a copied
    # fixture or a freshly rebuilt ENV contract. It is derived from the
    # original PostgreSQL BUY proof returned above and persisted by the
    # same production lifecycle repository used at AM/CLOSE.
    open_lifecycle = repos.save_us_position_lifecycle_atomic(
        symbol, "2026-11-10", {
            "lifecycle_id": original_buy_proof["position_cycle_id"],
            "position_cycle_id": original_buy_proof["position_cycle_id"],
            "portfolio_epoch_id": original_buy_proof["portfolio_epoch_id"],
            "trading_epoch_id": original_buy_proof["trading_epoch_id"],
            "account_id": original_buy_proof["account_id"],
            "strategy_owner": original_buy_proof["strategy_owner"],
            "env": original_buy_proof["env"], "is_open": True,
            "opened_trade_date": "2026-11-10",
            "opened_at": str(original_buy_proof["filled_at"]),
            "opened_at_source": "confirmed_buy_fill",
            "high_watermark": 10.0,
            "entry_policy": {
                "entry_exit_contract": signed,
                "entry_exit_contract_sha256": signed["sha256"],
                "entry_exit_contract_version": signed["version"],
            },
        },
    )
    assert open_lifecycle["position_cycle_id"] == cycle
    assert open_lifecycle["entry_policy"]["entry_exit_contract_sha256"] == signed["sha256"]
    # Restart: only a raw KIS-style balance is passed; no scope or contract
    # copy is added to that snapshot.
    assert repos.save_position_snapshot(
        [{"symbol":symbol,"exchange":"NASDAQ","qty":6,
          "avg_cost":10.0,"current_px":8.0,
          "balance_source":"kis_balance_authoritative","meta":{}}],
        trade_date="2026-11-11", balance_fetch_status="OK",
        balance_parse_status="OK", authoritative_positions=True,
        preserve_previous_positions=False,
    ) == 1
    with engine.connect() as conn:
        carried = conn.execute(text("""
            SELECT qty,account_id,portfolio_epoch_id,position_cycle_id,meta
            FROM us_positions WHERE as_of='2026-11-11' AND symbol='AMGN'
              AND trading_epoch_id=:epoch
        """), {"epoch":epoch}).mappings().one()
    assert carried["qty"] == 6
    assert (carried["account_id"],carried["portfolio_epoch_id"],
            carried["position_cycle_id"]) == (account_hash,portfolio,cycle)
    assert carried["meta"]["entry_exit_contract_sha256"] == signed["sha256"]

    # A different explicit engine is NOT a same-contract ADD, even when the
    # malicious/buggy producer copied the incumbent selected style. No lot
    # allocator exists, so do not create an intent, claim, or broker request.
    changed_engine = {
        **original_intent,
        "client_order_key": "ci-eight-conflicting-add",
        "trade_date": "2026-11-11",
        "strategy": "different_execution_engine",
        "position_action": "ADD_TO_EXISTING_BUY",
        "qty": 1,
        "notional_usd": 10.0,
    }
    denied = route_order(
        changed_engine, kis_client=broker,
        current_position_symbols={symbol}, allowed_symbols={symbol},
        total_portfolio_usd=20000, available_cash_usd=10000,
    )
    assert denied["status"] == "BLOCKED", denied
    assert denied["reason"] == (
        "US_ADD_DIFFERENT_CONTRACT_OR_STRATEGY_REQUIRES_LOT_POLICY"
    )
    assert broker.posts == [("BUY", symbol, 6)]
    with engine.connect() as conn:
        assert conn.execute(text("""
            SELECT COUNT(*) FROM us_order_intents
            WHERE client_order_key='ci-eight-conflicting-add'
        """)).scalar_one() == 0

    # A restarted claim repository and reloaded position cannot trust the
    # constructor's original intent object. Decisions read stored contract.
    fresh_repo = DurableExecutionClaimRepo(
        engine, us_execution_claims, us_execution_attempts,
    )
    monkeypatch.setattr(repos, "_execution_claim_repo", lambda: fresh_repo)
    monkeypatch.setattr(repos, "claim_execution_action", fresh_repo.acquire)
    monkeypatch.setattr(
        repos, "record_execution_action_observation",
        fresh_repo.record_observation,
    )
    loaded = repos.load_us_positions_by_symbols([symbol], as_of="2026-11-11")
    resolved, _ = enrich_us_positions_for_exit(
        list(loaded.values()), trade_date="2026-11-11", env="practice",
    )
    assert len(resolved) == 1
    held = resolved[0]
    held["resolved_current_price"] = 8.0
    held["orderable_qty"] = 6
    held["holding_qty"] = 6
    held["sellable_qty"] = 6
    held["entry_exit_contract"] = held["meta"]["entry_exit_contract"]
    assert held["entry_exit_contract"]["sha256"] == signed["sha256"]
    exits = generate_exit_intents(
        [], prepared_snapshots=[held],
        now=datetime(2026, 11, 11, 18, tzinfo=timezone.utc),
        include_trend_time=False,
    )
    sells = [x for x in exits if x.get("side") == "SELL"]
    assert len(sells) == 1, (selected_style, contract_mode, exits)
    sell = sells[0]
    assert sell["source_entry_contract_sha256"] == signed["sha256"]
    assert sell["position_lifecycle_id"] == cycle
    sell["trade_date"] = "2026-11-11"
    sell.setdefault("exchange", "NASDAQ")
    sell.setdefault("holding_qty", 6)
    sell.setdefault("orderable_qty", 6)
    sell.setdefault("sellable_qty", 6)
    sell.setdefault("available_qty", 6)
    route = route_order(
        sell, kis_client=broker, current_position_symbols={symbol},
        current_position_count=1, total_portfolio_usd=20000,
        available_cash_usd=10000,
    )
    assert route["status"] == "ACK", (selected_style, contract_mode, route)
    assert len(broker.posts) == 2
    # A broker ACK alone does not prove a durable claim. The actual SELL
    # order's action/attempt must resolve to the same PostgreSQL claim rows
    # BEFORE any native-format fill is accepted.
    with engine.connect() as conn:
        sell_claim = conn.execute(text("""
            SELECT o.meta->>'execution_action_key' AS action_key,
                   o.meta->>'submit_attempt_id' AS attempt_id,
                   c.action_key AS persisted_claim_key,
                   a.attempt_id AS persisted_attempt_id
            FROM us_orders o
            LEFT JOIN us_execution_claims c
              ON c.action_key=o.meta->>'execution_action_key'
            LEFT JOIN us_execution_attempts a
              ON a.action_key=c.action_key
             AND a.attempt_id=o.meta->>'submit_attempt_id'
            WHERE o.order_no='CI-SELL-EIGHT'
        """)).mappings().one()
    assert sell_claim["persisted_claim_key"] == sell_claim["action_key"], sell_claim
    assert sell_claim["persisted_attempt_id"] == sell_claim["attempt_id"], sell_claim
    actual_sell_qty = int(broker.posts[-1][2])
    # Reconciliation must reject a stale BUY attempt attached to a SELL
    # fill without mutating the SELL ACK or creating an accounting fill.
    wrong_claim = repos.mark_order_filled_by_reconcile(
        order_no="CI-SELL-EIGHT",
        client_order_key=route["intent"]["client_order_key"],
        symbol=symbol, side="SELL", filled_qty=actual_sell_qty,
        requested_qty=actual_sell_qty,
        cumulative_filled_qty=actual_sell_qty,
        avg_price_usd=8.0, trade_date="2026-11-11",
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        source="fills_by_order_no",
        meta={**fill_meta, "cumulative_filled_qty":actual_sell_qty},
    )
    assert wrong_claim["status"] == "FILL_EXECUTION_CLAIM_IDENTITY_CONFLICT", wrong_claim
    with engine.connect() as conn:
        assert conn.execute(text("""
            SELECT status FROM us_orders WHERE order_no='CI-SELL-EIGHT'
        """)).scalar_one() == "ACK"
        assert conn.execute(text("""
            SELECT COUNT(*) FROM us_fills WHERE order_no='CI-SELL-EIGHT'
        """)).scalar_one() == 0
    confirmed_sell = repos.mark_order_filled_by_reconcile(
        order_no="CI-SELL-EIGHT",
        client_order_key=route["intent"]["client_order_key"],
        symbol=symbol, side="SELL", filled_qty=actual_sell_qty,
        requested_qty=actual_sell_qty,
        cumulative_filled_qty=actual_sell_qty,
        avg_price_usd=8.0, trade_date="2026-11-11",
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        source="fills_by_order_no",
        # SELL evidence references the immutable BUY contract, but does not
        # impersonate the BUY's execution claim or submit attempt.
        meta={**{
            k: v for k, v in fill_meta.items()
            if k not in ("execution_action_key", "submit_attempt_id")
        }, "cumulative_filled_qty":actual_sell_qty,
              "source_entry_contract_sha256":signed["sha256"]},
    )
    assert confirmed_sell["status"] == "OK", confirmed_sell
    with engine.connect() as conn:
        sell_row = conn.execute(text("""
            SELECT o.status,o.meta AS order_meta, f.meta AS fill_meta,
                   o.position_cycle_id AS order_cycle,
                   f.position_cycle_id AS fill_cycle
            FROM us_orders o JOIN us_fills f
              ON f.order_no=o.order_no AND f.client_order_key=o.client_order_key
            WHERE o.order_no='CI-SELL-EIGHT'
        """)).mappings().one()
    assert sell_row["status"] == "FILLED"
    assert sell_row["order_meta"]["source_entry_contract_sha256"] == signed["sha256"]
    assert (sell_row["order_cycle"],sell_row["fill_cycle"]) == (cycle,cycle)
    assert sell_row["fill_meta"]["source_entry_contract_sha256"] == signed["sha256"]
    if paper_profile:
        with engine.connect() as conn:
            original_count = conn.execute(text("""
                SELECT count(*) FROM us_fills WHERE order_no='CI-SELL-EIGHT'
            """)).scalar_one()
        replay = repos.mark_order_filled_by_reconcile(
            order_no="CI-SELL-EIGHT",
            client_order_key=route["intent"]["client_order_key"],
            symbol=symbol, side="SELL",
            filled_qty=actual_sell_qty, requested_qty=actual_sell_qty,
            cumulative_filled_qty=actual_sell_qty, avg_price_usd=8.0,
            trade_date="2026-11-11",
            evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
            source="fills_by_order_no",
            meta={
                **{k: v for k, v in fill_meta.items()
                   if k not in ("execution_action_key", "submit_attempt_id")},
                "cumulative_filled_qty": actual_sell_qty,
                "source_entry_contract_sha256": signed["sha256"],
            },
        )
        assert replay["status"] == "OK", replay
        with engine.connect() as conn:
            assert conn.execute(text("""
                SELECT count(*) FROM us_fills WHERE order_no='CI-SELL-EIGHT'
            """)).scalar_one() == original_count


def test_pg_authoritative_balance_survives_optional_lineage_query_exception(
    amgn_pg_engine, monkeypatch, caplog,
):
    """A failed optional original-scope SELECT must not poison broker balance.

    This disposable real PostgreSQL database deliberately lacks the risk-state
    table: verified carried lifecycle cannot be proven. The real repo must
    preserve actual KIS-shaped qty but must NOT invent account/cycle authority.
    """
    import json
    import hashlib
    from sqlalchemy import text
    from trader.us.db import repos
    from trader.us.entry_exit_contract import build_us_entry_exit_contract

    engine = amgn_pg_engine
    epoch = "ci-savepoint-balance-epoch"
    raw_account = "practice:ci-savepoint-account"
    account_hash = hashlib.sha256(raw_account.encode()).hexdigest()
    frozen = build_us_entry_exit_contract({
        "symbol": "AMGN", "strategy_owner": "US_STANDARD",
        "position_action": "NEW_POSITION_BUY",
        "entry_style_selected": "ENTRY_PULLBACK",
        "book": "SWING_BOOK", "horizon": "SWING_CARRY",
    })
    cycle = "ci-savepoint-source-cycle"
    portfolio = "ci-savepoint-portfolio"
    prior_meta = {
        "env": "practice", "strategy_owner": "US_STANDARD",
        "broker_account_hash": account_hash,
        "trading_epoch_id": epoch, "portfolio_epoch_id": portfolio,
        "position_cycle_id": cycle, "position_lifecycle_id": cycle,
        "entry_exit_contract": frozen,
        "entry_exit_contract_sha256": frozen["sha256"],
        "entry_exit_contract_version": frozen["version"],
        "lineage_resolution": "EXACT_ORIGINAL_KIS_CUMULATIVE_FULL_BUY",
        "book": "SWING_BOOK", "horizon": "SWING_CARRY",
        "exit_policy": "US_SWING_DEFAULT",
    }
    monkeypatch.setattr(repos, "_active_us_epoch", lambda _conn=None, **_k: epoch)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE trading_epochs(
                trading_epoch_id TEXT PRIMARY KEY, env TEXT NOT NULL,
                account_id TEXT NOT NULL, status TEXT NOT NULL
            )
        """))
        conn.execute(text("""
            INSERT INTO trading_epochs(trading_epoch_id,env,account_id,status)
            VALUES (:epoch,'practice',:account,'ACTIVE')
        """), {"epoch": epoch, "account": raw_account})
        conn.execute(text("""
            INSERT INTO us_positions(
                as_of,symbol,exchange,qty,avg_cost,trading_epoch_id,
                account_id,portfolio_epoch_id,position_cycle_id,meta
            ) VALUES (
                '2026-11-10','AMGN','NASDAQ',4,10,:epoch,
                :account,:portfolio,:cycle,CAST(:meta AS jsonb)
            )
        """), {
            "epoch": epoch, "account": account_hash, "portfolio": portfolio,
            "cycle": cycle, "meta": json.dumps(prior_meta),
        })
        assert conn.execute(text(
            "SELECT to_regclass('us_position_risk_state')"
        )).scalar_one() is None

    # The missing lifecycle table causes a SELECT ProgrammingError inside
    # verified_carried_us_snapshot_identity. That read must roll back ONLY its
    # SAVEPOINT and must not lose the raw next-day broker quantity.
    saved = repos.save_position_snapshot(
        [{"symbol": "AMGN", "exchange": "NASDAQ", "qty": 4,
          "avg_cost": 10.0, "current_px": 8.0,
          "balance_source": "kis_balance_authoritative", "meta": {}}],
        trade_date="2026-11-11",
        balance_fetch_status="OK", balance_parse_status="OK",
        authoritative_positions=True, preserve_previous_positions=False,
    )
    assert saved == 1
    assert any(
        "[US_POSITIONS][ORIGINAL_SCOPE][UNAVAILABLE]" in rec.message
        and "error=ProgrammingError" in rec.message
        and "action=SAVE_UNVERIFIED_BALANCE_ONLY" in rec.message
        for rec in caplog.records
    ), "The savepoint failure branch was NOT exercised"
    with engine.connect() as conn:
        latest = conn.execute(text("""
            SELECT qty,account_id,portfolio_epoch_id,position_cycle_id,meta
            FROM us_positions
            WHERE as_of='2026-11-11' AND symbol='AMGN'
              AND trading_epoch_id=:epoch
        """), {"epoch": epoch}).mappings().one()
    assert latest["qty"] == 4
    assert (latest["account_id"],latest["portfolio_epoch_id"],
            latest["position_cycle_id"]) == (None,None,None)
    assert latest["meta"].get("lineage_identity_status") not in {
        "VERIFIED_CURRENT_EPOCH_CONTINUITY", "VERIFIED_ORIGINAL_KIS_BUY",
    }
