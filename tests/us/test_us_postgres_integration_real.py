import os
import pytest

# PR68 final accounting integration tests use the real PostgreSQL service in CI.
pytestmark = pytest.mark.skipif(not os.getenv("PBCORE_TEST_POSTGRES_URL"), reason="real PostgreSQL integration URL not configured")

@pytest.fixture()
def pg_engine(monkeypatch):
    from sqlalchemy import create_engine, text
    import trader.us.db.repos as repos
    engine = create_engine(os.environ["PBCORE_TEST_POSTGRES_URL"], future=True)
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS us_fills CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS us_orders CASCADE"))
        conn.execute(text("""CREATE TABLE us_orders (
            id bigserial PRIMARY KEY, trade_date date NOT NULL, client_order_key text NOT NULL,
            symbol text NOT NULL, exchange text NOT NULL, side text NOT NULL,
            qty_requested integer NOT NULL, qty_filled integer NOT NULL DEFAULT 0,
            avg_price_usd numeric, order_no text, status text,
            meta jsonb NOT NULL DEFAULT '{}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now())"""))
        conn.execute(text("""CREATE TABLE us_fills (
            id bigserial PRIMARY KEY, trade_date date NOT NULL, symbol text NOT NULL,
            exchange text NOT NULL, side text NOT NULL, qty integer NOT NULL,
            price_usd numeric, order_no text, client_order_key text NOT NULL,
            filled_at timestamptz, meta jsonb NOT NULL DEFAULT '{}'::jsonb,
            fill_idempotency_key text UNIQUE)"""))
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
