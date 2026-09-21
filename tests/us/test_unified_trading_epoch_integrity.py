from __future__ import annotations

import json
import os

import pytest
from sqlalchemy import create_engine, text

import trader.account_state as account_state
import trader.us.db.repos as repos


pytestmark = pytest.mark.skipif(
    not os.getenv("PBCORE_TEST_POSTGRES_URL"),
    reason="real PostgreSQL integration URL not configured",
)

_SCHEMA = "us_unified_epoch_integrity"


@pytest.fixture()
def epoch_pg(monkeypatch):
    url = os.environ["PBCORE_TEST_POSTGRES_URL"]
    admin = create_engine(url, future=True)
    with admin.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {_SCHEMA}"))
    admin.dispose()

    engine = create_engine(
        url,
        future=True,
        connect_args={"options": f"-csearch_path={_SCHEMA},public"},
    )
    with engine.begin() as conn:
        conn.exec_driver_sql(open("migrations/0038_us_agent_tables.sql", encoding="utf-8").read())
        conn.exec_driver_sql(open("migrations/0043_us_fills_idempotency_and_order_reconcile_fix.sql", encoding="utf-8").read())
        conn.exec_driver_sql(open("migrations/0046_us_orders_committed_notional.sql", encoding="utf-8").read())
        conn.exec_driver_sql("""
            CREATE TABLE trading_epochs (
                trading_epoch_id TEXT PRIMARY KEY,
                env TEXT NOT NULL,
                account_id TEXT NOT NULL,
                started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                ended_at TIMESTAMPTZ,
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                reason TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            ALTER TABLE us_order_intents ADD COLUMN trading_epoch_id TEXT;
            ALTER TABLE us_orders ADD COLUMN trading_epoch_id TEXT;
            ALTER TABLE us_fills ADD COLUMN trading_epoch_id TEXT;
            ALTER TABLE us_positions ADD COLUMN trading_epoch_id TEXT;
        """)
        conn.execute(text("""
            INSERT INTO trading_epochs(
                trading_epoch_id, env, account_id, status, reason
            ) VALUES ('epoch-current','practice','practice:test','ACTIVE','TEST')
        """))

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: engine)
    monkeypatch.setattr(account_state, "resolve_env_name", lambda env=None: "practice")
    monkeypatch.setattr(account_state, "get_account_key", lambda env=None: "practice:test")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("STRATEGY_ENV", "practice")
    try:
        yield engine
    finally:
        repos.reset_memory_stores()
        engine.dispose()
        cleanup = create_engine(url, future=True)
        with cleanup.begin() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE"))
        cleanup.dispose()


def _save_current_order(*, key: str, order_no: str, qty_requested: int = 10, qty_filled: int = 0):
    assert repos.save_order_ack({
        "client_order_key": key,
        "symbol": "AMD",
        "exchange": "NASDAQ",
        "side": "BUY",
        "qty_requested": qty_requested,
        "qty_filled": qty_filled,
        "avg_price_usd": 100,
        "order_no": order_no,
        "status": "ACK" if qty_filled == 0 else "PARTIALLY_FILLED",
        "env": "practice",
        "meta": {
            "strategy_owner": "US_STANDARD",
            "entry_exit_contract_sha256": "frozen-entry-contract",
        },
    }, trade_date="2026-09-22")


def _insert_fill(
    engine,
    *,
    epoch: str | None,
    order_no: str,
    key: str,
    qty: int,
    idem: str,
    synthetic: bool,
    cumulative: int,
):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO us_fills(
                trade_date,symbol,exchange,side,qty,price_usd,order_no,
                client_order_key,filled_at,trading_epoch_id,meta,fill_idempotency_key
            ) VALUES (
                '2026-09-22','AMD','NASDAQ','BUY',:qty,100,:order_no,
                :key,NOW(),:epoch,CAST(:meta AS jsonb),:idem
            )
        """), {
            "qty": qty,
            "order_no": order_no,
            "key": key,
            "epoch": epoch,
            "idem": idem,
            "meta": json.dumps({
                "is_synthetic": synthetic,
                "fill_evidence_type": (
                    "BALANCE_DELTA_SYNTHETIC"
                    if synthetic else "KIS_ORDER_CUMULATIVE_ACTUAL"
                ),
                "cumulative_filled_qty": cumulative,
                "accounting_active": True,
            }),
        })


def test_us_cumulative_fill_is_bound_to_source_order_epoch_and_accounting(epoch_pg):
    _save_current_order(key="epoch-order-1", order_no="EPOCH-O1", qty_requested=10)
    # Prior generation evidence with the same broker order number must not
    # participate in current cumulative/regression/accounting decisions.
    _insert_fill(
        epoch_pg, epoch="epoch-old", order_no="EPOCH-O1", key="old-key",
        qty=7, idem="old-cumulative", synthetic=False, cumulative=7,
    )

    incoming = [{
        "symbol": "AMD", "exchange": "NASDAQ", "side": "BUY",
        "qty": 2, "price_usd": 101, "order_no": "EPOCH-O1",
        "client_order_key": "epoch-order-1",
        "requested_qty": 10, "cumulative_filled_qty": 2,
        "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
        "meta": {
            "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
            "cumulative_filled_qty": 2,
            "requested_qty": 10,
            "is_synthetic": False,
        },
    }]
    result = repos.save_fills_with_result(incoming, trade_date="2026-09-22")
    assert result["status"] == "OK"

    with epoch_pg.connect() as conn:
        order = conn.execute(text("""
            SELECT qty_filled,trading_epoch_id,meta FROM us_orders
            WHERE client_order_key='epoch-order-1'
        """)).mappings().one()
        current = conn.execute(text("""
            SELECT qty,trading_epoch_id,meta FROM us_fills
            WHERE order_no='EPOCH-O1' AND trading_epoch_id='epoch-current'
        """)).mappings().all()
        old = conn.execute(text("""
            SELECT qty,trading_epoch_id,meta FROM us_fills
            WHERE fill_idempotency_key='old-cumulative'
        """)).mappings().one()

    assert order["qty_filled"] == 2
    assert order["trading_epoch_id"] == "epoch-current"
    assert len(current) == 1 and current[0]["qty"] == 2
    assert current[0]["trading_epoch_id"] == "epoch-current"
    assert current[0]["meta"]["strategy_owner"] == "US_STANDARD"
    assert current[0]["meta"]["entry_exit_contract_sha256"] == "frozen-entry-contract"
    assert old["qty"] == 7 and old["trading_epoch_id"] == "epoch-old"
    assert repos.verify_order_fill_accounting(
        trade_date="2026-09-22", order_no="EPOCH-O1"
    )["status"] == "OK"


def test_us_synthetic_to_actual_only_supersedes_same_epoch(epoch_pg):
    _save_current_order(
        key="epoch-order-2", order_no="EPOCH-O2", qty_requested=10, qty_filled=3
    )
    _insert_fill(
        epoch_pg, epoch="epoch-current", order_no="EPOCH-O2", key="epoch-order-2",
        qty=3, idem="current-synthetic", synthetic=True, cumulative=3,
    )
    _insert_fill(
        epoch_pg, epoch="epoch-old", order_no="EPOCH-O2", key="old-key-2",
        qty=8, idem="old-synthetic", synthetic=True, cumulative=8,
    )

    result = repos.mark_order_filled_by_reconcile(
        order_no="EPOCH-O2",
        client_order_key="epoch-order-2",
        symbol="AMD",
        side="BUY",
        filled_qty=6,
        requested_qty=10,
        cumulative_filled_qty=6,
        avg_price_usd=102,
        trade_date="2026-09-22",
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        source="fills_by_order_no",
    )
    assert result["status"] == "OK"
    assert result["synthetic_superseded_count"] == 1

    with epoch_pg.connect() as conn:
        current_synth = conn.execute(text("""
            SELECT meta FROM us_fills WHERE fill_idempotency_key='current-synthetic'
        """)).mappings().one()
        old_synth = conn.execute(text("""
            SELECT meta FROM us_fills WHERE fill_idempotency_key='old-synthetic'
        """)).mappings().one()
        actual = conn.execute(text("""
            SELECT qty,trading_epoch_id,meta FROM us_fills
            WHERE order_no='EPOCH-O2' AND trading_epoch_id='epoch-current'
              AND NOT COALESCE((meta->>'is_synthetic')::boolean,false)
        """)).mappings().one()

    assert current_synth["meta"]["accounting_active"] is False
    assert old_synth["meta"]["accounting_active"] is True
    assert actual["qty"] == 6
    assert actual["trading_epoch_id"] == "epoch-current"
    assert repos.verify_order_fill_accounting(
        trade_date="2026-09-22", order_no="EPOCH-O2"
    )["active_fill_qty"] == 6


def test_us_legacy_order_key_cannot_be_adopted_into_active_epoch(epoch_pg):
    with epoch_pg.begin() as conn:
        conn.execute(text("""
            INSERT INTO us_orders(
                trade_date,client_order_key,symbol,exchange,side,qty_requested,
                qty_filled,avg_price_usd,order_no,status,dry_run,env,meta,trading_epoch_id
            ) VALUES (
                '2026-09-22','epoch-collision','AMD','NASDAQ','BUY',1,
                0,100,'LEGACY-ORDER','ACK',false,'practice','{}'::jsonb,NULL
            )
        """))

    saved = repos.save_order_ack({
        "client_order_key": "epoch-collision",
        "symbol": "AMD", "exchange": "NASDAQ", "side": "BUY",
        "qty_requested": 1, "qty_filled": 0, "avg_price_usd": 100,
        "order_no": "LEGACY-ORDER", "status": "ACK", "env": "practice",
        "meta": {"strategy_owner": "US_STANDARD"},
    }, trade_date="2026-09-22")
    assert saved is False

    with epoch_pg.connect() as conn:
        row = conn.execute(text("""
            SELECT trading_epoch_id,status,meta FROM us_orders
            WHERE client_order_key='epoch-collision'
        """)).mappings().one()
    assert row["trading_epoch_id"] is None
    assert row["status"] == "ACK"
    assert row["meta"] == {}


def test_us_same_trade_date_order_key_reuse_is_rejected_across_epochs(epoch_pg):
    with epoch_pg.begin() as conn:
        conn.execute(text("""
            INSERT INTO us_orders(
                trade_date,client_order_key,symbol,exchange,side,qty_requested,
                qty_filled,avg_price_usd,order_no,status,dry_run,env,meta,trading_epoch_id
            ) VALUES (
                '2026-09-22','same-day-reused-key','AMD','NASDAQ','BUY',1,
                0,100,'OLD-EPOCH-ORDER','ACK',false,'practice','{}'::jsonb,'epoch-old'
            )
        """))

    saved = repos.save_order_ack({
        "client_order_key": "same-day-reused-key",
        "symbol": "AMD", "exchange": "NASDAQ", "side": "BUY",
        "qty_requested": 1, "qty_filled": 0, "avg_price_usd": 100,
        "order_no": "NEW-EPOCH-ORDER", "status": "ACK", "env": "practice",
        "meta": {"strategy_owner": "US_STANDARD"},
    }, trade_date="2026-09-22")
    assert saved is False

    with epoch_pg.connect() as conn:
        row = conn.execute(text("""
            SELECT trading_epoch_id,status,order_no,meta
            FROM us_orders
            WHERE client_order_key='same-day-reused-key'
        """)).mappings().one()
    assert row["trading_epoch_id"] == "epoch-old"
    assert row["status"] == "ACK"
    assert row["order_no"] == "OLD-EPOCH-ORDER"
    assert row["meta"] == {}


def test_us_old_epoch_fill_cannot_make_current_accounting_pass(epoch_pg):
    _save_current_order(
        key="epoch-order-3", order_no="EPOCH-O3", qty_requested=5, qty_filled=2
    )
    _insert_fill(
        epoch_pg, epoch="epoch-old", order_no="EPOCH-O3", key="old-key-3",
        qty=2, idem="old-only-fill", synthetic=False, cumulative=2,
    )
    result = repos.verify_order_fill_accounting(
        trade_date="2026-09-22", order_no="EPOCH-O3"
    )
    assert result["status"] == "FILL_ACCOUNTING_INVARIANT_FAILED"
    assert result["order_qty_filled"] == 2
    assert result["active_fill_qty"] == 0



def test_us_broker_observation_cannot_update_previous_epoch_order(epoch_pg):
    with epoch_pg.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO us_orders(
                    trade_date,client_order_key,symbol,exchange,side,qty_requested,
                    qty_filled,avg_price_usd,order_no,status,dry_run,env,meta,trading_epoch_id
                ) VALUES (
                    '2026-09-22','old-observation-key','AMD','NASDAQ','BUY',1,
                    0,100,'OLD-OBS','ACK',false,'practice',
                    CAST(:meta AS jsonb),'epoch-old'
                )
            """),
            {"meta": json.dumps({"legacy": True})},
        )

    result = repos.apply_broker_order_observation(
        trade_date="2026-09-22",
        client_order_key="old-observation-key",
        raw_order_no="OLD-OBS",
        canonical_order_no="OLD-OBS",
        symbol="AMD",
        side="BUY",
        requested_qty=1,
        filled_qty=0,
        remaining_qty=1,
        broker_status="OPEN",
        evidence_type="KIS_ORDER_STATUS_ACTUAL",
        observed_at="2026-09-22T14:00:00+00:00",
        raw_row={"status": "OPEN"},
    )
    assert result["status"] == "ORDER_NOT_FOUND"

    with epoch_pg.connect() as conn:
        row = conn.execute(text("""
            SELECT status,trading_epoch_id,meta FROM us_orders
            WHERE client_order_key='old-observation-key'
        """)).mappings().one()
    assert row["status"] == "ACK"
    assert row["trading_epoch_id"] == "epoch-old"
    assert row["meta"] == {"legacy": True}



def test_us_intent_lifecycle_mutators_never_touch_previous_epoch(epoch_pg):
    with epoch_pg.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO us_order_intents(
                    trade_date,client_order_key,symbol,exchange,side,qty,
                    strategy,status,trading_epoch_id,meta
                ) VALUES (
                    '2026-09-22','old-intent-key','AMD','NASDAQ','BUY',1,
                    'us_pb1','PENDING','epoch-old',CAST(:meta AS jsonb)
                )
            """),
            {"meta": json.dumps({"legacy": True})},
        )

    repos.mark_order_intent_sent("old-intent-key")
    repos.mark_order_intent_blocked("old-intent-key", "should-not-write")
    repos.mark_order_intent_rejected("old-intent-key", "should-not-write")
    repos.mark_order_intent_dry_run("old-intent-key")

    with epoch_pg.connect() as conn:
        row = conn.execute(text("""
            SELECT status,trading_epoch_id,meta FROM us_order_intents
            WHERE client_order_key='old-intent-key'
        """)).mappings().one()
    assert row["status"] == "PENDING"
    assert row["trading_epoch_id"] == "epoch-old"
    assert row["meta"] == {"legacy": True}
