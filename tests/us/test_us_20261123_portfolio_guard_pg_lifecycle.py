"""PORT-02/04/09/11: real isolated PostgreSQL stage lifecycle, no native HTTP.

The test suite explicitly simulates broker-confirmed order and fill rows
AFTER the same canonical action key was reserved. Never interpret a local
ACK-only row or a failed KIS response as an actual fill.
"""
from __future__ import annotations

import os
import json
import hashlib
import hmac
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4
from sqlalchemy.engine import make_url

import pytest
from sqlalchemy import create_engine, text

from trader.us.portfolio_profit_guard import canonical, policy_digest
from trader.us.portfolio_profit_guard_store import (
    persist_guard_observation_and_reserve,
    assert_portfolio_guard_reentry_budget,
    PortfolioGuardUnavailable,
)

URL = os.getenv("PBCORE_TEST_POSTGRES_URL")
KEY = "synthetic-test-only-" + "X" * 45
RAW_ACCOUNT = "unit-pg-account"
ACCOUNT = hashlib.sha256(RAW_ACCOUNT.encode()).hexdigest()
EPOCH = "unit-pg-epoch"
PORT = "unit-pg-portfolio"
CYCLE = "test-ko-cycle"


@pytest.fixture
def store(monkeypatch):
    if not URL:
        pytest.skip("isolated PostgreSQL URL not configured")
    # Never truncate another test's shared us_orders/us_fills. Each acceptance
    # receives its own disposable Postgres database, including in the generic
    # US Harness full-suite job.
    database_name = "pg_guard_" + uuid4().hex[:12]
    admin = create_engine(URL, isolation_level="AUTOCOMMIT")
    with admin.connect() as admin_conn:
        admin_conn.exec_driver_sql("CREATE DATABASE " + database_name)
    engine = create_engine(make_url(URL).set(database=database_name))
    with engine.begin() as conn:
        # Full migration is installed in a one-off GitHub CI service database.
        ddl = Path("migrations/0064_us_portfolio_guard_policy_state_actions.sql").read_text()
        conn.exec_driver_sql(ddl)
        conn.exec_driver_sql("""
            CREATE TABLE IF NOT EXISTS public.us_orders (
                id bigserial primary key,
                trade_date date NOT NULL DEFAULT current_date,
                client_order_key text,
                side text,
                env text,
                trading_epoch_id text,
                portfolio_epoch_id text,
                position_cycle_id text,
                account_id text,
                symbol text,
                order_no text,
                status text,
                qty_filled integer,
                meta jsonb
            );
            CREATE TABLE IF NOT EXISTS public.us_order_intents (
                id bigserial primary key,
                client_order_key text NOT NULL UNIQUE,
                account_id text,
                trading_epoch_id text,
                portfolio_epoch_id text,
                side text,
                symbol text,
                notional_usd numeric,
                created_at timestamptz DEFAULT now()
            );
            CREATE TABLE IF NOT EXISTS public.us_fills (
                id bigserial primary key,
                trade_date date NOT NULL DEFAULT current_date,
                symbol text,
                side text,
                env text,
                trading_epoch_id text,
                portfolio_epoch_id text,
                position_cycle_id text,
                account_id text,
                client_order_key text,
                order_no text,
                qty integer,
                meta jsonb
            );
        """)
        conn.exec_driver_sql("""
            TRUNCATE public.us_portfolio_guard_reentry_release,
                     public.us_portfolio_guard_actions,
                     public.us_portfolio_guard_state,
                     public.us_portfolio_guard_policy,
                     public.us_orders,
                     public.us_fills CASCADE
        """)
    from trader.us import portfolio_profit_guard_store as module
    monkeypatch.setenv("US_PORTFOLIO_GUARD_SIGNING_KEY", KEY)
    now = datetime.now(timezone.utc)
    policy = {
        "purpose": "US_STANDARD_PORTFOLIO_GIVEBACK_SELL_V1",
        "mode": "ACTIVE", "env": "practice", "sleeve": "US_STANDARD",
        "account_hash": ACCOUNT, "trading_epoch_id": EPOCH,
        "portfolio_epoch_id": PORT, "policy_id": "pg-ci",
        "policy_version": "v1",
        "effective_at": (now-timedelta(minutes=5)).isoformat(),
        "expires_at": (now+timedelta(days=1)).isoformat(),
        "accounting_method": "NATIVE_FILLS_PLUS_FRESH_KIS_MARK_V1",
        "mark_source": "KIS_BID", "activation_profit_usd": "100",
        "quote_ttl_sec": "5", "max_snapshot_skew_sec": "3",
        "rearm_mode": "OPERATOR_SIGNED_NEW_EPISODE",
        "reentry_mode": "SIGNED_RELEASE_AFTER_COOLDOWN",
        "reentry_min_cooldown_seconds": "600",
        "reentry_min_recovery_fraction": "0.1",
        "selection_method": "NATIVE_UNDERPERFORMER_FIRST",
        "min_order_notional_usd": "1",
        "max_order_notional_usd": "10000",
        "stages": [
            {"giveback_fraction": "0.25", "cumulative_reduction_fraction": "0.20"},
            {"giveback_fraction": "0.50", "cumulative_reduction_fraction": "0.40"},
        ],
    }
    policy["hmac_sha256"] = hmac.new(
        KEY.encode(), canonical(policy), hashlib.sha256,
    ).hexdigest()
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO public.us_portfolio_guard_policy
            (policy_id,policy_version,env,account_hash,trading_epoch_id,
             portfolio_epoch_id,sleeve,authorization_sha256,authorization_json,
             signed_at,effective_at,expires_at)
            VALUES ('pg-ci','v1','practice',:account,:epoch,:port,'US_STANDARD',
                    :digest,CAST(:doc AS jsonb),:now,:start,:end)
        """), {
            "account": ACCOUNT, "epoch": EPOCH, "port": PORT,
            "digest": policy_digest(policy), "doc": json.dumps(policy),
            "now": now, "start": now - timedelta(minutes=5),
            "end": now + timedelta(days=1),
        })
    yield engine,now
    engine.dispose()
    with admin.connect() as admin_conn:
        admin_conn.exec_driver_sql(
            "DROP DATABASE " + database_name + " WITH (FORCE)"
        )
    admin.dispose()


def _p(qty, bid):
    return {
        "symbol": "KO", "strategy_owner": "US_STANDARD",
        "position_cycle_id": CYCLE,
        "holding_qty": qty, "orderable_qty": qty,
        "mark_bid_usd": str(bid), "verified_entry_usd": "100",
        "snapshot_native_kis": True,
    }


def _record(conn, *, pnl, qty, bid, now, snap, sold,
            method="NATIVE_FILLS_PLUS_FRESH_KIS_MARK_V1"):
    return persist_guard_observation_and_reserve(
        conn, account_hash=ACCOUNT, trading_epoch_id=EPOCH,
        portfolio_epoch_id=PORT, verified_pnl_usd=Decimal(str(pnl)),
        verified_accounting={"method": method},
        complete_native_positions=[_p(qty,bid)],
        snapshot_id=snap,snapshot_at=now,
        existing_sell_symbols=set(),
        native_verified_sold_qty_by_cycle={CYCLE:sold},
    )


def test_pg_native_post_ack_fill_completion_and_stage2_no_double_sell(store):
    engine,now = store
    with engine.begin() as conn:
        baseline = _record(
            conn,pnl="100",qty=10,bid="100",now=now,
            snap="baseline",sold=0,
        )
    assert baseline["state"] == "WATCH"
    with engine.begin() as conn:
        triggered = _record(
            conn,pnl="75",qty=10,bid="95",
            now=now+timedelta(seconds=1),snap="trigger",sold=0,
        )
    assert triggered["state"] == "PLANNED"
    assert len(triggered["intents"]) == 1
    action = triggered["intents"][0]
    assert action["qty"] == 2
    assert action["stage_no"] == 1
    with engine.begin() as conn:
        repeated = _record(
            conn,pnl="75",qty=10,bid="95",
            now=now+timedelta(seconds=1),snap="trigger",sold=0,
        )
        assert repeated["state"] == "BLOCKED"
        assert conn.execute(text(
            "SELECT count(*) FROM public.us_portfolio_guard_actions"
        )).scalar_one() == 1
        # A real KIS-confirmed 2-share fill with a valid original order key,
        # not an ACK-only row, is injected for the reconciliation.
        conn.execute(text("""
            INSERT INTO public.us_orders
            (client_order_key,side,env,trading_epoch_id,portfolio_epoch_id,
             position_cycle_id,account_id,symbol,order_no,status,qty_filled,meta)
            VALUES (:key,'SELL','practice',:epoch,:port,:cycle,:account,
                    'KO','32121','FILLED',2,CAST(:meta AS jsonb))
        """), {
            "key":action["action_key"],"epoch":EPOCH,"port":PORT,
            "cycle":CYCLE,"account":ACCOUNT,
            "meta": json.dumps({"portfolio_guard_action_key":action["action_key"]}),
        })
        conn.execute(text("""
            INSERT INTO public.us_fills
            (client_order_key,symbol,side,env,trading_epoch_id,
             portfolio_epoch_id,position_cycle_id,account_id,order_no,qty,meta)
            VALUES (:key,'KO','SELL','practice',:epoch,:port,:cycle,:account,'00032121',2,
                    CAST(:meta AS jsonb))
        """), {
            "key":action["action_key"],"epoch":EPOCH,"port":PORT,
            "cycle":CYCLE, "account":ACCOUNT,
            "meta":json.dumps({
                "fill_evidence_type":"KIS_ORDER_CUMULATIVE_ACTUAL",
                "is_synthetic": False,
            }),
        })
    with engine.begin() as conn:
        complete = _record(
            conn,pnl="75",qty=8,bid="92",
            now=now+timedelta(seconds=2),snap="after-fill",sold=2,
        )
        state = conn.execute(text("""
            SELECT high_water_pnl_usd,state,stage_no
            FROM public.us_portfolio_guard_state
        """)).mappings().one()
        rows = conn.execute(text(
            "SELECT target_qty,cumulative_verified_fill_qty,state "
            "FROM public.us_portfolio_guard_actions"
        )).mappings().all()
    assert complete["state"] == "COMPLETE"
    assert state["state"] == "COMPLETE"
    assert state["high_water_pnl_usd"] == Decimal("100")
    assert rows[0]["state"] == "COMPLETE"
    assert rows[0]["cumulative_verified_fill_qty"] == 2
    # Stage 2 needs only 2 MORE initial-reference shares; the first 2 from
    # stage 1 remain credited, even though native market prices have fallen.
    with engine.begin() as conn:
        second = _record(
            conn,pnl="50",qty=8,bid="70",
            now=now+timedelta(seconds=3),snap="stage-two",sold=2,
        )
    assert second["state"] == "PLANNED"
    assert second["intents"][0]["stage_no"] == 2
    assert second["intents"][0]["qty"] == 2


def test_pg_no_buy_or_sell_during_unknown_action(store):
    engine,now=store
    with engine.begin() as conn:
        _record(conn,pnl=100,qty=10,bid=100,now=now,snap="baseline",sold=0)
    with engine.begin() as conn:
        _record(
            conn,pnl=75,qty=10,bid=95,now=now+timedelta(seconds=1),
            snap="trigger",sold=0,
        )
        conn.execute(text("""
            UPDATE public.us_portfolio_guard_actions
            SET state='SUBMIT_UNKNOWN'
        """))
    with engine.begin() as conn:
        held = _record(
            conn,pnl=40,qty=10,bid=60,
            now=now+timedelta(seconds=2),snap="deep-giveback",sold=0,
        )
        rows=conn.execute(text(
            "SELECT count(*) FROM public.us_portfolio_guard_actions"
        )).scalar_one()
    assert held["intents"] == []
    assert rows == 1


def test_reentry_two_workers_serialized_budget_and_retry_fenced(store, monkeypatch):
    """PORT-12 concurrent BUY worker race uses one durable spend reservation."""
    engine, now = store
    from trader.us.db import repos
    from trader.us.execution import recovery_identity
    from trader import account_state

    monkeypatch.setenv("US_PORTFOLIO_GUARD_ACTIVE", "1")
    monkeypatch.setenv("US_ACCOUNT_RESERVATION_MODE", "ENFORCE")
    monkeypatch.setenv("US_ATOMIC_CLAIM_OUTBOX_ENABLED", "1")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("US_PORTFOLIO_GUARD_REARM_SIGNING_KEY", KEY)
    monkeypatch.setattr(account_state, "get_account_key", lambda **kw: RAW_ACCOUNT)
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: engine)
    monkeypatch.setattr(repos, "_active_us_epoch", lambda *args, **kw: EPOCH)
    monkeypatch.setattr(
        recovery_identity, "current_us_portfolio_epoch", lambda *args, **kw: PORT,
    )

    effective = now - timedelta(minutes=2)
    release = {
        "purpose": "US_STANDARD_PORTFOLIO_REENTRY_RELEASE_V1",
        "env": "practice", "sleeve": "US_STANDARD",
        "account_hash": ACCOUNT,
        "trading_epoch_id": EPOCH, "portfolio_epoch_id": PORT,
        "policy_id": "pg-ci", "policy_version": "v1",
        "episode_id": "episode-ci",
        "max_new_buy_notional_usd": "100",
        "effective_at": effective.isoformat(),
        "expires_at": (now + timedelta(hours=1)).isoformat(),
    }
    with engine.begin() as conn:
        proof = conn.execute(text("""
            SELECT authorization_sha256 FROM public.us_portfolio_guard_policy
            WHERE policy_id='pg-ci'
        """)).scalar_one()
        release["policy_sha256"] = proof
        release_hash = hashlib.sha256(canonical(release)).hexdigest()
        release["hmac_sha256"] = hmac.new(
            KEY.encode(), canonical(release), hashlib.sha256,
        ).hexdigest()
        conn.execute(text("""
            INSERT INTO public.us_portfolio_guard_state
            (policy_id,policy_version,env,account_hash,trading_epoch_id,
             portfolio_epoch_id,sleeve,accounting_method,baseline_provenance,
             high_water_pnl_usd,latest_pnl_usd,last_snapshot_id,last_snapshot_at,
             episode_id,episode_anchor,state,stage_no)
            VALUES ('pg-ci','v1','practice',:account,:epoch,:portfolio,
                    'US_STANDARD','NATIVE_FILLS_PLUS_FRESH_KIS_MARK_V1',
                    '{}'::jsonb,100,90,'verified-recovery',:now,'episode-ci',
                    CAST(:anchor AS jsonb),'COMPLETE',1)
        """), {"account":ACCOUNT,"epoch":EPOCH,"portfolio":PORT,"now":now,
               "anchor":json.dumps({
                   "triggered_at_utc":(now-timedelta(hours=1)).isoformat(),
                   "triggered_pnl_usd":"75","initial_hwm_usd":"100"
               })})
        conn.execute(text("""
            INSERT INTO public.us_portfolio_guard_reentry_release
            (policy_id,policy_version,env,account_hash,trading_epoch_id,
             portfolio_epoch_id,episode_id,release_sha256,release_json,
             effective_at,expires_at)
            VALUES ('pg-ci','v1','practice',:account,:epoch,:portfolio,'episode-ci',
                    :digest,CAST(:document AS jsonb),:effective,:expires)
        """), {"account":ACCOUNT,"epoch":EPOCH,"portfolio":PORT,
               "digest":release_hash,"document":json.dumps(release),
               "effective":effective,"expires":now+timedelta(hours=1)})
        for idx in range(2):
            conn.execute(text("""
                INSERT INTO public.us_order_intents
                (client_order_key,account_id,trading_epoch_id,
                 portfolio_epoch_id,side,symbol,notional_usd)
                VALUES (:key,:account,:epoch,:portfolio,'BUY','KO',60)
            """), {"key":"budget-buy-"+str(idx),"account":ACCOUNT,
                   "epoch":EPOCH,"portfolio":PORT})

    def submit_mock(key):
        try:
            assert_portfolio_guard_reentry_budget(
                {"side":"BUY","strategy_owner":"US_STANDARD",
                 "symbol":"KO","client_order_key":key},
                order_notional_usd=Decimal("60"), now=now,
            )
            return "RESERVED"
        except PortfolioGuardUnavailable as exc:
            return str(exc)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(submit_mock, ("budget-buy-0","budget-buy-1")))
    assert results.count("RESERVED") == 1, results
    assert sum("RISK_BUDGET_EXCEEDED" in row for row in results) == 1, results
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT client_order_key,notional_usd
            FROM public.us_portfolio_guard_reentry_buy_reservations
        """)).mappings().all()
        assert len(rows) == 1
        assert rows[0]["notional_usd"] == Decimal("60")
    # A local retry may not create a duplicate budget reservation or HTTP.
    assert "BUY_KEY_ALREADY_RESERVED" in submit_mock(rows[0]["client_order_key"])



def test_pg_signed_trading_basis_triggers_and_cannot_reuse_net_hwm(store):
    from trader.us.portfolio_profit_guard import TRADING_ACCOUNTING_METHOD
    engine, now = store
    with engine.begin() as conn:
        with pytest.raises(PortfolioGuardUnavailable, match="SIGNED_ACCOUNTING_METHOD_MISMATCH"):
            _record(conn, pnl="100", qty=10, bid="100", now=now,
                    snap="unsigned-basis", sold=0, method=TRADING_ACCOUNTING_METHOD)
        doc = conn.execute(text(
            "SELECT authorization_json FROM public.us_portfolio_guard_policy"
        )).scalar_one()
        doc.pop("hmac_sha256")
        doc["accounting_method"] = TRADING_ACCOUNTING_METHOD
        doc["hmac_sha256"] = hmac.new(KEY.encode(), canonical(doc), hashlib.sha256).hexdigest()
        conn.execute(text("""
            UPDATE public.us_portfolio_guard_policy
            SET authorization_json=CAST(:doc AS jsonb), authorization_sha256=:digest
        """), {"doc": json.dumps(doc), "digest": policy_digest(doc)})
        baseline = _record(conn, pnl="100", qty=10, bid="100", now=now,
                           snap="trade-baseline", sold=0, method=TRADING_ACCOUNTING_METHOD)
        assert baseline["state"] == "WATCH"
        with pytest.raises(PortfolioGuardUnavailable, match="SIGNED_ACCOUNTING_METHOD_MISMATCH"):
            _record(conn, pnl="75", qty=10, bid="95", now=now+timedelta(seconds=1),
                    snap="net-mix", sold=0)
        trigger = _record(conn, pnl="75", qty=10, bid="95", now=now+timedelta(seconds=1),
                          snap="trade-trigger", sold=0, method=TRADING_ACCOUNTING_METHOD)
        assert trigger["state"] == "PLANNED"
        assert trigger["intents"][0]["qty"] == 2
        state = conn.execute(text(
            "SELECT accounting_method, high_water_pnl_usd FROM public.us_portfolio_guard_state"
        )).mappings().one()
        assert state["accounting_method"] == TRADING_ACCOUNTING_METHOD
        assert state["high_water_pnl_usd"] == Decimal("100")


@pytest.mark.parametrize("conflict", [
    "foreign_order", "missing_order", "regression", "wrong_symbol",
    "missing_order_account", "foreign_fill_day",
])
def test_pg_guard_fill_cannot_complete_wrong_or_regressed_native_order(store,conflict):
    engine,now=store
    with engine.begin() as conn:
        _record(conn,pnl=100,qty=10,bid=100,now=now,snap="baseline",sold=0)
        planned=_record(conn,pnl=75,qty=10,bid=95,now=now+timedelta(seconds=1),snap="trigger",sold=0)
        key=planned["intents"][0]["action_key"]
        conn.execute(text("""INSERT INTO public.us_orders
            (client_order_key,side,env,trading_epoch_id,portfolio_epoch_id,position_cycle_id,
             account_id,symbol,order_no,status,qty_filled,meta)
            VALUES (:key,'SELL','practice',:epoch,:port,:cycle,:account,:sym,'32121','FILLED',2,
                    CAST(:meta AS jsonb))"""),
            dict(key=key,epoch=EPOCH,port=PORT,cycle=CYCLE,
                 account=None if conflict=="missing_order_account" else ACCOUNT,
                 sym="PG" if conflict=="wrong_symbol" else "KO",
                 meta=json.dumps({"portfolio_guard_action_key":key})))
        conn.execute(text("""INSERT INTO public.us_fills
            (client_order_key,symbol,side,env,trading_epoch_id,portfolio_epoch_id,position_cycle_id,
             account_id,order_no,trade_date,qty,meta)
            VALUES (:key,'KO','SELL','practice',:epoch,:port,:cycle,:account,:no,
                    current_date - :days_before,:qty,
                    CAST(:meta AS jsonb))"""),
            dict(key=key,epoch=EPOCH,port=PORT,cycle=CYCLE,account=ACCOUNT,
                 no={"foreign_order":"55555","missing_order":None}.get(conflict,"00032121"),
                 days_before=1 if conflict=="foreign_fill_day" else 0,
                 qty=1 if conflict=="regression" else 2,
                 meta=json.dumps({"fill_evidence_type":"KIS_ORDER_CUMULATIVE_ACTUAL","is_synthetic":False})))
        if conflict=="regression":
            conn.execute(text("UPDATE public.us_portfolio_guard_actions SET state='PARTIAL', cumulative_verified_fill_qty=2"))
    if conflict in {"wrong_symbol", "missing_order_account"}:
        with pytest.raises(PortfolioGuardUnavailable,match="SCOPE_MISMATCH"):
            with engine.begin() as conn:
                _record(conn,pnl=50,qty=8,bid=70,now=now+timedelta(seconds=2),snap="next",sold=2)
    else:
        with engine.begin() as conn:
            result=_record(conn,pnl=50,qty=8,bid=70,now=now+timedelta(seconds=2),snap="next",sold=2)
            assert result["state"]=="BLOCKED" and not result["intents"]
    with engine.connect() as conn:
        rows=conn.execute(text("SELECT state,cumulative_verified_fill_qty FROM public.us_portfolio_guard_actions")).all()
        assert len(rows)==1 and rows[0][0]!="COMPLETE"
        assert rows[0][1]==(2 if conflict=="regression" else 0)
