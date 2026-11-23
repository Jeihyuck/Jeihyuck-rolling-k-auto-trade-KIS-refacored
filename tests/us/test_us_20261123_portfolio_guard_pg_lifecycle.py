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
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from trader.us.portfolio_profit_guard import canonical, policy_digest
from trader.us.portfolio_profit_guard_store import (
    persist_guard_observation_and_reserve,
)

URL = os.getenv("PBCORE_TEST_POSTGRES_URL")
KEY = "synthetic-test-only-" + "X" * 45
ACCOUNT = "a" * 64
EPOCH = "unit-pg-epoch"
PORT = "unit-pg-portfolio"
CYCLE = "test-ko-cycle"


@pytest.fixture
def store(monkeypatch):
    if not URL:
        pytest.skip("isolated PostgreSQL URL not configured")
    engine = create_engine(URL)
    with engine.begin() as conn:
        # Full migration is installed in a one-off GitHub CI service database.
        ddl = Path("migrations/0064_us_portfolio_guard_policy_state_actions.sql").read_text()
        conn.exec_driver_sql(ddl)
        conn.exec_driver_sql("""
            CREATE TABLE IF NOT EXISTS public.us_orders (
                id bigserial primary key,
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
            CREATE TABLE IF NOT EXISTS public.us_fills (
                id bigserial primary key,
                symbol text,
                side text,
                env text,
                trading_epoch_id text,
                portfolio_epoch_id text,
                position_cycle_id text,
                client_order_key text,
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


def _p(qty, bid):
    return {
        "symbol": "KO", "strategy_owner": "US_STANDARD",
        "position_cycle_id": CYCLE,
        "holding_qty": qty, "orderable_qty": qty,
        "mark_bid_usd": str(bid), "verified_entry_usd": "100",
        "snapshot_native_kis": True,
    }


def _record(conn, *, pnl, qty, bid, now, snap, sold):
    return persist_guard_observation_and_reserve(
        conn, account_hash=ACCOUNT, trading_epoch_id=EPOCH,
        portfolio_epoch_id=PORT, verified_pnl_usd=Decimal(str(pnl)),
        verified_accounting={"method": "NATIVE_FILLS_PLUS_FRESH_KIS_MARK_V1"},
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
             portfolio_epoch_id,position_cycle_id,qty,meta)
            VALUES (:key,'KO','SELL','practice',:epoch,:port,:cycle,2,
                    CAST(:meta AS jsonb))
        """), {
            "key":action["action_key"],"epoch":EPOCH,"port":PORT,
            "cycle":CYCLE,
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
