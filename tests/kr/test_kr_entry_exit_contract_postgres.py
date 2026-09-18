from __future__ import annotations

import os
from datetime import datetime, timezone

import pytest
import sqlalchemy as sa
from sqlalchemy import create_engine, text

from trader.db.repos import OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.exit_policy.router import resolve_exit_policy_for_position, apply_swing_exit_decision
from trader.kr.market_state_overlay import (
    build_kr_policy_missing_adoption,
    generate_kr_profit_capture_intents,
    is_verified_kr_policy_missing_adoption,
)
from trader.trade_plan import build_entry_exit_plan


pytestmark = pytest.mark.skipif(
    not os.getenv("PBCORE_TEST_POSTGRES_URL"),
    reason="real PostgreSQL integration URL not configured",
)

_SCHEMA = "kr_entry_exit_contract_e2e"


@pytest.fixture()
def pg_contract_engine():
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
    # The KR CI runs legacy-migration tests against public.* first.  Force
    # creation in this isolated search_path schema instead of letting
    # checkfirst=True mistake public legacy tables for our E2E tables.
    schema_for_engine(engine).metadata.create_all(engine, checkfirst=False)
    try:
        yield engine
    finally:
        engine.dispose()
        cleanup = create_engine(url, future=True)
        with cleanup.begin() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE"))
        cleanup.dispose()


def test_postgres_new_buy_contract_survives_fill_restart_and_drives_exact_sell(
    pg_contract_engine, monkeypatch,
):
    schema = schema_for_engine(pg_contract_engine)
    orders = OrdersRepo(pg_contract_engine)
    positions = PositionsRepo(pg_contract_engine)

    monkeypatch.setenv("PB1_SWING_TP1_PROFIT_PCT", "12")
    monkeypatch.setenv("PB1_SWING_TP1_SELL_PCT", "0.33")
    plan = build_entry_exit_plan(
        code="123450",
        market="KOSPI",
        entry_style_selected="ENTRY_PULLBACK",
        entry_reason="ENTRY_PULLBACK",
        entry_price=100.0,
        features={"stop_price": 95.0},
    ).to_dict()

    order_id, created = orders.create_intent_idempotent(
        env="practice",
        run_id=None,
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="123450",
        market="KOSPI",
        side="BUY",
        ord_type="LIMIT",
        qty=10,
        limit_price=100.0,
        stage="PB1-ENTRY",
        client_order_key="kr-pg-buy-sell-e2e",
        request_json={
            "enforce_entry_contract": True,
            "entry_exit_plan": plan,
            "pre_order_holding_qty": 0,
            "requested_qty": 10,
            "submitted_qty": 10,
            "balance_snapshot_id": "kr-pg-balance",
        },
        entry_meta_json={
            "entry_reason": "ENTRY_PULLBACK",
            "entry_style_selected": "ENTRY_PULLBACK",
        },
        status="ACKED",
        account_id="acct-pg-e2e",
    )
    assert created is True

    persisted_order = orders.get_order_by_client_order_key(
        "practice", "kr-pg-buy-sell-e2e"
    )
    root_sha = persisted_order["request_json"]["entry_contract_sha256"]
    assert root_sha

    positions.apply_fill(
        env="practice",
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="123450",
        market="KOSPI",
        side="BUY",
        qty=10,
        price=100.0,
        fee=0.0,
        tax=0.0,
        filled_at=datetime(2026, 9, 18, 1, 0, tzinfo=timezone.utc),
        order_id=order_id,
        account_id="acct-pg-e2e",
    )

    # Real PostgreSQL JSONB/FLOAT read-back boundary.
    with pg_contract_engine.connect() as conn:
        stored = dict(
            conn.execute(
                sa.select(schema.positions).where(schema.positions.c.code == "123450")
            ).mappings().one()
        )
    assert stored["entry_exit_plan_json"] == plan
    assert stored["entry_meta_json"]["entry_contract_sha256"] == root_sha
    assert stored["entry_meta_json"]["entry_exit_plan_sha256"]

    # A fresh repository instance is the restart boundary. Changing current ENV
    # must not change this already-filled lifecycle's TP threshold or sell ratio.
    restarted = PositionsRepo(pg_contract_engine)
    reloaded = restarted.get_position(
        env="practice",
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="123450",
        position_cycle_id=str(stored["position_cycle_id"]),
        portfolio_epoch_id=str(stored["portfolio_epoch_id"]),
    )
    assert reloaded

    monkeypatch.setenv("PB1_SWING_TP1_PROFIT_PCT", "99")
    monkeypatch.setenv("PB1_SWING_TP1_SELL_PCT", "0.90")
    policy = resolve_exit_policy_for_position(
        reloaded,
        {},
        {
            "current_return_pct": 13.0,
            "current_r": 2.6,
            "highest_return_pct": 13.0,
            "trading_days_held": 2,
            "calendar_days_held": 2,
            "holding_bars": 2,
            "mark": 113.0,
        },
        {"ma20": 105.0, "ma50": 100.0, "regime": "NORMAL"},
    )
    assert policy["policy_source"] == "ENTRY_EXIT_PLAN"

    decision = apply_swing_exit_decision(
        {**reloaded, "orderable_qty": 10, "qty": 10},
        113.0,
        policy,
        ret_pct=13.0,
        current_r=2.6,
        highest_ret_pct=13.0,
        days_held=2,
        stop_hit=False,
        ma20=105.0,
        effective_stop=95.0,
        effective_r=5.0,
        risk_ctx={"raw_stop_price": 95.0},
    )
    assert decision["exit_ok"] is True
    assert decision["reason"] == "SWING_PCT_TP1"
    assert decision["qty"] == 3
    assert decision["sell_pct"] == pytest.approx(0.33)


def test_postgres_any_profitable_imported_policy_missing_position_adopts_and_tp1s(
    pg_contract_engine,
):
    positions = PositionsRepo(pg_contract_engine)
    result = positions.upsert_positions_from_kis_holdings(
        env="practice",
        account_key="acct-legacy-pg",
        holdings=[
            {
                "pdno": "654321",
                "hldg_qty": "40",
                "ord_psbl_qty": "40",
                "pchs_avg_pric": "2358.671",
                "pchs_amt": str(2358.671 * 40),
                "prpr": "3600",
                "evlu_amt": str(3600 * 40),
                "prdt_type_cd": "KOSDAQ",
                "prdt_name": "GENERIC_LEGACY",
            }
        ],
    )
    assert result["inserted"] == 1

    rows = positions.list_positions_by_codes(
        env="practice", strategy="pb1", codes=["654321"]
    )
    assert len(rows) == 1
    original = rows[0]
    assert original["exit_policy_family"] == "POLICY_MISSING"

    adoption = build_kr_policy_missing_adoption(original, current_price=3600.0)
    assert adoption is not None
    positions.update_position_fields(
        env="practice",
        strategy="pb1",
        sid=1,
        mode=1,
        code="654321",
        fields=adoption["position_fields"],
        position_cycle_id=str(original["position_cycle_id"]),
        portfolio_epoch_id=str(original["portfolio_epoch_id"]),
    )

    # A fresh SELECT must verify the contract after actual JSONB/FLOAT round-trip.
    reloaded = positions.get_position(
        env="practice",
        strategy="pb1",
        sid=1,
        mode=1,
        code="654321",
        position_cycle_id=str(original["position_cycle_id"]),
        portfolio_epoch_id=str(original["portfolio_epoch_id"]),
    )
    assert reloaded
    assert is_verified_kr_policy_missing_adoption(reloaded) is True

    reloaded["orderable_qty"] = 40
    reloaded["qty"] = 40
    reloaded["unrealized_pnl_pct"] = (3600.0 - float(reloaded["avg_buy_price"])) / float(reloaded["avg_buy_price"])
    intents = generate_kr_profit_capture_intents(
        [reloaded], {"market_state": "KR_NORMAL"}
    )
    assert intents
    assert intents[0]["code"] == "654321"
    assert intents[0]["reason"] == "KR_TAKE_PROFIT_TP1"
    assert intents[0]["qty"] == 10
    assert intents[0]["exit_rule_source"] == "POLICY_ADOPTION_CONTRACT"
