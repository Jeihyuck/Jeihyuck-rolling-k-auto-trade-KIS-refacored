"""KR frozen-contract guard: pure audit + real PostgreSQL trigger tests.

PostgreSQL test is conditional on PBCORE_TEST_POSTGRES_URL and runs in an
isolated schema. NEVER point the test URL to an operator/production database.
"""
from __future__ import annotations

import copy
import os
import uuid
from pathlib import Path

import pytest
import sqlalchemy as sa

from scripts.kr_buy_sell_contract_audit import _sha, audit_buy
from trader.db.migrate import (
    assert_no_migration_transaction_control, split_postgres_sql,
)

MIGRATION = Path(__file__).resolve().parents[2] / "migrations" / (
    "0063_kr_signed_buy_position_contract_immutability.sql"
)


def _fixture_case():
    plan = {
        "entry_style_selected": "ENTRY_PULLBACK",
        "exit_policy_family": "SWING_STAGED_EXIT",
        "profit_plan": {"tp1_sell_pct": 0.33, "tp1_profit_pct": 12.0},
    }
    order = {
        "order_id": "order1", "strategy": "pb1_pullback_close", "code": "035760",
        "market": "KOSDAQ", "position_cycle_id": "cycle1",
        "portfolio_epoch_id": "portfolio1", "trading_epoch_id": "epoch1",
        "qty": 10, "request_json": {
            "entry_exit_plan": plan, "entry_contract_sha256": "a" * 64,
        },
    }
    position = {
        "code": "035760", "strategy": "pb1_pullback_close",
        "position_cycle_id": "cycle1", "portfolio_epoch_id": "portfolio1",
        "trading_epoch_id": "epoch1", "qty": 6, "status": "OPEN",
        "position_origin": "SYSTEM", "entry_style_selected": "ENTRY_PULLBACK",
        "exit_policy_family": "SWING_STAGED_EXIT", "entry_exit_plan_json": plan,
        "entry_meta_json": {
            "entry_contract_sha256": "a" * 64,
            "entry_exit_plan_sha256": _sha(plan),
            "source_buy_order_id": "order1",
        },
    }
    fill = {
        "order_id": "order1", "qty": 10, "price": 2000.0,
        "fill_meta_json": {"evidence_kind": "KIS_ORDER_CUMULATIVE_EXECUTION"},
    }
    sell = {
        "order_id": "sell1", "code": "035760", "strategy": "pb1_pullback_close",
        "position_cycle_id": "cycle1", "portfolio_epoch_id": "portfolio1",
        "qty": 4, "status": "FILLED",
    }
    sell_fill = {"order_id": "sell1", "qty": 4, "price": 2100.0}
    return order, position, fill, sell, sell_fill


def test_exact_signed_cycle_and_partial_sell_audits_expected():
    order, position, fill, sell, sell_fill = _fixture_case()
    result = audit_buy(
        order, positions=[position], fills=[fill], sells=[sell],
        sell_fills=[sell_fill], names={"035760": "CJ ENM"},
    )
    assert result["status"] == "EXPECTED"
    assert result["remaining_qty"] == 6
    assert result["sell_filled_qty_same_cycle"] == 4


def test_reconcile_imported_cycle_is_not_silently_rebound():
    order, position, fill, sell, sell_fill = _fixture_case()
    position["position_cycle_id"] = "imported_different_cycle"
    position["position_origin"] = "IMPORTED"
    result = audit_buy(order, positions=[position], fills=[], sells=[sell],
                       sell_fills=[], names={})
    assert result["status"] == "CYCLE_DISCONNECTED"
    assert "ORIGINAL_PRICED_BUY_FILL_UNPROVEN" in result["issues"]


def test_original_contract_change_is_detected_with_frozen_hash():
    order, position, fill, sell, sell_fill = _fixture_case()
    position = copy.deepcopy(position)
    position["entry_exit_plan_json"]["profit_plan"]["tp1_profit_pct"] = 99.0
    result = audit_buy(order, positions=[position], fills=[fill], sells=[sell],
                       sell_fills=[sell_fill], names={})
    assert result["status"] == "CONTRACT_MISMATCH"
    assert "STORED_PLAN_HASH_MISMATCH" in result["issues"]


def test_missing_original_same_cycle_sell_is_not_expected():
    order, position, fill, sell, sell_fill = _fixture_case()
    position["status"] = "CLOSED"
    position["qty"] = 0
    report = audit_buy(order, positions=[position], fills=[fill], sells=[],
                       sell_fills=[], names={})
    assert report["status"] == "CYCLE_DISCONNECTED"
    assert "CLOSED_WITHOUT_ORIGINAL_CYCLE_SELL" in report["issues"]


def test_migration_parser_and_no_manual_transaction_control():
    sql = MIGRATION.read_text(encoding="utf-8")
    statements = split_postgres_sql(sql)
    assert len(statements) >= 6
    assert_no_migration_transaction_control(statements, version=MIGRATION.name)
    assert "kr_signed_buy_order_guard_0063" in sql
    assert "kr_signed_position_guard_0063" in sql
    assert "COMMIT;" not in sql


@pytest.mark.skipif(
    not os.environ.get("PBCORE_TEST_POSTGRES_URL"),
    reason="requires isolated REAL PostgreSQL integration database",
)
def test_postgres_direct_sql_cannot_rewrite_signed_buy_or_position():
    # Must use a dedicated CI/test DB: never touch main public orders/positions.
    url = os.environ["PBCORE_TEST_POSTGRES_URL"]
    schema = "kr_immut_" + uuid.uuid4().hex[:12]
    admin = sa.create_engine(url, future=True)
    try:
        with admin.begin() as conn:
            conn.execute(sa.text(f"CREATE SCHEMA {schema}"))
        with admin.begin() as conn:
            conn.execute(sa.text(f"""
                CREATE TABLE {schema}.orders (
                    env text, strategy text, sid integer, mode integer,
                    code text, market text, side text, client_order_key text,
                    position_cycle_id text, portfolio_epoch_id text, trading_epoch_id text,
                    kis_odno text, request_json jsonb, response_json jsonb, status text
                )
            """))
            conn.execute(sa.text(f"""
                CREATE TABLE {schema}.positions (
                    env text, strategy text, sid integer, mode integer,
                    code text, market text, position_cycle_id text,
                    portfolio_epoch_id text, trading_epoch_id text,
                    entry_style_selected text, entry_reason text,
                    entry_decision_family text, entry_rule_version text,
                    entry_thesis text, trade_horizon text, eod_action text,
                    force_eod_close boolean, exit_policy_family text,
                    policy_source text, policy_version text,
                    initial_stop_price numeric, initial_risk_r numeric,
                    stop_price_at_entry numeric, pivot_price_at_entry numeric,
                    entry_exit_plan_json jsonb, entry_meta_json jsonb,
                    position_meta jsonb, qty integer, status text
                )
            """))
            original = MIGRATION.read_text(encoding="utf-8")
            # Exercise the exact production function bodies on a disposable schema.
            for statement in split_postgres_sql(original.replace("public.", schema + ".")):
                conn.exec_driver_sql(statement)
            conn.execute(sa.text(f"""
                INSERT INTO {schema}.orders
                 (env,strategy,side,code,request_json,status)
                VALUES ('practice','pb1_pullback_close','BUY','035760',
                  CAST(:request AS jsonb), 'INTENT')
            """), {"request": '{"entry_exit_plan":{"profit_plan":{"tp1":12}},'
                             '"entry_contract_sha256":"' + 'a' * 64 + '"}'})
            conn.execute(sa.text(f"""
                INSERT INTO {schema}.positions
                 (env,strategy,code,entry_exit_plan_json,entry_meta_json,qty,status)
                VALUES ('practice','pb1_pullback_close','035760',
                  CAST(:plan AS jsonb), CAST(:meta AS jsonb), 10, 'OPEN')
            """), {"plan": '{"profit_plan":{"tp1":12}}',
                   "meta": '{"entry_contract_sha256":"' + 'a' * 64 + '"}'})
            # Capture errors in SAVEPOINTs so test stays usable afterwards.
            with pytest.raises(sa.exc.DBAPIError):
                with conn.begin_nested():
                    conn.execute(sa.text(f"""
                      UPDATE {schema}.orders
                      SET request_json=jsonb_set(request_json,
                        '{{entry_exit_plan,profit_plan,tp1}}', '99'::jsonb)
                    """))
            with pytest.raises(sa.exc.DBAPIError):
                with conn.begin_nested():
                    conn.execute(sa.text(f"""
                      UPDATE {schema}.positions
                      SET entry_exit_plan_json='{{"profit_plan":{{"tp1":99}}}}'::jsonb
                    """))
            # Dynamic state and ACK are expected to keep changing.
            conn.execute(sa.text(f"UPDATE {schema}.orders SET kis_odno='0000011759',status='FILLED'"))
            conn.execute(sa.text(f"UPDATE {schema}.positions SET qty=6,position_meta=CAST(:metadata AS jsonb)"), {"metadata": '{"tp1_done":true}'})
            assert conn.execute(sa.text(f"SELECT qty FROM {schema}.positions")).scalar_one() == 6
    finally:
        with admin.begin() as conn:
            conn.execute(sa.text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        admin.dispose()
