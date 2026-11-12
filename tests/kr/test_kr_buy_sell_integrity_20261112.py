"""KR frozen-contract guard: pure audit + real PostgreSQL trigger tests.

PostgreSQL test is conditional on PBCORE_TEST_POSTGRES_URL and runs in an
isolated schema. NEVER point the test URL to an operator/production database.
"""
from __future__ import annotations

import copy
import json
import os
import uuid
from pathlib import Path

import pytest
import sqlalchemy as sa

from scripts.kr_buy_sell_contract_audit import _sha, audit_buy
from trader.db.repos import _kr_buy_entry_contract_hash, KR_BUY_ENTRY_CONTRACT_VERSION
from trader.kr.order_execution_evidence import summarize_native_order
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
    request = {
        "enforce_entry_contract": True, "entry_exit_plan": plan,
        "entry_contract_version": KR_BUY_ENTRY_CONTRACT_VERSION,
        "pre_order_holding_qty": 0, "requested_qty": 10, "submitted_qty": 10,
        "client_order_key": "client1", "position_cycle_id": "cycle1",
        "portfolio_epoch_id": "portfolio1", "trading_epoch_id": "epoch1",
    }
    request["entry_contract_sha256"] = _kr_buy_entry_contract_hash(request)
    order = {
        "order_id": "order1", "env": "practice", "strategy": "pb1_pullback_close",
        "code": "035760", "market": "KOSDAQ", "side": "BUY", "kis_odno": "0000011759",
        "position_cycle_id": "cycle1", "portfolio_epoch_id": "portfolio1",
        "trading_epoch_id": "epoch1", "qty": 10, "request_json": request,
        "response_json": {"_kr_buy_entry_contract": {
            "entry_contract_sha256": request["entry_contract_sha256"],
        }},
    }
    position = {
        "env": "practice", "market": "KOSDAQ", "code": "035760",
        "strategy": "pb1_pullback_close", "position_cycle_id": "cycle1",
        "portfolio_epoch_id": "portfolio1", "trading_epoch_id": "epoch1",
        "qty": 6, "status": "OPEN", "position_origin": "SYSTEM",
        "entry_style_selected": "ENTRY_PULLBACK",
        "exit_policy_family": "SWING_STAGED_EXIT",
        "entry_exit_plan_json": plan,
        "last_exit_eval_json": {
            "entry_exit_plan": plan, "entry_exit_plan_status": "OK",
        },
        "entry_meta_json": {
            "entry_contract_sha256": request["entry_contract_sha256"],
            "entry_exit_plan_sha256": _sha(plan), "source_buy_order_id": "order1",
        },
    }
    fill = {
        "order_id": "order1", "env": "practice", "market": "KOSDAQ",
        "code": "035760", "side": "BUY", "kis_odno": "0000011759",
        "position_cycle_id": "cycle1", "portfolio_epoch_id": "portfolio1",
        "trading_epoch_id": "epoch1",
        "qty": 10, "price": 2000.0, "broker_fill_id": "KR_KIS_CUM:proof1",
        "raw_json": {"kis_response": {"tot_ccld_qty": "10", "tot_ccld_amt": "20000"}},
        "filled_at": "2026-11-02T09:31:59+09:00",
        "fill_meta_json": {
            "evidence_kind": "KIS_ORDER_CUMULATIVE_EXECUTION",
            "filled_at_source": "KIS_EXECUTION_TIMESTAMP",
        },
    }
    sell = {
        "order_id": "sell1", "env": "practice", "market": "KOSDAQ",
        "side": "SELL", "kis_odno": "0000005003", "code": "035760",
        "strategy": "pb1_pullback_close", "position_cycle_id": "cycle1",
        "portfolio_epoch_id": "portfolio1", "trading_epoch_id": "epoch1",
        "qty": 4, "status": "FILLED",
    }
    sell_fill = {
        "order_id": "sell1", "env": "practice", "market": "KOSDAQ",
        "side": "SELL", "kis_odno": "0000005003", "code": "035760",
        "position_cycle_id": "cycle1", "portfolio_epoch_id": "portfolio1",
        "trading_epoch_id": "epoch1", "qty": 4, "price": 2100.0,
        "broker_fill_id": "KR_KIS_CUM:proof2",
        "raw_json": {"kis_response": {"tot_ccld_qty": "4", "tot_ccld_amt": "8400"}},
        "filled_at": "2026-11-06T09:06:36+09:00",
        "fill_meta_json": {
            "evidence_kind": "KIS_ORDER_CUMULATIVE_EXECUTION",
            "filled_at_source": "KIS_EXECUTION_TIMESTAMP",
        },
    }
    return order, position, fill, sell, sell_fill

def test_exact_signed_cycle_and_partial_sell_audits_expected():
    order, position, fill, sell, sell_fill = _fixture_case()
    result = audit_buy(
        order, positions=[position], fills=[fill], sells=[sell],
        sell_fills=[sell_fill], names={"035760": "CJ ENM"},
    )
    assert result["status"] == "EXPECTED"
    assert result["remaining_qty"] == 6
    assert result["sell_proven_filled_qty_same_cycle"] == 4


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


@pytest.mark.parametrize("field,bad_value", [
    ("env", "real"), ("code", "293490"), ("market", "KOSPI"),
    ("position_cycle_id", "other_cycle"),
    ("portfolio_epoch_id", "foreign_portfolio"),
    ("trading_epoch_id", "foreign_epoch"),
    ("kis_odno", "0000012888"), ("side", "SELL"),
])
def test_broker_fill_foreign_scope_is_never_accepted(field, bad_value):
    order, pos, fill, sell, sell_fill = _fixture_case()
    fill[field] = bad_value
    report = summarize_native_order(order, [fill])
    assert report["evidence_status"] in ("UNPROVEN", "CONFLICT")
    assert report["proven_filled_qty"] in (None, 0)
    assert any("FILL_IDENTITY_INVALID" in s for s in report["issues"])


def test_broker_fill_missing_identity_and_empty_order_number_review():
    order, pos, fill, sell, sell_fill = _fixture_case()
    order["kis_odno"] = ""
    fill["kis_odno"] = ""
    report = summarize_native_order(order, [fill])
    assert report["proven_filled_qty"] is None
    assert any("ORDER_SCOPE_INCOMPLETE" in s for s in report["issues"])


def test_sell_partial_fill_after_cancel_uses_priced_execution_not_order_qty():
    order, pos, fill, sell, sell_fill = _fixture_case()
    sell["qty"], sell["status"] = 8, "CANCELLED"
    pos["qty"] = 7
    sell_fill["qty"] = 3
    sell_fill["raw_json"]["kis_response"] = {"tot_ccld_qty": "3", "tot_ccld_amt": "6300"}
    result = audit_buy(
        order, positions=[pos], fills=[fill], sells=[sell],
        sell_fills=[sell_fill], names={},
    )
    assert result["sell_submitted_qty_same_cycle"] == 8
    assert result["sell_proven_filled_qty_same_cycle"] == 3
    assert "POSITION_ECONOMIC_QUANTITY_CHAIN_MISMATCH" not in result["issues"]


def test_duplicate_cumulative_watermark_is_counted_once():
    order, pos, fill, sell, sell_fill = _fixture_case()
    one = copy.deepcopy(fill)
    two = copy.deepcopy(fill)
    one["qty"] = 3
    two["qty"] = 10
    one["raw_json"]["kis_response"] = {"tot_ccld_qty": "3", "tot_ccld_amt": "6000"}
    two["raw_json"]["kis_response"] = {"tot_ccld_qty": "10", "tot_ccld_amt": "20000"}
    one["created_at"], two["created_at"] = "2026-11-02T00:31:59Z", "2026-11-02T00:32:00Z"
    report = summarize_native_order(order, [one, two])
    assert report["proven_filled_qty"] == 10
    assert report["cumulative_watermark"] == 10


def test_kis_cumulative_source_qty_and_notional_mismatch_are_not_proof():
    order, pos, fill, sell, sell_fill = _fixture_case()
    wrong_amount = copy.deepcopy(fill)
    wrong_amount["raw_json"]["kis_response"]["tot_ccld_amt"] = "21000"
    assert summarize_native_order(order, [wrong_amount])["proven_filled_qty"] is None
    wrong_qty = copy.deepcopy(fill)
    wrong_qty["raw_json"]["kis_response"]["tot_ccld_qty"] = "9"
    assert summarize_native_order(order, [wrong_qty])["proven_filled_qty"] is None


def test_duplicate_individual_broker_ids_are_not_double_counted():
    order, pos, fill, sell, sell_fill = _fixture_case()
    fill["broker_fill_id"] = "KIS-NATIVE-FILL-ID-001"
    fill["qty"] = 5
    fill["fill_meta_json"]["evidence_kind"] = "KIS_INDIVIDUAL_EXECUTION"
    duplicate = copy.deepcopy(fill)
    result = summarize_native_order(order, [fill, duplicate])
    assert result["proven_filled_qty"] == 5
    assert result["evidence_status"] == "ECONOMIC_AND_TIMESTAMP_PROVEN"


def test_conflicting_duplicate_individual_broker_id_is_not_proven():
    order, pos, fill, sell, sell_fill = _fixture_case()
    fill["broker_fill_id"] = "KIS-NATIVE-FILL-ID-001"
    fill["qty"] = 5
    fill["fill_meta_json"]["evidence_kind"] = "KIS_INDIVIDUAL_EXECUTION"
    conflict = copy.deepcopy(fill)
    conflict["qty"] = 6
    result = summarize_native_order(order, [fill, conflict])
    assert result["evidence_status"] == "CONFLICT"
    assert result["proven_filled_qty"] is None


def test_entry_contract_hash_canonical_key_order_and_plan_tamper():
    order, pos, fill, sell, sell_fill = _fixture_case()
    original = order["request_json"]
    assert _kr_buy_entry_contract_hash(original) == original["entry_contract_sha256"]
    reversed_plan = {key: original["entry_exit_plan"][key]
                     for key in reversed(original["entry_exit_plan"])}
    altered_order = {**original, "entry_exit_plan": reversed_plan}
    assert _kr_buy_entry_contract_hash(altered_order) == original["entry_contract_sha256"]
    tampered = copy.deepcopy(original)
    tampered["entry_exit_plan"]["profit_plan"]["tp1_profit_pct"] = 99.0
    assert _kr_buy_entry_contract_hash(tampered) != original["entry_contract_sha256"]


def test_mixed_native_cumulative_and_individual_is_unproven():
    order, pos, fill, sell, sell_fill = _fixture_case()
    duplicate = copy.deepcopy(fill)
    duplicate["broker_fill_id"] = "native-trade-1"
    duplicate["qty"] = 1
    duplicate["fill_meta_json"]["evidence_kind"] = "KIS_INDIVIDUAL_EXECUTION"
    report = summarize_native_order(order, [fill, duplicate])
    assert report["evidence_status"] == "CONFLICT"
    assert report["proven_filled_qty"] is None


def test_same_cycle_second_buy_never_reallocates_sells_per_buy():
    order, pos, fill, sell, sell_fill = _fixture_case()
    extra = copy.deepcopy(order)
    extra["order_id"] = "second_order"
    extra["kis_odno"] = "0000011760"
    result = audit_buy(
        order, positions=[pos], fills=[fill], sells=[sell],
        sell_fills=[sell_fill], names={}, cycle_buy_orders=[order, extra],
    )
    assert result["same_cycle_buy_orders"] == 2
    assert "MULTIPLE_BUYS_SAME_CYCLE_ALLOCATION_UNPROVEN" in result["issues"]
    assert "POSITION_ECONOMIC_QUANTITY_CHAIN_MISMATCH" not in result["issues"]


def test_execution_proxy_time_is_not_declared_full_evidence():
    order, pos, fill, sell, sell_fill = _fixture_case()
    fill["fill_meta_json"]["filled_at_source"] = "ORDER_TIME_PROXY"
    result = audit_buy(order, positions=[pos], fills=[fill], sells=[sell],
                       sell_fills=[sell_fill], names={})
    assert result["status"] == "REVIEW_REQUIRED"
    assert result["buy_time_quality"] == "ORDER_TIME_PROXY_OR_UNKNOWN"


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
                    kis_odno text, request_json jsonb, response_json jsonb, status text, qty integer,
                    ord_type text, entry_reason text, entry_style_selected text,
                    entry_decision_family text, entry_rule_version text,
                    stop_price_at_entry numeric, pivot_price_at_entry numeric
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
                if statement.strip() and not statement.lstrip().startswith("-- Broker ACK fields"):
                    conn.exec_driver_sql(statement)
            order, pos, fill, sell, sell_fill = _fixture_case()
            conn.execute(sa.text(f"""
                INSERT INTO {schema}.orders
                 (env,strategy,side,market,code,request_json,status,qty,
                  position_cycle_id,portfolio_epoch_id,trading_epoch_id,client_order_key)
                VALUES ('practice','pb1_pullback_close','BUY','KOSDAQ','035760',
                  CAST(:request AS jsonb), 'INTENT',10,
                  'cycle1','portfolio1','epoch1','client1')
            """), {"request": json.dumps(order["request_json"])})
            conn.execute(sa.text(f"""
                INSERT INTO {schema}.positions
                 (env,strategy,market,code,entry_exit_plan_json,entry_meta_json,
                  qty,status,position_cycle_id,portfolio_epoch_id,trading_epoch_id)
                VALUES ('practice','pb1_pullback_close','KOSDAQ','035760',
                  CAST(:plan AS jsonb), CAST(:meta AS jsonb), 10,'OPEN',
                  'cycle1','portfolio1','epoch1')
            """), {"plan": json.dumps(pos["entry_exit_plan_json"]),
                   "meta": json.dumps(pos["entry_meta_json"])})
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
            with pytest.raises(sa.exc.DBAPIError):
                with conn.begin_nested():
                    conn.execute(sa.text(f"""
                        UPDATE {schema}.orders SET request_json='{{}}'::jsonb
                    """))
            with pytest.raises(sa.exc.DBAPIError):
                with conn.begin_nested():
                    conn.execute(sa.text(f"""
                        DELETE FROM {schema}.positions
                    """))
            conn.execute(sa.text(f"UPDATE {schema}.orders SET kis_odno='0000011759',status='FILLED'"))
            conn.execute(sa.text(f"UPDATE {schema}.positions SET qty=6,position_meta=CAST(:metadata AS jsonb)"), {"metadata": '{"tp1_done":true}'})
            assert conn.execute(sa.text(f"SELECT qty FROM {schema}.positions")).scalar_one() == 6
    finally:
        with admin.begin() as conn:
            conn.execute(sa.text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        admin.dispose()
