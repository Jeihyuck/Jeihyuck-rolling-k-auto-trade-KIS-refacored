from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import copy
import json

import pandas as pd
import pytest
import sqlalchemy as sa

from trader.db.repos import FillsRepo, LedgerEventsRepo, OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.exit_policy.router import resolve_exit_policy_for_position
from trader.kr.market_state_overlay import (
    build_kr_policy_missing_adoption,
    generate_kr_profit_capture_intents,
    is_verified_kr_policy_missing_adoption,
)
from trader.pb1_engine import PB1Engine
from trader.trade_plan import build_entry_exit_plan
from trader.window_router import WindowDecision


def _plan(style: str = "ENTRY_PULLBACK"):
    return build_entry_exit_plan(
        code="005930", market="KOSPI",
        entry_style_selected=style, entry_reason=style,
        entry_price=100.0, features={"stop_price": 95.0},
    ).to_dict()


def test_kr_router_executes_buy_time_plan_after_env_changes(monkeypatch):
    monkeypatch.setenv("PB1_SWING_TP1_PROFIT_PCT", "12")
    monkeypatch.setenv("PB1_SWING_TP1_SELL_PCT", "0.33")
    plan = _plan()
    monkeypatch.setenv("PB1_SWING_TP1_PROFIT_PCT", "99")
    monkeypatch.setenv("PB1_SWING_TP1_SELL_PCT", "0.90")

    policy = resolve_exit_policy_for_position(
        {"code": "005930", "entry_exit_plan_json": plan,
         "entry_style_selected": "ENTRY_PULLBACK",
         "exit_policy_family": "SWING_STAGED_EXIT", "trade_horizon": "SWING"},
        {}, {"current_return_pct": 0, "trading_days_held": 1, "mark": 100},
        {"ma20": 90, "ma50": 80, "regime": "NORMAL"},
    )
    pct_rule = next(r for r in policy["partial_sell_rules"]
                    if r["trigger"] == "percent" and r.get("meta_flag") == "tp1_done")
    assert policy["policy_source"] == "ENTRY_EXIT_PLAN"
    assert pct_rule["profit_pct"] == 12.0
    assert pct_rule["sell_pct"] == pytest.approx(0.33)


def test_kr_day_plan_preserves_buy_time_50pct_tp1():
    plan = _plan("ENTRY_BREAKOUT")
    policy = resolve_exit_policy_for_position(
        {"code": "005930", "entry_exit_plan_json": plan,
         "entry_style_selected": "ENTRY_BREAKOUT",
         "exit_policy_family": "INTRADAY_PROFIT_PROTECT", "trade_horizon": "DAY_TRADE"},
        {}, {"current_return_pct": 0, "trading_days_held": 0, "mark": 100},
        {"ma20": 90, "ma50": 80, "regime": "NORMAL"},
    )
    tp1 = next(r for r in policy["partial_sell_rules"] if r["trigger"] == "r_hybrid")
    assert tp1["sell_pct"] == pytest.approx(0.50)





def test_kr_core_stored_plan_is_executed_without_rewriting_policy():
    plan = build_entry_exit_plan(
        code="005930", market="KOSPI", entry_style_selected="ENTRY_CORE",
        entry_reason="ENTRY_CORE", entry_price=100.0, features={"stop_price": 95.0},
    ).to_dict()

    # This PR is an integrity fix, not a strategy-policy rewrite: whatever was
    # frozen into the BUY contract must be what the SELL router consumes.
    assert plan["protection_plan"]["profit_protect_enabled"] is True
    assert plan["protection_plan"]["ma20_break_exit"] is False

    policy = resolve_exit_policy_for_position(
        {
            "code": "005930", "entry_exit_plan_json": plan,
            "entry_style_selected": "ENTRY_CORE",
            "exit_policy_family": "CORE_TREND_FOLLOW", "trade_horizon": "CORE",
        },
        {}, {"current_return_pct": 0.22, "trading_days_held": 5, "mark": 122},
        {"ma20": 115, "ma50": 105, "regime": "NORMAL"},
    )
    assert policy["policy_source"] == "ENTRY_EXIT_PLAN"
    assert policy["profit_protect_enabled"] is True
    assert any(r["trigger"] == "giveback" for r in policy["partial_sell_rules"])
    triggers = {r["trigger"] for r in policy["full_exit_rules"]}
    assert "ma20_break_after_tp1" not in triggers
    assert "ma50_break" in triggers
    assert "risk_off_bear" in triggers


def test_kr_runtime_stop_cap_does_not_reprice_buy_time_r_multiple(monkeypatch):
    from trader.pb1_engine import _resolve_swing_staged_exit

    plan = _plan()
    # BUY contract risk_R = 5 (100 entry / 95 stop). A runtime safety cap may
    # tighten the stop to 98, but TP R-multiples must still use risk_R=5.
    monkeypatch.setenv("PB1_EFFECTIVE_STOP_CAP_ENABLED", "1")
    monkeypatch.setenv("PB1_EFFECTIVE_STOP_CAP_KOSPI_PCT", "2")
    monkeypatch.setenv("PB1_EXIT_ROUTER_ENABLED", "1")
    position = {
        "code": "005930", "market": "KOSPI", "avg_buy_price": 100.0,
        "qty": 10, "orderable_qty": 10,
        "entry_exit_plan_json": plan,
        "entry_style_selected": "ENTRY_PULLBACK",
        "exit_policy_family": "SWING_STAGED_EXIT", "trade_horizon": "SWING",
        "position_meta": {"initial_stop_price": 95.0},
    }
    result = _resolve_swing_staged_exit(
        position, 106.0, 100.0,
        ret_pct=6.0, days_held=1, stop_hit=False,
        highest_ret_pct=6.0,
    )
    assert result["exit_ok"] is False


def test_kr_global_tp_cannot_front_run_standard_entry_plan():
    position = {
        "code": "005930", "qty": 20, "orderable_qty": 20,
        "unrealized_pnl_pct": 0.09,
        "entry_exit_plan_json": _plan(), "position_meta": {},
    }
    assert generate_kr_profit_capture_intents([position], {"market_state": "KR_NORMAL"}) == []


def test_kr_infinite_is_never_consumed_by_standard_profit_capture():
    position = {
        "code": "122630", "qty": 20, "orderable_qty": 20,
        "unrealized_pnl_pct": 0.20, "owner_strategy": "KR_INFINITE",
        "position_meta": {},
    }
    assert generate_kr_profit_capture_intents([position], {"market_state": "KR_NORMAL"}) == []


def test_kr_buy_contract_survives_order_fill_restart_and_drives_exit(monkeypatch):
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    orders = OrdersRepo(engine)
    positions = PositionsRepo(engine)

    monkeypatch.setenv("PB1_SWING_TP1_PROFIT_PCT", "12")
    monkeypatch.setenv("PB1_SWING_TP1_SELL_PCT", "0.33")
    plan = _plan()
    order_id, created = orders.create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close", sid=1, mode=1,
        code="005930", market="KOSPI", side="BUY", ord_type="LIMIT", qty=10,
        limit_price=100.0, stage="PB1-ENTRY", client_order_key="kr-e2e-contract-buy",
        request_json={
            "enforce_entry_contract": True,
            "entry_exit_plan": plan,
            "pre_order_holding_qty": 0,
            "requested_qty": 10,
            "submitted_qty": 10,
            "balance_snapshot_id": "balance-e2e",
        },
        entry_meta_json={"entry_reason": "ENTRY_PULLBACK", "entry_style_selected": "ENTRY_PULLBACK"},
        status="ACKED", account_id="acct",
    )
    assert created
    order = orders.get_order_by_client_order_key("practice", "kr-e2e-contract-buy")
    root_sha = order["request_json"]["entry_contract_sha256"]

    # Deliberately omit entry_exit_plan here. apply_fill must recover and verify
    # the exact durable BUY order contract by order_id.
    positions.apply_fill(
        env="practice", strategy="pb1_pullback_close", sid=1, mode=1,
        code="005930", market="KOSPI", side="BUY", qty=10, price=100.0,
        fee=0.0, tax=0.0, filled_at=datetime.now(timezone.utc),
        order_id=order_id, account_id="acct",
    )

    with engine.connect() as conn:
        stored = dict(conn.execute(sa.select(schema.positions)).mappings().one())
    assert stored["entry_exit_plan_json"] == plan
    assert stored["entry_meta_json"]["entry_contract_sha256"] == root_sha
    assert stored["entry_meta_json"]["entry_contract_version"] == "kr_buy_entry_contract_v1"

    # Simulate a process restart by constructing a fresh repository object and
    # changing today's ENV. Existing lifecycle must still execute the BUY plan.
    restarted = PositionsRepo(engine)
    with restarted.engine.connect() as conn:
        reloaded = dict(conn.execute(sa.select(schema.positions)).mappings().one())
    monkeypatch.setenv("PB1_SWING_TP1_PROFIT_PCT", "99")
    monkeypatch.setenv("PB1_SWING_TP1_SELL_PCT", "0.90")
    policy = resolve_exit_policy_for_position(
        reloaded, {}, {"current_return_pct": 0.13, "trading_days_held": 2, "mark": 113},
        {"ma20": 100, "ma50": 95, "regime": "NORMAL"},
    )
    assert policy["policy_source"] == "ENTRY_EXIT_PLAN"
    assert reloaded["entry_meta_json"]["entry_contract_sha256"] == root_sha
    tp1 = next(r for r in policy["partial_sell_rules"]
               if r["trigger"] == "percent" and r.get("meta_flag") == "tp1_done")
    assert tp1["profit_pct"] == 12.0
    assert tp1["sell_pct"] == pytest.approx(0.33)
    assert reloaded["entry_meta_json"]["entry_exit_plan_sha256"]

    tampered = copy.deepcopy(reloaded)
    tampered["entry_exit_plan_json"]["profit_plan"]["tp1_profit_pct"] = 99.0
    blocked = resolve_exit_policy_for_position(
        tampered, {}, {"current_return_pct": 0.20, "trading_days_held": 2, "mark": 120},
        {"ma20": 100, "ma50": 95, "regime": "NORMAL"},
    )
    assert blocked["entry_contract_integrity_failed"] is True
    assert blocked["hard_stop_enabled"] is True
    assert blocked["partial_sell_rules"] == []
    assert blocked["full_exit_rules"] == []



def test_kr_pyramid_add_is_bound_to_authoritative_parent_contract(monkeypatch):
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    orders = OrdersRepo(engine)
    positions = PositionsRepo(engine)

    plan = _plan()
    root_order_id, _ = orders.create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close", sid=1, mode=1,
        code="005930", market="KOSPI", side="BUY", ord_type="LIMIT", qty=10,
        limit_price=100.0, stage="PB1-ENTRY", client_order_key="kr-parent-buy",
        request_json={
            "enforce_entry_contract": True, "entry_exit_plan": plan,
            "pre_order_holding_qty": 0, "requested_qty": 10, "submitted_qty": 10,
            "balance_snapshot_id": "root-balance",
        },
        entry_meta_json={"entry_reason": "ENTRY_PULLBACK", "entry_style_selected": "ENTRY_PULLBACK"},
        status="ACKED", account_id="acct",
    )
    root_order = orders.get_order_by_client_order_key("practice", "kr-parent-buy")
    root_sha = root_order["request_json"]["entry_contract_sha256"]
    positions.apply_fill(
        env="practice", strategy="pb1_pullback_close", sid=1, mode=1,
        code="005930", market="KOSPI", side="BUY", qty=10, price=100.0,
        fee=0.0, tax=0.0, filled_at=datetime.now(timezone.utc),
        order_id=root_order_id, account_id="acct",
    )
    with engine.connect() as conn:
        parent = dict(conn.execute(sa.select(schema.positions)).mappings().one())

    add_request = {
        "enforce_entry_contract": True,
        "entry_exit_plan": plan,
        "pre_order_holding_qty": 10,
        "requested_qty": 2,
        "submitted_qty": 2,
        "balance_snapshot_id": "add-balance",
        "parent_entry_contract_sha256": root_sha,
        "parent_position_cycle_id": str(parent["position_cycle_id"]),
        "parent_portfolio_epoch_id": str(parent["portfolio_epoch_id"]),
    }
    _, created = orders.create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close", sid=1, mode=1,
        code="005930", market="KOSPI", side="BUY", ord_type="LIMIT", qty=2,
        limit_price=101.0, stage="PB1-ADD", client_order_key="kr-valid-add",
        request_json=add_request,
        entry_meta_json={"entry_reason": "ENTRY_PYRAMID", "parent_entry_contract_sha256": root_sha},
        status="CREATED", account_id="acct",
        portfolio_epoch_id=str(parent["portfolio_epoch_id"]),
        position_cycle_id=str(parent["position_cycle_id"]),
    )
    assert created
    add_order = orders.get_order_by_client_order_key("practice", "kr-valid-add")
    assert add_order["request_json"]["parent_entry_contract_sha256"] == root_sha
    assert add_order["request_json"]["parent_contract_binding_sha256"]

    bad_request = {**add_request, "parent_entry_contract_sha256": "0" * 64}
    with pytest.raises(RuntimeError, match="KR_ADD_PARENT_CONTRACT_SHA_MISMATCH"):
        orders.create_intent_idempotent(
            env="practice", run_id=None, strategy="pb1_pullback_close", sid=1, mode=1,
            code="005930", market="KOSPI", side="BUY", ord_type="LIMIT", qty=1,
            limit_price=102.0, stage="PB1-ADD", client_order_key="kr-tampered-add",
            request_json=bad_request,
            entry_meta_json={"entry_reason": "ENTRY_PYRAMID"},
            status="CREATED", account_id="acct",
            portfolio_epoch_id=str(parent["portfolio_epoch_id"]),
            position_cycle_id=str(parent["position_cycle_id"]),
        )



def _kr_engine_for_exit(now_kst: datetime):
    engine = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(engine).metadata.create_all(engine)
    pb1 = PB1Engine(
        universe_repo=object(),
        orders_repo=OrdersRepo(engine),
        fills_repo=FillsRepo(engine),
        positions_repo=PositionsRepo(engine),
        ledger_repo=LedgerEventsRepo(engine),
        kis=None,
        window=WindowDecision(name="close", phase="exit"),
        window_label="close",
        phase="exit",
        dry_run=True,
        env="practice",
        run_id="kr-p0-exit",
        intended_live=False,
        now_kst_value=now_kst,
        compute_only_full_run=True,
        trading_day=True,
        order_allowed=False,
    )
    pb1._kr_market_state_overlay = {"market_state": "KR_NORMAL"}
    return pb1


def test_jw_pharma_fractional_average_and_json_roundtrip_verify_in_core():
    original = {
        "code": "067290",
        "symbol": "067290",
        "qty": 593,
        "orderable_qty": 593,
        "avg_buy_price": 2358.671,
        "entry_thesis": "POLICY_MISSING",
        "exit_policy_family": "POLICY_MISSING",
        "position_cycle_id": "jw-cycle",
        "portfolio_epoch_id": "jw-epoch",
        "position_meta": {"position_origin": "IMPORTED"},
    }
    adoption = build_kr_policy_missing_adoption(original, current_price=3600.0)
    assert adoption is not None
    persisted = {**original, **adoption["position_fields"]}
    persisted["avg_buy_price"] = 2358.6709999999998
    persisted["entry_exit_plan_json"] = json.dumps(
        persisted["entry_exit_plan_json"], ensure_ascii=False
    )
    persisted["position_meta"] = json.dumps(
        persisted["position_meta"], ensure_ascii=False
    )
    assert is_verified_kr_policy_missing_adoption(persisted) is True


def test_eotech_legacy_unknown_entry_date_close_generates_tp1(monkeypatch, caplog):
    now = datetime(2026, 9, 18, 15, 20, tzinfo=ZoneInfo("Asia/Seoul"))
    pb1 = _kr_engine_for_exit(now)
    monkeypatch.setenv("KR_PROFIT_CAPTURE_ENABLE", "1")
    monkeypatch.setenv("KR_MARKET_STATE_OVERLAY_ENABLE", "1")
    monkeypatch.setattr(
        pb1, "_resolve_price_with_fallback",
        lambda code, ohlcv_close=None: (471000.0, "test"),
    )

    base = {
        "code": "039030",
        "symbol": "039030",
        "market": "KOSDAQ",
        "sid": 1,
        "mode": 1,
        "qty": 1,
        "orderable_qty": 1,
        "kis_qty": 1,
        "holding_source": "kis_balance",
        "avg_buy_price": 412500.0,
        "last_price": 471000.0,
        "entry_date": None,
        "entry_ts": None,
        "last_fill_at": None,
        "holding_days": 0,
        "trading_days_held": 0,
        "calendar_days_held": 0,
        "holding_bars": 0,
        "entry_thesis": "POLICY_MISSING",
        "exit_policy_family": "POLICY_MISSING",
        "position_cycle_id": "eotech-cycle",
        "portfolio_epoch_id": "eotech-epoch",
        "position_meta": {
            "position_origin": "IMPORTED",
            "holding_age_unknown": True,
        },
    }
    adoption = build_kr_policy_missing_adoption(base, current_price=471000.0)
    assert adoption is not None
    pos = {**base, **adoption["position_fields"]}
    assert is_verified_kr_policy_missing_adoption(pos)

    features = {
        "close": 471000.0,
        "ma20": 450000.0,
        "ma50": 430000.0,
        "_exit_ohlcv_source": "test",
    }
    caplog.set_level("INFO")
    payload = pb1._plan_exit_event(pos, features, pd.DataFrame(), "close")
    assert payload is not None
    assert payload["decision_reason"] == "KR_TAKE_PROFIT_TP1"
    assert payload["router_qty"] == 1
    assert payload["exit_ok"] is True
    assert "[KR_POSITION_AGE][UNKNOWN_ENTRY_NOT_SAME_DAY] code=039030" in caplog.text
    assert "[KR_MARKET_STATE][EXIT_OVERLAY_APPLIED] code=039030" in caplog.text
    assert "window=close" in caplog.text
