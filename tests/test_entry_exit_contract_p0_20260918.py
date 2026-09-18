from datetime import datetime, timezone
import copy

import pytest


def test_kr_router_executes_buy_time_plan_after_env_changes(monkeypatch):
    from trader.trade_plan import build_entry_exit_plan
    from trader.exit_policy.router import resolve_exit_policy_for_position

    monkeypatch.setenv("PB1_SWING_TP1_PROFIT_PCT", "12")
    monkeypatch.setenv("PB1_SWING_TP1_SELL_PCT", "0.33")
    plan = build_entry_exit_plan(
        code="005930", market="KR", entry_style_selected="ENTRY_PULLBACK",
        entry_reason="ENTRY_PULLBACK", entry_price=100.0,
        features={"initial_stop": 95.0},
    ).to_dict()

    monkeypatch.setenv("PB1_SWING_TP1_PROFIT_PCT", "99")
    monkeypatch.setenv("PB1_SWING_TP1_SELL_PCT", "0.90")
    policy = resolve_exit_policy_for_position(
        {
            "code": "005930",
            "entry_exit_plan_json": plan,
            "entry_style_selected": "ENTRY_PULLBACK",
            "exit_policy_family": "SWING_STAGED_EXIT",
            "trade_horizon": "SWING",
        },
        {},
        {"current_return_pct": 0, "trading_days_held": 1, "mark": 100},
        {"ma20": 90, "ma50": 80, "regime": "NORMAL"},
    )
    pct_rule = next(
        r for r in policy["partial_sell_rules"]
        if r["trigger"] == "percent" and r.get("meta_flag") == "tp1_done"
    )
    assert policy["policy_source"] == "ENTRY_EXIT_PLAN"
    assert pct_rule["profit_pct"] == 12.0
    assert pct_rule["sell_pct"] == pytest.approx(0.33)


def test_kr_day_plan_preserves_buy_time_50pct_tp1():
    from trader.trade_plan import build_entry_exit_plan
    from trader.exit_policy.router import resolve_exit_policy_for_position

    plan = build_entry_exit_plan(
        code="005930", market="KR", entry_style_selected="ENTRY_BREAKOUT",
        entry_reason="ENTRY_BREAKOUT", entry_price=100.0,
        features={"initial_stop": 97.0},
    ).to_dict()
    policy = resolve_exit_policy_for_position(
        {
            "code": "005930",
            "entry_exit_plan_json": plan,
            "entry_style_selected": "ENTRY_BREAKOUT",
            "exit_policy_family": "INTRADAY_PROFIT_PROTECT",
            "trade_horizon": "DAY_TRADE",
        },
        {},
        {"current_return_pct": 0, "trading_days_held": 0, "mark": 100},
        {"ma20": 90, "ma50": 80, "regime": "NORMAL"},
    )
    tp1 = next(r for r in policy["partial_sell_rules"] if r["trigger"] == "r_hybrid")
    assert tp1["sell_pct"] == pytest.approx(0.50)


def test_kr_global_tp_cannot_front_run_standard_entry_plan():
    from trader.trade_plan import build_entry_exit_plan
    from trader.kr.market_state_overlay import generate_kr_profit_capture_intents

    plan = build_entry_exit_plan(
        code="005930", market="KR", entry_style_selected="ENTRY_PULLBACK",
        entry_reason="ENTRY_PULLBACK", entry_price=100.0,
        features={"initial_stop": 95.0},
    ).to_dict()
    position = {
        "code": "005930", "qty": 20, "orderable_qty": 20,
        "unrealized_pnl_pct": 0.09,
        "entry_exit_plan_json": plan,
        "position_meta": {},
    }
    assert generate_kr_profit_capture_intents(
        [position], {"market_state": "KR_NORMAL"}
    ) == []


def test_kr_infinite_is_never_consumed_by_standard_profit_capture():
    from trader.kr.market_state_overlay import generate_kr_profit_capture_intents

    position = {
        "code": "122630", "qty": 20, "orderable_qty": 20,
        "unrealized_pnl_pct": 0.20,
        "owner_strategy": "KR_INFINITE",
        "position_meta": {},
    }
    assert generate_kr_profit_capture_intents(
        [position], {"market_state": "KR_NORMAL"}
    ) == []


def _us_contract(monkeypatch, *, symbol="AAPL", book="SWING_BOOK", horizon="SWING_CARRY", exit_policy="US_SWING_DEFAULT", signal="pullback"):
    from trader.us.entry_exit_contract import build_us_entry_exit_contract

    return build_us_entry_exit_contract({
        "symbol": symbol,
        "strategy_owner": "US_STANDARD",
        "sleeve_id": "US_STANDARD",
        "book": book,
        "horizon": horizon,
        "exit_policy": exit_policy,
        "entry_strategy": "us_pb1",
        "entry_signal_type": signal,
        "entry_style_selected": "ENTRY_" + signal.upper(),
        "reasons": ["ENTRY_" + signal.upper()],
        "score_breakdown": {signal: 0.81},
        "filters_passed": ["score", "risk"],
    })


def test_us_contract_freezes_swing_exit_after_env_change(monkeypatch):
    from trader.us.pb1.us_exit_engine import evaluate_exit

    monkeypatch.setenv("US_HARD_STOP_PCT", "0.08")
    contract = _us_contract(monkeypatch)
    monkeypatch.setenv("US_HARD_STOP_PCT", "0.20")

    position = {
        "symbol": "AAPL", "exchange": "NASDAQ", "qty": 10, "orderable_qty": 10,
        "entry_price": 100.0, "max_price": 100.0,
        "meta": {"entry_exit_contract": contract},
    }
    intent = evaluate_exit(
        position, 91.0, now=datetime.now(timezone.utc), include_trend_time=False
    )
    assert intent is not None
    assert intent["exit_type"] in {"hard_stop", "hard_stop_loss", "hard_stop_full_exit"}


def test_us_day_exit_uses_buy_time_contract_not_current_env(monkeypatch):
    from trader.us.pb1.us_exit_router import evaluate_day_exit

    monkeypatch.setenv("US_DAY_PROFIT_TAKE_PCT", "0.025")
    contract = _us_contract(
        monkeypatch, symbol="MSFT", book="DAY_BOOK",
        horizon="DAY_TRADE", exit_policy="DAY_BOOK", signal="breakout"
    )
    monkeypatch.setenv("US_DAY_PROFIT_TAKE_PCT", "0.50")

    position = {
        "symbol": "MSFT", "exchange": "NASDAQ", "qty": 5, "holding_qty": 5,
        "entry_price": 100.0, "meta": {"entry_exit_contract": contract},
    }
    intent = evaluate_day_exit(position, 103.0, include_trend_time=False)
    assert intent is not None
    assert intent["exit_type"] == "day_profit_take"


def test_us_contract_tamper_is_rejected(monkeypatch):
    from trader.us.entry_exit_contract import extract_us_entry_exit_contract

    contract = _us_contract(monkeypatch)
    tampered = copy.deepcopy(contract)
    tampered["management"]["swing"]["hard_stop"] = 0.50
    assert extract_us_entry_exit_contract({"entry_exit_contract": tampered}) == {}


def test_us_profit_capture_uses_frozen_buy_contract_after_env_change(monkeypatch):
    from trader.us.market_state_overlay import build_profit_capture_intents

    monkeypatch.setenv("US_TP1_PCT", "0.03")
    contract = _us_contract(monkeypatch)
    monkeypatch.setenv("US_TP1_PCT", "0.50")

    now = datetime.now(timezone.utc)
    position = {
        "symbol": "AAPL", "qty": 20, "orderable_qty": 20,
        "current_price_usd": 104.0,
        "position_lifecycle_id": "life-contract",
        "broker_avg_price": 100.0,
        "broker_avg_price_source": "kis_pchs_avg_pric",
        "broker_avg_price_asof": now.isoformat(),
        "broker_avg_price_currency": "USD",
        "balance_source": "kis_balance_authoritative",
        "authoritative_positions": True,
        "meta": {"entry_exit_contract": contract},
    }
    intents = build_profit_capture_intents(
        [position], {"profit_capture_enabled": True, "market_state": "NORMAL"},
        now=now, trade_date="2026-09-18", profit_capture_state={},
    )
    assert intents and intents[0]["reason"] == "TAKE_PROFIT_TP1"
    assert intents[0]["meta"]["source_entry_contract_sha256"] == contract["sha256"]


def test_us_locked_watchlist_meta_keeps_entry_provenance():
    from trader.us.db.repos import _merge_us_daily_metrics_meta

    row = {
        "symbol": "AAPL", "meta": {},
        "entry_style_selected": "ENTRY_PULLBACK",
        "pullback_score": 0.81,
        "breakout_score": 0.12,
        "momentum_score": 0.43,
        "reasons": ["ENTRY_PULLBACK"],
        "score_breakdown": {"pullback": 0.81},
    }
    meta = _merge_us_daily_metrics_meta(row)
    assert meta["entry_style_selected"] == "ENTRY_PULLBACK"
    assert meta["pullback_score"] == pytest.approx(0.81)
    assert meta["reasons"] == ["ENTRY_PULLBACK"]


def test_us_order_policy_contract_contains_full_v2_provenance(monkeypatch):
    from trader.us.db.repos import _ensure_us_entry_policy_contract

    policy = _ensure_us_entry_policy_contract({
        "symbol": "AAPL",
        "strategy_owner": "US_STANDARD",
        "sleeve_id": "US_STANDARD",
        "book": "SWING_BOOK",
        "horizon": "SWING_CARRY",
        "exit_policy": "US_SWING_DEFAULT",
        "entry_strategy": "us_pb1",
        "entry_signal_type": "pullback",
        "entry_style_selected": "ENTRY_PULLBACK",
        "entry_reason": "ENTRY_PULLBACK",
        "reasons": ["ENTRY_PULLBACK"],
        "score_breakdown": {"pullback": 0.81},
        "filters_passed": ["score", "risk"],
    })
    contract = policy["entry_exit_contract"]
    assert policy["entry_exit_contract_sha256"] == contract["sha256"]
    assert contract["entry_provenance"]["entry_reason"] == "ENTRY_PULLBACK"
    assert contract["entry_provenance"]["score_breakdown"]["pullback"] == pytest.approx(0.81)


def test_tqqq_infinite_never_receives_standard_us_contract():
    from trader.us.entry_exit_contract import build_us_entry_exit_contract

    assert build_us_entry_exit_contract({
        "symbol": "TQQQ",
        "strategy_owner": "TQQQ_INFINITE",
        "sleeve_id": "TQQQ_INFINITE",
    }) == {}
