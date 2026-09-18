from datetime import datetime, timezone
import copy

import pytest

from trader.us.entry_exit_contract import (
    build_us_entry_exit_contract,
    extract_us_entry_exit_contract,
)
from trader.us.market_state_overlay import build_profit_capture_intents
from trader.us.pb1.us_exit_engine import evaluate_exit
from trader.us.pb1.us_exit_router import evaluate_day_exit


def _contract(*, symbol="AAPL", book="SWING_BOOK", horizon="SWING_CARRY",
              exit_policy="US_SWING_DEFAULT", signal="pullback"):
    return build_us_entry_exit_contract({
        "symbol": symbol, "strategy_owner": "US_STANDARD", "sleeve_id": "US_STANDARD",
        "book": book, "horizon": horizon, "exit_policy": exit_policy,
        "entry_strategy": "us_pb1", "entry_signal_type": signal,
        "entry_style_selected": "ENTRY_" + signal.upper(),
        "reasons": ["ENTRY_" + signal.upper()],
        "score_breakdown": {signal: 0.81},
        "filters_passed": ["score", "risk"],
    })


def test_us_contract_freezes_swing_exit_after_env_change(monkeypatch):
    monkeypatch.setenv("US_HARD_STOP_PCT", "0.08")
    contract = _contract()
    monkeypatch.setenv("US_HARD_STOP_PCT", "0.20")
    position = {
        "symbol": "AAPL", "exchange": "NASDAQ", "qty": 10, "orderable_qty": 10,
        "entry_price": 100.0, "max_price": 100.0,
        "meta": {"entry_exit_contract": contract},
    }
    intent = evaluate_exit(position, 91.0, now=datetime.now(timezone.utc), include_trend_time=False)
    assert intent is not None
    assert intent["exit_type"] in {"hard_stop", "hard_stop_loss", "hard_stop_full_exit"}
    assert intent["meta"]["hard_stop_threshold_pct"] == pytest.approx(0.08)


def test_us_day_exit_uses_buy_time_contract_not_current_env(monkeypatch):
    monkeypatch.setenv("US_DAY_PROFIT_TAKE_PCT", "0.025")
    contract = _contract(
        symbol="MSFT", book="DAY_BOOK", horizon="DAY_TRADE",
        exit_policy="DAY_BOOK", signal="breakout",
    )
    monkeypatch.setenv("US_DAY_PROFIT_TAKE_PCT", "0.50")
    position = {
        "symbol": "MSFT", "exchange": "NASDAQ", "qty": 5, "holding_qty": 5,
        "entry_price": 100.0, "meta": {"entry_exit_contract": contract},
    }
    intent = evaluate_day_exit(position, 103.0, include_trend_time=False)
    assert intent is not None
    assert intent["exit_type"] == "day_profit_take"


def test_us_contract_tamper_is_rejected():
    contract = _contract()
    tampered = copy.deepcopy(contract)
    tampered["management"]["swing"]["hard_stop"] = 0.50
    assert extract_us_entry_exit_contract({"entry_exit_contract": tampered}) == {}


def test_us_profit_capture_uses_frozen_buy_contract_after_env_change(monkeypatch):
    monkeypatch.setenv("US_TP1_PCT", "0.03")
    contract = _contract()
    monkeypatch.setenv("US_TP1_PCT", "0.50")
    now = datetime.now(timezone.utc)
    position = {
        "symbol": "AAPL", "qty": 20, "orderable_qty": 20, "current_price_usd": 104.0,
        "position_lifecycle_id": "life-contract",
        "broker_avg_price": 100.0, "broker_avg_price_source": "kis_pchs_avg_pric",
        "broker_avg_price_asof": now.isoformat(), "broker_avg_price_currency": "USD",
        "balance_source": "kis_balance_authoritative", "authoritative_positions": True,
        "meta": {"entry_exit_contract": contract},
    }
    intents = build_profit_capture_intents(
        [position], {"profit_capture_enabled": True, "market_state": "NORMAL"},
        now=now, trade_date="2026-09-18", profit_capture_state={},
    )
    assert intents and intents[0]["reason"] == "TAKE_PROFIT_TP1"
    assert intents[0]["meta"]["source_entry_contract_sha256"] == contract["sha256"]
    assert intents[0]["meta"]["tp_threshold_fraction"] == "0.03"


def test_us_locked_watchlist_meta_keeps_entry_provenance():
    from trader.us.db.repos import _merge_us_daily_metrics_meta
    row = {
        "symbol": "AAPL", "meta": {}, "entry_style_selected": "ENTRY_PULLBACK",
        "pullback_score": 0.81, "breakout_score": 0.12, "momentum_score": 0.43,
        "reasons": ["ENTRY_PULLBACK"], "score_breakdown": {"pullback": 0.81},
    }
    meta = _merge_us_daily_metrics_meta(row)
    assert meta["entry_style_selected"] == "ENTRY_PULLBACK"
    assert meta["pullback_score"] == pytest.approx(0.81)
    assert meta["reasons"] == ["ENTRY_PULLBACK"]


def test_us_order_policy_contract_contains_full_v2_provenance():
    from trader.us.db.repos import _ensure_us_entry_policy_contract
    policy = _ensure_us_entry_policy_contract({
        "symbol": "AAPL", "strategy_owner": "US_STANDARD", "sleeve_id": "US_STANDARD",
        "book": "SWING_BOOK", "horizon": "SWING_CARRY", "exit_policy": "US_SWING_DEFAULT",
        "entry_strategy": "us_pb1", "entry_signal_type": "pullback",
        "entry_style_selected": "ENTRY_PULLBACK", "entry_reason": "ENTRY_PULLBACK",
        "reasons": ["ENTRY_PULLBACK"], "score_breakdown": {"pullback": 0.81},
        "filters_passed": ["score", "risk"],
    })
    contract = policy["entry_exit_contract"]
    assert policy["entry_exit_contract_sha256"] == contract["sha256"]
    assert contract["entry_provenance"]["entry_reason"] == "ENTRY_PULLBACK"
    assert contract["entry_provenance"]["score_breakdown"]["pullback"] == pytest.approx(0.81)


def test_tqqq_infinite_never_receives_standard_us_contract():
    assert build_us_entry_exit_contract({
        "symbol": "TQQQ", "strategy_owner": "TQQQ_INFINITE", "sleeve_id": "TQQQ_INFINITE",
    }) == {}
