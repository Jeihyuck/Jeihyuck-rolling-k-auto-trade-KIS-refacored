"""PB1 stop 3% / 6% policy: new contracts change; old contract remains immutable."""
from datetime import datetime, timezone

import pytest

from trader.us.entry_exit_contract import build_us_entry_exit_contract, contract_exit_config
from trader.us.pb1.us_exit_engine import evaluate_exit, soft_stop_repeat_allowed


def _new_contract():
    return build_us_entry_exit_contract({
        "symbol": "AAPL", "strategy_owner": "US_STANDARD",
        "sleeve_id": "US_STANDARD", "book": "SWING_BOOK",
        "horizon": "SWING_CARRY", "exit_policy": "US_SWING_DEFAULT",
        "entry_strategy": "us_pb1", "entry_signal_type": "pullback",
        "entry_style_selected": "ENTRY_PULLBACK",
        "reasons": ["ENTRY_PULLBACK"],
        "score_breakdown": {"pullback": 0.8},
        "filters_passed": ["score", "risk"],
    })


def _position(contract, *, breaches=0):
    return {
        "symbol": "AAPL", "exchange": "NASDAQ", "qty": 10,
        "orderable_qty": 10, "entry_price": 100.0, "max_price": 100.0,
        "soft_stop_breach_count": breaches,
        "meta": {"entry_exit_contract": contract},
    }


def _at_market():
    # 13:00 ET in October (outside the soft-stop opening guard).
    return datetime(2026, 10, 22, 17, 0, tzinfo=timezone.utc)


def test_new_pb1_contract_defaults_to_soft_three_hard_six(monkeypatch):
    monkeypatch.delenv("US_SOFT_STOP_LOSS_PCT", raising=False)
    monkeypatch.delenv("US_HARD_STOP_PCT", raising=False)
    contract = _new_contract()
    cfg = contract_exit_config(_position(contract))
    assert cfg["soft_stop"] == pytest.approx(0.03)
    assert cfg["hard_stop"] == pytest.approx(0.06)


def test_new_pb1_soft_two_confirmations_hard_immediate(monkeypatch):
    monkeypatch.delenv("US_SOFT_STOP_LOSS_PCT", raising=False)
    monkeypatch.delenv("US_HARD_STOP_PCT", raising=False)
    monkeypatch.setenv("US_OPEN_VOL_GUARD_ENABLED", "0")
    contract = _new_contract()
    first = evaluate_exit(_position(contract, breaches=1), 96.9, now=_at_market(), include_trend_time=False)
    assert first is None or first.get("side") != "SELL"
    second = evaluate_exit(_position(contract, breaches=2), 96.9, now=_at_market(), include_trend_time=False)
    assert second is not None and second["exit_type"] == "soft_stop_loss"
    assert second["qty"] == 5
    hard = evaluate_exit(_position(contract, breaches=0), 93.9, now=_at_market(), include_trend_time=False)
    assert hard is not None
    assert hard["exit_type"] in {"hard_stop", "hard_stop_loss", "hard_stop_full_exit"}
    assert hard["qty"] == 10


def test_legacy_buy_contract_retains_eight_percent_hard_stop(monkeypatch):
    monkeypatch.setenv("US_HARD_STOP_PCT", "0.08")
    monkeypatch.setenv("US_SOFT_STOP_LOSS_PCT", "0.05")
    contract = _new_contract()
    monkeypatch.setenv("US_HARD_STOP_PCT", "0.06")
    monkeypatch.setenv("US_SOFT_STOP_LOSS_PCT", "0.03")
    cfg = contract_exit_config(_position(contract))
    assert cfg["hard_stop"] == pytest.approx(0.08)
    assert cfg["soft_stop"] == pytest.approx(0.05)
    intent = evaluate_exit(_position(contract), 91.5, now=_at_market(), include_trend_time=False)
    assert intent is not None
    assert intent["exit_type"] in {"hard_stop", "hard_stop_loss", "hard_stop_full_exit"}


def test_soft_repeat_risk_escalation_uses_frozen_hard_stop(monkeypatch):
    monkeypatch.setenv("US_HARD_STOP_PCT", "0.08")
    contract = _new_contract()
    monkeypatch.setenv("US_HARD_STOP_PCT", "0.06")
    p = _position(contract)
    p.update({"soft_stop_triggered_today": True, "first_soft_stop_price": 94.0, "current_price": 93.5})
    allowed, reason = soft_stop_repeat_allowed(p)
    assert not allowed
    assert reason == "SOFT_STOP_REPEAT_BLOCKED"


def test_infinite_does_not_acquire_pb1_contract(monkeypatch):
    monkeypatch.delenv("US_HARD_STOP_PCT", raising=False)
    assert build_us_entry_exit_contract({
        "symbol": "TQQQ", "strategy_owner": "TQQQ_INFINITE", "sleeve_id": "TQQQ_INFINITE"
    }) == {}
