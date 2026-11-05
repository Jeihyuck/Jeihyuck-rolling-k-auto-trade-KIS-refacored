"""v3 strategy->router boundaries; all tests are broker-free."""
import pytest

from trader.us.pb1 import us_exit_engine as engine
from trader.us.pb1.swing_runner_exit_v3 import V3_RUNNER_POLICY
from trader.us.runner.trade_tick_runner import merge_exit_intents_by_symbol
from trader.us.execution.order_router import _semantic_action_instance
from trader.us.entry_exit_contract import build_us_entry_exit_contract


def frozen_v3(monkeypatch):
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("US_SWING_EXIT_MODE", "V3_ACTIVE_PRACTICE")
    monkeypatch.setenv("US_SWING_V3_DEPLOY_APPROVED", "1")
    return build_us_entry_exit_contract({
        "symbol": "SPY", "strategy_owner": "US_STANDARD",
        "position_action": "NEW_POSITION_BUY", "book": "SWING_BOOK",
        "horizon": "SWING_CARRY",
    })


def test_v3_does_not_fall_back_to_old_profit_protect(monkeypatch):
    contract = frozen_v3(monkeypatch)
    pos = {"symbol": "SPY", "exchange": "AMEX", "qty": 10,
           "entry_price": 100, "max_price": 120,
           "position_lifecycle_id": "life-123", "entry_exit_contract": contract}
    monkeypatch.setattr(
        "trader.us.pb1.swing_runner_exit_v3.evaluate_v3_from_position",
        lambda **_kwargs: {"action": "HOLD", "reason": "V3_RUNNER_HOLD"},
    )
    # At +16% with a 3.33% high pullback, v2 profit_protect would exit.
    # v3 must not produce that legacy full liquidation.
    assert engine.evaluate_exit(pos, current_price=116, include_trend_time=False) is None


def test_v3_partial_uses_signed_semantic_lifecycle(monkeypatch):
    contract = frozen_v3(monkeypatch)
    pos = {"symbol": "SPY", "exchange": "AMEX", "qty": 10,
           "entry_price": 100, "max_price": 110,
           "position_lifecycle_id": "life-123", "entry_exit_contract": contract}
    monkeypatch.setattr(
        "trader.us.pb1.swing_runner_exit_v3.evaluate_v3_from_position",
        lambda **_kwargs: {"action": "SELL", "stage": "PROFIT_DEFENSE_PARTIAL",
                           "qty": 5, "exit_type": "swing_giveback_partial_v3",
                           "reason": "GB1_PROFIT_GIVEBACK", "peak_gain": .1,
                           "current_gain": .08, "giveback": .2, "trend_state": "TRIM"},
    )
    monkeypatch.setattr(
        "trader.us.protective_stop_obligation.record_confirmed_stop",
        lambda *_a, **_kw: False,
    )
    monkeypatch.setattr(
        engine, "_make_exit_intent",
        lambda **kwargs: {
            "side": "SELL", "exit_type": kwargs["exit_type"],
            "qty": kwargs["qty"], "reason": kwargs["reason"],
            "meta": dict(kwargs.get("meta_extra") or {}),
        },
    )
    signal = engine.evaluate_exit(pos, current_price=108, include_trend_time=False)
    assert signal["qty"] == 5
    assert signal["strategy_owner"] == "US_STANDARD"
    assert signal["partial_exit_allowed"] is True
    assert signal["semantic_action"] == "PROFIT_DEFENSE_PARTIAL"
    m = signal["meta"]
    assert m["v3_signed_entry_contract"]["sha256"] == contract["sha256"]
    identity = _semantic_action_instance(signal, m, signal["semantic_action"], None)
    assert identity == contract["sha256"]
    m["v3_signed_entry_contract"] = {"version": "us_entry_exit_contract_v3"}
    with pytest.raises(ValueError, match="UNVERIFIED"):
        _semantic_action_instance(signal, m, signal["semantic_action"], None)


def test_final_preempts_profit_capture_without_changing_legacy_priority():
    tp = {"symbol": "SPY", "side": "SELL", "qty": 2, "reason": "TAKE_PROFIT_TP3"}
    full = {"symbol": "SPY", "side": "SELL", "qty": 8,
            "exit_type": "swing_runner_final_v3", "reason": "R4"}
    hard = {"symbol": "SPY", "side": "SELL", "qty": 10,
            "exit_type": "hard_stop_loss", "reason": "hard_stop_loss"}
    assert merge_exit_intents_by_symbol([tp, full])[0]["exit_type"] == "swing_runner_final_v3"
    assert merge_exit_intents_by_symbol([tp, full, hard])[0]["exit_type"] == "hard_stop_loss"


def test_v3_completed_partial_blocks_later_legacy_trend_trim(monkeypatch):
    contract = frozen_v3(monkeypatch)
    pos = {"symbol": "SPY", "exchange": "AMEX", "qty": 8,
           "entry_price": 100, "max_price": 110,
           "position_lifecycle_id": "life-123", "entry_exit_contract": contract,
           "trend": {"trend_state": "TRIM", "lifecycle_id": "life-123"}}
    monkeypatch.setattr(
        "trader.us.pb1.swing_runner_exit_v3.evaluate_v3_from_position",
        lambda **_kwargs: {"action": "HOLD",
                           "reason": "PARTIAL_STAGE_ALREADY_ACTIVE_OR_UNVERIFIED",
                           "partial_budget_state": "DONE"},
    )
    monkeypatch.setattr(
        "trader.us.position_trend_state.choose_trend_time_exit",
        lambda *_a, **_k: ("trend_deterioration_trim", 3, "trend_trim"),
    )
    assert engine.evaluate_exit(pos, 108, include_trend_time=True) is None


def test_v3_completed_partial_does_not_block_mandatory_full_trend_exit(monkeypatch):
    contract = frozen_v3(monkeypatch)
    pos = {"symbol": "SPY", "exchange": "AMEX", "qty": 8,
           "entry_price": 100, "max_price": 110,
           "position_lifecycle_id": "life-123", "entry_exit_contract": contract,
           "trend": {"trend_state": "EXIT", "lifecycle_id": "life-123"}}
    monkeypatch.setattr(
        "trader.us.pb1.swing_runner_exit_v3.evaluate_v3_from_position",
        lambda **_kwargs: {"action": "HOLD",
                           "reason": "V3_RUNNER_HOLD",
                           "partial_budget_state": "DONE"},
    )
    monkeypatch.setattr(
        "trader.us.position_trend_state.choose_trend_time_exit",
        lambda *_a, **_k: ("trend_deterioration_exit", 8, "trend_exit"),
    )
    monkeypatch.setattr(
        "trader.us.protective_stop_obligation.record_confirmed_stop",
        lambda *_a, **_k: False,
    )
    monkeypatch.setattr(
        engine, "_make_exit_intent",
        lambda **kwargs: {"side": "SELL", "qty": kwargs["qty"],
                           "exit_type": kwargs["exit_type"],
                           "reason": kwargs["reason"], "meta": kwargs.get("meta_extra", {})},
    )
    monkeypatch.setattr(
        "trader.us.db.repos.mark_us_position_exit_stage",
        lambda *_a, **_kw: None,
    )
    x = engine.evaluate_exit(pos, 108, include_trend_time=True)
    assert x and x["exit_type"] == "trend_deterioration_exit" and x["qty"] == 8
