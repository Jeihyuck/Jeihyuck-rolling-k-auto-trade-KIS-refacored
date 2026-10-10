"""US Momentum and Breakout must be independently proven on completed bars."""
from datetime import date, timedelta

from trader.us.candidate_pool_builder import _compute_us_explicit_signal_proofs
from trader.us.watchlist_builder import _select_entry_style


def _daily():
    begin = date(2026, 1, 1)
    rows = []
    for i in range(80):
        close = 80.0 + 0.5 * i
        rows.append({
            "xymd": (begin + timedelta(days=i)).strftime("%Y%m%d"),
            "clos": close,
            "high": close + 0.25,
            "low": close - 0.25,
            "tvol": 3000 if i == 79 else 1000,
        })
    return rows


def test_original_us_momentum_strategy_score_is_used_for_prep_signal():
    proof = _compute_us_explicit_signal_proofs("ABCD", _daily())
    assert proof["momentum_pass"] is True
    assert proof["standalone_momentum_score"] > 0
    assert proof["entry_signal_proof_source"] == "completed_daily_ohlcv"


def test_breakout_requires_prior_pivot_and_volume_confirmation():
    proof = _compute_us_explicit_signal_proofs("ABCD", _daily())
    assert proof["breakout_pass"] is True
    assert proof["breakout_pivot_price"] > 0
    rows = _daily()
    rows[-1]["tvol"] = 1000
    assert _compute_us_explicit_signal_proofs("ABCD", rows)["breakout_pass"] is False
    rows[-1]["clos"] = rows[-2]["clos"]
    assert _compute_us_explicit_signal_proofs("ABCD", rows)["breakout_pass"] is False


def test_independent_style_gates_both_missing_signals(monkeypatch):
    monkeypatch.setenv("US_INDEPENDENT_MOMENTUM_BREAKOUT_ENABLED", "1")
    row = {"independent_entry_contract_v1": True, "momentum_pass": False,
           "breakout_pass": False}
    assert _select_entry_style(
        row, pb1_score=0.65, momentum_score=0.95,
        pullback_score=0.50, breakout_score=0.96,
    ) == "pb1_pullback"


def test_independent_momentum_and_breakout_compete_on_validated_scores(monkeypatch):
    monkeypatch.setenv("US_INDEPENDENT_MOMENTUM_BREAKOUT_ENABLED", "1")
    momentum = {"independent_entry_contract_v1": True,
                "entry_signal_proof_source": "completed_daily_ohlcv",
                "momentum_pass": True, "breakout_pass": False}
    assert _select_entry_style(
        momentum, pb1_score=0.65, momentum_score=0.95,
        pullback_score=0.50, breakout_score=0.98,
    ) == "momentum"
    breakout = {"independent_entry_contract_v1": True,
                "entry_signal_proof_source": "completed_daily_ohlcv",
                "momentum_pass": False, "breakout_pass": True}
    assert _select_entry_style(
        breakout, pb1_score=0.65, momentum_score=0.95,
        pullback_score=0.50, breakout_score=0.98,
    ) == "breakout"


def test_disabled_flag_does_not_change_existing_style_choice(monkeypatch):
    monkeypatch.delenv("US_INDEPENDENT_MOMENTUM_BREAKOUT_ENABLED", raising=False)
    row = {"independent_entry_contract_v1": True, "momentum_pass": False,
           "breakout_pass": False}
    assert _select_entry_style(
        row, pb1_score=0.65, momentum_score=0.95,
        pullback_score=0.50, breakout_score=0.98,
    ) == "breakout"


def test_independent_us_short_history_fails_both_signal_proofs():
    result = _compute_us_explicit_signal_proofs("ABCD", _daily()[:30])
    assert result["independent_entry_contract_v1"] is True
    assert result["momentum_pass"] is False
    assert result["breakout_pass"] is False
