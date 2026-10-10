"""KR RS percentile is stored 0..1 but tech-score component is 0..100."""
import pytest

from trader.pb1_engine import PB1Engine
from trader.watchlist_builder import WatchlistBuilder


def _candidate(rs: float):
    return {
        "code": "096770", "market": "KOSPI",
        "rs_percentile": rs, "rs_score": rs,
        "vcp_score": 10.0, "trend_score": 100.0,
        "breakout_score": 30.0, "pullback_score": 40.0,
        "momentum_score": 85.0,
        "volume": 150, "volume_avg20": 100,
        "current_price": 100.0, "close": 100.0,
        "ma20": 95.0, "ma50": 90.0,
        "atr_pct": 0.04, "volume_missing": False,
    }


@pytest.mark.parametrize("ratio,percent", [(0.989, 98.9), (.85, 85.0), (.60, 60.0)])
def test_rs_ratio_and_percent_have_identical_technical_score(ratio, percent):
    builder = object.__new__(WatchlistBuilder)
    a, b = _candidate(ratio), _candidate(percent)
    score_a = builder._compute_tech_score(a)
    score_b = builder._compute_tech_score(b)
    assert score_a == pytest.approx(score_b, abs=0.0001)
    # .30*RS + .20*VCP + .20*Trend + .20*Entry + .10*Liquidity
    assert score_a == pytest.approx(
        0.30*percent + 0.20*10 + 0.20*100 + 0.20*85 + 0.10*80
    )
    assert a["entry_style_selected"] == b["entry_style_selected"] == "MOMENTUM"


def test_missing_rs_score_falls_back_to_ratio_then_normalizes():
    builder = object.__new__(WatchlistBuilder)
    row = _candidate(.9)
    row.pop("rs_score")
    assert builder._compute_tech_score(row) == pytest.approx(
        .30*90 + .20*10 + .20*100 + .20*85 + .10*80
    )


def test_rs_fix_restores_default_momentum_gate_without_lowering_policy(monkeypatch):
    monkeypatch.setenv("PB1_STYLE_GATE_ENABLED", "1")
    monkeypatch.setenv("PB1_MOMENTUM_GATE_ENABLED", "1")
    monkeypatch.setenv("PB1_MOMENTUM_MIN_SCORE", "60")
    monkeypatch.setenv("PB1_MOMENTUM_MIN_FINAL_SCORE", "55")
    monkeypatch.setenv("PB1_MOMENTUM_MIN_RS", "60")

    builder = object.__new__(WatchlistBuilder)
    row = _candidate(.989)
    builder._attach_scores([row], "regression")
    assert row["entry_style_selected"] == "MOMENTUM"
    assert row["score_final"] >= 55.0
    engine = object.__new__(PB1Engine)
    engine.env = "practice"
    engine.require_volume = False
    ok, reasons, meta = engine._evaluate_final30_entry_setup(
        "096770", row, "KOSPI",
    )
    assert ok is True, reasons
    assert meta["entry_reason"] == "ENTRY_MOMENTUM"


def test_zero_rs_is_zero_not_arbitrarily_inflated():
    builder = object.__new__(WatchlistBuilder)
    score = builder._compute_tech_score(_candidate(0.0))
    assert score == pytest.approx(.20*10 + .20*100 + .20*85 + .10*80)
