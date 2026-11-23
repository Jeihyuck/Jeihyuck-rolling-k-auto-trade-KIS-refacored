"""2026-11-23 v2: momentum dollar-price-unit invariance, offline only."""
from __future__ import annotations

import math

import pytest

from trader.us.strategy.us_momentum import USMomentumStrategy, _std


def _prices(scale=1.0):
    return [
        {"clos": 10.0 * scale * math.exp(0.003 * day + 0.006 * math.sin(day))}
        for day in range(260)
    ]


def _score(rows):
    return USMomentumStrategy(run_id="TEST_ONLY").score(
        "FAKE", rows, {"last": rows[-1]["clos"]}
    )


def test_score_is_invariant_under_nominal_price_scale():
    baseline = _score(_prices(1.0))
    assert baseline is not None
    assert 0 <= baseline <= 1
    for multiple in (0.1, 10.0, 100.0):
        assert _score(_prices(multiple)) == baseline


def test_volatility_is_a_dimensionless_return_fraction():
    daily = [row["clos"] for row in _prices()[-21:]]
    sigma = _std(daily)
    assert 0 < sigma < 1
    assert _std([v * 10 for v in daily]) == pytest.approx(sigma, rel=1e-9)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -2.0, 0.0])
def test_invalid_completed_close_rejects_candidate(bad):
    rows = _prices()
    rows[-2]["clos"] = bad
    assert _score(rows) is None


def test_insufficient_completed_bars_rejects_candidate():
    assert _score(_prices()[:62]) is None


def test_zero_volatility_is_not_replaced_with_one_dollar():
    # 21 constant closes have zero 20-day daily return variance.
    assert _std([100.0] * 21) == 0.0
    assert _std([100.0] * 20) == 0.0
    # Constant daily growth also has essentially zero return variance.
    rows = [{"clos": 100.0 * (1.01 ** i)} for i in range(260)]
    score = _score(rows)
    assert score is not None
    high_52w = max(row["clos"] for row in rows)
    proximity = rows[-1]["clos"] / high_52w
    assert score == pytest.approx(round(0.4 * proximity, 3), abs=0.001)


def test_original_20_60_day_eligibility_stays_unchanged():
    rows = [{"clos": 100.0} for _ in range(260)]
    assert _score(rows) is None
    # Positive 20-day move alone must not make 60-day momentum eligible.
    for row in rows[-20:]:
        row["clos"] = 104.0
    assert _score(rows) is None
