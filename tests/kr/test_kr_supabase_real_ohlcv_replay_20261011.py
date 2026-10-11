"""Real archived Supabase KR OHLCV -> current #199 entry-score functions.

Never queries vendor, broker, production PREP or modifies DB. `price_daily`
market=KOSPI is a DB bucket and is NOT evidence of actual KRX venue.
Final30 universe/flow/regime gates remain separate.
"""
import json
from pathlib import Path

import pytest

from trader.watchlist_builder import WatchlistBuilder

_DATA = json.loads(
    (Path(__file__).parent / "fixtures" / "supabase_ohlcv_proof_replay_20261011.json").read_text()
)


def _bars(symbol: str):
    return _DATA["samples"][symbol]["bars"]


def _breakout_row(symbol: str):
    bars = _bars(symbol)
    assert len(bars) == 260
    last = bars[-1]
    prior55 = bars[-56:-1]
    prior20 = bars[-21:-1]
    return {
        "code": symbol,
        "close": last[1],
        "high_20d": max(x[2] for x in prior20),
        "high_55d": max(x[2] for x in prior55),
        "pivot_price": max(x[2] for x in prior55),
        "volume": last[4],
        "volume_avg20": sum(x[4] for x in prior20) / 20,
    }


@pytest.mark.parametrize("symbol", ["000150", "000250", "006120", "096530", "277810"])
def test_real_kr_daily_ohlcv_produces_standalone_breakout_score_and_trigger(symbol):
    # Strict 55-day HIGH break and 1.5x previous 20-day volume, evaluated
    # on prior completed 2026-10-07 market data (not live quote or BUY).
    builder = object.__new__(WatchlistBuilder)
    row = _breakout_row(symbol)
    assert row["close"] > row["pivot_price"]
    assert row["volume"] >= row["volume_avg20"] * 1.5
    score = builder._compute_breakout_score(row)
    assert score is not None and score >= 60
    assert row["breakout_trigger_ok"] is True


def test_supabase_kr_ohlcv_creates_real_momentum_signal_and_normalizes_rs():
    builder = object.__new__(WatchlistBuilder)
    passing = []
    for symbol in ("006400", "036930", "078340", "096770", "131970"):
        bars = _bars(symbol)
        closes = [float(r[1]) for r in bars]
        price = closes[-1]
        rs = _DATA["samples"][symbol]["rs_percentile"]
        assert rs >= 0.8
        row = {
            "code": symbol,
            "close": price,
            "ma50": sum(closes[-50:]) / 50,
            "ma150": sum(closes[-150:]) / 150,
            "hi_52w": max(float(r[2]) for r in bars[-252:]),
            "rs_percentile": rs,
        }
        score = builder._compute_momentum_score(row)
        assert score is not None
        assert row["momentum_trigger_ok"] == (score >= 55 and rs >= 0.8)
        if row["momentum_trigger_ok"]:
            passing.append(symbol)

        # The #199 0..1 to 0..100 normalization must yield identical
        # technical scoring with actual RS percentile stored as either scale.
        complete = {
            **row,
            "rs_score": rs,
            "vcp_score": 40.0,
            "trend_score": 60.0,
            "breakout_score": 35.0,
            "pullback_score": 50.0,
            "momentum_score": score,
            "volume": bars[-1][4],
            "volume_avg20": sum(float(x[4]) for x in bars[-20:]) / 20,
        }
        as_ratio = builder._compute_tech_score(dict(complete))
        as_percent = builder._compute_tech_score(
            {**complete, "rs_score": rs * 100, "rs_percentile": rs * 100}
        )
        assert as_ratio == as_percent

    # At least one real momentum setup, but no unjustified quota on how many.
    assert passing


def test_kr_replay_is_prior_session_only_and_no_fake_vendor_venue():
    assert _DATA["latest_completed"] == "2026-10-07"
    assert _DATA["test_trade_date"] == "2026-10-08"
    for item in _DATA["samples"].values():
        dates = [row[0] for row in item["bars"]]
        assert len(dates) == len(set(dates)) == 260
        assert dates == sorted(dates)
        assert dates[-1] < "20261008"
