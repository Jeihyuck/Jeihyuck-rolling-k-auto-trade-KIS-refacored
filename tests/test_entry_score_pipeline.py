from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import sqlalchemy as sa

from trader.db.repos import DerivedMinerviniRepo
from trader.db.schema import schema_for_engine
from trader.minervini.compute import compute_minervini_features_for_asof
from trader.watchlist_builder import WatchlistBuilder


def _make_candles(days: int = 260) -> list[dict]:
    start = date(2025, 1, 1)
    out: list[dict] = []
    for i in range(days):
        px = 100.0 + i * 0.2
        out.append(
            {
                "date": (start + timedelta(days=i)).strftime("%Y%m%d"),
                "open": px,
                "high": px + 1.2,
                "low": px - 1.0,
                "close": px,
                "volume": 1_000_000.0 if i > days - 20 else 800_000.0,
                "value": px * 1_000_000.0,
            }
        )
    return out


def _builder() -> WatchlistBuilder:
    return WatchlistBuilder(
        pooln=120,
        topk=50,
        finaln=30,
        min_price=3000.0,
        liq_days=20,
        min_rows=30,
        ohlcv_provider=MagicMock(),
        flow_provider=MagicMock(),
        minervini_config={},
    )


def test_derived_compute_payload_has_entry_scores(monkeypatch):
    candles = _make_candles()

    def fake_load_price_daily(_engine, _symbol: str, _start, _end):
        return candles

    def fake_rank_rs(price_series, _bench_close, **_kwargs):
        return pd.DataFrame([{"ticker": s, "pctile": 82.0} for s in price_series.keys()])

    monkeypatch.setattr("trader.minervini.compute.load_price_daily", fake_load_price_daily)
    monkeypatch.setattr("trader.minervini.compute.rank_rs", fake_rank_rs)

    rows = compute_minervini_features_for_asof(
        engine=object(),
        symbols=["111111", "222222"],
        env="prep",
        as_of=date(2026, 3, 8),
        lookback_days=260,
    )

    assert rows
    for row in rows:
        assert "breakout_score" in row
        assert "pullback_score" in row
        assert "momentum_score" in row


def test_repo_load_derived_returns_entry_fields():
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)

    repo = DerivedMinerviniRepo(engine)
    repo.upsert_rows(
        env="prep",
        rows=[
            {
                "env": "prep",
                "symbol": "005930",
                "as_of": date(2026, 3, 8),
                "close": 70000.0,
                "rs_percentile": 85.0,
                "rs_score": 85.0,
                "vcp_score": 70.0,
                "trend_score": 75.0,
                "breakout_score": 65.0,
                "pullback_score": 45.0,
                "momentum_score": 80.0,
                "features_json": {},
            }
        ],
    )

    rows = repo.load_derived(env="prep", as_of="2026-03-08", allow_fallback=False)
    assert rows
    row = rows[0]
    assert row["breakout_score"] == 65.0
    assert row["pullback_score"] == 45.0
    assert row["momentum_score"] == 80.0


def test_watchlist_recompute_fallback_recovers_nonzero_entry_scores():
    builder = _builder()
    input_rows = [
        {
            "symbol": "000001",
            "code": "000001",
            "close": 100.0,
            "high_20d": 101.0,
            "high_55d": 103.0,
            "ma20": 98.0,
            "ma50": 95.0,
            "volume": 1_200_000.0,
            "volume_avg20": 800_000.0,
            "ret_20d": 0.05,
            "ret_60d": 0.12,
            "ret_120d": 0.2,
            "pivot": 99.0,
        }
    ]

    with patch.object(builder, "_load_minervini_source_map") as mock_load:
        mock_load.return_value = {
            "000001": {
                "rs_score": 82.0,
                "vcp_score": 60.0,
                "trend_score": 65.0,
                "breakout_score": 0.0,
                "pullback_score": 0.0,
                "momentum_score": 0.0,
            }
        }
        merged = builder._merge_derived_scores(input_rows, as_of=date(2026, 3, 8))

    assert merged[0]["breakout_score"] > 0 or merged[0]["pullback_score"] > 0 or merged[0]["momentum_score"] > 0


def test_watchlist_recompute_hard_fail_when_all_zero_after_recompute():
    builder = _builder()
    input_rows = [{"symbol": "000001", "code": "000001", "close": 0.0, "volume": 0.0}]

    with patch.object(builder, "_load_minervini_source_map") as mock_load:
        mock_load.return_value = {
            "000001": {
                "rs_score": 82.0,
                "vcp_score": 60.0,
                "trend_score": 65.0,
                "breakout_score": 0.0,
                "pullback_score": 0.0,
                "momentum_score": 0.0,
            }
        }
        with pytest.raises(RuntimeError, match="all_entry_scores_zero_after_recompute|entry_score_"):
            builder._merge_derived_scores(input_rows, as_of=date(2026, 3, 8))


def test_numeric_score_not_boolean_or_nan_after_numericize():
    builder = _builder()
    assert isinstance(builder._numericize_entry_score(True, "BREAKOUT"), float)
    assert builder._numericize_entry_score(True, "BREAKOUT") == 100.0

    nan_score = builder._numericize_entry_score(float("nan"), "PULLBACK")
    assert isinstance(nan_score, float)
    assert nan_score == 0.0
