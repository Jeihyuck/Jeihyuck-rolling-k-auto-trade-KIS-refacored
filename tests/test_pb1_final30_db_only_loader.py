from __future__ import annotations

import pandas as pd
import pytest

from trader import pb1_runner
from trader.db.repos import ScoredWatchlistNotFoundError


def _rows() -> list[dict]:
    return [
        {
            "code": f"{9000 + idx:06d}",
            "rank_final30": idx,
            "score_final": 100.0 + idx,
            "tech_score": 80.0 + idx,
            "breakout_score": 70.0 + idx,
            "pullback_score": 60.0 + idx,
            "momentum_score": 50.0 + idx,
            "rs_percentile": 95.0,
            "vcp_score": 75.0,
            "entry_style_selected": "BREAKOUT",
            "ma20": 100.0 + idx,
            "ma50": 90.0 + idx,
            "ma150": 80.0 + idx,
            "atr_pct": 0.03,
            "close": 110.0 + idx,
        }
        for idx in range(1, 31)
    ]


def test_load_locked_final30_from_db_succeeds_with_db_only_rows(monkeypatch) -> None:
    class FakeRepo:
        def __init__(self, *_args, **_kwargs):
            pass

        def load_watchlist_scored(self, **_kwargs):
            return _rows(), "2026-04-02"

    monkeypatch.setattr(pb1_runner, "WatchlistRepo", FakeRepo)
    monkeypatch.setattr(
        pb1_runner,
        "load_trade_final30_scored",
        lambda **_kwargs: {
            "df": pd.DataFrame(_rows()),
            "source_name": "db_pb1_watchlist_final_scored",
            "usable": True,
            "missing_scored_cols": [],
        },
    )

    result = pb1_runner.load_locked_final30_from_db(
        engine=object(),
        env="practice",
        derived_as_of="2026-04-02",
    )

    assert result["source_name"] == "db_pb1_watchlist_final_scored"
    assert result["locked"] is True
    assert result["rows"] == 30
    assert len(result["codes"]) == 30


def test_load_locked_final30_from_db_aborts_when_scored_rows_missing(monkeypatch) -> None:
    class FakeRepo:
        def __init__(self, *_args, **_kwargs):
            pass

        def load_watchlist_scored(self, **_kwargs):
            raise ScoredWatchlistNotFoundError(
                "scored_final30_missing",
                expected_strategy="pb1_watchlist_final_scored",
                actual_strategy="none",
                rows=0,
                missing_cols=["score_final"],
            )

    monkeypatch.setattr(pb1_runner, "WatchlistRepo", FakeRepo)

    with pytest.raises(RuntimeError, match="scored_final30_missing"):
        pb1_runner.load_locked_final30_from_db(
            engine=object(),
            env="practice",
            derived_as_of="2026-04-02",
        )