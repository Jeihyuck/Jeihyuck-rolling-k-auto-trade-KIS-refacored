from __future__ import annotations

from datetime import date

import pandas as pd
import sqlalchemy as sa

from trader.pb1_engine import PB1Engine
from trader.db.repos import REQUIRED_FINAL30_SCORED_COLS, WatchlistRepo
from trader.db.schema import schema_for_engine
from trader.watchlist_builder import _build_final30_saved_rows


def _scored_row(idx: int) -> dict:
    code = f"{7000 + idx:06d}"
    return {
        "as_of": "2026-03-11",
        "code": code,
        "name": f"N{code}",
        "rank": idx,
        "rank_pool120": idx,
        "rank_top50": idx,
        "rank_final30": idx,
        "score": 70.0 + idx,
        "score_final": 70.0 + idx,
        "final_score": 70.0 + idx,
        "tech_score": 50.0 + idx,
        "breakout_score": 40.0 + idx,
        "pullback_score": 30.0 + idx,
        "momentum_score": 20.0 + idx,
        "rs_percentile": 88.0,
        "vcp_score": 77.0,
        "entry_style_selected": "BREAKOUT",
        "ma20": 100.0 + idx,
        "ma50": 95.0 + idx,
        "ma150": 90.0 + idx,
        "atr_pct": 0.03,
        "close": 102.0 + idx,
    }


def test_final30_scored_roundtrip_keeps_trade_required_schema() -> None:
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)

    rows = [_scored_row(idx) for idx in range(1, 31)]
    saved_rows = _build_final30_saved_rows(rows)
    first_row_keys = set(saved_rows[0].keys())
    assert set(REQUIRED_FINAL30_SCORED_COLS).issubset(first_row_keys)

    repo = WatchlistRepo(engine)
    as_of = date(2026, 3, 11)
    repo.save_watchlist(
        env="practice",
        strategy="pb1_watchlist_final_scored",
        as_of=as_of,
        members=saved_rows,
    )

    loaded_rows, used_as_of = repo.load_watchlist_scored(
        env="practice",
        strategy="pb1_watchlist_final_scored",
        as_of=as_of,
        allow_latest_fallback=False,
    )

    assert used_as_of == as_of
    assert len(loaded_rows) == 30
    loaded_cols = list(pd.DataFrame(loaded_rows).columns)
    assert PB1Engine._scored_missing_cols(loaded_cols) == []
