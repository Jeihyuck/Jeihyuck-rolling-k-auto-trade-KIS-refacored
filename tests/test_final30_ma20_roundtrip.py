from __future__ import annotations

from datetime import date

import sqlalchemy as sa

from trader.db.repos import WatchlistRepo
from trader.db.schema import schema_for_engine


def _row(code: str, ma20):
    return {
        "as_of": "2026-03-20",
        "code": code,
        "name": f"N{code}",
        "rank": 1,
        "rank_pool120": 1,
        "rank_top50": 1,
        "rank_final30": 1,
        "score": 80.0,
        "score_final": 80.0,
        "score_flow": 10.0,
        "score_liq": 10.0,
        "score_tech": 70.0,
        "tech_score": 70.0,
        "flow_score": 10.0,
        "final_score": 80.0,
        "breakout_score": 20.0,
        "pullback_score": 30.0,
        "momentum_score": 40.0,
        "entry_style_selected": "BREAKOUT",
        "entry_component": "breakout",
        "rs_pctile": 85.0,
        "rs_percentile": 85.0,
        "rs_score": 85.0,
        "vcp_score": 72.0,
        "trend_score": 75.0,
        "atr_pct": 0.03,
        "pullback_pct": 0.02,
        "foreign_20_ratio": 0.1,
        "inst_20_ratio": 0.2,
        "liq_avg": 1_000_000.0,
        "last_close": 120.0,
        "close": 120.0,
        "volume": 100_000.0,
        "volume_avg20": 90_000.0,
        "ma20": ma20,
        "ma50": 105.0,
        "ma150": 95.0,
        "rows": 220,
        "meta": {"source": "test"},
        "scores": {"final": 80.0},
        "reasons": {"passed": ["ok"], "failed": []},
        "reject_reasons": [],
        "filters_passed": ["minervini"],
        "filters_failed": [],
    }


def test_ma20_roundtrip_preserves_valid_and_invalid_values() -> None:
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    repo = WatchlistRepo(engine)
    rows = [_row("000001", 111.5), _row("000002", None), _row("000003", 0)]

    repo.save_watchlist(
        env="practice",
        strategy="pb1_universe_scored",
        as_of=date(2026, 3, 20),
        members=rows,
    )
    loaded, _ = repo.load_watchlist_scored(
        env="practice",
        strategy="pb1_universe_scored",
        as_of=date(2026, 3, 20),
        allow_latest_fallback=False,
    )
    loaded_map = {row["code"]: row for row in loaded}

    assert loaded_map["000001"]["ma20"] == 111.5
    assert loaded_map["000002"]["ma20"] is None
    assert loaded_map["000003"]["ma20"] == 0.0