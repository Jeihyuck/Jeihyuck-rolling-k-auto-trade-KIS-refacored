from __future__ import annotations

from datetime import date

import sqlalchemy as sa

from trader import pb1_runner, prep_runner
from trader.db.repos import WatchlistRepo, verify_final30_scored_contract
from trader.db.schema import schema_for_engine


def _row(idx: int, *, ma20) -> dict:
    code = f"{idx + 1:06d}"
    return {
        "as_of": "2026-03-20",
        "code": code,
        "name": f"N{code}",
        "rank": idx + 1,
        "rank_pool120": idx + 1,
        "rank_top50": idx + 1,
        "rank_final30": idx + 1,
        "score": 80.0 + idx,
        "score_final": 80.0 + idx,
        "score_flow": 10.0,
        "score_liq": 10.0,
        "score_tech": 70.0 + idx,
        "tech_score": 70.0 + idx,
        "flow_score": 10.0,
        "final_score": 80.0 + idx,
        "breakout_score": 20.0 + (idx % 5),
        "pullback_score": 30.0 + (idx % 7),
        "momentum_score": 40.0 + (idx % 9),
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
        "last_close": 120.0 + idx,
        "close": 120.0 + idx,
        "volume": 100_000.0,
        "volume_avg20": 90_000.0,
        "ma20": ma20,
        "ma50": 105.0 + idx,
        "ma150": 95.0 + idx,
        "rows": 220,
        "meta": {"source": "test"},
        "scores": {"final": 80.0 + idx},
        "reasons": {"passed": ["ok"], "failed": []},
        "reject_reasons": [],
        "filters_passed": ["minervini"],
        "filters_failed": [],
    }


def test_regression_ma20_invalid_abort_uses_same_helper_everywhere() -> None:
    rows = [_row(idx, ma20=None if idx == 0 else 110.0 + idx) for idx in range(30)]
    prep_result = prep_runner._strict_validate_final30_rows(rows, source="TEST")
    trade_result = pb1_runner._strict_validate_trade_final30_rows(rows, source="TEST")

    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    repo = WatchlistRepo(engine)
    repo.save_watchlist(
        env="practice",
        strategy="pb1_watchlist_final_scored",
        as_of=date(2026, 3, 20),
        members=rows,
    )
    db_result = verify_final30_scored_contract(
        engine,
        env="practice",
        as_of=date(2026, 3, 20),
        allow_latest_fallback=False,
        log_result=False,
    )

    assert prep_result["ok"] is False
    assert trade_result["ok"] is False
    assert db_result["ok"] is False
    assert "ma20_invalid_rows" in prep_result["errors"]
    assert prep_result["errors"] == trade_result["errors"] == db_result["errors"]