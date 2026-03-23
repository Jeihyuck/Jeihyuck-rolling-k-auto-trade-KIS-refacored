from __future__ import annotations

from datetime import date

import sqlalchemy as sa

from trader import pb1_runner, prep_runner
from trader.db.repos import WatchlistRepo, verify_final30_scored_contract
from trader.db.schema import schema_for_engine


def _row(idx: int, *, ma20: float | None = None) -> dict:
    code = f"{100000 + idx:06d}"
    value = float(idx)
    return {
        "as_of": "2026-03-20",
        "code": code,
        "name": f"N{code}",
        "rank": idx,
        "rank_pool120": idx,
        "rank_top50": idx,
        "rank_final30": idx,
        "score": 80.0 + value,
        "score_final": 80.0 + value,
        "score_flow": 10.0,
        "score_liq": 10.0,
        "score_tech": 70.0 + value,
        "tech_score": 70.0 + value,
        "flow_score": 10.0,
        "final_score": 80.0 + value,
        "breakout_score": 20.0 + (idx % 5),
        "pullback_score": 35.0 + (idx % 7),
        "momentum_score": 50.0 + (idx % 9),
        "entry_style_selected": ["BREAKOUT", "PULLBACK", "MOMENTUM"][idx % 3],
        "entry_component": "momentum",
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
        "last_close": 120.0 + value,
        "close": 120.0 + value,
        "volume": 100_000.0,
        "volume_avg20": 90_000.0,
        "ma20": 110.0 + value if ma20 is None else ma20,
        "ma50": 105.0 + value,
        "ma150": 95.0 + value,
        "rows": 220,
        "meta": {"source": "test"},
        "scores": {"final": 80.0 + value},
        "reasons": {"passed": ["ok"], "failed": []},
        "reject_reasons": [],
        "filters_passed": ["minervini"],
        "filters_failed": [],
    }


def test_prep_trade_db_contract_results_match() -> None:
    rows = [_row(idx) for idx in range(1, 31)]
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

    comparable_keys = ["ok", "errors", "invalid_row_count", "valid_ma20_ratio", "valid_atr_ratio", "score_monoculture", "momentum_monoculture"]
    for key in comparable_keys:
        assert prep_result[key] == trade_result[key] == db_result[key]