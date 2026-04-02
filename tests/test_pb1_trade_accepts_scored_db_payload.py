from __future__ import annotations

from trader import pb1_runner
from trader.constants import FLOW_OPTIONAL_COLS


def _scored_row(idx: int) -> dict:
    code = f"{8000 + idx:06d}"
    return {
        "as_of": "2026-03-11",
        "code": code,
        "name": f"N{code}",
        "rank_final30": idx,
        "score": 80.0 + idx,
        "score_final": 80.0 + idx,
        "tech_score": 60.0 + idx,
        "breakout_score": 55.0 + idx,
        "pullback_score": 45.0 + idx,
        "momentum_score": 35.0 + idx,
        "rs_percentile": 90.0,
        "vcp_score": 70.0,
        "entry_style_selected": "PULLBACK",
        "ma20": 100.0 + idx,
        "ma50": 95.0 + idx,
        "ma150": 90.0 + idx,
        "atr_pct": 0.03,
        "close": 101.0 + idx,
    }


def test_scored_db_payload_is_accepted_without_flow_fields(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(pb1_runner, "resolve_repo_root", lambda: tmp_path)
    monkeypatch.setattr(pb1_runner, "repo_root", lambda: tmp_path)
    pb1_runner._FINAL30_LOAD_CACHE.clear()

    class FakeRepo:
        def __init__(self, *_args, **_kwargs):
            pass

        def verify_watchlist_scored_contract(self, **_kwargs):
            rows = [_scored_row(idx) for idx in range(1, 31)]
            return {
                "ok": True,
                "rows": 30,
                "uniq_codes": 30,
                "uniq_ranks": 30,
                "null_critical": 0,
                "missing_fields": [],
                "columns": sorted(rows[0].keys()),
                "rows_data": rows,
                "errors": [],
                "warnings": [],
            }

    monkeypatch.setattr(pb1_runner, "WatchlistRepo", FakeRepo)

    result = pb1_runner.load_trade_final30_scored(engine=object(), env="practice", as_of="2026-03-11")

    assert result["source_name"] == "db_pb1_watchlist_final_scored"
    assert result["usable"] is True
    assert result["missing_scored_cols"] == []
    assert result["flow_optional_missing"] == list(FLOW_OPTIONAL_COLS)