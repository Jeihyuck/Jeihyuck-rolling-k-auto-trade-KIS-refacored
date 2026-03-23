from __future__ import annotations

from trader import pb1_runner, runtime_paths


REQUIRED = {
    "code",
    "score_final",
    "tech_score",
    "breakout_score",
    "pullback_score",
    "momentum_score",
    "rs_percentile",
    "vcp_score",
    "entry_style_selected",
    "ma20",
    "ma50",
    "ma150",
    "close",
    "atr_pct",
}


def _row(idx: int) -> dict:
    code = f"{idx + 1:06d}"
    return {
        "code": code,
        "name": f"N{code}",
        "rank_final30": idx + 1,
        "score_final": 90.0 + idx,
        "tech_score": 80.0 + idx,
        "flow_score": 10.0,
        "breakout_score": 20.0 + (idx % 5),
        "pullback_score": 30.0 + (idx % 7),
        "momentum_score": 40.0 + (idx % 9),
        "rs_percentile": 85.0,
        "vcp_score": 75.0,
        "entry_style_selected": "BREAKOUT",
        "ma20": 100.0 + idx,
        "ma50": 95.0 + idx,
        "ma150": 90.0 + idx,
        "close": 101.0 + idx,
        "atr_pct": 0.03,
    }


def test_trade_file_repair_reuses_db_scored(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_paths, "repo_root", lambda: tmp_path)
    monkeypatch.setattr(pb1_runner, "repo_root", lambda: tmp_path)
    monkeypatch.setattr(pb1_runner, "resolve_repo_root", lambda: tmp_path)

    class FakeRepo:
        def __init__(self, *_args, **_kwargs):
            pass

        def verify_watchlist_scored_contract(self, **_kwargs):
            rows = [_row(idx) for idx in range(30)]
            return {
                "ok": True,
                "rows": 30,
                "uniq_codes": 30,
                "uniq_ranks": 30,
                "null_critical": 0,
                "missing_fields": [],
                "columns": list(REQUIRED),
                "rows_data": rows,
                "errors": [],
                "warnings": [],
                "invalid_row_count": 0,
                "invalid_sample_codes": [],
                "invalid_details": {},
            }

    monkeypatch.setattr(pb1_runner, "WatchlistRepo", FakeRepo)

    result = pb1_runner.load_trade_final30_scored(engine=object(), env="practice", as_of="2026-03-20")
    file_validation = pb1_runner._validate_trade_final30_files(repo_root_path=tmp_path, env="practice", as_of="2026-03-20")

    assert result["source_name"] == "db_pb1_watchlist_final_scored"
    assert result["is_scored"] is True
    assert len(result["df"]) == 30
    assert file_validation["ok"] is True