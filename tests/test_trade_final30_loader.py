from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

from trader import pb1_runner
from trader import runtime_paths
from trader.path_contract import build_final30_paths


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


def _scored_row(code: str) -> dict:
    idx = int(code)
    return {
        "code": code,
        "name": f"N{code}",
        "rank_final30": 1,
        "score_final": 90.0 + (idx % 11),
        "tech_score": 80.0 + (idx % 7),
        "flow_score": 70.0,
        "breakout_score": 55.0 + (idx % 9),
        "pullback_score": 45.0 + (idx % 7),
        "momentum_score": 35.0 + (idx % 5),
        "rs_percentile": 85.0,
        "vcp_score": 75.0,
        "entry_style_selected": "breakout" if idx % 3 == 0 else ("pullback" if idx % 3 == 1 else "momentum"),
        "ma20": 100.0,
        "ma50": 95.0 + (idx % 4),
        "ma150": 90.0 + (idx % 3),
        "close": 101.0 + (idx % 6),
        "atr_pct": 0.03,
    }


def _scored_rows(n: int = 30) -> list[dict]:
    return [_scored_row(f"{idx + 1:06d}") | {"rank_final30": idx + 1} for idx in range(n)]


def _patch_repo_root(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(runtime_paths, "repo_root", lambda: tmp_path)
    monkeypatch.setattr(pb1_runner, "repo_root", lambda: tmp_path)
    monkeypatch.setattr(pb1_runner, "resolve_repo_root", lambda: tmp_path)


def test_load_trade_final30_scored_prefers_db_contract_over_file(tmp_path, monkeypatch):
    _patch_repo_root(monkeypatch, tmp_path)
    as_of = "2026-03-11"
    path = tmp_path / "runtime" / "watchlist" / as_of / "final30_scored.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = _scored_rows()
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    class FakeRepo:
        def __init__(self, *_args, **_kwargs):
            pass

        def verify_watchlist_scored_contract(self, **_kwargs):
            return {
                "ok": True,
                "rows": 30,
                "uniq_codes": 30,
                "uniq_ranks": 30,
                "null_critical": 0,
                "missing_fields": [],
                "columns": list(REQUIRED),
                "rows_data": _scored_rows(),
            }

        def load_watchlist(self, **_kwargs):
            return [], None

    monkeypatch.setattr(pb1_runner, "WatchlistRepo", FakeRepo)

    result = pb1_runner.load_trade_final30_scored(engine=object(), env="practice", as_of=as_of)

    assert result["source_name"] == "db_pb1_watchlist_final_scored"
    assert result["used_fallback"] is False
    assert result["is_scored"] is True
    assert REQUIRED.issubset(set(result["columns"]))


def test_load_trade_final30_scored_uses_db_scored_when_file_missing(tmp_path, monkeypatch):
    _patch_repo_root(monkeypatch, tmp_path)

    class FakeRepo:
        def __init__(self, *_args, **_kwargs):
            pass

        def verify_watchlist_scored_contract(self, **_kwargs):
            return {
                "ok": True,
                "rows": 30,
                "uniq_codes": 30,
                "uniq_ranks": 30,
                "null_critical": 0,
                "missing_fields": [],
                "columns": list(REQUIRED),
                "rows_data": _scored_rows(),
            }

        def load_watchlist(self, *, strategy, **_kwargs):
            return [], None

    monkeypatch.setattr(pb1_runner, "WatchlistRepo", FakeRepo)

    result = pb1_runner.load_trade_final30_scored(engine=object(), env="practice", as_of="2026-03-11")

    assert result["source_name"] == "db_pb1_watchlist_final_scored"
    assert result["is_scored"] is True
    assert isinstance(result["df"], pd.DataFrame)
    assert not result["df"].empty


def test_load_trade_final30_scored_blocks_plain_fallback_by_default(tmp_path, monkeypatch):
    _patch_repo_root(monkeypatch, tmp_path)
    monkeypatch.delenv("TRADE_ALLOW_PLAIN_WATCHLIST_FALLBACK", raising=False)

    class FakeRepo:
        def __init__(self, *_args, **_kwargs):
            pass

        def verify_watchlist_scored_contract(self, **_kwargs):
            return {
                "ok": False,
                "rows": 0,
                "uniq_codes": 0,
                "uniq_ranks": 0,
                "null_critical": 0,
                "missing_fields": ["score_final"],
                "columns": ["code"],
                "rows_data": [],
            }

        def load_watchlist(self, *, strategy, **_kwargs):
            if strategy == "pb1_watchlist_final_scored":
                return [], None
            if strategy == "pb1_watchlist_final":
                return [{"code": "005930", "rank": 1, "score": 1.0, "meta": {}}], date(2026, 3, 11)
            return [], None

    monkeypatch.setattr(pb1_runner, "WatchlistRepo", FakeRepo)

    result = pb1_runner.load_trade_final30_scored(engine=object(), env="practice", as_of="2026-03-11")

    assert result["df"].empty
    assert result["source_name"] == "none"


def test_load_trade_final30_scored_rejects_non_exact_contract(tmp_path, monkeypatch):
    _patch_repo_root(monkeypatch, tmp_path)

    class FakeRepo:
        def __init__(self, *_args, **_kwargs):
            pass

        def verify_watchlist_scored_contract(self, **_kwargs):
            rows = _scored_rows(29)
            return {
                "ok": True,
                "rows": 29,
                "uniq_codes": 29,
                "uniq_ranks": 29,
                "null_critical": 0,
                "missing_fields": [],
                "columns": list(REQUIRED),
                "rows_data": rows,
            }

    monkeypatch.setattr(pb1_runner, "WatchlistRepo", FakeRepo)

    result = pb1_runner.load_trade_final30_scored(engine=object(), env="practice", as_of="2026-03-11")

    assert result["df"].empty
    assert result["source_name"] == "none"


def test_load_trade_final30_scored_repairs_missing_mirrors_from_db(tmp_path, monkeypatch):
    _patch_repo_root(monkeypatch, tmp_path)

    class FakeRepo:
        def __init__(self, *_args, **_kwargs):
            pass

        def verify_watchlist_scored_contract(self, **_kwargs):
            return {
                "ok": True,
                "rows": 30,
                "uniq_codes": 30,
                "uniq_ranks": 30,
                "null_critical": 0,
                "missing_fields": [],
                "columns": list(REQUIRED),
                "rows_data": _scored_rows(),
            }

    monkeypatch.setattr(pb1_runner, "WatchlistRepo", FakeRepo)

    result = pb1_runner.load_trade_final30_scored(engine=object(), env="practice", as_of="2026-03-11")

    assert result["source_name"] == "db_pb1_watchlist_final_scored"
    assert (tmp_path / "runtime" / "watchlist" / "2026-03-11" / "final30_scored.json").exists()
    assert (tmp_path / "bot_state" / "trader_ledger" / "final30" / "practice" / "2026-03-11" / "final30_scored.json").exists()
    assert (tmp_path / "signals" / "final30.json").exists()


def test_load_trade_final30_scored_repairs_ma20_only_contract_failure(tmp_path, monkeypatch):
    _patch_repo_root(monkeypatch, tmp_path)
    rows = _scored_rows()
    rows[0] = {**rows[0], "ma20": None, "meta": {"ma20": None}}
    universe_rows = _scored_rows()

    class FakeRepo:
        def __init__(self, *_args, **_kwargs):
            pass

        def verify_watchlist_scored_contract(self, **_kwargs):
            return {
                "ok": False,
                "rows": 30,
                "uniq_codes": 30,
                "uniq_ranks": 30,
                "null_critical": 0,
                "missing_fields": [],
                "columns": list(REQUIRED),
                "rows_data": rows,
                "errors": ["ma20_invalid_rows"],
                "warnings": [],
                "invalid_row_count": 1,
                "invalid_details": {rows[0]["code"]: ["ma20"]},
                "invalid_sample_codes": [rows[0]["code"]],
            }

        def load_watchlist_scored(self, *, strategy, **_kwargs):
            if strategy == "pb1_universe_scored":
                return universe_rows, date(2026, 3, 11)
            return [], None

        def load_watchlist(self, *, strategy, **_kwargs):
            if strategy == "pb1_top50":
                return universe_rows[:30], date(2026, 3, 11)
            return [], None

    monkeypatch.setattr(pb1_runner, "WatchlistRepo", FakeRepo)

    result = pb1_runner.load_trade_final30_scored(engine=object(), env="practice", as_of="2026-03-11")

    assert result["source_name"] == "db_pb1_watchlist_final_scored"
    assert not result["df"].empty
    assert int((result["df"]["ma20"] > 0).sum()) == len(result["df"])


def test_get_final30_artifact_paths_are_repo_root_anchored(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_paths, "repo_root", lambda: tmp_path)

    paths = runtime_paths.get_final30_artifact_paths("practice", "2026-03-11", include_legacy=True)

    assert paths[0][1] == tmp_path / "runtime" / "watchlist" / "2026-03-11" / "final30_scored.json"
    assert paths[1][1] == tmp_path / "bot_state" / "trader_ledger" / "final30" / "practice" / "2026-03-11" / "final30_scored.json"
    assert paths[2][1] == tmp_path / "signals" / "final30.json"


def test_build_final30_scored_paths_returns_three_contract_paths(tmp_path):
    paths = runtime_paths.build_final30_scored_paths(tmp_path, "practice", "2026-03-11")

    assert paths == {
        "runtime": tmp_path / "runtime" / "watchlist" / "2026-03-11" / "final30_scored.json",
        "ledger": tmp_path / "bot_state" / "trader_ledger" / "final30" / "practice" / "2026-03-11" / "final30_scored.json",
        "signals": tmp_path / "signals" / "final30.json",
    }


def test_path_contract_matches_runtime_paths(tmp_path):
    assert build_final30_paths(tmp_path, "practice", date(2026, 3, 11)) == runtime_paths.build_final30_scored_paths(
        tmp_path,
        "practice",
        "2026-03-11",
    )
