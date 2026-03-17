from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

from trader import pb1_runner
from trader import runtime_paths


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
}


def _scored_row(code: str) -> dict:
    return {
        "code": code,
        "name": f"N{code}",
        "rank_final30": 1,
        "score_final": 90.0,
        "tech_score": 80.0,
        "flow_score": 70.0,
        "breakout_score": 60.0,
        "pullback_score": 50.0,
        "momentum_score": 40.0,
        "rs_percentile": 85.0,
        "vcp_score": 75.0,
        "entry_style_selected": "breakout",
    }


def test_load_trade_final30_scored_prefers_canonical_file(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_paths, "repo_root", lambda: tmp_path)
    monkeypatch.setattr(pb1_runner, "repo_root", lambda: tmp_path)
    as_of = "2026-03-11"
    path = tmp_path / "runtime" / "watchlist" / as_of / "final30_scored.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = [_scored_row("005930")]
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    class FakeRepo:
        def __init__(self, *_args, **_kwargs):
            pass

        def load_watchlist(self, **_kwargs):
            return [], None

        def load_watchlist_scored(self, **_kwargs):
            return [], None

    monkeypatch.setattr(pb1_runner, "WatchlistRepo", FakeRepo)

    result = pb1_runner.load_trade_final30_scored(engine=object(), env="practice", as_of=as_of)

    assert result["source_name"] == "runtime_final30_scored"
    assert result["used_fallback"] is False
    assert result["is_scored"] is True
    assert REQUIRED.issubset(set(result["columns"]))


def test_load_trade_final30_scored_uses_db_scored_when_file_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_paths, "repo_root", lambda: tmp_path)
    monkeypatch.setattr(pb1_runner, "repo_root", lambda: tmp_path)

    class FakeRepo:
        def __init__(self, *_args, **_kwargs):
            pass

        def load_watchlist_scored(self, *, strategy, **_kwargs):
            if strategy == "pb1_watchlist_final_scored":
                return [_scored_row("000660")], date(2026, 3, 11)
            return [], None

        def load_watchlist(self, *, strategy, **_kwargs):
            return [], None

    monkeypatch.setattr(pb1_runner, "WatchlistRepo", FakeRepo)

    result = pb1_runner.load_trade_final30_scored(engine=object(), env="practice", as_of="2026-03-11")

    assert result["source_name"] == "db_pb1_watchlist_final_scored"
    assert result["is_scored"] is True
    assert isinstance(result["df"], pd.DataFrame)
    assert not result["df"].empty


def test_load_trade_final30_scored_blocks_plain_fallback_by_default(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_paths, "repo_root", lambda: tmp_path)
    monkeypatch.setattr(pb1_runner, "repo_root", lambda: tmp_path)
    monkeypatch.delenv("TRADE_ALLOW_PLAIN_WATCHLIST_FALLBACK", raising=False)

    class FakeRepo:
        def __init__(self, *_args, **_kwargs):
            pass

        def load_watchlist_scored(self, *, strategy, **_kwargs):
            if strategy == "pb1_watchlist_final_scored":
                return [], None
            return [], None

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
