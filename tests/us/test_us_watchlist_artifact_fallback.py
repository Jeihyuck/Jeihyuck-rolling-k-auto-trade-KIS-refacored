from __future__ import annotations

import json
from pathlib import Path

import pytest


def _rows(n=12, *, score_key="score", duplicate=False):
    rows = []
    for i in range(n):
        row = {"symbol": f"T{i:02d}", "exchange": "NASDAQ", score_key: float(n - i), "agent_a_score": 1, "agent_b_score": 1, "rank_final30": i + 1}
        rows.append(row)
    if duplicate:
        rows.append({"symbol": "T00", "exchange": "NASDAQ", score_key: 99.0, "rank_final30": 99})
    return rows


def test_latest_summary_embedded_rows_success(monkeypatch, tmp_path):
    from trader.us.runner.trade_tick_runner import load_watchlist_from_artifact
    monkeypatch.chdir(tmp_path)
    base = Path("reports/us_prep")
    base.mkdir(parents=True)
    (base / "latest_us_prep_summary.json").write_text(json.dumps({"trade_date": "2026-06-26", "payload": {"final30_scored": _rows()}}))
    result = load_watchlist_from_artifact("2026-06-26")
    assert len(result) == 12
    assert result[0]["entry_watchlist_source"] == "latest_summary_embedded_rows"


def test_latest_summary_referenced_artifact_success(monkeypatch, tmp_path):
    from trader.us.runner.trade_tick_runner import load_watchlist_from_artifact
    monkeypatch.chdir(tmp_path)
    base = Path("reports/us_prep")
    ref = base / "ref_final30.json"
    base.mkdir(parents=True)
    ref.write_text(json.dumps({"trade_date": "2026-06-26", "payload": {"rows": _rows(score_key="score_final")}}))
    (base / "latest_us_prep_summary.json").write_text(json.dumps({"trade_date": "2026-06-26", "contract": {"final30_scored_path": str(ref)}}))
    result = load_watchlist_from_artifact("2026-06-26")
    assert len(result) == 12
    assert result[0]["entry_watchlist_source"] == "latest_summary_referenced_artifact"
    assert result[0]["score"] == result[0]["score_final"]


def test_latest_final30_scored_success(monkeypatch, tmp_path):
    from trader.us.runner.trade_tick_runner import load_watchlist_from_artifact
    monkeypatch.chdir(tmp_path)
    base = Path("reports/us_prep")
    base.mkdir(parents=True)
    (base / "latest_final30_scored.json").write_text(json.dumps({"trade_date": "2026-06-26", "final30_scored": _rows()}))
    result = load_watchlist_from_artifact("2026-06-26")
    assert result[0]["entry_watchlist_source"] == "latest_final30_scored"


def test_latest_trade_date_mismatch_fails(monkeypatch, tmp_path):
    from trader.us.runner.trade_tick_runner import load_watchlist_from_artifact
    monkeypatch.chdir(tmp_path)
    base = Path("reports/us_prep")
    base.mkdir(parents=True)
    (base / "latest_final30_scored.json").write_text(json.dumps({"trade_date": "2026-06-25", "final30_scored": _rows()}))
    with pytest.raises(FileNotFoundError):
        load_watchlist_from_artifact("2026-06-26")


def test_score_final_normalize_and_duplicate_dedupe(monkeypatch, tmp_path):
    from trader.us.runner.trade_tick_runner import load_watchlist_from_artifact
    monkeypatch.chdir(tmp_path)
    path = Path("runtime/us/prep/2026-06-26")
    path.mkdir(parents=True)
    (path / "final30_scored.json").write_text(json.dumps({"final30_scored": _rows(score_key="score_final", duplicate=True)}))
    result = load_watchlist_from_artifact("2026-06-26")
    symbols = [r["symbol"] for r in result]
    assert symbols.count("T00") == 1
    assert result[0]["symbol"] == "T00"
    assert result[0]["score"] == 99.0
