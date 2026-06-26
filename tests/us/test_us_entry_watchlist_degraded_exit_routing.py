# -*- coding: utf-8 -*-
"""US AM tick must route exits even when entry watchlist DB load degrades."""
from __future__ import annotations

import json
from pathlib import Path


def test_exit_intents_timeout_path_routes_sell_instead_of_failed_return():
    text = Path("trader/us/runner/trade_tick_runner.py").read_text(encoding="utf-8")
    start = text.index("except concurrent.futures.TimeoutError:")
    end = text.index("except Exception as exc:", start)
    timeout_block = text[start:end]
    assert "[US_ENTRY][WATCHLIST][LOAD][TIMEOUT]" in timeout_block
    assert "[US_ENTRY][WATCHLIST][FALLBACK_ARTIFACT][START]" in timeout_block
    assert "[US_ENTRY][WATCHLIST][LOAD][DEGRADED_SKIP_ENTRY]" in timeout_block
    assert '"status": "FAILED"' not in timeout_block
    assert "return {" not in timeout_block
    assert "[US_ORDER][ROUTE][EXIT_CONTINUE_AFTER_ENTRY_DEGRADED]" in text


def test_edbhandlerexited_is_transient_and_has_no_early_failed_return():
    from trader.us.runner import trade_tick_runner as runner

    assert runner._is_transient_watchlist_db_error(RuntimeError("EDBHANDLEREXITED"))
    assert runner._is_transient_watchlist_db_error(RuntimeError("connection already closed"))
    text = Path("trader/us/runner/trade_tick_runner.py").read_text(encoding="utf-8")
    assert "_is_transient_watchlist_db_error(exc)" in text
    assert "watchlist_load_transient_db_error" in text


def test_artifact_fallback_loads_30_valid_rows_and_dedupes(tmp_path, monkeypatch):
    from trader.us.runner.trade_tick_runner import load_watchlist_from_artifact

    monkeypatch.chdir(tmp_path)
    path = tmp_path / "runtime/us/watchlist/2026-06-26/final30_scored.json"
    path.parent.mkdir(parents=True)
    rows = [
        {"symbol": f"T{i:02d}", "final_score": i + 1, "exchange": "NYSE" if i % 2 else ""}
        for i in range(30)
    ]
    rows.append({"symbol": "T01", "rank_score": 999})
    path.write_text(json.dumps({"final30_scored": rows}), encoding="utf-8")

    loaded = load_watchlist_from_artifact("2026-06-26")

    assert len(loaded) == 30
    assert loaded[0]["symbol"] == "T01"
    assert loaded[0]["score"] == 999
    assert all(row["strategy"] == "us_pb1" for row in loaded)
    assert all(row["exchange"] for row in loaded)


def test_artifact_missing_or_all_zero_fails_and_tick_degrades_entry_only(tmp_path, monkeypatch):
    from trader.us.runner.trade_tick_runner import load_watchlist_from_artifact

    monkeypatch.chdir(tmp_path)
    path = tmp_path / "signals/us/2026-06-26/final30_scored.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps([{"symbol": f"Z{i:02d}", "score": 0} for i in range(30)]), encoding="utf-8")

    try:
        load_watchlist_from_artifact("2026-06-26")
    except FileNotFoundError as exc:
        assert "artifact_all_scores_zero" in str(exc)
    else:
        raise AssertionError("all-zero artifact fallback must fail")

    text = Path(__file__).resolve().parents[2].joinpath("trader/us/runner/trade_tick_runner.py").read_text(encoding="utf-8")
    assert "entry_intents = []" in text
    assert "exit_intents + entry_intents" in text
    assert "entry_degraded_exit_routed" in text


def test_daily_report_preserves_exit_and_entry_degraded_fields():
    text = Path("trader/us/runner/trade_session_runner.py").read_text(encoding="utf-8")
    assert '"exit_intents_last_tick": int(final_tick.get("exit_intents", 0) or 0)' in text
    assert '"exit_intents_total": total_sell_decisions' in text
    assert '"entry_degraded": int(final_tick.get("entry_degraded", 0) or 0)' in text
    assert '"entry_degraded_reason": final_tick.get("entry_degraded_reason", "")' in text
    assert '"watchlist_fallback_used": int(final_tick.get("watchlist_fallback_used", 0) or 0)' in text
    assert '"exit_routed_after_entry_degraded": int(final_tick.get("exit_routed_after_entry_degraded", 0) or 0)' in text
