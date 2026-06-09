from __future__ import annotations

import os
from datetime import datetime
from zoneinfo import ZoneInfo

from trader import pb1_runner
from trader.diagnostics.run_validator import validate_run_pipeline
from trader.pb1_engine import resolve_pb1_phase


def test_afternoon_1341_resolves_pm_entry(monkeypatch):
    monkeypatch.setenv("FORCE_MARKET_WINDOW", "afternoon")
    monkeypatch.delenv("PB1_EXIT_ONLY_MODE", raising=False)
    phase, reason, _ = resolve_pb1_phase(datetime(2026, 6, 9, 13, 41, tzinfo=ZoneInfo("Asia/Seoul")), True, "exit")
    assert phase == "pm_entry"
    assert reason == "afternoon_entry_allowed"


def test_afternoon_1512_resolves_close(monkeypatch):
    monkeypatch.setenv("FORCE_MARKET_WINDOW", "afternoon")
    monkeypatch.delenv("PB1_EXIT_ONLY_MODE", raising=False)
    phase, reason, _ = resolve_pb1_phase(datetime(2026, 6, 9, 15, 12, tzinfo=ZoneInfo("Asia/Seoul")), True, "pm_entry")
    assert phase == "close"
    assert reason == "close_window"


def test_exit_only_mode_resolves_exit(monkeypatch):
    monkeypatch.setenv("FORCE_MARKET_WINDOW", "afternoon")
    monkeypatch.setenv("PB1_EXIT_ONLY_MODE", "1")
    phase, reason, _ = resolve_pb1_phase(datetime(2026, 6, 9, 13, 41, tzinfo=ZoneInfo("Asia/Seoul")), True, "pm_entry")
    assert phase == "exit"
    assert reason == "exit_only_mode"


def test_runner_normalize_keeps_pm_entry(monkeypatch):
    monkeypatch.setenv("PB1_SESSION_KIND", "afternoon")
    _window, phase = pb1_runner._normalize_window_phase(raw_window="day", market_window="afternoon", phase="pm_entry")
    assert phase == "pm_entry"


def test_mode_apply_sets_run_ctx_and_env(monkeypatch):
    run_ctx = {}
    result = pb1_runner._apply_afternoon_mode_decision(run_ctx=run_ctx, entry_enabled=True, exit_only=False)
    assert result["phase"] == "pm_entry"
    assert run_ctx["phase_name"] == "pm_entry"
    assert run_ctx["entry_enabled"] is True
    assert run_ctx["exit_only"] is False
    assert os.environ["PB1_EXIT_ONLY_MODE"] == "0"
    assert os.environ["FORCE_PB1_PHASE"] == "pm_entry"


def test_run_validator_errors_when_entry_pipeline_missing(tmp_path, caplog):
    with caplog.at_level("ERROR"):
        payload = validate_run_pipeline(
            session_kind="afternoon",
            phase="pm_entry",
            final30_rows=30,
            entry_enabled=True,
            exit_only=False,
            mode_applied=True,
            entry_decision_seen=False,
            setup_ok_seen=False,
            order_candidates_seen=False,
            buyable_gate_seen=False,
            order_submit_seen=False,
            now=datetime(2026, 6, 9, 13, 41, tzinfo=ZoneInfo("Asia/Seoul")),
            artifact_dir=tmp_path,
        )
    assert payload["status"] == "ERROR"
    assert (tmp_path / "run_validation_summary.json").exists()
