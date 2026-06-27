from __future__ import annotations

import json


def test_report_provenance_prefers_github_env(monkeypatch):
    from trader.us.runner.daily_report_runner import _report_provenance
    monkeypatch.setenv("GITHUB_REF_NAME", "dual-agent")
    monkeypatch.setenv("GITHUB_SHA", "abc123")
    monkeypatch.setenv("GITHUB_WORKFLOW", "wf")
    monkeypatch.setenv("GITHUB_RUN_ID", "42")
    p = _report_provenance("am")
    assert p["branch"] == "dual-agent"
    assert p["commit_sha"] == "abc123"
    assert p["workflow"] == "wf"
    assert p["run_id"] == "42"


def test_report_provenance_uses_git_fallback(monkeypatch):
    from trader.us.runner import daily_report_runner
    for key in ["GITHUB_REF_NAME", "GITHUB_SHA", "GITHUB_WORKFLOW", "GITHUB_RUN_ID"]:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(daily_report_runner, "_git_value", lambda args: "dual-agent" if "--abbrev-ref" in args else "deadbeef")
    p = daily_report_runner._report_provenance("am")
    assert p["branch"] == "dual-agent"
    assert p["commit_sha"] == "deadbeef"
    assert p["workflow"] == "local"
    assert p["code_version_source"] == "git_fallback"
    assert all(str(p[k]) for k in ["branch", "commit_sha", "workflow", "run_id"])


def test_report_provenance_unknown_when_git_fallback_fails(monkeypatch):
    from trader.us.runner import daily_report_runner
    for key in ["GITHUB_REF_NAME", "GITHUB_SHA", "GITHUB_WORKFLOW", "GITHUB_RUN_ID"]:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(daily_report_runner, "_git_value", lambda args: "")
    p = daily_report_runner._report_provenance("am")
    assert p["branch"] == "unknown"
    assert p["commit_sha"] == "unknown"
    assert p["workflow"] == "local"


def test_schedule_health_writes_provenance(monkeypatch, tmp_path):
    from trader.us.runner import trade_session_runner
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GITHUB_REF_NAME", "dual-agent")
    monkeypatch.setenv("GITHUB_SHA", "abc123")
    monkeypatch.setenv("GITHUB_WORKFLOW", "wf")
    monkeypatch.setenv("GITHUB_RUN_ID", "42")
    trade_session_runner._write_us_schedule_health({
        "trade_date": "2026-06-26",
        "final_status": "OK_WITH_WARNINGS",
        "run_id": "42",
        "entry_degraded": 1,
        "entry_degraded_reason": "watchlist_load_timeout",
        "entry_watchlist_source": "none",
        "watchlist_fallback_used": 0,
        "exit_routed_before_entry": 1,
        "buy_notional_routed": 0,
        "sell_notional_routed": 2000,
        "total_order_notional_routed": 2000,
        "ack_reconcile_before_route_status": "OK",
        "ack_reconcile_after_route_status": "OK",
        "ack_reconcile_after_route_unresolved_count": 0,
        "ack_pending_reconcile_count": 0,
        "pending_order_count": 0,
    }, "am")
    payload = json.loads((tmp_path / "reports/us_schedule_health/2026-06-26.json").read_text())
    for key in ["branch", "commit_sha", "workflow", "github_run_id", "run_id", "code_version_source"]:
        assert payload.get(key)
        assert payload["sessions"]["am"].get(key)
    session = payload["sessions"]["am"]
    for key in [
        "entry_degraded", "entry_degraded_reason", "entry_watchlist_source", "watchlist_fallback_used",
        "exit_routed_before_entry", "buy_notional_routed", "sell_notional_routed",
        "total_order_notional_routed", "ack_reconcile_before_route_status",
        "ack_reconcile_after_route_status", "ack_reconcile_after_route_unresolved_count",
        "ack_pending_reconcile_count", "pending_order_count",
    ]:
        assert key in session
