"""US AM/Afternoon schedule-only trade workflow contract tests.

These tests intentionally stay at workflow/validator contract level so they can run
without GitHub Actions, KIS credentials, or a live database.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from scripts.validate_us_daily_report import validate_report

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_DIR = REPO_ROOT / ".github" / "workflows"


def _read_workflow(name: str) -> str:
    return (WORKFLOW_DIR / name).read_text(encoding="utf-8")


def _load_workflow(name: str) -> dict:
    return yaml.safe_load(_read_workflow(name))


def _schedule_entries(name: str) -> list[dict]:
    data = _load_workflow(name)
    return data["on"]["schedule"]


@pytest.mark.parametrize(
    ("workflow", "expected_cron"),
    [
        ("us-trade-am.yml", "30 9 * * 1-5"),
        ("us-trade-afternoon.yml", "03 13 * * 1-5"),
    ],
)
def test_workflow_yaml_syntax_and_timezone_schedule(workflow: str, expected_cron: str) -> None:
    """Workflow YAML parses and schedule is NY-timezone based, not UTC hard-coded."""
    entries = _schedule_entries(workflow)

    assert len(entries) == 1
    assert entries[0]["cron"] == expected_cron
    assert entries[0]["timezone"] == "America/New_York"


@pytest.mark.parametrize(
    ("workflow", "forbidden"),
    [
        ("us-trade-am.yml", "30 13 * * 1-5"),
        ("us-trade-afternoon.yml", "00 17 * * 1-5"),
    ],
)
def test_us_trade_schedules_do_not_use_utc_dst_hardcoding(workflow: str, forbidden: str) -> None:
    text = _read_workflow(workflow)

    assert forbidden not in text


def test_am_schedule_context_smoke_contract() -> None:
    """AM schedule context should enter the 09:30~12:30 ET trade window."""
    text = _read_workflow("us-trade-am.yml")

    assert 'cron: "30 9 * * 1-5"' in text
    assert 'timezone: "America/New_York"' in text
    assert "scheduled_trigger_et=0930" in text
    assert "session_window=0930-1230" in text
    assert "target_min=$((9 * 60 + 30))" in text
    assert "schedule_expected_min=$((9 * 60 + 30))" in text
    assert "[US_TRADE_RUNNER][START] session=am" in text
    assert "[US_TRADE_RUNNER][DONE] session=am" in text


def test_afternoon_schedule_context_smoke_contract() -> None:
    """Afternoon schedule trigger is offset to 13:03 ET while window remains 13:00~15:50 ET."""
    text = _read_workflow("us-trade-afternoon.yml")

    assert 'cron: "03 13 * * 1-5"' in text
    assert 'timezone: "America/New_York"' in text
    assert "scheduled_trigger_et=1303" in text
    assert "session_window=1300-1550" in text
    assert "target_min=$((13 * 60 + 0))" in text
    assert "schedule_expected_min=$((13 * 60 + 3))" in text
    assert "[US_TRADE_RUNNER][START] session=afternoon" in text
    assert "[US_TRADE_RUNNER][DONE] session=afternoon" in text


@pytest.mark.parametrize(
    ("workflow", "manual_confirm", "trade_confirm"),
    [
        ("us-trade-am.yml", "RUN_US_AM_MANUAL", "RUN_US_AM_TRADE_MANUAL"),
        (
            "us-trade-afternoon.yml",
            "RUN_US_AFTERNOON_MANUAL",
            "RUN_US_AFTERNOON_TRADE_MANUAL",
        ),
    ],
)
def test_workflow_dispatch_manual_context_smoke_contract(
    workflow: str,
    manual_confirm: str,
    trade_confirm: str,
) -> None:
    """Manual workflow_dispatch remains smoke/debug gated by explicit confirmation."""
    data = _load_workflow(workflow)
    text = _read_workflow(workflow)
    inputs = data["on"]["workflow_dispatch"]["inputs"]

    assert inputs["dry_run"]["default"] is True
    assert inputs["confirm"]["default"] == ""
    assert manual_confirm in text
    assert trade_confirm in text
    assert "manual_" in text and "_not_confirmed" in text


@pytest.mark.parametrize(
    ("workflow", "log_marker"),
    [
        (
            "us-trade-am.yml",
            "[US_TRADE_AM][DISPATCH_GUARD][SKIP] reason=auto_dispatch_disabled actor=${GITHUB_ACTOR:-}",
        ),
        (
            "us-trade-afternoon.yml",
            "[US_TRADE_AFTERNOON][DISPATCH_GUARD][SKIP] reason=auto_dispatch_disabled actor=${GITHUB_ACTOR:-}",
        ),
    ],
)
def test_auto_dispatch_bot_actor_skip_contract(workflow: str, log_marker: str) -> None:
    text = _read_workflow(workflow)

    assert '"workflow_dispatch" && "${GITHUB_ACTOR:-}" == "github-actions[bot]"' in text
    assert "skip_reason=auto_dispatch_disabled" in text
    assert log_marker in text


def test_watchdog_is_monitor_only_for_trade_workflows() -> None:
    text = _read_workflow("us-trade-watchdog.yml")

    assert "actions: read" in text
    assert "createWorkflowDispatch" not in text
    assert "[US_WATCHDOG][DISPATCH_DISABLED]" in text


def test_validate_us_daily_report_fatal_when_expected_trade_runner_not_started(tmp_path: Path) -> None:
    report = tmp_path / "latest_us_daily_report.json"
    report.write_text(
        json.dumps(
            {
                "trade_date": "2026-06-08",
                "run_id": "12345",
                "session": "am",
                "env": "practice",
                "dry_run": False,
                "final_status": "OK",
                "last_stage": "init",
                "expected_to_trade": 1,
                "trade_runner_started": 0,
                "trade_status": "INIT",
                "trade_runner_block_reason": "not_started",
                "orders_sent": 0,
            }
        ),
        encoding="utf-8",
    )

    exit_code, fatals, warnings = validate_report(
        report_path=str(report),
        expected_trade_date="2026-06-08",
        expected_run_id="12345",
        expected_dry_run=False,
        session="am",
    )

    assert exit_code == 1
    assert any("trade_runner_not_started" in fatal for fatal in fatals)
    assert warnings
