import subprocess
import sys
from pathlib import Path

SCRIPT = Path("scripts/us_assert_trade_runner_started.py")


def run_assertion(tmp_path, *, session="am", log_text="", runner_started="0"):
    log = tmp_path / "us.log"
    log.write_text(log_text, encoding="utf-8")
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--session",
            session,
            "--log",
            str(log),
            "--event-name",
            "schedule",
            "--kis-env",
            "practice",
            "--phase-should-run",
            "1",
            "--run-mode",
            "TRADE",
            "--order-allowed",
            "1",
            "--kis-order-allowed",
            "1",
            "--runner-started",
            runner_started,
            "--emit-final-status",
        ],
        text=True,
        capture_output=True,
        check=False,
    ), log


def test_am_runner_not_started_fails(tmp_path):
    result, log = run_assertion(tmp_path, session="am", log_text="[US_TRADE_AM][EXPECTATION] expected_to_trade=1\n")
    assert result.returncode == 1
    assert "FAILED_TRADE_NOT_STARTED" in result.stdout
    assert "[US_WORKFLOW][FINAL_STATUS] status=FAILED_TRADE_NOT_STARTED" in log.read_text(encoding="utf-8")


def test_afternoon_runner_not_started_fails(tmp_path):
    result, log = run_assertion(
        tmp_path,
        session="afternoon",
        log_text="[US_TRADE_AFTERNOON][INTENT_EXPECTATION] trade_intent_expected=1\n",
    )
    assert result.returncode == 1
    assert "FAILED_TRADE_NOT_STARTED" in result.stdout
    assert "[US_WORKFLOW][FINAL_STATUS] status=FAILED_TRADE_NOT_STARTED" in log.read_text(encoding="utf-8")


def test_duplicate_session_is_accepted_skip(tmp_path):
    result, _ = run_assertion(
        tmp_path,
        session="am",
        log_text="[US_TRADE_SESSION_LOCK][SKIP] reason=session_already_running_or_completed trade_date=2026-06-09\n",
    )
    assert result.returncode == 0
    assert "[US_WORKFLOW][FINAL_STATUS] status=SKIPPED_DUPLICATE_SESSION" in result.stdout
    assert "FAILED_TRADE_NOT_STARTED" not in result.stdout


def test_already_ran_is_accepted_skip(tmp_path):
    result, _ = run_assertion(
        tmp_path,
        session="afternoon",
        log_text="[RUN_SUMMARY][RESULT] status=OK reason=already_ran session=us-afternoon\n",
    )
    assert result.returncode == 0
    assert "[US_WORKFLOW][FINAL_STATUS] status=OK_ALREADY_RAN" in result.stdout
    assert "FAILED_TRADE_NOT_STARTED" not in result.stdout


def test_runner_start_marker_passes(tmp_path):
    result, _ = run_assertion(tmp_path, session="am", log_text="[US_TRADE_RUNNER][START] session=am\n")
    assert result.returncode == 0
    assert "FAILED_TRADE_NOT_STARTED" not in result.stdout
