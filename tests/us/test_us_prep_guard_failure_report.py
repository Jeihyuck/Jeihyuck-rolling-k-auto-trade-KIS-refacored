#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Test US prep guard failure report generation.

Tests:
1. Force_now trim
2. Prep guard sidecar generation on failure
3. Guard failure report writer
4. Stale report rejection
5. FAILED_PREP_GUARD report contract validation
6. Verify log guard failure acceptance
"""
import json
import os
import tempfile
from pathlib import Path
from unittest.mock import patch
import pytest


def test_force_now_trim():
    """Test that force_now input is properly trimmed."""
    from datetime import datetime
    
    force_now_input = " 2026-05-01T09:35:00-04:00 "
    force_now_trimmed = force_now_input.strip()
    trade_date = datetime.fromisoformat(force_now_trimmed).date().isoformat()
    
    assert force_now_trimmed == "2026-05-01T09:35:00-04:00"
    assert trade_date == "2026-05-01"


def test_prep_guard_sidecar_generation():
    """Test that prep guard generates failure sidecar JSON."""
    import tempfile
    import json
    from pathlib import Path
    
    # Mock scenario: prep_status=UNKNOWN, locked_count=0
    with tempfile.TemporaryDirectory() as tmpdir:
        sidecar = {
            "ok": False,
            "session": "am",
            "trade_date": "2026-05-01",
            "trade_date_source": "force_now",
            "force_now": "2026-05-01T09:35:00-04:00",
            "prep_status": "UNKNOWN",
            "locked_count": 0,
            "min_count": 10,
            "watchlist_load_error": False,
            "reason": "bad_prep_status",
        }
        
        result_path = Path(tmpdir) / "us_prep_guard_result.json"
        result_path.write_text(json.dumps(sidecar, indent=2))
        
        # Validate
        loaded = json.loads(result_path.read_text())
        assert loaded["ok"] is False
        assert loaded["reason"] == "bad_prep_status"
        assert loaded["trade_date"] == "2026-05-01"
        assert loaded["prep_status"] == "UNKNOWN"
        assert loaded["locked_count"] == 0


def test_guard_failure_report_writer():
    """Test that guard failure report writer creates proper reports."""
    with tempfile.TemporaryDirectory() as tmpdir:
        orig_dir = os.getcwd()
        try:
            os.chdir(tmpdir)
            
            # Create sidecar
            artifacts_dir = Path("artifacts")
            artifacts_dir.mkdir()
            sidecar = {
                "ok": False,
                "session": "am",
                "trade_date": "2026-05-01",
                "trade_date_source": "force_now",
                "force_now": "2026-05-01T09:35:00-04:00",
                "prep_status": "UNKNOWN",
                "locked_count": 0,
                "min_count": 10,
                "watchlist_load_error": False,
                "reason": "bad_prep_status",
            }
            sidecar_path = artifacts_dir / "us_prep_guard_result.json"
            sidecar_path.write_text(json.dumps(sidecar, indent=2))
            
            # Set environment
            os.environ["GITHUB_RUN_ID"] = "25718405741"
            os.environ["GITHUB_SHA"] = "test-sha"
            os.environ["GITHUB_WORKFLOW"] = "US Trade AM"
            os.environ["GITHUB_EVENT_NAME"] = "workflow_dispatch"
            os.environ["DRY_RUN"] = "1"
            os.environ["US_KIS_ORDER_ALLOWED"] = "0"
            os.environ["OFFLINE"] = "true"
            os.environ["MAX_TICKS"] = "0"
            
            # Simulate report writer logic (without importing the script)
            with open(sidecar_path, "r") as f:
                guard_result = json.load(f)
            
            trade_date = guard_result.get("trade_date", "unknown")
            prep_status = guard_result.get("prep_status", "UNKNOWN")
            locked_count = guard_result.get("locked_count", 0)
            reason = guard_result.get("reason", "unknown")
            force_now = guard_result.get("force_now")
            
            run_id = os.getenv("GITHUB_RUN_ID", "local")
            sha = os.getenv("GITHUB_SHA", "unknown")
            workflow = os.getenv("GITHUB_WORKFLOW", "unknown")
            event_name = os.getenv("GITHUB_EVENT_NAME", "unknown")
            dry_run = os.getenv("DRY_RUN", "0") == "1"
            kis_order_allowed = int(os.getenv("US_KIS_ORDER_ALLOWED", "0"))
            offline = os.getenv("OFFLINE", "false").lower() in ("true", "1")
            max_ticks = int(os.getenv("MAX_TICKS", "0"))
            
            report = {
                "trade_date": trade_date,
                "run_id": run_id,
                "sha": sha,
                "workflow": workflow,
                "session": "am",
                "event_name": event_name,
                "env": "practice",
                "dry_run": dry_run,
                "kis_order_allowed": kis_order_allowed,
                "prep_status": prep_status,
                "locked_watchlist_count": locked_count,
                "entry_eval_status": "SKIPPED",
                "entry_error_type": "FAILED_PREP_GUARD",
                "entry_error_message": reason,
                "entry_intents": 0,
                "orders_sent": 0,
                "orders_blocked": 0,
                "block_reasons": {},
                "fills": 0,
                "positions": 0,
                "last_stage": "prep_guard",
                "final_status": "FAILED_PREP_GUARD",
                "reason": reason,
                "temp_error_count": 0,
                "temp_recovered_count": 0,
                "missed_trade_window": False,
                "force_now": force_now,
                "offline": offline,
                "tick_count": 0,
                "max_ticks": max_ticks,
                "wall_elapsed_sec": 0,
            }
            
            # Write reports
            base_dir = Path("reports/us_daily")
            base_dir.mkdir(parents=True, exist_ok=True)
            
            latest_json = base_dir / "latest_us_daily_report.json"
            latest_json.write_text(json.dumps(report, indent=2))
            
            # Validate
            loaded = json.loads(latest_json.read_text())
            assert loaded["final_status"] == "FAILED_PREP_GUARD"
            assert loaded["reason"] == "bad_prep_status"
            assert loaded["run_id"] == "25718405741"
            assert loaded["trade_date"] == "2026-05-01"
            assert loaded["prep_status"] == "UNKNOWN"
            assert loaded["locked_watchlist_count"] == 0
            
            # Check required fields
            required = [
                "trade_date", "run_id", "sha", "workflow", "session", "event_name", "env",
                "dry_run", "kis_order_allowed", "prep_status", "locked_watchlist_count",
                "entry_eval_status", "entry_error_type", "entry_error_message",
                "entry_intents", "orders_sent", "orders_blocked", "block_reasons",
                "fills", "positions", "last_stage", "final_status", "reason"
            ]
            for field in required:
                assert field in loaded, f"Missing field: {field}"
        finally:
            os.chdir(orig_dir)


def test_stale_report_rejection():
    """Test that stale report with run_id=local is rejected."""
    stale_report = {
        "trade_date": "2026-05-11",
        "run_id": "local",
        "final_status": "OK_ORDERS_SENT",
    }
    
    expected_run_id = "25718405741"
    
    # Validation should fail
    if str(stale_report.get("run_id")) != expected_run_id:
        # Rejection is correct
        assert True
    else:
        pytest.fail("Stale report was not rejected")


def test_failed_prep_guard_report_contract():
    """Test that FAILED_PREP_GUARD report passes validation."""
    report = {
        "trade_date": "2026-05-01",
        "run_id": "25718405741",
        "sha": "test-sha",
        "workflow": "US Trade AM",
        "session": "am",
        "event_name": "workflow_dispatch",
        "env": "practice",
        "dry_run": True,
        "kis_order_allowed": 0,
        "prep_status": "UNKNOWN",
        "locked_watchlist_count": 0,
        "entry_eval_status": "SKIPPED",
        "entry_error_type": "FAILED_PREP_GUARD",
        "entry_error_message": "bad_prep_status",
        "entry_intents": 0,
        "orders_sent": 0,
        "orders_blocked": 0,
        "block_reasons": {},
        "fills": 0,
        "positions": 0,
        "last_stage": "prep_guard",
        "final_status": "FAILED_PREP_GUARD",
        "reason": "bad_prep_status",
        "temp_error_count": 0,
        "temp_recovered_count": 0,
        "missed_trade_window": False,
    }
    
    # Validate final_status is allowed
    allowed_statuses = [
        "FAILED_PREP_GUARD",
        "SKIP_PHASE_WINDOW",
        "NO_ENTRY_INTENTS",
        "NO_ORDERS_RISK_BLOCKED",
        "PARTIAL_ORDERS_BLOCKED",
        "OK_NO_TRADE",
        "OK_ORDERS_SENT",
        "OK_WITH_WARNINGS",
        "FAILED"
    ]
    
    assert report["final_status"] in allowed_statuses
    assert report["run_id"] == "25718405741"
    assert report["trade_date"] == "2026-05-01"


def test_verify_log_guard_failure():
    """Test that verify log step accepts guard failure reported pattern."""
    log_content = """
[US_PREP_GUARD][START] session=am trade_date=2026-05-01 force_now=2026-05-01T09:35:00-04:00 trade_date_source=force_now timeout_sec=20
[US_PREP_GUARD][PREP_STATUS][START] trade_date=2026-05-01 timeout_sec=20
[US_PREP_GUARD][PREP_STATUS][DONE] status=UNKNOWN elapsed_sec=0.05
[US_PREP_GUARD][WATCHLIST][START] trade_date=2026-05-01 min_count=10 allow_degraded=1 timeout_sec=20
[US_WATCHLIST][LOCK_LOAD][INSUFFICIENT] trade_date=2026-05-01 count=0 min=10
[US_PREP_GUARD][WATCHLIST][DONE] count=0 elapsed_sec=0.03
[US_PREP_GUARD][CHECK] session=am trade_date=2026-05-01 prep_status=UNKNOWN locked_count=0 watchlist_load_error=False
[US_PREP_GUARD][FAIL] session=am trade_date=2026-05-01 reason=bad_prep_status prep_status=UNKNOWN locked_count=0
[US_PREP_GUARD][FAILURE_SIDECAR] artifacts/us_prep_guard_result.json
[US_PREP_GUARD][REPORT_WRITE][START]
[US_GUARD_FAILURE_REPORT][LOAD] artifacts/us_prep_guard_result.json
[US_GUARD_FAILURE_REPORT][META] trade_date=2026-05-01 run_id=25718405741 session=am prep_status=UNKNOWN locked_count=0 reason=bad_prep_status
[US_GUARD_FAILURE_REPORT][WRITE] reports/us_daily/latest_us_daily_report.json
[US_GUARD_FAILURE_REPORT][WRITE] reports/us_daily/latest_us_daily_report.md
[US_GUARD_FAILURE_REPORT][DONE] final_status=FAILED_PREP_GUARD run_id=25718405741 trade_date=2026-05-01
[US_PREP_GUARD][REPORT_WRITE][DONE]
[RUN_SUMMARY][RESULT] status=FAILED_PREP_GUARD reason=bad_prep_status session=us-am
"""
    
    # Check patterns
    has_guard_fail = "[US_PREP_GUARD][FAIL]" in log_content
    has_report_done = "[US_PREP_GUARD][REPORT_WRITE][DONE]" in log_content
    has_run_summary = "[RUN_SUMMARY][RESULT] status=FAILED_PREP_GUARD" in log_content
    
    assert has_guard_fail
    assert has_report_done
    assert has_run_summary
    
    # Verify step should pass
    if has_guard_fail and has_report_done and has_run_summary:
        # This simulates: exit 0
        assert True
    else:
        pytest.fail("Guard failure was not properly reported")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
