# -*- coding: utf-8 -*-
"""Tests for US prep guard timeout behavior.

Tests that guard_us_prep_contract.py fails fast (within timeout + 5sec)
when DB operations hang or exceed timeout.
"""
import os
import subprocess
import sys
import time
from unittest.mock import patch, MagicMock


def test_timeout_guard_utility():
    """Test the timeout_guard utility returns correct structure."""
    from trader.us.utils.timeout_guard import run_with_timeout
    
    # Fast function
    result = run_with_timeout(
        fn=lambda: {"status": "OK"},
        stage="test_fast",
        timeout_sec=2,
    )
    assert result["ok"] is True
    assert result["value"] == {"status": "OK"}
    assert result["timeout"] is False
    assert result["elapsed_sec"] < 1.0
    
    # Timeout function
    result = run_with_timeout(
        fn=lambda: time.sleep(10),
        stage="test_timeout",
        timeout_sec=1,
    )
    assert result["ok"] is False
    assert result["value"] is None
    assert result["timeout"] is True
    assert result["error"] == "timeout after 1s"
    # Should return within timeout + 5 sec
    assert result["elapsed_sec"] < 6.0
    
    # Exception function
    def raise_error():
        raise ValueError("test error")
    
    result = run_with_timeout(
        fn=raise_error,
        stage="test_error",
        timeout_sec=2,
    )
    assert result["ok"] is False
    assert result["timeout"] is False
    assert "test error" in result["error"]


def test_guard_prints_start_before_db_calls(tmp_path, monkeypatch):
    """Guard must print START log before calling DB functions."""
    # Mock DB functions to delay
    def slow_prep_status(trade_date, timeout_sec=20):
        time.sleep(0.1)
        return {"status": "OK"}
    
    def slow_watchlist(trade_date, min_count, allow_degraded, timeout_sec=20):
        time.sleep(0.1)
        return [{"symbol": f"SYM{i}"} for i in range(15)]
    
    with patch("trader.us.db.repos.load_latest_us_prep_status", side_effect=slow_prep_status), \
         patch("trader.us.db.repos.load_locked_us_watchlist", side_effect=slow_watchlist):
        
        script_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "scripts", "guard_us_prep_contract.py"
        )
        # Change to repo root
        repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        
        env = os.environ.copy()
        env.update({
            "SESSION": "am",
            "FORCE_NOW_INPUT": "2026-05-07T09:35:00-04:00",
            "US_PREP_GUARD_TIMEOUT_SEC": "3",
            "PBCORE_DB_URL": "",  # Force in-memory mode
        })
        
        proc = subprocess.run(
            [sys.executable, script_path],
            cwd=repo_root,
            capture_output=True,
            text=True,
            env=env,
            timeout=10,
        )
        
        output = proc.stdout + proc.stderr
        
        # START should appear first
        assert "[US_PREP_GUARD][START]" in output
        lines = output.split("\n")
        start_idx = next(i for i, line in enumerate(lines) if "[US_PREP_GUARD][START]" in line)
        
        # START should contain all key info
        start_line = lines[start_idx]
        assert "session=am" in start_line
        assert "trade_date=2026-05-07" in start_line
        assert "trade_date_source=force_now" in start_line
        assert "timeout_sec=3" in start_line


def test_force_now_trade_date_source():
    """With FORCE_NOW_INPUT, trade_date_source should be force_now."""
    script_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "scripts", "guard_us_prep_contract.py"
    )
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    
    env = os.environ.copy()
    env.update({
        "SESSION": "am",
        "FORCE_NOW_INPUT": "2026-05-07T09:35:00-04:00",
        "US_PREP_GUARD_TIMEOUT_SEC": "2",
        "PBCORE_DB_URL": "",  # Force in-memory mode
    })
    
    proc = subprocess.run(
        [sys.executable, script_path],
        cwd=repo_root,
        capture_output=True,
        text=True,
        env=env,
        timeout=10,
    )
    
    output = proc.stdout + proc.stderr
    assert "trade_date=2026-05-07" in output
    assert "trade_date_source=force_now" in output


def test_actual_today_trade_date_source():
    """Without FORCE_NOW_INPUT, trade_date_source should be actual_ny_today."""
    script_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "scripts", "guard_us_prep_contract.py"
    )
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    
    env = os.environ.copy()
    env.update({
        "SESSION": "am",
        "FORCE_NOW_INPUT": "",
        "US_PREP_GUARD_TIMEOUT_SEC": "2",
        "PBCORE_DB_URL": "",  # Force in-memory mode
    })
    
    proc = subprocess.run(
        [sys.executable, script_path],
        cwd=repo_root,
        capture_output=True,
        text=True,
        env=env,
        timeout=10,
    )
    
    output = proc.stdout + proc.stderr
    assert "trade_date_source=actual_ny_today" in output


def test_prep_status_timeout_fails_fast():
    """When prep status load times out, guard should fail within timeout + 5 sec."""
    import threading
    
    def delayed_prep_status(trade_date, timeout_sec=20):
        """Simulate hanging DB call."""
        time.sleep(30)  # Sleep longer than timeout
        return {"status": "OK"}
    
    with patch("trader.us.db.repos.load_latest_us_prep_status", side_effect=delayed_prep_status):
        script_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "scripts", "guard_us_prep_contract.py"
        )
        repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        
        env = os.environ.copy()
        env.update({
            "SESSION": "am",
            "FORCE_NOW_INPUT": "2026-05-07T09:35:00-04:00",
            "US_PREP_GUARD_TIMEOUT_SEC": "1",
            "PBCORE_DB_URL": "postgresql://dummy",  # Force DB mode
        })
        
        start = time.monotonic()
        proc = subprocess.run(
            [sys.executable, script_path],
            cwd=repo_root,
            capture_output=True,
            text=True,
            env=env,
            timeout=10,  # subprocess timeout as safety
        )
        elapsed = time.monotonic() - start
        
        # Should fail fast (within 1 + 5 = 6 sec)
        assert elapsed < 6.0, f"Guard took {elapsed:.1f}s, expected < 6s"
        
        # Should exit with error
        assert proc.returncode == 1
        
        output = proc.stdout + proc.stderr
        assert "[US_PREP_GUARD][FAIL]" in output
        assert "reason=prep_status_timeout" in output or "[US_TIMEOUT_GUARD][TIMEOUT]" in output


def test_watchlist_timeout_fails_fast():
    """When watchlist load times out, guard should fail within timeout + 5 sec."""
    def ok_prep_status(trade_date, timeout_sec=20):
        return {"status": "OK"}
    
    def delayed_watchlist(trade_date, min_count, allow_degraded, timeout_sec=20):
        """Simulate hanging DB call."""
        time.sleep(30)
        return [{"symbol": f"SYM{i}"} for i in range(15)]
    
    with patch("trader.us.db.repos.load_latest_us_prep_status", side_effect=ok_prep_status), \
         patch("trader.us.db.repos.load_locked_us_watchlist", side_effect=delayed_watchlist):
        
        script_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "scripts", "guard_us_prep_contract.py"
        )
        repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        
        env = os.environ.copy()
        env.update({
            "SESSION": "am",
            "FORCE_NOW_INPUT": "2026-05-07T09:35:00-04:00",
            "US_PREP_GUARD_TIMEOUT_SEC": "1",
            "PBCORE_DB_URL": "postgresql://dummy",  # Force DB mode
        })
        
        start = time.monotonic()
        proc = subprocess.run(
            [sys.executable, script_path],
            cwd=repo_root,
            capture_output=True,
            text=True,
            env=env,
            timeout=10,
        )
        elapsed = time.monotonic() - start
        
        # Should fail fast (within 1 + 5 = 6 sec, but prep_status will take ~1s too, so < 12s)
        assert elapsed < 12.0, f"Guard took {elapsed:.1f}s, expected < 12s"
        
        # Should exit with error
        assert proc.returncode == 1
        
        output = proc.stdout + proc.stderr
        assert "[US_PREP_GUARD][FAIL]" in output
        assert "reason=watchlist_load_timeout" in output or "[US_TIMEOUT_GUARD][TIMEOUT]" in output
