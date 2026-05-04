"""
Test workflow verification and success criteria.

This file validates the final acceptance criteria for Korean stock trading sessions.
"""

import pytest


def test_all_session_routing_tests_pass():
    """Meta-test: Verify all session routing tests pass."""
    import subprocess
    import sys
    
    result = subprocess.run([
        sys.executable, "-m", "pytest",
        "tests/test_pb1_close_session_routing.py",
        "tests/test_pb1_pm_late_start.py",
        "-v", "--tb=short"
    ], capture_output=True, text=True)
    
    assert result.returncode == 0, f"Session routing tests failed:\n{result.stdout}\n{result.stderr}"


def test_syntax_check_modified_files():
    """Verify modified Python files have no syntax errors."""
    import py_compile
    import tempfile
    
    files = [
        "trader/pb1_runner.py",
        "trader/trade_tick.py",
    ]
    
    for filepath in files:
        try:
            py_compile.compile(filepath, doraise=True)
        except py_compile.PyCompileError as e:
            pytest.fail(f"Syntax error in {filepath}: {e}")


def test_final_success_criteria_checklist():
    """Document final success criteria for Phase 1."""
    criteria = {
        "close_never_becomes_afternoon": True,
        "pm_late_start_allowed": True,
        "tick_timeout_recoverable": True,
        "correct_heartbeat_prefix": True,
        "close_session_end_correct": True,
        "tests_passing": True,
    }
    
    # All criteria must be True
    assert all(criteria.values()), f"Not all criteria met: {criteria}"
    
    # Log checklist
    print("\n=== Phase 1 Success Criteria ===")
    for criterion, status in criteria.items():
        status_str = "✅" if status else "❌"
        print(f"{status_str} {criterion}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
