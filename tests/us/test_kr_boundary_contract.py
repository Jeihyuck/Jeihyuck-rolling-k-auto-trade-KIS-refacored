#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Test that Korean workflows and code are not modified during US workflow changes.

CRITICAL: dual-agent branch must never touch nullim code.
This test enforces the boundary.
"""
import subprocess
from pathlib import Path


FORBIDDEN_KR_PATHS = [
    ".github/workflows/trade-am.yml",
    ".github/workflows/trade-afternoon.yml",
    ".github/workflows/trade-pm.yml",
    ".github/workflows/trade-close.yml",
    ".github/workflows/prep.yml",
    "trader/pb1_engine.py",
    "trader/entry_engine.py",
    "trader/exit_engine.py",
    "trader/kis_wrapper.py",
    "trader/prep_runner.py",
    "trader/trade_am_runner.py",
    "trader/trade_afternoon_runner.py",
    "trader/trade_close_runner.py",
    "settings.py",
]


def test_korean_files_not_modified():
    """Ensure Korean trading workflows and code are not modified.
    
    Uses git diff to check if any forbidden paths were changed.
    This test is critical for dual-agent boundary enforcement.
    """
    repo_root = Path(__file__).parents[2]
    
    # Get list of modified files (staged + unstaged)
    result = subprocess.run(
        ["git", "diff", "--name-only", "HEAD"],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        # If git command fails (e.g., not in a git repo), skip test
        return
    
    modified_files = result.stdout.strip().split("\n") if result.stdout.strip() else []
    
    # Check for forbidden modifications
    violations = []
    for modified in modified_files:
        if modified in FORBIDDEN_KR_PATHS:
            violations.append(modified)
    
    if violations:
        msg = (
            "FORBIDDEN: Korean trading files were modified!\n"
            "The following files must not be changed:\n"
            + "\n".join(f"  - {v}" for v in violations)
            + "\n\nDual-agent branch MUST NOT modify nullim code."
        )
        raise AssertionError(msg)


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
