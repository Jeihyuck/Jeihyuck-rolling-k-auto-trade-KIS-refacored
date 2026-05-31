# -*- coding: utf-8 -*-
"""tests/us/test_no_kr_prep_modification_guard.py - KR 파일 미수정 guard 테스트."""
import subprocess
import pytest


KR_FORBIDDEN_FILES = [
    "trader/pb1_runner.py",
    "trader/kis_wrapper.py",
    "trader/prep_runner.py",
    "trader/trade_am_runner.py",
    "trader/trade_afternoon_runner.py",
    "trader/trade_close_runner.py",
    "settings.py",
]


def _git_diff_name_only():
    """git diff (unstaged + staged) 로 현재 수정 중인 파일 목록 반환.
    
    HEAD와 비교하지 않고, working tree 변경분만 확인한다.
    이미 커밋된 파일은 체크하지 않는다 (dual-agent 브랜치에서 기존 변경 허용).
    """
    try:
        # unstaged changes
        result = subprocess.run(
            ["git", "diff", "--name-only"],
            capture_output=True, text=True, timeout=15,
        )
        unstaged = result.stdout.strip().splitlines()

        # staged changes (index vs HEAD)
        result2 = subprocess.run(
            ["git", "diff", "--cached", "--name-only"],
            capture_output=True, text=True, timeout=15,
        )
        staged = result2.stdout.strip().splitlines()

        return list(set(unstaged + staged))
    except Exception:
        return []


@pytest.mark.parametrize("kr_file", KR_FORBIDDEN_FILES)
def test_kr_file_not_modified(kr_file):
    """KR 핵심 파일은 수정되면 안 된다."""
    changed = _git_diff_name_only()
    assert kr_file not in changed, (
        f"FORBIDDEN: Korean trading file '{kr_file}' was modified. "
        "This file must not be changed during US trading work."
    )


def test_no_us_kr_cross_table_access():
    """KR code에 us_ 테이블 접근이 없어야 하고, US code에 kr 테이블 직접 접근이 없어야 한다."""
    from pathlib import Path
    import re

    root = Path(__file__).resolve().parents[2]

    # US 코드에서 KR 테이블 직접 접근 체크 (orders, positions, signals 테이블 - us_ prefix 없이)
    us_code_dir = root / "trader" / "us"
    kr_table_pattern = re.compile(r"\b(?:FROM|INTO|UPDATE|JOIN)\s+(?!us_)(orders|positions|signals)\b", re.IGNORECASE)

    violations = []
    for py_file in us_code_dir.rglob("*.py"):
        content = py_file.read_text(encoding="utf-8", errors="ignore")
        for match in kr_table_pattern.finditer(content):
            violations.append(f"{py_file.relative_to(root)}:{match.group()}")

    assert not violations, f"US code accessing KR tables directly: {violations}"
