# -*- coding: utf-8 -*-
"""AM workflow가 11:30~12:30 ET 지연 시작을 stale로 막지 않는지 확인한다."""
from __future__ import annotations

import re
from pathlib import Path

WORKFLOW_PATH = Path(__file__).parents[2] / ".github" / "workflows" / "us-trade-am.yml"


def _load_workflow() -> str:
    return WORKFLOW_PATH.read_text(encoding="utf-8")


def test_recovery_until_is_1230():
    """recovery_until이 12:30(= 12 * 60 + 30 = 750)이어야 한다."""
    src = _load_workflow()
    # recovery_until=$((12 * 60 + 30)) 또는 recovery_until=$((750))
    assert re.search(r"recovery_until=\$\(\(\s*12\s*\*\s*60\s*\+\s*30\s*\)\)", src), (
        "recovery_until이 12:30(12 * 60 + 30)이 아니다. AM 지연 시작이 차단될 수 있다."
    )


def test_skip_stale_after_session_end():
    """skip_stale_start_us_am은 session_end 이후에만 발생해야 한다."""
    src = _load_workflow()
    lines = src.splitlines()
    # skip_stale_start_us_am이 있는 라인 찾기
    stale_lines = [i for i, l in enumerate(lines) if "skip_stale_start_us_am" in l]
    assert stale_lines, "skip_stale_start_us_am이 workflow에 없다"

    # skip_stale_start_us_am 직전에 session_end 비교 조건이 있어야 한다
    for idx in stale_lines:
        # 앞 5줄에서 session_end 비교가 있는지 확인
        context = "\n".join(lines[max(0, idx - 8): idx + 1])
        assert "session_end" in context or "12 * 60 + 30" in context, (
            f"skip_stale_start_us_am이 session_end 기준이 아닌 곳({idx+1}번째 줄)에 있다:\n{context}"
        )


def test_no_recovery_until_1130():
    """recovery_until이 11:30(11 * 60 + 30)으로 설정된 줄이 없어야 한다."""
    src = _load_workflow()
    assert not re.search(r"recovery_until=\$\(\(\s*11\s*\*\s*60\s*\+\s*30\s*\)\)", src), (
        "recovery_until=11:30이 아직 남아있다. 12:30으로 변경되어야 한다."
    )


def test_session_window_1230():
    """session_end가 12:30으로 정의되어 있어야 한다."""
    src = _load_workflow()
    assert re.search(r"session_end=\$\(\(\s*12\s*\*\s*60\s*\+\s*30\s*\)\)", src), (
        "session_end=12:30이 없다"
    )
