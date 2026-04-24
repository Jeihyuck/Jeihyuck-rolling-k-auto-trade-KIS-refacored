"""
테스트 5: artifact log 덮어쓰기 금지
- trade-am.yml / trade-pm.yml / trade-close.yml에서 "tee artifacts/" 패턴(append 없는)이 있으면 fail
- "tee -a artifacts/"는 허용
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

WORKFLOWS_DIR = Path(__file__).parent.parent / ".github" / "workflows"

WORKFLOW_FILES = [
    "trade-am.yml",
    "trade-pm.yml",
    "trade-close.yml",
]


def _load_workflow(name: str) -> str:
    path = WORKFLOWS_DIR / name
    assert path.exists(), f"workflow file not found: {path}"
    return path.read_text(encoding="utf-8")


class TestArtifactOverwriteForbidden:
    @pytest.mark.parametrize("filename", WORKFLOW_FILES)
    def test_no_bare_tee_artifacts(self, filename: str):
        """'| tee artifacts/' 패턴(append 없는)이 존재하면 fail."""
        text = _load_workflow(filename)
        lines = text.splitlines()
        violations = []
        for i, line in enumerate(lines, start=1):
            # tee artifacts/XXX.log 이지만 tee -a artifacts/XXX.log 가 아닌 경우
            if re.search(r"\btee\s+artifacts/", line) and not re.search(r"\btee\s+-a\s+artifacts/", line):
                violations.append((i, line.strip()))
        assert not violations, (
            f"{filename}: bare 'tee artifacts/' (non-append) found at lines: "
            + ", ".join(f"L{ln}: {text}" for ln, text in violations)
        )

    @pytest.mark.parametrize("filename", WORKFLOW_FILES)
    def test_tee_append_is_allowed(self, filename: str):
        """'tee -a artifacts/' 패턴이 존재해야 한다 (run 단계에서 사용)."""
        text = _load_workflow(filename)
        assert re.search(r"\btee\s+-a\s+artifacts/", text), (
            f"{filename}: expected 'tee -a artifacts/' pattern but none found"
        )

    @pytest.mark.parametrize("filename,session", [
        ("trade-am.yml", "am"),
        ("trade-pm.yml", "pm"),
        ("trade-close.yml", "close"),
    ])
    def test_phase_start_uses_tee_append(self, filename: str, session: str):
        """[PHASE][START] 로그 출력 시 tee -a를 사용해야 한다."""
        text = _load_workflow(filename)
        lines = text.splitlines()
        for line in lines:
            if f"[PHASE][START] phase={session}" in line:
                assert "tee -a" in line, (
                    f"{filename}: [PHASE][START] phase={session} must use 'tee -a', got: {line.strip()}"
                )
