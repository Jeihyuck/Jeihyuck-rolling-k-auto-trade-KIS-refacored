"""
tests/test_unified_manual_workflow_contract.py

unified-pipeline.yml 계약 검증:
- timeout 90분
- tee -a (append 모드)로 로그 누적
- artifact 경로에 prep_manifest, final30_locked 등 포함
"""
from __future__ import annotations

import re
from pathlib import Path

UNIFIED_YML = Path(__file__).parent.parent / ".github" / "workflows" / "unified-pipeline.yml"


def _content() -> str:
    return UNIFIED_YML.read_text()


def test_unified_timeout_is_90():
    assert re.search(r"timeout-minutes:\s*90", _content()), (
        "unified-pipeline.yml timeout-minutes should be 90"
    )


def test_unified_uses_tee_append_not_overwrite():
    content = _content()
    # 파이썬 runner가 tee -a 사용하는지 확인 (overwrite tee 금지)
    # 최소 2개 이상의 tee -a 가 있어야 함
    tee_a_count = content.count("tee -a")
    assert tee_a_count >= 2, (
        f"unified-pipeline.yml should use 'tee -a' for append mode, found {tee_a_count} occurrences"
    )


def test_unified_no_bare_overwrite_tee_on_runner():
    """python -m trader 실행 줄에 '| tee '(비 append)가 없어야 함."""
    content = _content()
    for line in content.splitlines():
        if "trader.prep_runner" in line or "trader.trade_tick" in line:
            if "tee " in line and "tee -a" not in line:
                raise AssertionError(
                    f"Found overwrite tee (should be tee -a): {line.strip()}"
                )


def test_unified_artifact_includes_prep_manifest():
    assert "prep_manifest" in _content() or "final30_locked" in _content(), (
        "unified-pipeline.yml artifact upload should include prep_manifest or final30_locked"
    )
