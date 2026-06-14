"""
tests/test_trade_prep_workflow_contract.py

trade-prep.yml artifact upload paths 계약 검증.
final30_locked.json, quality-summary.json, flow-summary.json 포함 여부.
"""
from __future__ import annotations

from pathlib import Path

TRADE_PREP_YML = Path(__file__).parent.parent / ".github" / "workflows" / "trade-prep.yml"


def _content() -> str:
    return TRADE_PREP_YML.read_text()


def test_artifact_includes_final30_locked():
    assert "final30_locked.json" in _content(), (
        "trade-prep.yml artifact upload must include final30_locked.json"
    )


def test_artifact_includes_quality_summary():
    assert "quality-summary.json" in _content(), (
        "trade-prep.yml artifact upload must include quality-summary.json"
    )


def test_artifact_includes_flow_summary():
    assert "flow-summary.json" in _content(), (
        "trade-prep.yml artifact upload must include flow-summary.json"
    )


def test_prep_timeout_is_90():
    import re
    content = _content()
    assert re.search(r"timeout-minutes:\s*90", content), (
        "trade-prep.yml timeout-minutes should be 90 (was 40)"
    )


def test_prep_has_no_schedule_and_keeps_dispatch():
    content = _content()
    assert "schedule:" not in content, "trade-prep.yml must not have schedule trigger"
    assert "workflow_dispatch:" in content, "trade-prep.yml must keep workflow_dispatch"
