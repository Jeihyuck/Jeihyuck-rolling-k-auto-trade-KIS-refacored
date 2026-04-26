"""
[CONTRACT] trade-prep.yml 로그 모드 계약.

- MODE: prep (trade 아님)
- PB1_SESSION_KIND: "prep"
- PB1_FORCE_TRADE_SESSION: "prep"
- FORCE_PB1_PHASE: "prep"
- 로그 라인: mode=prep phase=prep session=prep
"""
from __future__ import annotations

from pathlib import Path

import pytest

WORKFLOW_PATH = Path(__file__).parents[1] / ".github" / "workflows" / "trade-prep.yml"


@pytest.fixture(scope="module")
def workflow_text() -> str:
    return WORKFLOW_PATH.read_text(encoding="utf-8")


def test_mode_is_prep_not_trade(workflow_text: str) -> None:
    """env.MODE가 'prep'이어야 한다 (trade가 아님)."""
    assert "MODE: prep" in workflow_text, "trade-prep.yml env.MODE must be 'prep'"
    assert "MODE: trade" not in workflow_text, (
        "trade-prep.yml must NOT set MODE: trade"
    )


def test_pb1_session_kind_prep(workflow_text: str) -> None:
    """PB1_SESSION_KIND가 'prep'이어야 한다."""
    assert 'PB1_SESSION_KIND: "prep"' in workflow_text, (
        'trade-prep.yml must set PB1_SESSION_KIND: "prep"'
    )


def test_force_trade_session_prep(workflow_text: str) -> None:
    """PB1_FORCE_TRADE_SESSION이 'prep'이어야 한다."""
    assert 'PB1_FORCE_TRADE_SESSION: "prep"' in workflow_text, (
        'trade-prep.yml must set PB1_FORCE_TRADE_SESSION: "prep"'
    )


def test_force_pb1_phase_prep(workflow_text: str) -> None:
    """FORCE_PB1_PHASE가 'prep'이어야 한다."""
    assert 'FORCE_PB1_PHASE: "prep"' in workflow_text, (
        'trade-prep.yml must set FORCE_PB1_PHASE: "prep"'
    )


def test_trigger_log_shows_mode_prep(workflow_text: str) -> None:
    """트리거 로그 라인에 mode=prep 또는 mode=${MODE:-prep}가 찍혀야 한다."""
    assert "mode=prep" in workflow_text or "mode=${MODE:-prep}" in workflow_text, (
        "trade-prep.yml trigger log must emit mode=prep"
    )


def test_trigger_log_shows_phase_prep(workflow_text: str) -> None:
    """트리거 로그 라인에 phase=prep이 찍혀야 한다."""
    assert "phase=prep" in workflow_text, (
        "trade-prep.yml trigger log must emit phase=prep"
    )


def test_trigger_log_shows_session_prep(workflow_text: str) -> None:
    """트리거 로그 라인에 session=prep이 찍혀야 한다."""
    assert "session=prep" in workflow_text, (
        "trade-prep.yml trigger log must emit session=prep"
    )
