"""
[CONTRACT] PREP_DONE ledger FK 계약.

- GITHUB_RUN_ID 숫자 run_id가 ledger_events.run_id FK 컬럼에 직접 삽입되지 않는다.
- _resolve_internal_run_id_or_none(): 숫자 문자열 → None 반환
- upsert_prep_event(): 숫자 run_id → resolved_run_id=None, workflow_run_id=숫자
- check_prep_duplicate_guard.py: run_id=None, workflow_run_id=GITHUB_RUN_ID 전달
"""
from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ── _resolve_internal_run_id_or_none ─────────────────────────────────────────

def test_resolve_returns_none_for_github_numeric_run_id() -> None:
    """숫자 run_id (GitHub GITHUB_RUN_ID)는 None을 반환해야 한다."""
    from trader.db.repos import _resolve_internal_run_id_or_none  # type: ignore
    conn = MagicMock()
    result = _resolve_internal_run_id_or_none(conn, MagicMock(), "24941871819")
    assert result is None, "Numeric run_id must resolve to None (not a UUID)"


def test_resolve_returns_none_for_none_input() -> None:
    """None 입력 → None 반환."""
    from trader.db.repos import _resolve_internal_run_id_or_none  # type: ignore
    conn = MagicMock()
    result = _resolve_internal_run_id_or_none(conn, MagicMock(), None)
    assert result is None


def test_resolve_does_not_hit_db_for_numeric_input() -> None:
    """숫자 run_id 입력 시 DB 쿼리를 실행하지 않아야 한다."""
    from trader.db.repos import _resolve_internal_run_id_or_none  # type: ignore
    conn = MagicMock()
    schema = MagicMock()
    _resolve_internal_run_id_or_none(conn, schema, "99999999999")
    conn.execute.assert_not_called()


# ── upsert_prep_event: resolved_run_id=None 전달 ─────────────────────────────

def test_upsert_prep_event_uses_resolved_run_id_none_for_github_id() -> None:
    """upsert_prep_event에 숫자 run_id를 전달하면 append_event에 run_id=None이 전달된다."""
    from trader.db.repos import LedgerEventsRepo

    engine = MagicMock()
    repo = LedgerEventsRepo(engine)

    captured = {}

    def fake_append_event(**kwargs):
        captured.update(kwargs)
        return "fake-event-id"

    # exists_check returns empty (no prior event)
    engine.connect.return_value.__enter__ = lambda s: MagicMock(
        execute=MagicMock(return_value=MagicMock(mappings=MagicMock(return_value=MagicMock(all=MagicMock(return_value=[])))))
    )
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(repo, "append_event", side_effect=fake_append_event):
        repo.upsert_prep_event(
            env="practice",
            strategy="pb1",
            as_of="2026-04-25",
            trade_date="2026-04-25",
            event_type="PREP_DONE",
            status="READY_FROM_CANONICAL",
            reason="canonical_prep_already_ready",
            final30_count=30,
            quality_ok=True,
            trade_can_proceed=True,
            run_id="24941871819",  # GitHub numeric run_id
            run_window="prep",
        )

    assert captured.get("run_id") is None, (
        "append_event must receive run_id=None when a GitHub numeric run_id is passed"
    )


def test_upsert_prep_event_stores_github_id_in_workflow_run_id() -> None:
    """숫자 run_id는 payload.workflow_run_id에 저장되어야 한다."""
    from trader.db.repos import LedgerEventsRepo

    engine = MagicMock()
    repo = LedgerEventsRepo(engine)
    payload_captured = {}

    def fake_append_event(**kwargs):
        payload_captured.update(kwargs.get("payload_json", {}))
        return "fake-event-id"

    engine.connect.return_value.__enter__ = lambda s: MagicMock(
        execute=MagicMock(return_value=MagicMock(mappings=MagicMock(return_value=MagicMock(all=MagicMock(return_value=[])))))
    )
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(repo, "append_event", side_effect=fake_append_event):
        repo.upsert_prep_event(
            env="practice",
            strategy="pb1",
            as_of="2026-04-25",
            trade_date="2026-04-25",
            event_type="PREP_DONE",
            status="READY",
            reason="test",
            final30_count=30,
            quality_ok=True,
            trade_can_proceed=True,
            run_id="24941871819",
            run_window="prep",
        )

    assert payload_captured.get("workflow_run_id") == "24941871819", (
        "GitHub numeric run_id must be saved to payload.workflow_run_id"
    )


# ── check_prep_duplicate_guard.py: run_id=None 사용 ──────────────────────────

GUARD_SCRIPT = Path(__file__).parents[1] / "scripts" / "check_prep_duplicate_guard.py"


def test_guard_script_passes_run_id_none() -> None:
    """check_prep_duplicate_guard.py _upsert_duplicate_ready_event에서 run_id=None을 전달해야 한다."""
    text = GUARD_SCRIPT.read_text(encoding="utf-8")
    assert "run_id=None" in text, (
        "check_prep_duplicate_guard.py must pass run_id=None to upsert_prep_event"
    )


def test_guard_script_passes_workflow_run_id() -> None:
    """check_prep_duplicate_guard.py가 workflow_run_id=os.getenv('GITHUB_RUN_ID')를 전달해야 한다."""
    text = GUARD_SCRIPT.read_text(encoding="utf-8")
    assert 'workflow_run_id=os.getenv("GITHUB_RUN_ID")' in text, (
        "check_prep_duplicate_guard.py must pass workflow_run_id=os.getenv('GITHUB_RUN_ID')"
    )


def test_guard_script_no_bare_github_run_id_as_run_id() -> None:
    """check_prep_duplicate_guard.py에서 run_id=os.getenv('GITHUB_RUN_ID')를 직접 쓰면 안 된다."""
    import re
    text = GUARD_SCRIPT.read_text(encoding="utf-8")
    # workflow_run_id=... 는 허용, 그냥 run_id=... 는 금지
    assert not re.search(r'(?<!\w)run_id=os\.getenv\("GITHUB_RUN_ID"\)', text), (
        "check_prep_duplicate_guard.py must NOT pass GitHub run_id directly as run_id"
    )


def test_guard_script_has_job_checkpoint_fallback() -> None:
    """check_prep_duplicate_guard.py에 save_job_checkpoint 폴백이 있어야 한다."""
    text = GUARD_SCRIPT.read_text(encoding="utf-8")
    assert "save_job_checkpoint" in text, (
        "check_prep_duplicate_guard.py must import and call save_job_checkpoint as fallback"
    )
