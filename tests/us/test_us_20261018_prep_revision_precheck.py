"""Revision precheck must never rebuild before rejecting a pinned day."""
import json
from pathlib import Path
import pytest

from trader.us import run_manifest
from trader.us.runner import prep_runner


def _pinned(tmp_path, monkeypatch, trade_date="2026-10-14", rev="old"):
    monkeypatch.setenv("US_RUN_MANIFEST_DIR", str(tmp_path))
    path = run_manifest.manifest_path(trade_date)
    path.write_text(json.dumps({"trade_date": trade_date, "run_revision": rev}))
    return path


def test_valid_completed_prep_revision_mismatch_blocks_before_market_data(tmp_path, monkeypatch):
    _pinned(tmp_path, monkeypatch)
    monkeypatch.setattr("trader.us.prep_contract._current_git_commit_sha", lambda: "new")
    monkeypatch.setattr("trader.us.path_contract.load_us_prep_contract",
                        lambda date: {"status": "OK", "entry_can_proceed": 1})
    monkeypatch.setattr(prep_runner, "save_us_prep_run",
                        lambda **kw: pytest.fail("STARTED must not mask valid completed PREP"))
    before = run_manifest.manifest_path("2026-10-14").read_bytes()
    result = prep_runner.run_prep(force_now="2026-10-14T10:00:00-04:00", offline=True)
    assert result["stage"] == "revision_preflight"
    assert result["reason"] == "trade_day_revision_already_pinned"
    assert result["existing_contract_preserved"] is True
    assert run_manifest.manifest_path("2026-10-14").read_bytes() == before


@pytest.mark.parametrize("old_status", [
    "DEFENSE_CRASH_ENTRY_BLOCKED", "ERROR", "DEGRADED",
    "OK_WITH_WARNINGS_ENTRY_BLOCKED_CLUSTER_CAP",
])
def test_existing_recoverable_entry_block_retains_upgrade_path(tmp_path, monkeypatch, old_status):
    _pinned(tmp_path, monkeypatch)
    result = run_manifest.precheck_prep_revision(
        "2026-10-14", "new",
        {"status": old_status, "entry_can_proceed": 0}, environ={},
    )
    assert result == {"ok": True, "reason": "defer_to_existing_recovery_upgrade_gate"}


def test_authorized_manual_reprep_keeps_existing_operator_workflow(tmp_path, monkeypatch):
    _pinned(tmp_path, monkeypatch)
    env = {"US_REPREP_NEW_REVISION": "1", "US_REPREP_APPROVED": "1",
           "US_REPREP_AUDIT_REASON": "approved incident recovery"}
    assert run_manifest.precheck_prep_revision("2026-10-14", "new", {}, environ=env)["ok"]
    assert not run_manifest.precheck_prep_revision(
        "2026-10-14", "new", {}, environ={"US_REPREP_NEW_REVISION": "1"}
    )["ok"]


def test_incomplete_manifest_fails_closed(tmp_path, monkeypatch):
    path = _pinned(tmp_path, monkeypatch)
    path.write_text("{bad json")
    result = run_manifest.precheck_prep_revision("2026-10-14", "new")
    assert result["ok"] is False
    assert result["reason"] == "existing_revision_manifest_unreadable"


def test_same_revision_does_not_block(tmp_path, monkeypatch):
    _pinned(tmp_path, monkeypatch, rev="new")
    assert run_manifest.precheck_prep_revision(
        "2026-10-14", "new", {"status": "OK", "entry_can_proceed": 1}
    )["ok"]



def test_unknown_runtime_sha_refuses_existing_pin_before_rebuild(tmp_path, monkeypatch):
    path = _pinned(tmp_path, monkeypatch)
    previous = path.read_bytes()
    result = run_manifest.precheck_prep_revision("2026-10-14", "unknown")
    assert result == {"ok": False, "reason": "current_revision_unavailable"}
    assert path.read_bytes() == previous
