"""Read-only Stage4 deployment and health evidence must fail closed."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from scripts.us_stage4_operational_readiness import (
    EXPECTED_FLAGS, evaluate_writer_snapshot, git_code_pin,
)


def fixture():
    now = datetime.now(timezone.utc)
    token = uuid.uuid4()
    flags = dict(EXPECTED_FLAGS)
    scope = {"env": "practice", "account_id": "a" * 64,
             "trading_epoch_id": "test-only-epoch"}
    row = {"worker_state": "READY", "reconcile_status": "OK",
           "replay_status": "OK", "evidence_at": now,
           "writer_generation": token}
    report = {"worker_state": "READY", "read_status": "OK",
              "sampled_at_utc": now.isoformat(),
              "writer_generation": token.hex}
    return flags, scope, row, report, now


def test_fresh_writer_report_matches_db_uuid_even_if_format_differs():
    flags, scope, row, report, now = fixture()
    result = evaluate_writer_snapshot(flags=flags, scope=scope, row=row,
                                      report=report, now=now)
    assert result["writer_readiness"] == "PASS"
    assert result["full_operational_acceptance"] == "NOT_PROVEN"
    assert result["orders_or_cancels_submitted"] == 0


def test_stale_report_cannot_pass_with_fresh_db_heartbeat():
    flags, scope, row, report, now = fixture()
    report["sampled_at_utc"] = (now - timedelta(seconds=100)).isoformat()
    result = evaluate_writer_snapshot(flags=flags, scope=scope, row=row,
                                      report=report, now=now)
    assert result["writer_readiness"] == "BLOCK"
    assert "STAGE4_REPORT_STALE" in result["blocking_reasons"]


def test_mismatched_generation_and_missing_health_fail_closed():
    flags, scope, row, report, now = fixture()
    report["writer_generation"] = str(uuid.uuid4())
    res = evaluate_writer_snapshot(flags=flags, scope=scope, row=row,
                                   report=report, now=now)
    assert "STAGE4_REPORT_GENERATION_MISMATCH" in res["blocking_reasons"]
    res = evaluate_writer_snapshot(flags=flags, scope=scope, row=None,
                                   report=report, now=now)
    assert "STAGE4_DB_HEALTH_MISSING" in res["blocking_reasons"]


def test_missing_writer_authorization_is_not_automatically_enabled():
    flags, scope, row, report, now = fixture()
    flags["US_STAGE4_WRITER_AUTHORIZED"] = "0"
    res = evaluate_writer_snapshot(flags=flags, scope=scope, row=row,
                                   report=report, now=now)
    assert res["writer_readiness"] == "BLOCK"
    assert any("STAGE4_FLAGS_NOT_ARMED" in x for x in res["blocking_reasons"])


def test_code_pin_must_be_exact_dual_agent_head(tmp_path, monkeypatch):
    # git commands are read-only; no local checkout needs to be mutated.
    from scripts import us_stage4_operational_readiness as mod
    class Output:
        def __init__(self, stdout): self.stdout = stdout
    def git_mock(command, **kwargs):
        return Output("dual-agent\\n" if "branch" in command else "a"*40+"\\n")
    monkeypatch.setattr(mod.subprocess, "run", git_mock)
    pin_dir = tmp_path / "runtime" / "code-pins"
    pin_dir.mkdir(parents=True)
    pin = pin_dir / "US-2026-10-23.sha"
    pin.write_text("a"*40, encoding="utf-8")
    result = git_code_pin("2026-10-23", root=tmp_path)
    assert result["code_pin_status"] == "PASS"
    pin.write_text("b"*40, encoding="utf-8")
    assert git_code_pin("2026-10-23", root=tmp_path)["code_pin_status"] == "BLOCK"
