"""Regression cases for the Nov24 native-minute recovery incident; no broker I/O."""
from datetime import datetime
import fcntl

import pytest

from trader.kr import opening_bar_contract as contract
from scripts import kr_opening_observer as observer


def test_contended_proof_lock_times_out_without_releasing_other_writer(tmp_path):
    path = tmp_path / "proof.lock"
    with path.open("a+b") as held, path.open("a+b") as waiting:
        fcntl.flock(held.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(contract.OpeningProofLockTimeout, match="OPENING_PROOF_LOCK_TIMEOUT"):
            contract._acquire_opening_proof_lock(waiting, timeout_seconds=0.01)
        with pytest.raises(BlockingIOError):
            fcntl.flock(waiting.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(held.fileno(), fcntl.LOCK_UN)
        contract._acquire_opening_proof_lock(waiting, timeout_seconds=0.01)
        fcntl.flock(waiting.fileno(), fcntl.LOCK_UN)


def test_conflict_recovery_has_separate_minute_cadence(monkeypatch):
    monkeypatch.setattr(observer, "_CONFLICT_REPAIR_NEXT", {})
    monkeypatch.setattr(observer, "_OPENING_REPAIR_NEXT", {"093320": (10000.0, 7)})
    clock = [100.0]
    monkeypatch.setattr(observer.time, "monotonic", lambda: clock[0])
    assert not observer._repair_opening_due("093320")
    assert observer._repair_conflict_due("093320")
    clock[0] += 59
    assert not observer._repair_conflict_due("093320")
    clock[0] += 1
    assert observer._repair_conflict_due("093320")


def test_conflict_repair_rotates_all_valid_native_minutes(monkeypatch):
    monkeypatch.setattr(observer, "_CONFLICT_REPAIR_CURSOR", {})
    now = datetime(2026, 11, 24, 10, 35, tzinfo=contract.KST)
    stamps = [f"2026-11-24T09:{minute:02d}:00+09:00" for minute in range(12, 17)]
    stamps += ["2026-11-23T09:10:00+09:00", "2026-11-24T10:35:00+09:00",
               "2026-11-24T09:20:00", "not-a-time"]
    windows = [observer._conflict_repair_anchors("093320", stamps, now) for _ in range(3)]
    assert all(len(window) <= 2 for window in windows)
    assert set().union(*map(set, windows)) == {"091300", "091400", "091500", "091600", "091700"}
