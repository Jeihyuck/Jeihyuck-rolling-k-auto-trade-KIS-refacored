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


def _timing_skip(state, reason):
    return {"terminal_event": "FINAL_SKIP", "skipped": 1,
            "skipped_reason": "KR_ENTRY_TIMING_NOT_READY_OR_PRICE_TOO_HIGH",
            "timing_state": state, "timing_reason": reason}


def test_zero_submit_mixed_timing_wait_and_data_error_is_retryable_not_build_failure():
    from trader.pb1_engine import _classify_kr_timing_zero_submit
    from trader.kr.runner.trade_session_runner import normalize_kr_session_completion
    results = [_timing_skip("WAIT", "PULLBACK_SUPPORT_REBOUND_NOT_CONFIRMED"),
               _timing_skip("UNKNOWN", "RECENT_MINUTE_GAP"),
               _timing_skip("WAIT", "ORDER_PRICE_MOVED_FROM_CONFIRMED_BAR")]
    decision = _classify_kr_timing_zero_submit({"results": results}, expected_count=3)
    assert decision == ("RETRYABLE_MARKET_DATA_ERROR", "KR_ENTRY_TIMING_DATA_UNVERIFIED")
    assert normalize_kr_session_completion(
        status="OK", summary_reason="PB1_SESSION_DONE", marker={"completed": 1},
        pb1_status=decision[0], pb1_exit_reason=decision[1],
    ) == (*decision, 0, 1)


def test_all_timing_wait_is_successful_evaluation_not_session_completion():
    from trader.pb1_engine import _classify_kr_timing_zero_submit
    from trader.kr.runner.trade_session_runner import normalize_kr_session_completion
    results = [_timing_skip("WAIT", "PULLBACK_SUPPORT_REBOUND_NOT_CONFIRMED"),
               _timing_skip("READY_SHADOW", "ORDER_PRICE_ABOVE_CONFIRMED_CAP")]
    decision = _classify_kr_timing_zero_submit({"results": results}, expected_count=2)
    assert decision == ("OK_NO_TRADE", "KR_ENTRY_TIMING_POLICY_WAIT")
    assert normalize_kr_session_completion(
        status="OK", summary_reason="PB1_SESSION_DONE", marker={"completed": 1},
        pb1_status=decision[0], pb1_exit_reason=decision[1],
    ) == (*decision, 0, 1)


@pytest.mark.parametrize("change", [
    {"failed": 1}, {"attempted": 1}, {"api_submitted": 1},
    {"results": []}, {"results": [_timing_skip("READY_SHADOW", "READY")]},
    {"results": [{"terminal_event": "ORDER_SUBMIT_UNRESOLVED", "skipped": 1}]},
    {"results": [{"terminal_event": "FINAL_SKIP", "skipped": 1, "skipped_reason": "ENTRY_PLAN_INVALID"}]},
])
def test_timing_classifier_does_not_hide_actual_or_unaccounted_failures(change):
    from trader.pb1_engine import _classify_kr_timing_zero_submit
    result = {"results": [_timing_skip("WAIT", "PULLBACK_SUPPORT_REBOUND_NOT_CONFIRMED")]}
    result.update(change)
    assert _classify_kr_timing_zero_submit(result, expected_count=1) is None
