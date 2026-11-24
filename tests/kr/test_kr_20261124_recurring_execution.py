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


def test_recent_gap_repair_targets_missing_minute_without_synthetic_bar():
    now = datetime(2026, 11, 24, 10, 35, tzinfo=contract.KST)
    bars = [{"timestamp": f"2026-11-24T10:{minute:02d}:00+09:00"}
            for minute in (30, 32, 34)]
    assert observer._recent_gap_anchor({"reason": "RECENT_MINUTE_GAP"}, bars, now) == "103400"
    assert observer._recent_gap_anchor({"reason": "COMPLETED_BAR_STALE"}, bars, now) is None
    assert observer._recent_gap_anchor({"reason": "RECENT_MINUTE_GAP"}, bars[:2], now) is None


@pytest.mark.parametrize("status,reason", [
    ("OK_NO_TRADE", "KR_ENTRY_TIMING_POLICY_WAIT"),
    ("RETRYABLE_MARKET_DATA_ERROR", "KR_ENTRY_TIMING_DATA_UNVERIFIED"),
])
def test_inner_pb1_timing_marker_never_completes_session(status, reason):
    from trader import pb1_runner
    normalized = pb1_runner.normalize_session_result(
        status=status, reason=reason, order_candidates=3, api_submitted=0, skipped=3)
    assert normalized.status == status
    assert normalized.completed == 0 and normalized.retryable == 1
    marker = pb1_runner._compute_session_marker_payload(
        status=status, exit_reason=reason, exit_code=0, entry_status="DONE", sell_orders_ack=1)
    assert marker["status"] == "PARTIAL_SUCCESS_RETRYABLE"
    assert marker["sell_completed"] and not marker["completed"] and marker["retryable"]
    assert not pb1_runner.should_skip_duplicate_from_marker(marker, session="afternoon")


def test_priority_manifest_lock_contention_returns_without_blocking_tick(monkeypatch, tmp_path):
    from trader.kr import entry_priority_queue as priority
    codes = [str(i).zfill(6) for i in range(1, 31)]
    monkeypatch.setattr(priority, "_prep_snapshot", lambda *a, **k: {
        "codes": codes, "contract_hash": "contract", "final30_digest": "digest"})
    monkeypatch.setattr(priority, "_current_sha", lambda: "a"*40)
    monkeypatch.setattr(priority, "manifest_path", lambda *a: tmp_path / "priority.json")
    def bounded_failure(lock, *, timeout_seconds):
        assert timeout_seconds == 2.0
        raise contract.OpeningProofLockTimeout("contended")
    monkeypatch.setattr(priority, "_acquire_opening_proof_lock", bounded_failure)
    result = priority.publish_pb1_priority(
        date="2026-11-24", account_scope="offline", eligible_codes=codes[:3],
        now=datetime(2026, 11, 24, 10, 35, tzinfo=contract.KST))
    assert result["state"] == "PRIORITY_LOCK_TIMEOUT"
    assert not (tmp_path / "priority.json").exists()


def test_timeout_finalization_has_separate_deadline_and_does_not_swallow_timeout(monkeypatch):
    import signal
    from trader.kr import runtime_integrity_20260928 as runtime
    class TickTimeoutError(TimeoutError):
        pass
    timers = []
    monkeypatch.setattr(signal, "setitimer", lambda which, seconds: timers.append(seconds))
    def timeout():
        raise runtime._HardTickAbort("tick_hard_timeout")
    def blocked_finalize(**kwargs):
        assert timers[-1] == 5.0
        raise runtime._HardTickAbort("finalize deadline")
    monkeypatch.setattr(runtime, "_finalize_tracked_tick_run_on_timeout", blocked_finalize)
    with pytest.raises(TickTimeoutError, match="tick_hard_timeout"):
        runtime._build_hard_timeout_runner(TickTimeoutError)(timeout_sec=1, call=timeout)
    assert timers == [1.0, 0, 5.0, 0, 0]
