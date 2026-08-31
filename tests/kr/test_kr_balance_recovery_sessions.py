from datetime import datetime, timedelta, timezone

from trader.execution_state import BalanceRecoveryState
from trader.kr.runner.trade_session_runner import recover_temporary_balance


def test_pm_temporary_outage_keeps_runner_in_recovery_until_fresh_balance():
    state = BalanceRecoveryState(retry_interval_seconds=60)
    at = datetime(2026, 8, 31, 4, 0, tzinfo=timezone.utc)
    state.failed(at)
    assert state.state == "BALANCE_RECOVERY_ONLY"
    assert not state.new_order_allowed
    assert state.next_retry_at == at + timedelta(seconds=60)
    state.failed(state.next_retry_at)
    assert state.retry_count == 2
    state.recovered()
    assert state.state == "NORMAL"
    assert state.new_order_allowed


def test_production_runner_retries_two_timeouts_then_resumes(monkeypatch):
    monkeypatch.setenv("KR_BALANCE_RECOVERY_INTERVAL_SEC", "1")
    calls = []
    results = [
        {"status": "WARN", "exit_allowed": 0, "reason": "KIS_BALANCE_TIMEOUT"},
        None,
    ]

    def probe():
        calls.append(len(calls) + 1)
        return results.pop(0)

    slept = []
    result = recover_temporary_balance(
        "afternoon", {"status": "WARN", "exit_allowed": 0, "reason": "KIS_BALANCE_TIMEOUT"},
        probe=probe, sleep_fn=slept.append, max_attempts=3,
    )
    assert result is None
    assert calls == [1, 2]
    assert slept == [1, 1]
    assert __import__("os").environ["KR_BALANCE_RECOVERY_ONLY"] == "0"
    assert __import__("os").environ["ORDER_ALLOWED"] == "1"
