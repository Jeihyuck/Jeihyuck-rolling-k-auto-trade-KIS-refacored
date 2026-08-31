from datetime import datetime, timedelta, timezone

from trader.execution_state import BalanceRecoveryState


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
