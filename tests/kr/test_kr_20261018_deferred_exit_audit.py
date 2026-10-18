"""Actual installed KR watchdog and deferred diagnostics; no broker/network."""
import signal
import time
from types import SimpleNamespace

import pytest

from trader.pb1_engine import PB1Engine


def _engine(write):
    engine = object.__new__(PB1Engine)
    engine.env = 'practice'
    engine.positions_repo = SimpleNamespace(update_position_fields=write)
    return engine


def test_diagnostic_write_cannot_delay_order_work_and_records_final_result(monkeypatch):
    events = []
    def write(**row):
        events.append(('audit', dict(row['fields']['last_exit_eval_json'])))
    engine = _engine(write)
    payload = {'submitted': 0}
    engine._queue_exit_audit_update(sid=1, mode=1, code='005930', fields={'last_exit_eval_json': payload})
    assert events == []
    events.append(('submit', 1))
    payload['submitted'] = 1
    monkeypatch.setattr('trader.kis_wrapper.kr_tick_remaining_sec', lambda: 10)
    engine._flush_exit_audit_updates()
    assert events == [('submit', 1), ('audit', {'submitted': 1})]
    engine._flush_exit_audit_updates()
    assert len(events) == 2


def test_tail_budget_rechecked_per_row_and_stale_audits_never_replayed(monkeypatch):
    writes = []
    remaining = iter([10, 0])
    engine = _engine(lambda **row: writes.append(row))
    for code in ('005930', '000660'):
        engine._queue_exit_audit_update(sid=1, mode=1, code=code, fields={'last_exit_eval_json': {}})
    monkeypatch.setattr('trader.kis_wrapper.kr_tick_remaining_sec', lambda: next(remaining))
    engine._flush_exit_audit_updates()
    assert [row['code'] for row in writes] == ['005930']
    engine._flush_exit_audit_updates()
    assert len(writes) == 1


@pytest.mark.skipif(not hasattr(signal, 'setitimer'), reason='POSIX watchdog')
def test_installed_watchdog_interrupts_slow_audit_without_fail_soft_swallow(monkeypatch):
    # Test the runtime wrapper used by the real KR entrypoint, not just the
    # undecorated runner or a source-code string assertion.
    from trader import install_legacy_pb1_runtime_guards
    from trader import pb1_runner
    install_legacy_pb1_runtime_guards()
    monkeypatch.setattr('trader.kis_wrapper.kr_tick_remaining_sec', lambda: 10)
    engine = _engine(lambda **row: time.sleep(1))
    engine._queue_exit_audit_update(sid=1, mode=1, code='005930', fields={'last_exit_eval_json': {}})
    previous = signal.getsignal(signal.SIGALRM)
    start = time.monotonic()
    with pytest.raises(pb1_runner.TickTimeoutError):
        pb1_runner._run_once_with_hard_timeout(timeout_sec=.05, call=engine._flush_exit_audit_updates)
    assert time.monotonic() - start < .75
    assert signal.getitimer(signal.ITIMER_REAL)[0] == 0
    assert signal.getsignal(signal.SIGALRM) is previous
    assert engine._pending_exit_audit_updates == []
