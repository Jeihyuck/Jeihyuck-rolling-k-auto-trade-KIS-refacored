"""Run the real session loop with a virtual clock and offline repositories."""
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest


@pytest.mark.parametrize('outcome,expected_second_start', [('OK',60), ('OK_NO_TRADE',60), ('timeout',120), ('wrapped_timeout',120)])
def test_real_loop_aligns_all_completion_and_timeout_paths(monkeypatch, tmp_path, outcome, expected_second_start):
    import trader.pb1_runner as runner
    now = datetime(2026,10,16,9,0,tzinfo=ZoneInfo('Asia/Seoul'))
    clock = {'t':0.0}
    sleeps = []
    def sleep(seconds):
        assert seconds >= 0
        sleeps.append(seconds)
        clock['t'] += seconds
    monkeypatch.setattr(runner, 'time_mod', SimpleNamespace(monotonic=lambda:clock['t'], sleep=sleep))
    monkeypatch.setattr(runner, '_get_now_kst', lambda: now + timedelta(seconds=clock['t']))
    monkeypatch.setattr(runner, '_resolve_session_kind', lambda *a,**k:'am')
    monkeypatch.setattr(runner, '_resolve_session_end_dt', lambda _:now+timedelta(seconds=500))
    monkeypatch.setattr(runner, '_resolve_loop_limits', lambda:(10,10,500,False))
    monkeypatch.setattr(runner, 'make_engine', lambda:MagicMock())
    monkeypatch.setattr(runner, 'RunsRepo', lambda _:MagicMock())
    monkeypatch.setattr(runner, 'try_acquire_lock', lambda *a,**k:True)
    monkeypatch.setattr(runner, '_acquire_session_guard_or_takeover', lambda **k:{})
    monkeypatch.setattr(runner, 'runtime_root', lambda:tmp_path)
    monkeypatch.setattr(runner, 'decide_window', lambda **k:object())
    monkeypatch.setattr(runner, '_run_nontrading_smoke_if_needed', lambda **k:False)
    # The session registers process handlers; this virtual-clock test must not
    # replace pytest's SIGINT/SIGTERM handlers for subsequent test modules.
    monkeypatch.setattr(runner.signal, 'signal', lambda *a, **k: None)
    for name in ('_touch_session_guard','_ensure_bootstrap_migrations','_write_change_flag',
                 '_finish_session_guard','release_advisory_xact_lock','dispose_engine_safely',
                 '_record_session_execution_marker','_write_session_result_file'):
        monkeypatch.setattr(runner,name,lambda *a,**k:None)
    monkeypatch.setattr(runner,'_run_once_with_hard_timeout',lambda *,timeout_sec,call:call())
    monkeypatch.setenv('PB1_CANDIDATE_ONLY','1')
    monkeypatch.setenv('PB1_LOOP_INTERVAL_SEC','60')
    monkeypatch.setenv('PB1_MIN_TICK_BUDGET_SEC','5')
    monkeypatch.setenv('PB1_DB_RECOVERY_SLEEP_SEC','2')
    monkeypatch.setenv('STRATEGY_MODE','LIVE')
    monkeypatch.setenv('KIS_ENV','practice')
    monkeypatch.setenv('STRATEGY_ENV','practice')
    starts=[]
    def tick(**kwargs):
        starts.append(clock['t'])
        if len(starts)>1:
            clock['t']=500
            return [],False,{},'entry','OK_NO_TRADE'
        clock['t'] += 80 if 'timeout' in outcome else 40
        if outcome == 'timeout':
            raise runner.TickTimeoutError('tick_hard_timeout')
        if outcome == 'wrapped_timeout':
            raise RuntimeError('driver wrapper') from runner.TickTimeoutError('tick_hard_timeout')
        return [],False,{},'entry',outcome
    monkeypatch.setattr(runner,'run_once',tick)
    runner._run_loop(args=SimpleNamespace(window='day',env='practice',strategy_mode='LIVE'))
    assert starts == [0,expected_second_start]
    if 'timeout' in outcome:
        assert sleeps == [2,38]
    else:
        assert sleeps == [20]
