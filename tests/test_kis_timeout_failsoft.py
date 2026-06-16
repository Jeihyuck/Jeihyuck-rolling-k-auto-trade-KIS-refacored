from trader.kis_wrapper import KisBalanceUnavailable, resolve_kr_balance_fail_soft
from trader.kr.runner import trade_session_runner as runner

def test_balance_timeout_failsoft_practice_no_unhandled():
    r=resolve_kr_balance_fail_soft(TimeoutError('boom'), env='practice')
    assert r['reason']=='BALANCE_TIMEOUT_FAIL_SOFT'
    assert r['entry_allowed'] is False and r['exit_allowed'] is True

def test_balance_timeout_actual_assert_path_failsoft(monkeypatch):
    class DummyKis:
        def get_balance_cached(self):
            raise KisBalanceUnavailable('timeout')
    monkeypatch.setenv('STRATEGY_ENV','practice')
    monkeypatch.setattr(runner, 'KisAPI', lambda: DummyKis())
    result=runner._assert_balance_available('am')
    assert result['status']=='WARN'
    assert result['reason']=='BALANCE_TIMEOUT_FAIL_SOFT'
    assert result['entry_allowed']==0 and result['exit_allowed']==1

def test_run_pb1_session_continues_after_balance_failsoft(monkeypatch, tmp_path):
    from trader.kr.runner import prep_artifacts as pa
    import json, sys, types
    monkeypatch.setattr(runner, 'ROOT', tmp_path)
    monkeypatch.setattr(pa, 'ROOT', tmp_path)
    final=tmp_path/'signals/kr/latest_final30_scored.json'
    final.parent.mkdir(parents=True, exist_ok=True)
    final.write_text(json.dumps([{'code':str(i)} for i in range(30)]), encoding='utf-8')
    (tmp_path/'signals/kr/latest_prep_contract.json').write_text(json.dumps({'market':'KR','env':'practice','as_of':'2026-06-15','trade_date':'2026-06-16','final30_rows':30,'final30_scored_rows':30,'contract_ok':True,'trade_can_proceed':True}), encoding='utf-8')
    monkeypatch.setenv('KR_TRADE_DATE','2026-06-16')
    class DummyKis:
        def get_balance_cached(self):
            raise KisBalanceUnavailable('timeout')
    monkeypatch.setattr(runner, 'KisAPI', lambda: DummyKis())
    calls={'n':0}
    mod=types.SimpleNamespace(main=lambda: calls.__setitem__('n', calls['n']+1) or 0)
    monkeypatch.setitem(sys.modules, 'trader.pb1_runner', mod)
    result=runner._run_pb1_session('afternoon','practice')
    assert calls['n']==1
    assert result['status']=='OK'
    assert result['balance_fail_soft']['reason']=='BALANCE_TIMEOUT_FAIL_SOFT'
    assert __import__('os').environ['ENTRY_ALLOWED']=='0'
    assert __import__('os').environ['EXIT_ALLOWED']=='1'
