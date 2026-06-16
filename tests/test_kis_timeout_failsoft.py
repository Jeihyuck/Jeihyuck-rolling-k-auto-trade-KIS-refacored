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
