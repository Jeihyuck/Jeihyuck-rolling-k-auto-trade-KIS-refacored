from trader.kis_wrapper import resolve_kr_balance_fail_soft

def test_balance_timeout_failsoft_practice_no_unhandled():
    r=resolve_kr_balance_fail_soft(TimeoutError('boom'), env='practice')
    assert r['reason']=='BALANCE_TIMEOUT_FAIL_SOFT'
    assert r['entry_allowed'] is False and r['exit_allowed'] is True
