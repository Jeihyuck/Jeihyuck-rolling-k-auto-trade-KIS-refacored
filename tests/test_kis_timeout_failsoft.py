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
    monkeypatch.setattr(runner, 'resolve_kr_balance_fail_soft', lambda exc, env='practice': {'status':'WARN','reason':'BALANCE_TIMEOUT_FAIL_SOFT','entry_allowed':False,'exit_allowed':True,'order_allowed':0})
    calls={'n':0}
    mod=types.SimpleNamespace(main=lambda: calls.__setitem__('n', calls['n']+1) or 0)
    monkeypatch.setitem(sys.modules, 'trader.pb1_runner', mod)
    result=runner._run_pb1_session('afternoon','practice')
    assert calls['n']==1
    assert result['status']=='OK'
    assert result['balance_fail_soft']['reason']=='BALANCE_TIMEOUT_FAIL_SOFT'
    assert __import__('os').environ['ENTRY_ALLOWED']=='0'
    assert __import__('os').environ['EXIT_ALLOWED']=='1'


def test_pb1_engine_balance_failsoft_env_gates(monkeypatch):
    from trader.pb1_engine import PB1Engine
    engine = PB1Engine.__new__(PB1Engine)
    engine.trading_day = True
    engine.order_allowed = True
    engine.force_block_live = False
    engine.intended_live = True
    engine.strategy_mode = "LIVE"
    engine.market_window_name = "day"
    engine.force_entry_window_override = False
    engine.session_recovery_continue = False
    engine.am_recovery_continue = False
    engine.balance_fail_soft_active = True
    engine.balance_fail_soft_entry_allowed = False
    engine.balance_fail_soft_exit_allowed = True
    assert "balance_fail_soft_entry_disabled" in engine._order_precheck_gate_reasons(side="BUY", stage="PB1-ENTRY")
    assert engine._order_precheck_gate_reasons(side="SELL", stage="PB1-EXIT") == []
