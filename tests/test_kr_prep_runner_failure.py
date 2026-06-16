import json, os, time
from trader.kr.runner import trade_session_runner as runner
from trader.kr.runner import prep_artifacts as pa

def test_prep_runner_failure_old_final30_not_success(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, 'ROOT', tmp_path)
    monkeypatch.setattr(pa, 'ROOT', tmp_path)
    monkeypatch.setenv('ALLOW_KR_PREP_OUTSIDE_WINDOW','1')
    d=tmp_path/'signals/kr'; d.mkdir(parents=True)
    final=d/'latest_final30_scored.json'
    contract=d/'latest_prep_contract.json'
    final.write_text(json.dumps([{'code':str(i)} for i in range(30)]), encoding='utf-8')
    contract.write_text(json.dumps({'env':'practice','trade_date':'2026-06-16','as_of':'2026-06-15','final30_rows':30,'contract_ok':True,'trade_can_proceed':True}), encoding='utf-8')
    old=time.time()-100
    os.utime(final,(old,old)); os.utime(contract,(old,old))
    class DummyPrep:
        @staticmethod
        def main(): return 1
    monkeypatch.setattr(runner, 'prep_runner', DummyPrep, raising=False)
    import sys, types
    mod=types.SimpleNamespace(main=lambda: 1)
    monkeypatch.setitem(sys.modules, 'trader.prep_runner', mod)
    result=runner._run_prep('practice')
    assert result['status']=='FAIL'
    assert result['reason']=='PREP_RUNNER_FAILED'

def test_prep_success_keeps_rich_contract(tmp_path, monkeypatch):
    import sys, types, json
    monkeypatch.setattr(runner, 'ROOT', tmp_path)
    monkeypatch.setattr(pa, 'ROOT', tmp_path)
    monkeypatch.setenv('ALLOW_KR_PREP_OUTSIDE_WINDOW','1')
    monkeypatch.setenv('KR_TRADE_DATE','2026-06-16')
    monkeypatch.setenv('KR_PREP_AS_OF','2026-06-15')
    legacy=tmp_path/'runtime/watchlist/2026-06-15/final30_scored.json'
    legacy.parent.mkdir(parents=True)
    def prep_main():
        legacy.write_text(json.dumps([{'code':str(i)} for i in range(30)]), encoding='utf-8')
        return 0
    monkeypatch.setitem(sys.modules, 'trader.prep_runner', types.SimpleNamespace(main=prep_main))
    result=runner._run_prep('practice')
    assert result['status']=='OK'
    for rel in ['signals/kr/prep_contract.json','signals/kr/latest_prep_contract.json','runtime/kr/watchlist/2026-06-16/prep_contract.json']:
        c=json.loads((tmp_path/rel).read_text(encoding='utf-8'))
        assert c['trade_date']=='2026-06-16'
        assert c['as_of']=='2026-06-15'
        assert c['env']=='practice'
        assert c['final30_rows']==30
        assert c['contract_ok'] is True
        assert c['trade_can_proceed'] is True
