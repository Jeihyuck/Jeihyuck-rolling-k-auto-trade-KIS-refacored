import json
from pathlib import Path
from trader.kr.runner import prep_artifacts as pa

def test_legacy_final30_repaired_to_canonical(tmp_path, monkeypatch):
    monkeypatch.setattr(pa, 'ROOT', tmp_path)
    legacy=tmp_path/'runtime/watchlist/2026-06-15/final30_scored.json'
    legacy.parent.mkdir(parents=True)
    legacy.write_text(json.dumps([{'code':str(i).zfill(6)} for i in range(30)]), encoding='utf-8')
    r=pa.find_and_repair_kr_prep_artifact(env='practice', trade_date='2026-06-16', as_of='2026-06-15')
    assert r.ok and r.repaired and r.rows==30
    assert (tmp_path/'signals/kr/latest_final30_scored.json').exists()
    c=json.loads((tmp_path/'signals/kr/latest_prep_contract.json').read_text())
    assert c['as_of']=='2026-06-15' and c['trade_date']=='2026-06-16' and c['trade_can_proceed'] is True

def test_stale_latest_contract_fails(tmp_path, monkeypatch):
    monkeypatch.setattr(pa, 'ROOT', tmp_path)
    d=tmp_path/'signals/kr'; d.mkdir(parents=True)
    (d/'latest_final30_scored.json').write_text(json.dumps([{'code':str(i)} for i in range(30)]), encoding='utf-8')
    (d/'latest_prep_contract.json').write_text(json.dumps({'env':'practice','trade_date':'2026-06-15','as_of':'2026-06-12','final30_rows':30,'contract_ok':True,'trade_can_proceed':True}), encoding='utf-8')
    r=pa.find_and_repair_kr_prep_artifact(env='practice', trade_date='2026-06-16')
    assert not r.ok and r.reason=='STALE_PREP_ARTIFACT'

def test_as_of_defaults_previous_business_day(tmp_path, monkeypatch):
    monkeypatch.setattr(pa, 'ROOT', tmp_path)
    legacy=tmp_path/'runtime/watchlist/2026-06-15/final30_scored.json'
    legacy.parent.mkdir(parents=True)
    legacy.write_text(json.dumps([{'code':str(i)} for i in range(30)]), encoding='utf-8')
    r=pa.find_and_repair_kr_prep_artifact(env='practice', trade_date='2026-06-16')
    assert r.ok
    assert r.contract['as_of']=='2026-06-15'
