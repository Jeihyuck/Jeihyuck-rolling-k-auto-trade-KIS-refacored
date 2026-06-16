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
