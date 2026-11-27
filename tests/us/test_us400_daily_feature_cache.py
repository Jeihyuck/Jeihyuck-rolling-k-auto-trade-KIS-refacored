from datetime import date,timedelta
import copy
import pytest

from trader.us.runner import trade_tick_runner as tick
from trader.us.runner.daily_feature_cache import reuse_daily_metrics

@pytest.mark.parametrize("mode",["append","correction","truncate","future","duplicate","wrong_scope"])
def test_daily_ma_rs_incremental_exact_parity(tmp_path,monkeypatch,mode):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("WSL_RUN_MARKET","US")
    monkeypatch.setenv("KIS_ENV","practice")
    monkeypatch.setenv("US_SHARED_NATIVE_FEATURES_ENABLED","1")
    start=date(2025,12,1)
    rows=[{"date":(start+timedelta(days=i)).isoformat(),"close":100+i*.013+(i%13)*.21}
          for i in range(260)]
    day="2026-11-25"
    pos=dict(broker_account_hash="a",trading_epoch_id="e",symbol="ABC",
             exchange="NASDAQ",strategy_owner="US_STANDARD")
    original=tick._compute_completed_daily_metrics
    calls=[]
    def compute(r,d):
        calls.append(len(r))
        return original(r,d)
    def run(r,p=pos):
        return reuse_daily_metrics(r,day,position=p,row_date=tick._row_date,
                                   row_close=tick._row_close,original=compute)
    assert run(rows[:200])==original(rows[:200],day)
    assert run(rows[:221])==original(rows[:221],day)
    assert calls==[200]
    next_rows=copy.deepcopy(rows)
    scope=pos
    if mode=="correction": next_rows[5]["close"]+=1
    if mode=="truncate": next_rows=next_rows[10:210]
    if mode=="future": next_rows.append({"date":day,"close":999})
    if mode=="duplicate": next_rows.append(dict(next_rows[-1]))
    if mode=="wrong_scope": scope=dict(pos,broker_account_hash="b")
    assert run(next_rows,scope)==original(next_rows,day)
    assert calls==([200] if mode=="append" else [200,len(next_rows)])
    assert run(next_rows,scope)==original(next_rows,day)
    assert calls==([200] if mode=="append" else [200,len(next_rows)])
