from datetime import date
from decimal import Decimal
import pandas as pd
import pytest
from trader.kr.infinite.config import InfiniteConfig
from trader.kr.infinite.data_loader import load_and_validate_csv
from trader.kr.infinite.models import SleeveState
from trader.kr.infinite.replay import replay_decision, run_replay, validate_policy_identity


def test_data_quality_and_checksum(tmp_path):
    path=tmp_path/"122630.csv"
    pd.DataFrame([{"date":"2026-08-11","open":10,"high":12,"low":9,"close":11,"volume":100},
                  {"date":"2026-08-12","open":11,"high":13,"low":10,"close":12,"volume":110}]).to_csv(path,index=False)
    frame,report=load_and_validate_csv(path,source="user_csv")
    assert report.rows==2 and len(report.checksum)==64 and report.adjusted_status=="UNVERIFIED"


def test_runtime_replay_identity_and_point_in_time():
    cfg=InfiniteConfig(); validate_policy_identity(cfg,cfg)
    signal={"as_of":date(2026,8,12),"execution_date":date(2026,8,13),"state":"KR_NORMAL"}
    assert replay_decision(cfg,SleeveState("c"),signal,Decimal("20000")).action=="BUY"
    signal["as_of"]=signal["execution_date"]
    with pytest.raises(ValueError): replay_decision(cfg,SleeveState("c"),signal,Decimal("20000"))


def test_multi_day_replay_uses_prior_signal_and_completes_cycle():
    rows=[]
    for day,price in [(2,10000),(3,10000),(4,13000),(5,10000)]:
        rows.append({"signal_date":date(2026,1,day-1),"trade_date":date(2026,1,day),"open":price,
                     "regime_state":"KR_NORMAL","data_quality":"OK"})
    result=run_replay(rows)
    assert result.completed_cycles==1 and len(result.equity_curve)==4 and result.maximum_used_units<=40
