from datetime import datetime
from zoneinfo import ZoneInfo
from trader.us.entry_timing import opening_observer as observer
from trader.us.entry_timing.hot_event_evaluator import poll_hot_events


def test_hot_event_reevaluates_only_changed_native_evidence(tmp_path,monkeypatch):
    from trader.us.entry_timing import opportunity
    monkeypatch.setattr(opportunity,"effective_mode",lambda:"ENFORCE")
    now=datetime(2026,11,25,10,10,tzinfo=ZoneInfo("America/New_York"))
    td=now.date().isoformat()
    pairs=[("AAA","NYSE"),("BBB","NYSE"),("TQQQ","NASDAQ")]
    observer.publish_preminute_candidates(env="practice",scope="scope:epoch",trade_date=td,
        candidates=[(10,s,e,None,{"position_state":"NOT_HELD","entry_style":"ENTRY_PULLBACK"})
                    for s,e in pairs],now_epoch=now.timestamp()-1,base=tmp_path)
    db=observer.open_store(observer.store_path(env="practice",account_scope="scope:epoch",trade_date=td,base=tmp_path))
    calls=[]
    monkeypatch.setattr(opportunity,"evaluate_price_opportunity",lambda **kw:
        calls.append(kw) or {"state":"WAIT","reason":"opening_minute_bars_insufficient","facts":{},
                              "broker_authorized":False})
    seen={}
    quote={"bid":99.9,"ask":100,"last":100,"received_at":now.timestamp()-1,"collector_generation":"g1"}
    kw=dict(env="practice",scope="scope:epoch",trade_date=td,hot_pairs=[pairs[0],pairs[2]],now_et=now,
            quote_reader=lambda *a:dict(quote),seen=seen,base=tmp_path)
    try:
        first=poll_hot_events(db,**kw)
        assert len(first)==1 and first[0]["symbol"]=="AAA"
        assert first[0]["is_order_authorization"] is False
        assert poll_hot_events(db,**kw)==[]
        quote["ask"]=100.1
        assert len(poll_hot_events(db,**kw))==1
        quote["received_at"]=now.timestamp()-6
        assert poll_hot_events(db,**kw)==[]
        assert len(calls)==2
    finally:
        db.close()
