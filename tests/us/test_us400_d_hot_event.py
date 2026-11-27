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


def test_real_hot_evaluator_never_creates_ready_before_30_native_bars(tmp_path,monkeypatch):
    from trader.us.entry_timing import opportunity
    monkeypatch.setattr(opportunity,"effective_mode",lambda:"ENFORCE")
    now=datetime(2026,11,25,10,10,tzinfo=ZoneInfo("America/New_York"))
    td=now.date().isoformat()
    observer.publish_preminute_candidates(env="practice",scope="scope:epoch",trade_date=td,
        candidates=[(10,"AAA","NYSE",None,{"position_state":"NOT_HELD","entry_style":"ENTRY_PULLBACK"})],
        now_epoch=now.timestamp()-1,base=tmp_path)
    db=observer.open_store(observer.store_path(env="practice",account_scope="scope:epoch",trade_date=td,base=tmp_path))
    try:
        result=poll_hot_events(db,env="practice",scope="scope:epoch",trade_date=td,
            hot_pairs=[("AAA","NYSE")],now_et=now,
            quote_reader=lambda *a:{"bid":99.9,"ask":100,"last":100,"received_at":now.timestamp()-1,
                                    "collector_generation":"g1"},seen={},base=tmp_path)
        assert len(result)==1
        assert result[0]["decision"]["state"]=="WAIT"
        assert result[0]["decision"]["reason"]=="opening_minute_bars_insufficient"
        assert result[0]["decision"]["broker_authorized"] is False
        assert result[0]["is_order_authorization"] is False
    finally:
        db.close()


def test_ready_event_handoff_only_retries_waited_candidate_once(tmp_path,monkeypatch):
    from trader.us.entry_timing import opportunity
    from trader.us.entry_timing.hot_event_evaluator import read_hot_retry_hints,iter_candidates_with_hot_retries
    monkeypatch.setattr(opportunity,"effective_mode",lambda:"ENFORCE")
    now=datetime(2026,11,25,10,10,tzinfo=ZoneInfo("America/New_York"))
    td=now.date().isoformat()
    candidates=[(10,"AAA","NASD",None,{"position_state":"NOT_HELD","entry_style":"ENTRY_PULLBACK"}),
                (9,"BBB","NYSE",None,{"position_state":"NOT_HELD","entry_style":"ENTRY_PULLBACK"})]
    manifest=observer.publish_preminute_candidates(env="practice",scope="scope:epoch",trade_date=td,
        candidates=candidates,now_epoch=now.timestamp()-1,base=tmp_path)
    db=observer.open_store(observer.store_path(env="practice",account_scope="scope:epoch",trade_date=td,base=tmp_path))
    monkeypatch.setattr(opportunity,"evaluate_price_opportunity",lambda **kw:
        {"state":"READY","reason":"test_only","facts":{},"broker_authorized":False})
    try:
        event=poll_hot_events(db,env="practice",scope="scope:epoch",trade_date=td,
            hot_pairs=[("AAA","NASDAQ")],now_et=now,
            quote_reader=lambda *a:{"bid":99.9,"ask":100,"last":100,"received_at":now.timestamp()-1,
                                    "collector_generation":"g1"},seen={},base=tmp_path)
        assert len(event)==1 and not event[0]["is_order_authorization"]
        kw=dict(env="practice",scope="scope:epoch",trade_date=td,
                generation=manifest["generation"],now_epoch=now.timestamp(),base=tmp_path)
        waiting=set()
        def reader(keys): return read_hot_retry_hints(waiting=keys,**kw)
        iterator=iter_candidates_with_hot_retries(candidates,waiting=waiting,ready_reader=reader)
        assert next(iterator)==(0,candidates[0])
        waiting.add(("AAA","NASDAQ"))
        assert next(iterator)==(0,candidates[0])
        waiting.add(("AAA","NASDAQ"))  # repeated WAIT cannot retry again
        assert list(iterator)==[(1,candidates[1])]
        assert read_hot_retry_hints(waiting=waiting,**dict(kw,generation="different"))==set()
        assert read_hot_retry_hints(waiting=waiting,**dict(kw,now_epoch=now.timestamp()+6))==set()
        assert read_hot_retry_hints(waiting=waiting,**dict(kw,scope="foreign"))==set()
        assert read_hot_retry_hints(waiting={("BBB","NYSE")},**kw)==set()
    finally:
        db.close()


def test_same_price_new_native_receipt_refreshes_hint_and_suspect_revokes(tmp_path, monkeypatch):
    from datetime import timedelta
    from trader.us.entry_timing import opportunity
    from trader.us.entry_timing.hot_event_evaluator import read_hot_retry_hints
    monkeypatch.setattr(opportunity, "effective_mode", lambda: "ENFORCE")
    monkeypatch.setattr(opportunity, "evaluate_price_opportunity", lambda **kw:
        {"state":"READY","reason":"test_only","facts":{},"broker_authorized":False})
    now = datetime(2026,11,25,10,10,tzinfo=ZoneInfo("America/New_York"))
    td = now.date().isoformat()
    manifest = observer.publish_preminute_candidates(env="practice",scope="scope:epoch",trade_date=td,
        candidates=[(10,"AAA","NYSE",None,{"position_state":"NOT_HELD","entry_style":"ENTRY_PULLBACK"})],
        now_epoch=now.timestamp()-1,base=tmp_path)
    db = observer.open_store(observer.store_path(env="practice",account_scope="scope:epoch",trade_date=td,base=tmp_path))
    quote = {"bid":99.9,"ask":100,"last":100,"received_at":now.timestamp()-1,"collector_generation":"g1"}
    kw = dict(env="practice",scope="scope:epoch",trade_date=td,hot_pairs=[("AAA","NYSE")],
              quote_reader=lambda *a:dict(quote),seen={},base=tmp_path)
    read = dict(env="practice",scope="scope:epoch",trade_date=td,generation=manifest["generation"],
                waiting={("AAA","NYSE")},base=tmp_path)
    try:
        assert len(poll_hot_events(db,now_et=now,**kw)) == 1
        assert poll_hot_events(db,now_et=now,**kw) == []
        later = now + timedelta(seconds=6)
        assert read_hot_retry_hints(now_epoch=later.timestamp(),**read) == set()
        quote["received_at"] = later.timestamp()-1
        assert len(poll_hot_events(db,now_et=later,**kw)) == 1
        assert read_hot_retry_hints(now_epoch=later.timestamp(),**read) == {("AAA","NYSE")}
        quote["suspect"] = True
        assert poll_hot_events(db,now_et=later,**kw) == []
        assert read_hot_retry_hints(now_epoch=later.timestamp(),**read) == set()
    finally:
        db.close()


def test_hint_limit_applies_after_waiting_and_evidence_filter(tmp_path):
    from trader.us.entry_timing.hot_event_evaluator import read_hot_retry_hints
    td="2026-11-25"
    db=observer.open_store(observer.store_path(env="practice",account_scope="scope:epoch",trade_date=td,base=tmp_path))
    try:
        rows=[(td,s,"NYSE","g","valid",at,at-1,"READY")
              for s,at in [("AAA",97),("BBB",99),("CCC",99),("DDD",99),("EEE",99)]]
        rows += [(td,"BAD","NYSE","g","future",99,100,"READY")]
        db.executemany("INSERT INTO hot_retry_events VALUES (?,?,?,?,?,?,?,?)",rows)
        db.commit()
        got=read_hot_retry_hints(env="practice",scope="scope:epoch",trade_date=td,generation="g",
            waiting={("AAA","NYSE"),("BAD","NYSE")},now_epoch=100,base=tmp_path)
        assert got=={("AAA","NYSE")}
    finally:
        db.close()


def test_hot_collection_hysteresis_is_bounded_and_does_not_retain_removed_symbol(tmp_path):
    td="2026-11-25"
    symbols=["AAA","BBB","CCC","DDD","EEE","FFF","GGG"]
    kw=dict(env="practice",scope="scope:epoch",trade_date=td,base=tmp_path)
    def publish(order, epoch):
        observer.publish_preminute_candidates(**kw,now_epoch=epoch,
            candidates=[(20-i,s,"NYSE",None,{"position_state":"NOT_HELD"}) for i,s in enumerate(order)])
        lanes,state=observer.load_preminute_priority(**kw,now_epoch=epoch,pairs=[(s,"NYSE") for s in order])
        assert state=="VALID"
        assert len(lanes["HOT"])==4
        assert set(sum(lanes.values(),[]))=={(s,"NYSE") for s in order}
        assert len(sum(lanes.values(),[]))==len(order)
        return {s for s,e in lanes["HOT"]}
    assert publish(symbols,1000)=={"AAA","BBB","CCC","DDD"}
    changed=["AAA","BBB","CCC","EEE","DDD","FFF","GGG"]
    assert publish(changed,1010)=={"AAA","BBB","CCC","DDD"}
    assert publish(changed,1020)=={"AAA","BBB","CCC","EEE"}
    # An ineligible incumbent is removed immediately, not retained by hysteresis.
    assert publish(["AAA","BBB","CCC","FFF","DDD","GGG"],1030)=={"AAA","BBB","CCC","FFF"}
    # Expired manifests never keep a previous rank over the fresh desired one.
    assert publish(["AAA","BBB","CCC","DDD","FFF","GGG"],1300)=={"AAA","BBB","CCC","DDD"}

def test_quote_expiring_during_evaluation_revokes_ready_hint(tmp_path, monkeypatch):
    from datetime import timedelta
    from trader.us.entry_timing import opportunity

    now = datetime(2026, 11, 25, 10, 10, tzinfo=ZoneInfo("America/New_York"))
    scope = dict(env="practice", scope="scope:epoch", trade_date=now.date().isoformat(), base=tmp_path)
    observer.publish_preminute_candidates(**scope, now_epoch=now.timestamp()-1,
        candidates=[(10, "AAA", "NYSE", None, {"position_state": "NOT_HELD", "entry_style": "ENTRY_PULLBACK"})])
    monkeypatch.setattr(opportunity, "effective_mode", lambda: "ENFORCE")
    monkeypatch.setattr(opportunity, "evaluate_price_opportunity", lambda **kwargs:
        {"state": "READY", "reason": "test", "facts": {}})
    db = observer.open_store(observer.store_path(env=scope["env"], account_scope=scope["scope"],
        trade_date=scope["trade_date"], base=tmp_path))
    try:
        q = {"bid": 100, "ask": 101, "last": 100, "received_at": now.timestamp()-2, "collector_generation": "g"}
        options = dict(**scope, hot_pairs=[("AAA", "NYSE")], now_et=now,
                       quote_reader=lambda *args: q, seen={})
        assert len(poll_hot_events(db, **options)) == 1
        q["ask"] = 100.5
        clock = iter([now, now + timedelta(seconds=4)])
        assert poll_hot_events(db, **options, live_clock=lambda: next(clock)) == []
        assert db.execute("SELECT count(*) FROM hot_retry_events").fetchone()[0] == 0
    finally:
        db.close()
