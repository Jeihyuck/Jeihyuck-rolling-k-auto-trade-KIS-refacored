import pytest
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
    quote={"bid":99.9,"ask":100,"last":100,"received_at":now.timestamp()-1,"collector_generation":"g1","source":"KIS_WEBSOCKET_COLLECTOR"}
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
                                    "collector_generation":"g1","source":"KIS_WEBSOCKET_COLLECTOR"},seen={},base=tmp_path)
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
                                    "collector_generation":"g1","source":"KIS_WEBSOCKET_COLLECTOR"},seen={},base=tmp_path)
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
    quote = {"bid":99.9,"ask":100,"last":100,"received_at":now.timestamp()-1,"collector_generation":"g1","source":"KIS_WEBSOCKET_COLLECTOR"}
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
        # A REST/cache or missing-source payload cannot be relabeled native.
        for source in ("REST", None):
            quote["source"] = source
            assert poll_hot_events(db,now_et=later,**kw) == []
            assert read_hot_retry_hints(now_epoch=later.timestamp(),**read) == set()
            quote["source"] = "KIS_WEBSOCKET_COLLECTOR"
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
    manifest=observer.publish_preminute_candidates(env="practice",scope="scope:epoch",trade_date=td,
        candidates=[(10,s,"NYSE",None,{"position_state":"NOT_HELD","entry_style":"ENTRY_PULLBACK"})
                    for s in ("AAA","BBB","CCC","DDD","EEE","BAD")],now_epoch=90,base=tmp_path)
    generation=manifest["generation"]
    db=observer.open_store(observer.store_path(env="practice",account_scope="scope:epoch",trade_date=td,base=tmp_path))
    try:
        rows=[(td,s,"NYSE",generation,"valid",at,at-1,"READY")
              for s,at in [("AAA",97),("BBB",99),("CCC",99),("DDD",99),("EEE",99)]]
        rows += [(td,"BAD","NYSE",generation,"future",99,100,"READY")]
        db.executemany("INSERT INTO hot_retry_events VALUES (?,?,?,?,?,?,?,?)",rows)
        db.commit()
        got=read_hot_retry_hints(env="practice",scope="scope:epoch",trade_date=td,generation=generation,
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
        q = {"bid": 100, "ask": 101, "last": 100, "received_at": now.timestamp()-2, "collector_generation": "g", "source": "KIS_WEBSOCKET_COLLECTOR"}
        options = dict(**scope, hot_pairs=[("AAA", "NYSE")], now_et=now,
                       quote_reader=lambda *args: q, seen={})
        assert len(poll_hot_events(db, **options)) == 1
        q["ask"] = 100.5
        clock = iter([now, now + timedelta(seconds=4)])
        assert poll_hot_events(db, **options, live_clock=lambda: next(clock)) == []
        assert db.execute("SELECT count(*) FROM hot_retry_events").fetchone()[0] == 0
    finally:
        db.close()


def test_hot_quote_evaluated_while_native_minute_request_waits(tmp_path, monkeypatch):
    import threading
    from trader.marketdata import us_ws_collector
    from trader.us.entry_timing import hot_event_evaluator
    from trader.us.execution import kis_us_client

    blocked, observed, halt = threading.Event(), threading.Event(), threading.Event()
    now = datetime(2026, 11, 25, 10, 10, tzinfo=ZoneInfo("America/New_York"))
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.astimezone(tz)
    monkeypatch.setattr(observer, "datetime", Clock)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("US_WS_COLLECTOR_ENABLED", "1")
    # Restore the process-local observer permission overrides after the fixture.
    for key in ("KIS_ENV", "DRY_RUN", "ALLOW_REAL_ORDER", "US_KIS_ORDER_ALLOWED", "TRADING_REGION",
                "US_ORDER_ARMED", "LIVE_TRADING_ENABLED", "US_LIVE_TRADING_ENABLED",
                "US_OPENING_OBSERVER_READ_ONLY", "US_SHARED_KIS_LIMITER_ENABLED"):
        monkeypatch.setenv(key, "0")
    monkeypatch.setattr(observer.os, "getppid", lambda: 1 if halt.is_set() else 123)
    monkeypatch.setattr(kis_us_client, "KisUSClient", lambda **kw: object())
    monkeypatch.setattr(us_ws_collector, "ensure_started", lambda **kw: None)
    monkeypatch.setattr(us_ws_collector, "subscribe_many", lambda *args, **kw: None)
    monkeypatch.setattr(us_ws_collector, "get_quote", lambda *args, **kw: None)
    lanes = {"HOT": [("AAA", "NYSE")], "WARM": [], "COLD": []}
    monkeypatch.setattr(observer, "collection_priorities", lambda **kw: (lanes, "VALID"))
    def native_wait(*args, **kwargs):
        blocked.set()
        observed.wait(3)
        halt.set()
        return {"requests": 1, "changed": 0}
    def evaluate(db, **kwargs):
        if blocked.wait(2):
            db.execute("SELECT count(*) FROM verified_bars").fetchone()
            observed.set()
    monkeypatch.setattr(observer, "collect_hot_minutes", native_wait)
    monkeypatch.setattr(hot_event_evaluator, "poll_hot_events", evaluate)
    assert observer.run_observer(env="practice", trade_date="2026-11-25", scope="scope:epoch",
        parent_pid=123, watchlist=[{"symbol": "AAA", "exchange": "NYSE"}], max_runtime_sec=5) == 0
    assert observed.is_set() and halt.is_set()


def test_identical_republication_keeps_only_fresh_cross_tick_hot_hint(tmp_path,monkeypatch):
    from trader.us.entry_timing import opportunity
    from trader.us.entry_timing.hot_event_evaluator import read_hot_retry_hints
    monkeypatch.setattr(opportunity,"effective_mode",lambda:"ENFORCE")
    monkeypatch.setattr(opportunity,"evaluate_price_opportunity",lambda **kw:
        {"state":"READY","reason":"test_only","facts":{},"broker_authorized":False})
    now=datetime(2026,11,25,10,10,tzinfo=ZoneInfo("America/New_York"))
    epoch=now.timestamp()
    td=now.date().isoformat()
    scope=dict(env="practice",scope="scope:epoch",trade_date=td,base=tmp_path)
    def publish(at, family="ENTRY_PULLBACK", pivot=100):
        return observer.publish_preminute_candidates(**scope,now_epoch=at,
            candidates=[(10,"AAA","NYSE",None,{"position_state":"NOT_HELD",
                         "entry_style":family,"pivot_price":pivot})])
    first=publish(epoch-1)
    db=observer.open_store(observer.store_path(env="practice",account_scope="scope:epoch",trade_date=td,base=tmp_path))
    try:
        assert len(poll_hot_events(db,**scope,hot_pairs=[("AAA","NYSE")],now_et=now,
            quote_reader=lambda *a:{"bid":99.9,"ask":100,"last":100,"received_at":epoch-1,
                        "collector_generation":"g","source":"KIS_WEBSOCKET_COLLECTOR"},seen={}))==1
        second=publish(epoch+1)
        assert second["generation"]==first["generation"]
        def hints(manifest, at):
            return read_hot_retry_hints(**scope,generation=manifest["generation"],
                       waiting={("AAA","NYSE")},now_epoch=at)
        assert hints(second,epoch+1)=={("AAA","NYSE")}
        assert hints(second,epoch+5)==set()  # quote age is six seconds
        changed=publish(epoch+2,pivot=101)
        assert changed["generation"]!=second["generation"]
        assert hints(changed,epoch+2)==set()
        assert hints(second,epoch+2)==set()  # superseded generation still has a fresh quote
        expired=publish(epoch+183,pivot=101)
        assert expired["generation"]!=changed["generation"]
        assert hints(expired,epoch+183)==set()
        manifest_path=observer.priority_manifest_path(**scope)
        manifest_path.unlink()
        assert hints(expired,epoch+183)==set()
    finally:
        db.close()


def test_collection_only_warm_promotion_cannot_emit_or_consume_execution_hot_hint(tmp_path,monkeypatch):
    from trader.us.entry_timing import opportunity
    from trader.us.entry_timing.hot_event_evaluator import read_hot_retry_hints
    monkeypatch.setattr(opportunity,"effective_mode",lambda:"ENFORCE")
    now=datetime(2026,11,25,10,10,tzinfo=ZoneInfo("America/New_York"))
    scope=dict(env="practice",scope="scope:epoch",trade_date=now.date().isoformat(),base=tmp_path)
    manifest=observer.publish_preminute_candidates(**scope,now_epoch=now.timestamp()-1,
        candidates=[(10-i,s,"NYSE",None,{"position_state":"NOT_HELD","entry_style":"ENTRY_PULLBACK"})
                    for i,s in enumerate(("AAA","BBB","CCC","DDD","EEE"))])
    db=observer.open_store(observer.store_path(env=scope["env"],account_scope=scope["scope"],
        trade_date=scope["trade_date"],base=tmp_path))
    try:
        # A collection fallback or stale caller can label EEE HOT, but the
        # current canonical execution manifest places EEE in WARM.
        def quote(*args):
            raise AssertionError("WARM cannot consume execution quote")
        assert poll_hot_events(db,**scope,hot_pairs=[("EEE","NYSE")],now_et=now,
            quote_reader=quote,seen={})==[]
        db.execute("INSERT INTO hot_retry_events VALUES (?,?,?,?,?,?,?,?)",
            (scope["trade_date"],"EEE","NYSE",manifest["generation"],"old-ready",
             now.timestamp(),now.timestamp()-1,"READY"))
        db.commit()
        assert read_hot_retry_hints(**scope,generation=manifest["generation"],
            waiting={("EEE","NYSE")},now_epoch=now.timestamp())==set()
    finally:
        db.close()


def test_dynamic_collection_rank_requires_distinct_fresh_evidence_and_preserves_execution_scope():
    from trader.us.entry_timing.hot_event_evaluator import CollectionRankState
    rank=CollectionRankState()
    h=[(s,"NYSE") for s in ("AAA","BBB","CCC","DDD")]
    w=[("EEE","NYSE"),("FFF","NYSE")]
    lanes={"HOT":h,"WARM":w,"COLD":[("GGG","NYSE")]}
    def obs(at,weak=True):
        return {h[-1]:dict(state="PRICE_WAIT" if weak else "READY",received=at,receipt_id=("g",at)),
                w[0]:dict(state="READY",received=at,receipt_id=("g",at))}
    assert rank.update(lanes,"g1",obs(100),100) is None
    assert rank.update(lanes,"g1",obs(100),101) is None
    assert rank.update(lanes,"g1",obs(102),102)==(h[-1],w[0])
    promoted=rank.apply(lanes,"g1",103)
    assert promoted["HOT"]==h[:3]+[w[0]]
    assert promoted["WARM"]==[h[-1],w[1]]
    assert promoted["COLD"]==lanes["COLD"] and lanes["HOT"]==h
    assert rank.apply(lanes,"g2",103)==lanes
    assert rank.apply(lanes,"g1",108)==lanes
    assert rank.update(lanes,"g1",obs(104,False),104) is None
    assert rank.apply(lanes,"g1",104)==lanes


def test_dynamic_collection_does_not_demote_missing_native_evidence():
    from trader.us.entry_timing.hot_event_evaluator import CollectionRankState
    rank=CollectionRankState()
    lanes={"HOT":[("AAA","NYSE")],"WARM":[("BBB","NYSE")],"COLD":[]}
    for at in (100,102):
        assert rank.update(lanes,"g",{("BBB","NYSE"):dict(state="READY",received=at,receipt_id=("g",at))},at) is None
    assert rank.apply(lanes,"g",102)==lanes


def test_independent_collection_refresh_uses_canonical_native_evidence_without_writing_hints(tmp_path,monkeypatch):
    from datetime import timedelta
    from trader.us.entry_timing import opportunity
    from trader.us.entry_timing.hot_event_evaluator import CollectionRankState,refresh_collection_rank
    monkeypatch.setattr(opportunity,"effective_mode",lambda:"ENFORCE")
    now=datetime(2026,11,25,10,10,tzinfo=ZoneInfo("America/New_York"))
    epoch=now.timestamp()
    scope=dict(env="practice",scope="scope:epoch",trade_date=now.date().isoformat(),base=tmp_path)
    syms=("AAA","BBB","CCC","DDD","EEE")
    pairs=[(s,"NYSE") for s in syms]
    manifest=observer.publish_preminute_candidates(**scope,now_epoch=epoch-1,
        candidates=[(20-i,s,"NYSE",None,{"position_state":"NOT_HELD","entry_style":"ENTRY_PULLBACK","pivot_price":100+i})
                    for i,s in enumerate(syms)])
    lanes,state=observer.load_preminute_priority(**scope,pairs=pairs,now_epoch=epoch)
    assert state=="VALID"
    rank=CollectionRankState()
    db=observer.open_store(observer.store_path(env=scope["env"],account_scope=scope["scope"],
        trade_date=scope["trade_date"],base=tmp_path))
    evaluations=[]
    def decision(**kw):
        evaluations.append(kw)
        return {"state":"READY" if kw["prep"]["pivot_price"]==104 else "WAIT",
                "reason":"test" if kw["prep"]["pivot_price"]==104 else "reversal_not_confirmed"}
    monkeypatch.setattr(opportunity,"evaluate_price_opportunity",decision)
    try:
        clock=[now]
        batch_calls=[]
        def quotes(items):
            batch_calls.append(tuple(items))
            return {pair:{"bid":99.9,"ask":100,"last":100,"received_at":clock[0].timestamp()-1,
                          "collector_generation":"g","source":"KIS_WEBSOCKET_COLLECTOR"} for pair in items}
        def refresh(at):
            clock[0]=at
            return refresh_collection_rank(db,**scope,rank=rank,lanes=lanes,now_et=at,
                       quote_many=quotes,live_clock=lambda:clock[0])
        assert refresh(now) is None
        assert refresh(now+timedelta(seconds=2))==(pairs[3],pairs[4])
        assert rank.apply(lanes,manifest["generation"],epoch+2)["HOT"]==pairs[:3]+[pairs[4]]
        assert len(batch_calls)==2 and all(len(x)==5 for x in batch_calls)
        assert len(evaluations)==10
        assert db.execute("SELECT count(*) FROM hot_retry_events").fetchone()[0]==0
        current,state=observer.load_preminute_priority(**scope,pairs=pairs,now_epoch=epoch+2)
        assert current==lanes and state=="VALID"
        assert rank.apply(lanes,manifest["generation"],epoch+7)==lanes
    finally:
        db.close()


@pytest.mark.parametrize("delay_stage", ["none", "quote", "bars", "evaluate"])
def test_dynamic_rank_does_not_promote_using_out_of_budget_evidence(tmp_path, monkeypatch, delay_stage):
    from datetime import timedelta
    from trader.us.entry_timing import opportunity
    from trader.us.entry_timing.hot_event_evaluator import CollectionRankState, refresh_collection_rank

    now = datetime(2026, 11, 25, 10, 10, tzinfo=ZoneInfo("America/New_York"))
    scope = dict(env="practice", scope="scope:epoch", trade_date=now.date().isoformat(), base=tmp_path)
    pairs = [(s, "NYSE") for s in ("AAA", "BBB", "CCC", "DDD", "EEE")]
    manifest = observer.publish_preminute_candidates(**scope, now_epoch=now.timestamp()-1,
        candidates=[(20-i, s, e, None, {"position_state":"NOT_HELD", "entry_style":"ENTRY_PULLBACK",
                                      "pivot_price":100+i}) for i,(s,e) in enumerate(pairs)])
    lanes, status = observer.load_preminute_priority(**scope, pairs=pairs, now_epoch=now.timestamp())
    assert status == "VALID"
    rank = CollectionRankState()
    clock = [now, 0.0, "none"]
    evaluated = []
    monkeypatch.setattr(opportunity, "effective_mode", lambda: "ENFORCE")

    def quotes(items):
        if clock[2] == "quote":
            clock[1] = 0.81
        return {p: {"bid":99.9, "ask":100, "last":100, "received_at":clock[0].timestamp()-1,
                    "collector_generation":"g", "source":"KIS_WEBSOCKET_COLLECTOR"} for p in items}

    def bars(db, **kw):
        if clock[2] == "bars" and kw["symbol"] == "EEE":
            clock[1] = 0.81
        return []

    def evaluate(**kw):
        pivot = kw["prep"]["pivot_price"]
        evaluated.append(pivot)
        if clock[2] == "evaluate" and pivot == 104:
            clock[1] = 0.81
        return {"state":"READY" if pivot == 104 else "WAIT",
                "reason":"test" if pivot == 104 else "reversal_not_confirmed"}

    monkeypatch.setattr(observer, "load_bars", bars)
    monkeypatch.setattr(opportunity, "evaluate_price_opportunity", evaluate)

    def refresh():
        return refresh_collection_rank(None, **scope, rank=rank, lanes=lanes, now_et=clock[0],
            quote_many=quotes, live_clock=lambda:clock[0], monotonic_clock=lambda:clock[1])

    assert refresh() is None  # first fresh receipt is not sufficient
    evaluated.clear()
    clock[:] = [now+timedelta(seconds=2), 0.0, delay_stage]
    chosen = refresh()
    if delay_stage == "none":
        assert chosen == (pairs[3], pairs[4])
        assert rank.apply(lanes, manifest["generation"], clock[0].timestamp())["HOT"][-1] == pairs[4]
    else:
        assert chosen is None
        assert rank.apply(lanes, manifest["generation"], clock[0].timestamp()) == lanes
        if delay_stage == "quote":
            assert evaluated == []
        elif delay_stage == "bars":
            assert 104 not in evaluated
        else:
            assert 104 in evaluated
