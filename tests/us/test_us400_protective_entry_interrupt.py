from datetime import datetime
from threading import Event
from zoneinfo import ZoneInfo
import time
import pytest

from trader.us.runner.protective_entry_interrupt import make_hard_stop_poll, ProtectiveEntryInterrupted
from trader.us.runner.trade_tick_runner import run_entry_eval_with_timeout


def test_protective_interrupt_cancels_entry_before_return():
    cancelled = Event()
    snap = {"symbol":"ABC"}
    with pytest.raises(ProtectiveEntryInterrupted) as err:
        run_entry_eval_with_timeout(lambda: [], timeout_sec=1, cancel_event=cancelled,
                                    protective_poll=lambda: snap)
    assert err.value.snapshot == snap and cancelled.is_set()


@pytest.mark.parametrize("case",["valid","owner","duplicate","stale","future","no_scope","not_breach","excluded","zero_qty"])
def test_hard_stop_poll_only_verified_fresh_unique_swing(monkeypatch,case):
    from trader.us import entry_exit_contract as contract, market_calendar
    from trader.marketdata import us_ws_collector
    from types import SimpleNamespace
    monkeypatch.setattr(contract,"us_entry_exit_contract_integrity_state",lambda *a:"VALID")
    monkeypatch.setattr(contract,"contract_exit_config",lambda p:{"hard_stop":.06})
    monkeypatch.setattr(market_calendar,"now_ny",lambda:datetime(2026,11,25,11,tzinfo=ZoneInfo("America/New_York")))
    now=time.time()
    q={"last":93 if case!="not_breach" else 97,"source":"KIS_WEBSOCKET_COLLECTOR",
       "collector_generation":"g","received_at":now+1 if case=="future" else now-10 if case=="stale" else now}
    monkeypatch.setattr(us_ws_collector,"get_quote",lambda *a,**kw:q)
    p=dict(symbol="ABC",exchange="NASDAQ",strategy_owner="US_STANDARD",book="SWING_BOOK",
           horizon="SWING_CARRY",position_lifecycle_id="life",position_cycle_id="life",
           portfolio_epoch_id="portfolio",trading_epoch_id="epoch",broker_account_hash="account",
           qty=5,orderable_qty=5,entry_price=100)
    if case=="owner": p["strategy_owner"]="TQQQ_INFINITE"
    if case=="no_scope": p.pop("position_cycle_id")
    if case=="zero_qty": p["orderable_qty"]=0
    ctx=SimpleNamespace(broker_submit_allowed=lambda:True,remaining_sec=lambda:30)
    poll=make_hard_stop_poll([p,p] if case=="duplicate" else [p],
              excluded_symbols={"ABC"} if case=="excluded" else set(),
              trade_date="2026-11-25",env="practice",context=ctx)
    result=poll() if poll else None
    assert (result is not None)==(case=="valid")
    assert "resolved_current_price" not in p
    if result: assert result["resolved_current_price"]==93


@pytest.mark.parametrize("case",["valid","stale","no_permission","wrong_family","wrong_symbol",
                                 "pending","expired_after_evaluate","permission_after_pending","wrong_cycle","wrong_account","wrong_type","scope_conflict"])
def test_interrupt_requires_canonical_hard_stop_and_pending_gate(monkeypatch,case):
    from trader.us import market_calendar
    from trader.us.runner import protective_entry_interrupt as guard
    from types import SimpleNamespace
    monkeypatch.setattr(market_calendar,"now_ny",lambda:datetime(2026,11,25,11,tzinfo=ZoneInfo("America/New_York")))
    active=[case!="no_permission"]
    calls=[]
    identity=dict(symbol="ABC",strategy_owner="US_STANDARD",position_lifecycle_id="l",
                  position_cycle_id="l",portfolio_epoch_id="p",trading_epoch_id="e",broker_account_hash="a")
    snap={**identity,"exit_quote_evidence":{"asof_epoch":time.time()-(10 if case=="stale" else 0)}}
    ctx=SimpleNamespace(broker_submit_allowed=lambda:active[0])
    def evaluate(snapshot,clock):
        calls.append("canonical")
        if case=="expired_after_evaluate":
            snapshot["exit_quote_evidence"]["asof_epoch"]-=10
        intent={**identity,"side":"SELL","symbol":"XYZ" if case=="wrong_symbol" else "ABC",
                "family":"TAKE_PROFIT" if case=="wrong_family" else "HARD_STOP",
                "exit_type":"persistent_soft_stop_full_exit" if case=="wrong_type" else "hard_stop"}
        if case=="wrong_cycle": intent.update(position_cycle_id="new",position_lifecycle_id="new")
        if case=="wrong_account": intent["broker_account_hash"]="other"
        if case=="scope_conflict": intent["meta"]={"broker_account_hash":"other"}
        return [intent]
    def suppress(intents):
        calls.append("pending")
        if case=="permission_after_pending": active[0]=False
        return [] if case=="pending" else intents
    def route(intents):
        calls.append("router")
        return {"orders":[{"status":"ACK"}]}
    selected,result=guard.execute_hard_stop_interrupt(
        snap,trade_date="2026-11-25",context=ctx,evaluate=evaluate,
        classify_family=lambda i:i["family"],suppress=suppress,route=route)
    assert (result is not None)==(case=="valid")
    assert ("router" in calls)==(case=="valid")
    if case=="valid": assert calls==["canonical","pending","router"] and selected[0]["symbol"]=="ABC"
    if case in {"stale","no_permission"}: assert not calls


@pytest.mark.parametrize("case",["fresh","stale","recovered","closed","bad_identity"])
def test_idle_watch_is_readonly_identity_only(monkeypatch,case):
    from trader.us import market_calendar
    from trader.marketdata import us_ws_collector
    from trader.us.runner.protective_entry_interrupt import make_idle_hard_stop_poll
    clock=datetime(2026,11,25,17 if case=="closed" else 11,tzinfo=ZoneInfo("America/New_York"))
    monkeypatch.setattr(market_calendar,"now_ny",lambda:clock)
    identity=dict(symbol="ABC",strategy_owner="US_STANDARD",position_lifecycle_id="l",
                  position_cycle_id="l",portfolio_epoch_id="p",trading_epoch_id="e",broker_account_hash="a")
    if case=="bad_identity": identity.pop("position_cycle_id")
    calls=[]
    def quote(*a,**kw):
        calls.append(a)
        return dict(last=98 if case=="recovered" else 93,source="KIS_WEBSOCKET_COLLECTOR",
                    collector_generation="g",received_at=time.time()-(10 if case=="stale" else 0))
    monkeypatch.setattr(us_ws_collector,"get_quote",quote)
    poll=make_idle_hard_stop_poll([dict(identity=identity,entry=100,hard=.06,exchange="NASDAQ")],
                                trade_date="2026-11-25",env="practice")
    found=poll() if poll else None
    assert found==(identity if case=="fresh" else None)
    if case in {"closed","bad_identity"}: assert not calls


def test_idle_sleep_wakeup_does_not_wait_full_slot(monkeypatch):
    from trader.us.runner import trade_session_runner as session
    slept=[]
    monkeypatch.setattr(session.time_mod,"sleep",lambda n:slept.append(n))
    monkeypatch.setattr(session,"write_heartbeat_file",lambda *a,**kw:None)
    identity={"symbol":"ABC"}
    assert session.sleep_with_heartbeat("am","run",1,60,protective_poll=lambda:identity)==identity
    assert slept==[5]


def test_protective_wakeup_rejects_unverified_identity_before_broker(monkeypatch):
    from trader.us.runner.trade_tick_runner import run_trade_tick
    assert run_trade_tick(protective_wakeup_identity={"symbol":"ABC"})["reason"]=="protective_wakeup_not_authorized"


def test_idle_route_exception_is_uncertain_and_never_retried():
    from trader.us.runner.protective_entry_interrupt import route_idle_hard_stop_once
    calls=[]
    intent={"symbol":"ABC","side":"SELL"}
    def route(items):
        calls.append(items)
        raise RuntimeError("unknown outcome after route")
    result=route_idle_hard_stop_once([intent],route=route)
    assert calls==[[intent]]
    assert result["orders"]==[{"status":"BROKER_SUBMIT_RESULT_UNKNOWN","requires_reconcile":True,"intent":intent}]


def test_idle_batch_checks_35_holdings_in_three_ipc_calls(monkeypatch):
    from trader.us import market_calendar
    from trader.marketdata import us_ws_collector
    from trader.us.runner.protective_entry_interrupt import make_idle_hard_stop_poll
    monkeypatch.setattr(market_calendar,"now_ny",lambda:datetime(2026,11,25,11,tzinfo=ZoneInfo("America/New_York")))
    watch=[]
    for i in range(35):
        identity=dict(symbol=f"A{i}",strategy_owner="US_STANDARD",position_lifecycle_id=f"l{i}",
                      position_cycle_id=f"l{i}",portfolio_epoch_id="p",trading_epoch_id="e",broker_account_hash="a")
        watch.append(dict(identity=identity,exchange="NASDAQ",entry=100,hard=.06))
    calls=[]
    def quotes(pairs,**kwargs):
        calls.append(pairs)
        return {p:dict(last=93 if p[0]=="A34" else 100,received_at=time.time(),
                       source="KIS_WEBSOCKET_COLLECTOR",collector_generation="g") for p in pairs}
    monkeypatch.setattr(us_ws_collector,"get_quotes_many",quotes)
    monkeypatch.setattr(us_ws_collector,"get_quote",lambda *a,**kw:pytest.fail("individual IPC"))
    poll=make_idle_hard_stop_poll(watch,trade_date="2026-11-25",env="practice")
    assert poll()==watch[-1]["identity"]
    assert [len(p) for p in calls]==[16,16,3]


@pytest.mark.parametrize("case",["valid","stale","wrong_pair","duplicate","no_generation"])
def test_quote_many_provenance_and_freshness(monkeypatch,case):
    from trader.marketdata import us_ws_collector as ws
    monkeypatch.setattr(ws,"enabled",lambda:True)
    monkeypatch.setattr(ws,"_account_identity",lambda env:"identity")
    class Service:
        def subscribe_us(self,s,e): return True
        def get_fresh_quote(self,market,s,*,max_age_sec):
            return dict(last=100,received_at=time.time()-(10 if case=="stale" else 0))
    def send(env,request,*,timeout_sec):
        result=ws._reply({**request,"env":env,"identity":"identity"},env=env,generation="g",service=Service())
        if case=="wrong_pair": result["quotes"][0]["symbol"]="WRONG"
        if case=="duplicate": result["quotes"][1]=dict(result["quotes"][0])
        if case=="no_generation": result["generation"]=""
        return result
    monkeypatch.setattr(ws,"_send_request",send)
    pairs=[("ABC","NASDAQ"),("DEF","NYSE")]
    result=ws.get_quotes_many(pairs,env="practice")
    assert set(result)==(set(pairs) if case=="valid" else set())
    if result: assert all(q["collector_generation"]=="g" for q in result.values())


@pytest.mark.parametrize("case",["ack","no_signal","uncertain","buy","missing_proof"])
def test_remaining_idle_watch_keeps_only_independent_unattempted_holdings(case):
    from trader.us.runner.protective_entry_interrupt import remaining_idle_watch
    def identity(symbol):
        return dict(symbol=symbol,strategy_owner="US_STANDARD",position_lifecycle_id=symbol,
                    position_cycle_id=symbol,portfolio_epoch_id="p",trading_epoch_id="e",broker_account_hash="a")
    watch=[dict(identity=identity(s),entry=100,hard=.06,exchange="NASDAQ") for s in ("ABC","DEF","GHI")]
    ack=case=="ack"
    result=dict(status="OK_EXIT_ORDERS_SENT" if ack else "OK_NO_TRADE",
                reason="protective_wakeup_routed" if ack else "protective_wakeup_no_fresh_signal",
                protective_wakeup=case!="missing_proof",entry_intents=0,buy_orders=1 if case=="buy" else 0,
                exit_intents=int(ack),orders_sent=int(ack),orders_ack=int(ack),
                exit_route_reconcile_required=case=="uncertain",manual_reconcile_required=0)
    assert remaining_idle_watch(watch,identity("ABC"),result)==(watch[1:] if case in {"ack","no_signal"} else [])
    assert len(watch)==3


@pytest.mark.parametrize("case",["valid","unconfirmed","old_day","wrong_lifecycle","wrong_account","recovered","not_committed","stale_revision","wrong_portfolio","wrong_cycle","bool_count"])
def test_confirmed_soft_poll_never_creates_new_confirmations(monkeypatch,case):
    from hashlib import sha256
    from types import SimpleNamespace
    from trader.us import entry_exit_contract as contract,market_calendar
    from trader.us.db.repos import CommittedUSRiskState
    from trader.us.pb1 import us_exit_position_resolver as resolver
    from trader.marketdata import us_ws_collector
    monkeypatch.setenv("US_EARLY_SOFT_STOP_PILOT_ENABLED","1")
    monkeypatch.setattr(contract,"us_entry_exit_contract_integrity_state",lambda *a:"VALID")
    monkeypatch.setattr(contract,"contract_exit_config",lambda p:{"hard_stop":.06,"soft_stop":.03,"soft_stop_confirm_ticks":2})
    monkeypatch.setattr(market_calendar,"now_ny",lambda:datetime(2026,11,25,11,tzinfo=ZoneInfo("America/New_York")))
    monkeypatch.setattr(resolver,"_position_identity",lambda *a,**kw:dict(env="practice",account_id="other" if case=="wrong_account" else "raw",trading_epoch_id="e",position_lifecycle_id="l",strategy_owner="US_STANDARD"))
    monkeypatch.setattr(us_ws_collector,"get_quote",lambda *a,**kw:dict(last=98 if case=="recovered" else 96,source="KIS_WEBSOCKET_COLLECTOR",collector_generation="g",received_at=time.time()))
    row=CommittedUSRiskState(dict(trade_date="2026-11-24" if case=="old_day" else "2026-11-25",
        trading_epoch_id="e",soft_stop_breach_count=True if case=="bool_count" else 1 if case=="unconfirmed" else 2,last_pnl_pct=-.04,
        state=dict(lifecycle=dict(lifecycle_id="other" if case=="wrong_lifecycle" else "l",
            env="practice",account_id="raw",trading_epoch_id="e",strategy_owner="US_STANDARD",is_open=True,
            portfolio_epoch_id="other" if case=="wrong_portfolio" else "p",
            position_cycle_id="other" if case=="wrong_cycle" else "l"))))
    p=dict(symbol="ABC",exchange="NASDAQ",strategy_owner="US_STANDARD",book="SWING_BOOK",horizon="SWING_CARRY",
        position_lifecycle_id="l",position_cycle_id="l",portfolio_epoch_id="p",trading_epoch_id="e",
        broker_account_hash=sha256(b"raw").hexdigest(),qty=5,orderable_qty=5,entry_price=100,
        risk_state=dict(row) if case=="not_committed" else row,_verified_pb1_stop_revision=case=="stale_revision")
    ctx=SimpleNamespace(broker_submit_allowed=lambda:True,remaining_sec=lambda:30)
    poll=make_hard_stop_poll([p],excluded_symbols=set(),trade_date="2026-11-25",env="practice",context=ctx,allow_confirmed_soft=True)
    result=poll()
    assert (result is not None)==(case=="valid")
    if result:
        assert result["_protective_trigger_family"]=="CONFIRMED_SOFT_STOP"
        assert result["soft_stop_breach_count"]==2
    assert row["soft_stop_breach_count"]==(True if case=="bool_count" else 1 if case=="unconfirmed" else 2)
    assert "resolved_current_price" not in p


@pytest.mark.parametrize("kind,family,allowed",[("soft_stop_loss","SOFT_STOP",True),("persistent_soft_stop_full_exit","HARD_STOP",True),("trailing_stop","OTHER_EXIT",False),("hard_stop","HARD_STOP",False)])
def test_confirmed_soft_lane_preserves_canonical_arbitration(monkeypatch,kind,family,allowed):
    from trader.us import market_calendar
    from trader.us.runner.protective_entry_interrupt import execute_hard_stop_interrupt
    from types import SimpleNamespace
    monkeypatch.setattr(market_calendar,"now_ny",lambda:datetime(2026,11,25,11,tzinfo=ZoneInfo("America/New_York")))
    scope=dict(symbol="ABC",strategy_owner="US_STANDARD",position_lifecycle_id="l",position_cycle_id="l",portfolio_epoch_id="p",trading_epoch_id="e",broker_account_hash="a")
    snapshot={**scope,"_protective_trigger_family":"CONFIRMED_SOFT_STOP","exit_quote_evidence":{"asof_epoch":time.time()}}
    intent={**scope,"side":"SELL","exit_type":kind}
    calls=[]
    selected,result=execute_hard_stop_interrupt(snapshot,trade_date="2026-11-25",
        context=SimpleNamespace(broker_submit_allowed=lambda:True),evaluate=lambda *a:[intent],
        classify_family=lambda i:family,suppress=lambda i:i,route=lambda i:calls.append(i) or {"orders":[]})
    assert bool(calls)==allowed
    assert bool(selected)==allowed


@pytest.mark.parametrize("case",["up","down","no_cross","stale","hard_priority"])
def test_profit_crossing_only_wakes_canonical_lane(monkeypatch,case):
    from trader.us import market_calendar
    from trader.marketdata import us_ws_collector
    from trader.us.runner.protective_entry_interrupt import make_idle_hard_stop_poll,split_wakeup
    monkeypatch.setattr(market_calendar,"now_ny",lambda:datetime(2026,11,25,11,tzinfo=ZoneInfo("America/New_York")))
    identity=dict(symbol="ABC",strategy_owner="US_STANDARD",position_lifecycle_id="l",
                  position_cycle_id="l",portfolio_epoch_id="p",trading_epoch_id="e",broker_account_hash="a")
    px={"up":111,"down":104,"no_cross":107,"stale":111,"hard_priority":93}[case]
    monkeypatch.setattr(us_ws_collector,"get_quote",lambda *a,**kw:
        dict(last=px,received_at=time.time()-(10 if case=="stale" else 0),
             source="KIS_WEBSOCKET_COLLECTOR",collector_generation="g"))
    poll=make_idle_hard_stop_poll([dict(identity=identity,exchange="NASDAQ",entry=100,hard=.06,
          profit={"reference":108,"up":[110],"down":[105]})],trade_date="2026-11-25",env="practice")
    result=poll()
    if case in {"up","down"}:
        assert split_wakeup(result)==(identity,"profit")
    elif case=="hard_priority":
        assert split_wakeup(result)==(identity,"stop")
    else:
        assert result is None


def test_profit_wakeup_does_not_create_soft_confirmations(monkeypatch):
    from trader.us.runner import protective_entry_interrupt as guard
    monkeypatch.setattr(guard,"_confirmed_soft_snapshot",lambda *a,**kw:None)
    p=dict(soft_stop_breach_count=9,risk_state={"soft_stop_breach_count":9},high_watermark=123,
           soft_stop_partial_done=True,_legacy_prior_soft_stop_execution_candidate={"old":True})
    guard.preserve_confirmed_soft_state(p,"2026-11-25")
    assert p["soft_stop_breach_count"]==0 and p["risk_state"]=={} and p["high_watermark"]==123
    assert not p["soft_stop_partial_done"] and "_legacy_prior_soft_stop_execution_candidate" not in p


def _scoped_sell(symbol):
    return dict(symbol=symbol, side="SELL", strategy_owner="US_STANDARD",
                position_lifecycle_id=symbol, position_cycle_id=symbol,
                portfolio_epoch_id="p", trading_epoch_id="e", broker_account_hash="a")


def _persisted_sell_ack(intent, order_no="100"):
    return dict(status="ACK", intent=dict(intent), order_no=order_no, kis_ack=True,
                ack_db_saved=True, requires_reconcile=False)


@pytest.mark.parametrize("case",["empty","two_ack","unknown","non_sell","missing_route"])
def test_profit_wakeup_reports_all_outcomes_without_buy(case):
    from trader.us.runner.protective_entry_interrupt import profit_wakeup_result
    intents=[_scoped_sell(s) for s in ("ABC","DEF")] if case!="empty" else []
    orders=[_persisted_sell_ack(i, str(n)) for n,i in enumerate(intents, 1)]
    if case=="unknown": orders[-1]["status"]="BROKER_SUBMIT_RESULT_UNKNOWN"
    if case=="non_sell": orders[-1]["intent"]={"symbol":"DEF","side":"BUY"}
    if case=="missing_route": orders.pop()
    result=profit_wakeup_result(orders,intents,trade_date="2026-11-25",session="am",position_count=2,decisions=[])
    assert (result["status"] in {"OK_NO_TRADE","OK_EXIT_ORDERS_SENT"})==(case in {"empty","two_ack"})
    assert result["buy_orders"]==result["entry_intents"]==0 and not result["entry_can_proceed"]


def test_tp_advisory_survives_missing_peak(monkeypatch):
    from trader.us.runner.protective_entry_interrupt import profit_watch_levels
    from trader.us import entry_exit_contract,profit_capture
    monkeypatch.setattr(entry_exit_contract,"contract_profit_capture",lambda p:dict(enabled=True,
        partial_exit_allowed=True,stages=[dict(flag="tp1_done",threshold_fraction=.03)]))
    monkeypatch.setattr(profit_capture,"authoritative_broker_avg",lambda p:(100,{}))
    result=profit_watch_levels({"resolved_current_price":101},100)
    assert result=={"reference":101.0,"up":[103.0],"down":[]}


def test_profit_wakeup_keeps_verified_forward_contract_without_soft_confirmation(monkeypatch):
    from trader.us.runner import protective_entry_interrupt as guard
    monkeypatch.setattr(guard,"_confirmed_soft_snapshot",lambda *a,**kw:None)
    row={"soft_stop_breach_count":1,"state":{"lifecycle":{"lifecycle_id":"l"},
         "us_v2_v3_forward_handoff":{"signed":True},"soft_stop_execution":{"old":True}}}
    monkeypatch.setattr(guard,"_verified_risk_row",lambda *a,**kw:row)
    p={}
    guard.preserve_confirmed_soft_state(p,"2026-11-25")
    assert p["risk_state"]["state"]["us_v2_v3_forward_handoff"]=={"signed":True}
    assert p["risk_state"]["state"]["lifecycle"]=={"lifecycle_id":"l"}
    assert p["soft_stop_breach_count"]==0 and "soft_stop_execution" not in p["risk_state"]["state"]
    assert row["soft_stop_breach_count"]==1 and row["state"]["soft_stop_execution"]=={"old":True}


@pytest.mark.parametrize("overrun", [False, True])
def test_idle_poll_overhead_does_not_extend_normal_slot(monkeypatch, overrun):
    from trader.us.runner import trade_session_runner as session
    clock = [0.0]
    sleeps = []
    polls = []
    def sleep(seconds):
        sleeps.append(seconds)
        clock[0] += seconds
    def poll():
        polls.append(clock[0])
        clock[0] += 8 if overrun else 2
        return {"symbol": "ABC"} if overrun else None
    monkeypatch.setattr(session.time_mod, "monotonic", lambda: clock[0])
    monkeypatch.setattr(session.time_mod, "sleep", sleep)
    monkeypatch.setattr(session, "write_heartbeat_file", lambda *a, **kw: None)
    assert session.sleep_with_heartbeat("am", "run", 1, 12, protective_poll=poll) is None
    assert polls == [5]
    assert sleeps == ([5] if overrun else [5, 5])
    assert clock[0] == (13 if overrun else 12)


@pytest.mark.parametrize("protective_signal",[False, True])
def test_entry_poll_overrun_never_returns_buy_result(monkeypatch, protective_signal):
    from types import SimpleNamespace
    from trader.us.runner import trade_tick_runner as runner
    clock = [0.0]
    cancelled = Event()
    snapshot = {"symbol":"ABC"}
    monkeypatch.setattr(runner,"time",SimpleNamespace(monotonic=lambda:clock[0]))
    def poll():
        clock[0] += 2.0
        return snapshot if protective_signal else None
    expected = ProtectiveEntryInterrupted if protective_signal else TimeoutError
    with pytest.raises(expected) as exc:
        run_entry_eval_with_timeout(lambda:[{"side":"BUY","symbol":"XYZ"}],
                                    timeout_sec=1, cancel_event=cancelled,
                                    protective_poll=poll)
    assert cancelled.is_set()
    if protective_signal:
        assert exc.value.snapshot == snapshot


@pytest.mark.parametrize("case",["no_trade","ack","foreign","unknown","duplicate","missing_proof","buy"])
def test_profit_wakeup_keeps_only_unattempted_exact_holdings(case):
    from trader.us.runner.protective_entry_interrupt import remaining_profit_idle_watch
    def identity(s):
        return dict(symbol=s,strategy_owner="US_STANDARD",position_lifecycle_id=s,
                    position_cycle_id=s,portfolio_epoch_id="p",trading_epoch_id="e",broker_account_hash="a")
    watch=[dict(identity=identity(s),exchange="NYSE",entry=100,hard=.06) for s in ("ABC","DEF","GHI")]
    orders=[] if case=="no_trade" else [dict(status="ACK",intent={**identity("DEF"),"side":"SELL"})]
    if case=="foreign": orders[0]["intent"]["broker_account_hash"]="foreign"
    if case=="unknown": orders[0]["status"]="BROKER_SUBMIT_RESULT_UNKNOWN"
    if case=="duplicate": orders.append(dict(orders[0]))
    result=dict(protective_wakeup=True,protective_wakeup_kind="profit",exit_route_reconcile_required=False,
                manual_reconcile_required=0,entry_intents=0,buy_orders=int(case=="buy"),
                status="OK_EXIT_ORDERS_SENT" if orders else "OK_NO_TRADE",
                reason="protective_profit_wakeup_routed" if orders else "protective_profit_wakeup_no_trade",
                orders=orders,orders_sent=len(orders),orders_ack=len(orders),exit_intents=len(orders))
    if case=="missing_proof": result.pop("exit_route_reconcile_required")
    expected=watch[1:] if case=="no_trade" else watch[2:] if case=="ack" else []
    assert remaining_profit_idle_watch(watch,identity("ABC"),result)==expected
    assert len(watch)==3


@pytest.mark.parametrize("failure",["raise","malformed"])
def test_idle_protective_ipc_failure_does_not_starve_other_holdings(monkeypatch,failure):
    from trader.us import market_calendar
    from trader.marketdata import us_ws_collector
    from trader.us.runner.protective_entry_interrupt import make_idle_hard_stop_poll
    monkeypatch.setattr(market_calendar,"now_ny",lambda:datetime(2026,11,25,11,tzinfo=ZoneInfo("America/New_York")))
    watch=[dict(identity=dict(symbol=f"A{i}",strategy_owner="US_STANDARD",position_lifecycle_id=f"l{i}",
             position_cycle_id=f"l{i}",portfolio_epoch_id="p",trading_epoch_id="e",broker_account_hash="a"),
             exchange="NASDAQ",entry=100,hard=.06) for i in range(35)]
    calls=[]
    def quotes(pairs,**kwargs):
        calls.append(pairs)
        if len(calls)==1:
            if failure=="raise":
                raise OSError("collector reconnect")
            return None
        return {p:dict(last=93 if p[0]=="A34" else 100,received_at=time.time(),
                       source="KIS_WEBSOCKET_COLLECTOR",collector_generation="g") for p in pairs}
    monkeypatch.setattr(us_ws_collector,"get_quotes_many",quotes)
    monkeypatch.setattr(us_ws_collector,"get_quote",lambda *a,**kw:pytest.fail("individual IPC"))
    poll=make_idle_hard_stop_poll(watch,trade_date="2026-11-25",env="practice")
    assert poll()==watch[-1]["identity"]
    assert [len(p) for p in calls]==[16,16,3]


def test_protective_checkpoint_uses_main_thread_and_bounded_cadence():
    from trader.us.runner.protective_entry_interrupt import make_protective_checkpoint
    import threading
    clock=[0.0]
    excluded=set()
    snapshots=iter([{"symbol":"ABC"},{"symbol":"ABC"},{"symbol":"DEF"}])
    polled=[]
    handled=[]
    def poll():
        polled.append(clock[0])
        return next(snapshots)
    check=make_protective_checkpoint(poll,on_snapshot=lambda s:handled.append((s["symbol"],threading.get_ident())),
                                    excluded_symbols=excluded,monotonic=lambda:clock[0])
    for t in (0,1,4.9,5,6,10):
        clock[0]=t
        check()
    assert polled==[0,5,10]
    assert handled==[("ABC",threading.get_ident()),("DEF",threading.get_ident())]
    assert excluded=={"ABC","DEF"}


@pytest.mark.parametrize("case",["valid","hard_priority","blocked","unknown","other_identity","buy","missing_proof"])
def test_active_entry_profit_signal_requires_clean_serial_followup(monkeypatch,case):
    from trader.us.runner import protective_entry_interrupt as guard
    from types import SimpleNamespace
    identity=dict(symbol="ABC",strategy_owner="US_STANDARD",position_lifecycle_id="l",
                  position_cycle_id="l",portfolio_epoch_id="p",trading_epoch_id="e",broker_account_hash="a")
    watch=[{"identity":identity,"profit":{"up":[110]}}]
    signal={"kind":"profit","identity":dict(identity)}
    if case=="other_identity": signal["identity"]["broker_account_hash"]="foreign"
    monkeypatch.setattr(guard,"make_idle_hard_stop_poll",lambda *a,**kw:lambda:signal)
    ctx=SimpleNamespace(broker_submit_allowed=lambda:case!="blocked",remaining_sec=lambda:30)
    hard={"symbol":"DEF"} if case=="hard_priority" else None
    poll=guard.make_entry_protective_poll(lambda:hard,watch,trade_date="2026-11-25",env="practice",context=ctx)
    snap=poll()
    if case=="hard_priority":
        assert snap==hard
        return
    if case=="blocked":
        assert snap is None
        return
    result=dict(orders=[],entry_intents=0,exit_intents=0,orders_sent=0,
                manual_reconcile_required=0,exit_route_reconcile_required=False,
                exit_route_liveness_failure=False,entry_degraded_reason="PROTECTIVE_PROFIT_ENTRY_INTERRUPTED")
    if case=="unknown": result["exit_route_reconcile_required"]=True
    if case=="buy": result["entry_intents"]=1
    if case=="missing_proof": result.pop("exit_route_liveness_failure")
    assert guard.validated_entry_profit_followup(snap,watch=watch,result=result)==(signal if case=="valid" else None)


def test_bounded_trend_daily_read_preserves_main_writer_and_late_result_is_discarded():
    from threading import Event,get_ident
    from trader.us.runner.protective_entry_interrupt import BoundedTrendDailyReads
    entered=Event()
    release=Event()
    finished=Event()
    main=get_ident()
    checks=[]
    def checkpoint():
        checks.append(get_ident())
    def reader(**kwargs):
        assert get_ident()!=main
        entered.set()
        try:
            release.wait(2)
            return [{"close":100}]
        finally:
            finished.set()
    bounded=BoundedTrendDailyReads(checkpoint,timeout_sec=.06,poll_sec=.01)
    try:
        with pytest.raises(TimeoutError,match="trend_daily_read_deadline"):
            bounded.read(reader,symbol="ABC")
        assert entered.is_set() and len(checks)>=2 and set(checks)=={main}
        with pytest.raises(TimeoutError,match="already_abandoned"):
            bounded.read(lambda:pytest.fail("second read must not start"))
    finally:
        release.set()
        assert finished.wait(1)


def test_bounded_trend_daily_read_preserves_value_and_loader_error():
    from trader.us.runner.protective_entry_interrupt import BoundedTrendDailyReads
    reader=BoundedTrendDailyReads(lambda:None)
    assert reader.read(lambda **kw:[{"close":100,"symbol":kw["symbol"]}],symbol="ABC")==[{"close":100,"symbol":"ABC"}]
    def failed():
        raise ValueError("native read failed")
    with pytest.raises(ValueError,match="native read failed"):
        reader.read(failed)
    assert not reader.abandoned


@pytest.mark.parametrize("case", ["exact", "wrong_symbol", "duplicate_ack", "wrong_account",
                                  "wrong_owner", "wrong_lifecycle", "missing_scope", "malformed",
                                  "missing_scope_on_both_sides", "unpersisted_ack"])
def test_profit_wakeup_requires_one_to_one_sell_ack_scope(case):
    from trader.us.runner.protective_entry_interrupt import profit_wakeup_result
    intents=[_scoped_sell("ABC"),_scoped_sell("DEF")]
    orders=[_persisted_sell_ack(i, str(n)) for n,i in enumerate(intents, 1)]
    if case=="wrong_symbol":
        orders[-1]["intent"]["symbol"]="GHI"
    elif case=="duplicate_ack":
        orders[-1]["intent"]=dict(intents[0])
    elif case=="wrong_account":
        orders[-1]["intent"]["broker_account_hash"]="other"
    elif case=="wrong_owner":
        orders[-1]["intent"]["strategy_owner"]="TQQQ_INFINITE"
    elif case=="wrong_lifecycle":
        orders[-1]["intent"]["position_lifecycle_id"]="old"
    elif case=="missing_scope":
        orders[-1]["intent"].pop("trading_epoch_id")
    elif case=="missing_scope_on_both_sides":
        intents[-1].pop("trading_epoch_id")
        orders[-1]["intent"].pop("trading_epoch_id")
    elif case=="unpersisted_ack":
        orders[-1]["ack_db_saved"] = False
    elif case=="malformed":
        orders[-1]=None
    result=profit_wakeup_result(orders,intents,trade_date="2026-11-25",session="am",
                               position_count=2,decisions=[])
    assert result["status"]==("OK_EXIT_ORDERS_SENT" if case=="exact" else "WARN_RECONCILE_PENDING")
    assert result["exit_route_reconcile_required"] is (case!="exact")
    assert result["entry_can_proceed"] is False


@pytest.mark.parametrize("case", ["missing_reconcile", "missing_manual", "missing_identity", "manual_debt"])
def test_idle_stop_cannot_continue_without_explicit_reconciliation_proof(case):
    from trader.us.runner.protective_entry_interrupt import remaining_idle_watch
    def identity(s):
        return dict(symbol=s,strategy_owner="US_STANDARD",position_lifecycle_id=s,
                    position_cycle_id=s,portfolio_epoch_id="p",trading_epoch_id="e",broker_account_hash="a")
    watch=[dict(identity=identity(s),entry=100,hard=.06,exchange="NASDAQ") for s in ("ABC","DEF")]
    result=dict(protective_wakeup=True,status="OK_NO_TRADE",reason="protective_wakeup_no_fresh_signal",
                entry_intents=0,buy_orders=0,exit_intents=0,orders_sent=0,orders_ack=0,
                exit_route_reconcile_required=False,manual_reconcile_required=0)
    attempted=identity("ABC")
    assert remaining_idle_watch(watch,attempted,result)==watch[1:]
    if case=="missing_reconcile":
        result.pop("exit_route_reconcile_required")
    elif case=="missing_manual":
        result.pop("manual_reconcile_required")
    elif case=="missing_identity":
        attempted=None
    elif case=="manual_debt":
        result["manual_reconcile_required"]=1
    assert remaining_idle_watch(watch,attempted,result)==[]


@pytest.mark.parametrize("case", ["exact", "wrong_identity", "duplicate", "missing_ack", "empty",
                                  "missing_broker_number", "missing_reconcile_proof"])
def test_idle_stop_route_result_requires_exact_single_ack(case):
    from trader.us.runner.protective_entry_interrupt import stop_wakeup_route_result
    intent=_scoped_sell("ABC")
    intents=[intent]
    orders=[_persisted_sell_ack(intent)]
    if case=="wrong_identity":
        orders[0]["intent"]["broker_account_hash"]="foreign"
    elif case=="duplicate":
        orders.append(dict(orders[0]))
    elif case=="missing_ack":
        orders=[]
    elif case=="empty":
        intents,orders=[],[]
    elif case=="missing_broker_number":
        orders[0]["order_no"] = "000000"
    elif case=="missing_reconcile_proof":
        orders[0].pop("requires_reconcile")
    result=stop_wakeup_route_result(orders,intents,trade_date="2026-11-25",session="am",
                                   position_count=1,decisions=[])
    assert result["protective_wakeup_kind"]=="stop"
    assert result["status"]==("OK_EXIT_ORDERS_SENT" if case=="exact" else "WARN_RECONCILE_PENDING")
    assert result["exit_route_reconcile_required"] is (case!="exact")
    assert result["entry_can_proceed"] is False


def test_active_protective_watch_polls_on_cadence_and_prefers_stop():
    from trader.us.runner.protective_entry_interrupt import ActiveProtectiveWatch

    identity = {k: v for k, v in _scoped_sell("ABC").items() if k != "side"}
    clock = [0.0]
    calls = []
    signals = iter([{"kind": "profit", "identity": identity}, identity,
                    {"kind": "profit", "identity": identity}])

    def poll():
        calls.append(clock[0])
        return next(signals)

    watch = ActiveProtectiveWatch(poll, clock=lambda: clock[0])
    for now in (0, 1, 4.99, 5, 8, 10):
        clock[0] = now
        watch()
    assert calls == [0, 5, 10]
    result = dict(runtime_integrity_status="OK", sell_liveness_status="OK",
                  exit_can_proceed=True, exit_route_reconcile_required=False,
                  manual_reconcile_required=0)
    assert watch.followup(result, [{"identity": identity}]) == identity
    clock[0] = 15.01
    assert watch.followup(result, [{"identity": identity}]) is None


@pytest.mark.parametrize("case", ["unreconciled", "degraded", "manual", "routed", "reopened", "exit_blocked"])
def test_active_protective_followup_requires_current_unrouted_holding(case):
    from trader.us.runner.protective_entry_interrupt import ActiveProtectiveWatch

    identity = {k: v for k, v in _scoped_sell("ABC").items() if k != "side"}
    watch = ActiveProtectiveWatch(lambda: identity, clock=lambda: 0.0)
    watch()
    result = dict(runtime_integrity_status="OK", sell_liveness_status="OK",
                  exit_can_proceed=True, exit_route_reconcile_required=False,
                  manual_reconcile_required=0)
    current = [{"identity": identity}]
    if case == "unreconciled":
        result.pop("exit_route_reconcile_required")
    elif case == "degraded":
        result["sell_liveness_status"] = "LIVENESS_DEGRADED"
    elif case == "manual":
        result["manual_reconcile_required"] = 1
    elif case == "routed":
        current = []
    elif case == "exit_blocked":
        result["exit_can_proceed"] = False
    else:
        current = [{"identity": {**identity, "position_cycle_id": "new",
                                 "position_lifecycle_id": "new"}}]
    assert watch.followup(result, current) is None


def _tick_waits_for_readonly_observer(marker, tick_cancellation_event=None):
    from pathlib import Path
    import time

    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and not Path(marker).exists():
        time.sleep(0.01)
    return {"seen": Path(marker).exists(), "cancelled": tick_cancellation_event.is_set()}


@pytest.mark.parametrize("observer_error", [False, True])
def test_process_supervisor_observes_busy_child_without_cancelling(tmp_path, observer_error):
    import os
    from trader.us.runner.tick_process import run_tick_in_process

    marker = tmp_path / "readonly-observed"
    calls = []

    def observe():
        calls.append(os.getpid())
        marker.touch()
        if observer_error:
            raise OSError("collector unavailable")

    outcome = run_tick_in_process(
        _tick_waits_for_readonly_observer, kwargs={"marker": str(marker)}, timeout_sec=4,
        observe_while_running=observe,
    )
    assert calls and set(calls) == {os.getpid()}
    assert outcome.child_pid != os.getpid()
    assert outcome.result == {"seen": True, "cancelled": False}


@pytest.mark.parametrize("latest", ["same", "recovered", "different", "collector_failure"])
def test_active_protective_long_tick_rechecks_fresh_trigger_before_serial_followup(latest):
    from trader.us.runner.protective_entry_interrupt import ActiveProtectiveWatch

    identity = {k: v for k, v in _scoped_sell("ABC").items() if k != "side"}
    other = {**identity, "position_lifecycle_id": "new-cycle",
             "position_cycle_id": "new-cycle"}
    clock = [0.0]
    calls = []

    def poll():
        calls.append(clock[0])
        if len(calls) == 1:
            return identity
        if latest == "same":
            return dict(identity)
        if latest == "different":
            return other
        if latest == "collector_failure":
            raise ConnectionError("offline collector")
        return None

    watch = ActiveProtectiveWatch(poll, clock=lambda: clock[0])
    watch()
    clock[0] = 120.0  # normal Tick took two minutes; no new order writer
    result = dict(runtime_integrity_status="OK", sell_liveness_status="OK",
                  exit_can_proceed=True, exit_route_reconcile_required=False,
                  manual_reconcile_required=0)
    followup = watch.followup(result, [{"identity": identity}])
    assert followup == (identity if latest == "same" else None)
    assert calls == [0.0, 120.0]


def test_active_protective_long_tick_never_rechecks_when_order_fence_uncertain():
    from trader.us.runner.protective_entry_interrupt import ActiveProtectiveWatch

    identity = {k: v for k, v in _scoped_sell("ABC").items() if k != "side"}
    clock = [0.0]
    calls = []
    def poll():
        calls.append(clock[0])
        return identity
    watch = ActiveProtectiveWatch(poll, clock=lambda: clock[0])
    watch()
    clock[0] = 120.0
    result = dict(runtime_integrity_status="OK", sell_liveness_status="OK",
                  exit_can_proceed=True, exit_route_reconcile_required=True,
                  manual_reconcile_required=0)
    assert watch.followup(result, [{"identity": identity}]) is None
    assert calls == [0.0]
