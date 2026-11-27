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
