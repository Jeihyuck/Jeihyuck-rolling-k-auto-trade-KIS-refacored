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
                                 "pending","expired_after_evaluate","permission_after_pending"])
def test_interrupt_requires_canonical_hard_stop_and_pending_gate(monkeypatch,case):
    from trader.us import market_calendar
    from trader.us.runner import protective_entry_interrupt as guard
    from types import SimpleNamespace
    monkeypatch.setattr(market_calendar,"now_ny",lambda:datetime(2026,11,25,11,tzinfo=ZoneInfo("America/New_York")))
    active=[case!="no_permission"]
    calls=[]
    snap={"symbol":"ABC","exit_quote_evidence":{"asof_epoch":time.time()-(10 if case=="stale" else 0)}}
    ctx=SimpleNamespace(broker_submit_allowed=lambda:active[0])
    def evaluate(snapshot,clock):
        calls.append("canonical")
        if case=="expired_after_evaluate":
            snapshot["exit_quote_evidence"]["asof_epoch"]-=10
        return [{"side":"SELL","symbol":"XYZ" if case=="wrong_symbol" else "ABC",
                 "family":"TAKE_PROFIT" if case=="wrong_family" else "HARD_STOP"}]
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
