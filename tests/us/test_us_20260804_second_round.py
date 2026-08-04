import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from trader.us import data_provider
from trader.us.db import repos
from trader.us.execution.order_journal import aggregate_order_events, append_order_event
from trader.us.execution.order_router import route_order
from trader.us.market_state_overlay import build_profit_capture_intents
from trader.us.profit_capture import sync_profit_capture_stage_from_order
from trader.us.run_manifest import pin_run_revision, verify_run_revision


def position(lifecycle="L1", price=106):
    return {"symbol":"JPM","qty":10,"orderable_qty":10,"current_price_usd":price,
            "broker_avg_price":100,"broker_avg_price_source":"kis_pchs_avg_pric",
            "broker_avg_price_currency":"USD","broker_avg_price_asof":datetime.now(timezone.utc).isoformat(),
            "balance_source":"kis_balance_authoritative","authoritative_positions":True,
            "position_lifecycle_id":lifecycle}


def test_authoritative_cost_basis_is_fail_closed():
    overlay={"profit_capture_enabled":True,"market_state":"NORMAL"}
    assert build_profit_capture_intents([{"symbol":"JPM","qty":10,"current_price_usd":106,"entry_price":100}],overlay,profit_capture_state={}) == []
    assert build_profit_capture_intents([{"symbol":"JPM","qty":10,"current_price_usd":106,"avg_cost":100}],overlay,profit_capture_state={}) == []
    stale=position(); stale["broker_avg_price_asof"]=(datetime.now(timezone.utc)-timedelta(hours=1)).isoformat()
    assert build_profit_capture_intents([stale],overlay,profit_capture_state={}) == []
    wrong=position(); wrong["broker_avg_price_currency"]="KRW"
    assert build_profit_capture_intents([wrong],overlay,profit_capture_state={}) == []
    assert build_profit_capture_intents([position()],overlay,profit_capture_state={})[0]["reason"] == "TAKE_PROFIT_TP1"


def test_tp_order_lifecycle_and_late_old_lifecycle_fill(monkeypatch):
    monkeypatch.setattr(repos,"_get_engine_or_none",lambda:None); repos._MEM_PROFIT_CAPTURE_STATE.clear()
    common=dict(trade_date="2026-08-04",symbol="JPM",position_lifecycle_id="L1",client_order_key="K1",broker_order_no="O1",profit_capture_stage="tp1",requested_qty=2)
    for status,qty,evidence in [("ACK",0,None),("OPEN",0,None),("PARTIALLY_FILLED",1,"KIS_ORDER_CUMULATIVE_ACTUAL")]:
        sync_profit_capture_stage_from_order(**common,order_status=status,evidence_type=evidence,filled_qty=qty)
        state=repos.load_us_profit_capture_state("2026-08-04",["JPM"],{"JPM":"L1"})["JPM"]
        assert state["tp1_pending"] and not state["tp1_done"]
    sync_profit_capture_stage_from_order(**common,order_status="FILLED",evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",filled_qty=2)
    sync_profit_capture_stage_from_order(**common,order_status="FILLED",evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",filled_qty=2)
    state=repos.load_us_profit_capture_state("2026-08-04",["JPM"],{"JPM":"L1"})["JPM"]
    assert state["tp1_done"] and not state["tp1_pending"]
    assert build_profit_capture_intents([position("L1")],{"profit_capture_enabled":True},profit_capture_state={"JPM":state})[0]["reason"]=="TAKE_PROFIT_TP2"
    assert build_profit_capture_intents([position("L2",103)],{"profit_capture_enabled":True},profit_capture_state={})[0]["reason"]=="TAKE_PROFIT_TP1"
    old=repos.load_us_profit_capture_state("2026-08-04",["JPM"],{"JPM":"L2"})["JPM"]
    assert not old["tp1_done"]


def test_reject_releases_pending(monkeypatch):
    monkeypatch.setattr(repos,"_get_engine_or_none",lambda:None); repos._MEM_PROFIT_CAPTURE_STATE.clear()
    kw=dict(trade_date="2026-08-04",symbol="JPM",position_lifecycle_id="L1",client_order_key="K1",broker_order_no=None,profit_capture_stage="tp1",evidence_type=None,filled_qty=0,requested_qty=2)
    sync_profit_capture_stage_from_order(**kw,order_status="ACK")
    sync_profit_capture_stage_from_order(**kw,order_status="REJECTED")
    state=repos.load_us_profit_capture_state("2026-08-04",["JPM"],{"JPM":"L1"})["JPM"]
    assert not state["tp1_pending"] and not state["tp1_done"]


def test_http_500_is_ambiguous_not_rejected(tmp_path,monkeypatch):
    monkeypatch.setenv("US_ORDER_JOURNAL_DIR",str(tmp_path)); client=MagicMock()
    client.place_us_buy_order.side_effect=RuntimeError("HTTP 500 Internal Server Error")
    intent={"trade_date":"2026-08-04","client_order_key":"K","symbol":"AAPL","exchange":"NASDAQ","side":"BUY","qty":1,"limit_price":10,"notional_usd":10}
    with patch("trader.us.execution.order_router.resolve_dry_run_for_us_order",return_value=False), patch("trader.us.execution.order_router.assert_order_allowed"), patch("trader.us.db.repos.save_order_intent",return_value=True), patch("trader.us.db.repos.load_today_order_keys",return_value=set()), patch("trader.us.db.repos.save_order_reject") as reject:
        result=route_order(intent,kis_client=client)
    assert result["status"]=="BROKER_SUBMIT_RESULT_UNKNOWN" and result["requires_reconcile"] and not result["retry_order"]
    reject.assert_not_called(); assert client.place_us_buy_order.call_count==1


def test_jpm_seven_row_fixture_preserved_and_matched():
    rows=json.loads((Path(__file__).parent.parent/"fixtures/us/jpm_7_order_rows.json").read_text())
    normalized=[data_provider.normalize_us_order_status_row(r) for r in rows]
    assert len(normalized)==7 and sum(r["normalization_result"]=="normalized" for r in normalized)==7
    jpm=[r for r in normalized if r["symbol"]=="JPM"]
    assert len(jpm)==2 and {r["canonical_order_no"] for r in jpm}=={"40991"}
    assert {r["status"] for r in jpm}=={"OPEN","FILLED"}


def test_journal_unique_attempt_aggregation_and_revision_pin(tmp_path,monkeypatch):
    monkeypatch.setenv("US_ORDER_JOURNAL_DIR",str(tmp_path/"journal")); monkeypatch.setenv("US_RUN_MANIFEST_DIR",str(tmp_path/"manifest"))
    for i in range(9):
        intent={"trade_date":"2026-08-04","client_order_key":f"K{i}","submit_attempt_id":f"A{i}","symbol":f"S{i}","side":"BUY" if i<4 else "SELL","qty":1}
        append_order_event("BROKER_SUBMIT_STARTED",intent)
        append_order_event("ORDER_REJECTED" if i%3==0 else "BROKER_ACK_RECEIVED",intent,broker_order_no=f"O{i}")
        for _ in range(10): append_order_event("ORDER_OPEN",intent,broker_order_no=f"O{i}")
    counts=aggregate_order_events("2026-08-04")
    assert (counts["orders_sent_total"],counts["buy_orders_count"],counts["sell_orders_count"])==(9,4,5)
    pin_run_revision("2026-08-04","A")
    assert verify_run_revision("2026-08-04","A")["entry_can_proceed"]
    mismatch=verify_run_revision("2026-08-04","B")
    assert not mismatch["entry_can_proceed"] and mismatch["reconciliation_can_proceed"]


def test_actual_session_survives_ten_degraded_reconcile_ticks():
    from trader.us.runner import trade_session_runner as session_runner
    tick_calls=[]
    def tick(*args,**kwargs):
        tick_calls.append(kwargs.get("tick_index")); return {"status":"OK_RECONCILE_ONLY_PENDING","severity":"DEGRADED","allow_new_orders":False,"session_should_continue":True,"reason":"unresolved_ack_exists","orders":[],"orders_sent":0}
    with patch("trader.us.runner.trade_tick_runner.run_trade_tick",side_effect=tick), patch("trader.us.market_calendar.is_us_trading_day",return_value=True), patch("trader.us.budget.resolve_us_order_budget",return_value={"capital_usd_cap":10000.0}):
        result=session_runner.run_trade_session(session="am",env="practice",offline=True,force_now="2026-05-01T09:35:00-04:00",max_ticks=10,interval_sec=1,max_minutes=60)
    assert result["tick_count"]==10 and len(tick_calls)==10
    assert result["status"] in {"OK","OK_WITH_WARNINGS"} and result.get("orders_sent_total",0)==0
