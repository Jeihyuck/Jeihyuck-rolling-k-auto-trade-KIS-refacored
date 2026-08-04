from datetime import datetime, timezone
from unittest.mock import patch
import json
from pathlib import Path

from trader.us.db import repos
from trader.us.execution.order_journal import append_order_event, aggregate_order_events, replay_order_journal
from trader.us.execution.order_journal import match_ambiguous_submit_to_broker_order
from trader.us.market_state_overlay import build_profit_capture_intents


def _meta():
    now = datetime.now(timezone.utc).isoformat()
    return {"profit_capture_stage":"tp1","position_lifecycle_id":"L1","broker_avg_price":"100",
            "broker_avg_price_source":"kis_pchs_avg_pric","broker_avg_price_currency":"USD",
            "broker_avg_price_asof":now,"balance_source":"kis_balance_authoritative",
            "authoritative_positions":True,"tp_threshold_fraction":"0.03",
            "return_rate_at_decision":"0.06","reason":"TAKE_PROFIT_TP1","submit_attempt_id":"A1"}


def _intent():
    return {"trade_date":"2026-08-04","client_order_key":"K1","submit_attempt_id":"A1",
            "symbol":"JPM","exchange":"NYSE","side":"SELL","qty":2,"limit_price":106,
            "position_lifecycle_id":"L1","meta":_meta()}


class Provider:
    def __init__(self, rows, fill=None): self.rows=rows; self.fill=fill
    def get_balance(self, **kwargs): return {"positions":[]}
    def get_today_orders(self, trade_date): return self.rows
    def get_fills_by_order_no(self, order_no, symbol, trade_date): return self.fill


def _row(status="OPEN", order_no="40991", filled=0, remaining=2):
    return {"trade_date":"2026-08-04","symbol":"JPM","side":"SELL","requested_qty":2,
            "filled_qty":filled,"remaining_qty":remaining,"status":status,"order_no":order_no,
            "raw_order_no":order_no,"exchange":"NYSE","limit_price":106,
            "submitted_at_utc":datetime.now(timezone.utc).isoformat(),"observed_at":datetime.now(timezone.utc).isoformat(),
            "avg_price":106}


def _seed(tmp_path, monkeypatch):
    monkeypatch.setenv("US_ORDER_JOURNAL_DIR",str(tmp_path)); monkeypatch.setattr(repos,"_get_engine_or_none",lambda:None)
    repos.reset_memory_stores(); intent=_intent(); repos.save_order_intent(intent,trade_date=intent["trade_date"])
    append_order_event("BROKER_SUBMIT_STARTED",intent); append_order_event("BROKER_SUBMIT_RESULT_UNKNOWN",intent,broker_status="AMBIGUOUS_ACK")
    return intent


def test_http500_unique_open_recovers_then_filled_advances_tp(tmp_path,monkeypatch):
    intent=_seed(tmp_path,monkeypatch)
    first=replay_order_journal("2026-08-04",provider=Provider([_row()]))
    assert first["status"]=="OK" and repos._MEM_ORDERS[-1]["status"]=="OPEN"
    assert repos._MEM_ORDERS[-1]["meta"]["profit_capture_stage"]=="tp1"
    fill={**_row("FILLED","40991",2,0),"cumulative_filled_qty":2}
    second=replay_order_journal("2026-08-04",provider=Provider([fill],fill))
    assert second["status"]=="OK" and repos._MEM_ORDERS[-1]["status"]=="FILLED"
    state=repos.load_us_profit_capture_state("2026-08-04",["JPM"],{"JPM":"L1"})["JPM"]
    assert state["tp1_done"] and not state["tp1_pending"]
    pos={"symbol":"JPM","qty":10,"orderable_qty":10,"current_price_usd":106,"broker_avg_price":100,
         "broker_avg_price_source":"kis_pchs_avg_pric","broker_avg_price_currency":"USD",
         "broker_avg_price_asof":datetime.now(timezone.utc).isoformat(),"balance_source":"kis_balance_authoritative",
         "authoritative_positions":True,"position_lifecycle_id":"L1"}
    assert build_profit_capture_intents([pos],{"profit_capture_enabled":True},profit_capture_state={"JPM":state})[0]["reason"]=="TAKE_PROFIT_TP2"
    counts=aggregate_order_events("2026-08-04")
    assert counts["orders_sent_total"]==counts["orders_ack_total"]==counts["orders_fill_confirmed"]==1


def test_ambiguous_zero_or_multiple_candidates_never_links(tmp_path,monkeypatch):
    _seed(tmp_path,monkeypatch)
    assert replay_order_journal("2026-08-04",provider=Provider([]))["status"]=="UNRESOLVED"
    result=replay_order_journal("2026-08-04",provider=Provider([_row("OPEN","1"),_row("OPEN","2")]))
    assert result["status"]=="UNRESOLVED" and result["broker_unknown_count"]==1
    assert aggregate_order_events("2026-08-04")["orders_ack_total"]==0


def test_recovered_ack_preserves_all_tp_metadata(tmp_path,monkeypatch):
    intent=_seed(tmp_path,monkeypatch)
    append_order_event("BROKER_ACK_RECEIVED",intent,broker_order_no="0000040991",broker_status="ACK")
    fill={**_row("FILLED","40991",2,0),"cumulative_filled_qty":2}
    result=replay_order_journal("2026-08-04",provider=Provider([fill],fill))
    assert result["status"]=="OK"
    meta=repos._MEM_ORDERS[-1]["meta"]
    for field in ("profit_capture_stage","position_lifecycle_id","broker_avg_price","tp_threshold_fraction","return_rate_at_decision","submit_attempt_id"):
        assert meta[field]==intent["meta"][field]


def test_explicit_cancel_and_reject_update_db_and_release_tp(tmp_path,monkeypatch):
    for status in ("CANCELLED","REJECTED","EXPIRED"):
        case=tmp_path/status; _seed(case,monkeypatch)
        append_order_event("BROKER_ACK_RECEIVED",_intent(),broker_order_no="0000040991",broker_status="ACK")
        result=replay_order_journal("2026-08-04",provider=Provider([_row(status)]))
        assert result["status"]=="OK" and repos._MEM_ORDERS[-1]["status"]==status
        state=repos.load_us_profit_capture_state("2026-08-04",["JPM"],{"JPM":"L1"})["JPM"]
        assert not state["tp1_pending"] and not state["tp1_done"]


def test_jpm_two_page_provider_pipeline_preserves_all_seven(monkeypatch):
    from trader.us.execution.kis_us_client import KisUSClient, KisUSClientError
    from trader.us.data_provider import USDataProvider
    rows=json.loads((Path(__file__).parent.parent/"fixtures/us/jpm_7_order_rows.json").read_text())
    client=KisUSClient.__new__(KisUSClient)
    client._offline=False; client._assert_not_offline=lambda *_:None; client._build_headers=lambda *_:{}
    client._build_us_fills_params=lambda *_:{"CTX_AREA_NK200":"","CTX_AREA_FK200":""}
    pages=iter([{"output":rows[:5],"CTX_AREA_NK200":"NEXT","CTX_AREA_FK200":"F","_response_meta":{"tr_cont":"M"}},
                {"output":rows[5:],"_response_meta":{"tr_cont":""}}])
    client._get=lambda *a,**k:next(pages)
    with patch("trader.us.execution.kis_us_client.get_tr_info",return_value={"tr_id":"T","path":"/x"}):
        provider=USDataProvider(offline=False); provider._get_client=lambda:client
        normalized=provider.get_today_orders("2026-08-04")
    assert len(normalized)==7 and {row["page_index"] for row in normalized}=={1,2}
    assert sum(row["normalization_result"]=="normalized" for row in normalized)==7
    jpm=[row for row in normalized if row["symbol"]=="JPM"]
    assert len(jpm)==2 and {row["canonical_order_no"] for row in jpm}=={"40991"}
    assert all(row["submitted_at_utc"] for row in normalized)
    # 09:31:06 EDT == 13:31:06 UTC.  No synthetic timestamp is injected into broker rows.
    matched=match_ambiguous_submit_to_broker_order(trade_date="2026-07-16",symbol="JPM",side="SELL",
        requested_qty=2,limit_price=353.69,submitted_at_utc="2026-07-16T13:31:06+00:00",
        exchange="NYSE",broker_rows=normalized)
    assert matched["status"]=="MATCHED"
    assert matched["match"]["canonical_order_no"]=="40991"
    assert matched["match"]["filled_qty"]==2

    repeated=iter([{"output":[],"CTX_AREA_NK200":"SAME","CTX_AREA_FK200":"F","_response_meta":{"tr_cont":"M"}},
                   {"output":[],"CTX_AREA_NK200":"SAME","CTX_AREA_FK200":"F","_response_meta":{"tr_cont":"M"}}])
    client._get=lambda *a,**k:next(repeated)
    with patch("trader.us.execution.kis_us_client.get_tr_info",return_value={"tr_id":"T","path":"/x"}):
        try: client.get_us_fills_today("2026-08-04")
        except KisUSClientError as exc: assert "pagination contract error" in str(exc)
        else: raise AssertionError("repeated cursor must fail closed")
