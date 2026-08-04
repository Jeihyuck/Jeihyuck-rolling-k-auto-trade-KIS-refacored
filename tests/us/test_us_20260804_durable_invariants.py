from pathlib import Path

from trader.us.db import repos
from trader.us.run_manifest import verify_run_revision
from unittest.mock import MagicMock, patch


def _order(status="ACK", filled=0):
    return {"trade_date":"2026-08-04","client_order_key":"K","symbol":"JPM","exchange":"NYSE",
            "side":"SELL","order_no":"0000040991","qty_requested":2,"qty_filled":filled,
            "status":status,"meta":{"position_lifecycle_id":"L1","submit_attempt_id":"A1"}}


def test_revision_uses_db_expected_value_without_local_manifest(tmp_path,monkeypatch):
    monkeypatch.setenv("US_RUN_MANIFEST_DIR",str(tmp_path/"missing"))
    result=verify_run_revision("2026-08-04","a"*40,expected_revision="a"*40)
    assert result["entry_can_proceed"] and result["source"]=="db_prep_contract"
    mismatch=verify_run_revision("2026-08-04","b"*40,expected_revision="a"*40)
    assert not mismatch["entry_can_proceed"] and mismatch["reconciliation_can_proceed"]


def test_workflows_install_full_requirements_only_after_pinned_checkout():
    for name in ("am","afternoon","close"):
        text=(Path(".github/workflows")/f"us-trade-{name}.yml").read_text()
        checkout=text.index("Checkout pinned US trade-day revision")
        full=text.index("Install dependencies from pinned revision")
        assert checkout < full
        assert "psycopg[binary]" in text[:checkout]
        assert "pip install -r requirements.txt" not in text[:checkout]


def test_order_state_machine_never_regresses_terminal_or_cumulative(monkeypatch):
    monkeypatch.setattr(repos,"_get_engine_or_none",lambda:None); repos.reset_memory_stores()
    repos._MEM_ORDERS.append(_order("FILLED",2))
    for stale in ("ACK","OPEN","PARTIALLY_FILLED"):
        result=repos.apply_broker_order_observation(trade_date="2026-08-04",client_order_key="K",
            raw_order_no="40991",canonical_order_no="40991",symbol="JPM",side="SELL",
            requested_qty=2,filled_qty=1,remaining_qty=1,broker_status=stale,
            evidence_type="KIS_ORDER_STATUS_ACTUAL",observed_at=None,raw_row={})
        assert result["observation_ignored"]=="ORDER_OBSERVATION_IGNORED_STALE"
        assert repos._MEM_ORDERS[0]["status"]=="FILLED" and repos._MEM_ORDERS[0]["qty_filled"]==2
    overflow=repos.apply_broker_order_observation(trade_date="2026-08-04",client_order_key="K",
        raw_order_no="40991",canonical_order_no="40991",symbol="JPM",side="SELL",
        requested_qty=2,filled_qty=3,remaining_qty=0,broker_status="FILLED",
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",observed_at=None,raw_row={})
    assert overflow["status"]=="BROKER_OBSERVATION_QUARANTINED"


def test_late_l1_fill_does_not_mutate_l2(monkeypatch):
    monkeypatch.setattr(repos,"_get_engine_or_none",lambda:None); repos._MEM_PROFIT_CAPTURE_STATE.clear()
    repos.mark_us_profit_capture_stage("2026-08-04","JPM","tp1",status="PENDING",position_lifecycle_id="L2",order_key="L2K")
    repos.mark_us_profit_capture_stage("2026-08-04","JPM","tp1",status="FILLED",position_lifecycle_id="L1",order_key="L1K")
    l1=repos.load_us_profit_capture_state("2026-08-04",["JPM"],{"JPM":"L1"})["JPM"]
    l2=repos.load_us_profit_capture_state("2026-08-04",["JPM"],{"JPM":"L2"})["JPM"]
    assert l1["tp1_done"] and l2["tp1_pending"] and not l2["tp1_done"]


def test_durable_submit_event_failure_blocks_broker_post():
    from trader.us.execution.order_router import route_order
    client=MagicMock()
    intent={"trade_date":"2026-08-04","client_order_key":"K-DURABLE","symbol":"AAPL",
            "exchange":"NASDAQ","side":"BUY","qty":1,"limit_price":10,"notional_usd":10}
    with patch("trader.us.execution.order_router.resolve_dry_run_for_us_order",return_value=False), \
         patch("trader.us.execution.order_router.assert_order_allowed"), \
         patch("trader.us.db.repos.save_order_intent",return_value=True), \
         patch("trader.us.db.repos.load_today_order_keys",return_value=set()), \
         patch("trader.us.execution.order_journal.append_order_event",side_effect=OSError("db down")):
        result=route_order(intent,kis_client=client)
    assert result["status"]=="ORDER_DISABLED_DURABLE_LEDGER_UNAVAILABLE"
    client.place_us_buy_order.assert_not_called()
