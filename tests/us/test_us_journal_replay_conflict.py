from trader.us.execution.order_journal import append_order_event, replay_order_journal

class P:
    def get_balance(self, force_refresh=True): return {"positions":[]}
    def get_today_orders(self, trade_date): return []
    def get_fills_by_order_no(self, order_no, symbol, trade_date): return {"filled_qty":7,"avg_price":100,"symbol":"AMD","side":"SELL"}

def test_replay_evidence_conflict_is_unresolved_not_confirmed(monkeypatch, tmp_path):
    import trader.us.db.repos as repos
    monkeypatch.setenv("US_ORDER_JOURNAL_DIR", str(tmp_path))
    repos.reset_memory_stores(); monkeypatch.setattr(repos,'_get_engine_or_none',lambda:None)
    repos.save_order_ack({"client_order_key":"K","symbol":"AMD","exchange":"NASDAQ","side":"SELL","qty_requested":10,"order_no":"O1","status":"ACK"}, trade_date="2026-07-16")
    repos.mark_order_filled_by_reconcile(order_no="O1",client_order_key="K",symbol="AMD",side="SELL",filled_qty=10,requested_qty=10,cumulative_filled_qty=10,avg_price_usd=100,trade_date="2026-07-16",evidence_type="BALANCE_DELTA_SYNTHETIC")
    append_order_event("BROKER_SUBMIT_STARTED", {"trade_date":"2026-07-16","session_run_id":"S","tick_id":"T","client_order_key":"K","symbol":"AMD","side":"SELL","qty":10})
    append_order_event("BROKER_ACK_RECEIVED", {"trade_date":"2026-07-16","session_run_id":"S","tick_id":"T","client_order_key":"K","symbol":"AMD","side":"SELL","qty":10}, broker_order_no="O1")
    r=replay_order_journal("2026-07-16",session_run_id="S",tick_id="T",provider=P())
    assert r["broker_confirmed_count"] == 0
    assert r["unresolved_count"] == 1
