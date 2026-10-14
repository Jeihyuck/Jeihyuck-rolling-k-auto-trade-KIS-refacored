"""Journal replay must reuse fresh read-only order evidence per date and tick."""
import pytest


def test_two_old_claims_share_single_source_date_order_book(monkeypatch):
    import trader.us.db.repos as repos
    import trader.us.execution.order_journal as journal
    import trader.us.execution.fills as fills

    source_date = "2026-10-06"
    current_date = "2026-10-13"
    events = []
    claims = []
    for sym, no in (("MRVL", "0000034237"), ("AMD", "0000035281")):
        key, attempt = f"key-{sym}", f"attempt-{sym}"
        claims.append({"trade_date": source_date, "client_order_key": key,
                       "active_attempt_id": attempt})
        events.extend([
            {"event_type": "BROKER_SUBMIT_STARTED",
             "event_id": f"submit-{sym}", "trade_date": source_date,
             "client_order_key": key, "submit_attempt_id": attempt,
             "symbol": sym, "exchange": "NASDAQ", "side": "SELL", "qty": 1,
             "session": "am", "session_run_id": "historical-session"},
            {"event_type": "BROKER_ACK_RECEIVED",
             "event_id": f"ack-{sym}", "trade_date": source_date,
             "client_order_key": key, "submit_attempt_id": attempt,
             "symbol": sym, "exchange": "NASDAQ", "side": "SELL", "qty": 1,
             "session": "am", "session_run_id": "historical-session",
             "broker_order_no": no},
        ])

    monkeypatch.setattr(journal, "load_order_events",
                        lambda date, session_run_id=None: events if date == source_date else [])
    monkeypatch.setattr(journal, "append_order_event", lambda *a, **kw: {})
    monkeypatch.setattr(repos, "load_active_execution_claim_attempts", lambda: claims)
    monkeypatch.setattr(repos, "save_order_ack", lambda *a, **kw: False)
    monkeypatch.setattr(repos, "apply_broker_order_observation",
                        lambda **kw: {"status": "OK"})
    monkeypatch.setattr(fills, "get_fills_today",
                        lambda provider, trade_date: {"status": "OK", "fills": []})

    class Provider:
        def __init__(self):
            self.reads = []

        def get_balance(self, force_refresh=False):
            return {"positions": []}

        def get_order_lookup_provenance(self, trade_date):
            return {"verified": True, "source": "kis_authenticated_order_book",
                    "query_trade_date": trade_date, "market": "US",
                    "account_id": "mock-account", "session": "am",
                    "session_run_id": "historical-session"}

        def get_today_orders(self, *, trade_date):
            self.reads.append(trade_date)
            if trade_date != source_date:
                return []
            return [
                {"symbol": sym, "side": "SELL", "order_no": no,
                 "filled_qty": 0, "filled_qty_present": True,
                 "remaining_qty": 1, "requested_qty": 1,
                 "status": "OPEN", "normalization_result": "normalized"}
                for sym, no in (("MRVL", "0000034237"), ("AMD", "0000035281"))
            ]

        def get_fills_by_order_no(self, **kw):
            pytest.fail("fresh exact broker order was already fetched for this date")

    provider = Provider()
    result = journal.replay_order_journal(
        current_date, provider=provider,
        include_active_claims=True, active_claims_only=True,
    )
    assert provider.reads == [current_date, source_date]
    assert result["failed_count"] == 0
    assert result["broker_unfilled_ack_count"] == 2
    assert result["broker_full_fill_count"] == 0
    # A NEW invocation must fetch fresh evidence again, never reuse stale state.
    journal.replay_order_journal(
        current_date, provider=provider,
        include_active_claims=True, active_claims_only=True,
    )
    assert provider.reads == [current_date, source_date, current_date, source_date]
