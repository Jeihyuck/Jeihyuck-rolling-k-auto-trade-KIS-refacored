import pytest

from trader.us.execution.order_journal import append_order_event,replay_order_journal

def test_ack_replay_restores_db_once(tmp_path,monkeypatch):
    monkeypatch.setenv('US_ORDER_JOURNAL_DIR',str(tmp_path)); i={'trade_date':'2026-07-16','client_order_key':'k','symbol':'AMD','exchange':'NASDAQ','side':'SELL','qty':2}
    append_order_event('BROKER_SUBMIT_STARTED',i); append_order_event('BROKER_ACK_RECEIVED',i,broker_order_no='O1')
    monkeypatch.setattr('trader.us.db.repos.save_order_ack',lambda *a,**k: True)
    r=replay_order_journal('2026-07-16'); assert r['restored_ack_count']==1 and r['unresolved_count']==1 and r['status']=='UNRESOLVED'


def test_cross_date_replay_uses_source_date_fills_for_reused_broker_order_number(
    monkeypatch,
):
    import trader.us.db.repos as repos
    import trader.us.execution.order_journal as journal

    source_date = "2026-10-01"
    invocation_date = "2026-10-02"
    client_key = "prior-date-order"
    attempt_id = "prior-date-attempt"
    source_events = [
        {
            "event_id": "prior-submit",
            "event_type": "BROKER_SUBMIT_STARTED",
            "trade_date": source_date,
            "client_order_key": client_key,
            "submit_attempt_id": attempt_id,
            "symbol": "QXYZ",
            "exchange": "NASDAQ",
            "side": "SELL",
            "qty": 2,
            "submitted_at_utc": "2026-10-01T15:00:00+00:00",
        },
        {
            "event_id": "prior-ack",
            "event_type": "BROKER_ACK_RECEIVED",
            "trade_date": source_date,
            "client_order_key": client_key,
            "submit_attempt_id": attempt_id,
            "symbol": "QXYZ",
            "exchange": "NASDAQ",
            "side": "SELL",
            "qty": 2,
            "broker_order_no": "REUSED-ORDER-NO",
        },
    ]
    monkeypatch.setattr(
        journal,
        "load_order_events",
        lambda trade_date, session_run_id=None: (
            source_events if trade_date == source_date else []
        ),
    )
    monkeypatch.setattr(
        repos,
        "load_active_execution_claim_attempts",
        lambda: [{
            "trade_date": source_date,
            "client_order_key": client_key,
            "active_attempt_id": attempt_id,
        }],
    )
    monkeypatch.setattr(journal, "append_order_event", lambda *args, **kwargs: {})
    monkeypatch.setattr(repos, "save_order_ack", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        repos,
        "apply_broker_order_observation",
        lambda **kwargs: pytest.fail("invocation-date fill must not reconcile prior-date claim"),
    )
    monkeypatch.setattr(
        repos,
        "mark_order_filled_by_reconcile",
        lambda **kwargs: pytest.fail("invocation-date fill must not reconcile prior-date claim"),
    )

    requested_fill_dates = []

    def fills_today(*, provider, trade_date):
        requested_fill_dates.append(trade_date)
        if trade_date == invocation_date:
            return {
                "status": "OK",
                "fills": [{
                    "order_no": "REUSED-ORDER-NO",
                    "symbol": "QXYZ",
                    "side": "SELL",
                    "filled_qty": 2,
                    "avg_price": 11.0,
                }],
            }
        return {"status": "OK", "fills": []}

    monkeypatch.setattr("trader.us.execution.fills.get_fills_today", fills_today)

    class Provider:
        def get_balance(self, force_refresh=False):
            return {"positions": []}

        def get_today_orders(self, *, trade_date):
            return []

    result = journal.replay_order_journal(
        invocation_date,
        provider=Provider(),
        include_active_claims=True,
        active_claims_only=True,
    )

    assert set(requested_fill_dates) == {source_date, invocation_date}
    assert result["filled_count"] == 0
    assert result["unresolved_count"] == 1
