"""Read-only replay I/O: do not omit evidence when source order book misses."""
from __future__ import annotations


def test_missing_orderbook_still_fetches_original_day_fills(monkeypatch):
    from trader.us.db import repos
    from trader.us.execution import fills, order_journal as journal

    source_date, tick_date = "2026-10-06", "2026-10-16"
    key, attempt, order_no = "lazy-fallback-key", "lazy-fallback-attempt", "0000034237"
    submit = {
        "event_type": "BROKER_SUBMIT_STARTED",
        "event_id": "submitted", "trade_date": source_date,
        "client_order_key": key, "submit_attempt_id": attempt,
        "symbol": "MRVL", "exchange": "NASDAQ", "side": "SELL", "qty": 1,
        "session": "am", "session_run_id": "old-session",
    }
    ack = {
        **submit, "event_type": "BROKER_ACK_RECEIVED",
        "event_id": "ack", "broker_order_no": order_no,
    }
    monkeypatch.setattr(journal, "load_order_events",
                        lambda day, session_run_id=None: [submit, ack] if day == source_date else [])
    monkeypatch.setattr(journal, "append_order_event", lambda *a, **k: {})
    monkeypatch.setattr(repos, "load_active_execution_claim_attempts",
                        lambda: [{"trade_date": source_date, "client_order_key": key,
                                  "active_attempt_id": attempt}])
    monkeypatch.setattr(repos, "save_order_ack", lambda *a, **k: False)

    fill_days = []
    monkeypatch.setattr(
        fills, "get_fills_today",
        lambda provider, trade_date: (
            fill_days.append(trade_date) or {"status": "OK", "fills": []}
        ),
    )

    class Provider:
        def __init__(self):
            self.order_days = []
            self.detail_calls = []

        def get_balance(self, force_refresh=False):
            return {"positions": []}

        def get_order_lookup_provenance(self, day):
            return {"verified": True, "source": "kis_authenticated_order_book",
                    "query_trade_date": day, "market": "US", "account_id": "mock",
                    "account_product_code": "01", "env": "practice"}

        def get_today_orders(self, *, trade_date):
            self.order_days.append(trade_date)
            return []

        def get_fills_by_order_no(self, **kwargs):
            self.detail_calls.append(kwargs)
            return None

    provider = Provider()
    result = journal.replay_order_journal(
        tick_date, provider=provider,
        include_active_claims=True, active_claims_only=True,
    )
    assert provider.order_days == [source_date]
    assert fill_days == [source_date], "missing exact order proof must retain fill fallback"
    assert len(provider.detail_calls) == 1
    assert result["broker_unknown_count"] == 1
    assert result["broker_full_fill_count"] == 0
    assert result["unresolved_count"] == 1
