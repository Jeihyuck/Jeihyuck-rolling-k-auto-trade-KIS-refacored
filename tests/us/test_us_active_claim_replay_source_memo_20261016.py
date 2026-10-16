"""Behavioral tests for safe per-replay source-date journal memoization."""
from trader.us.execution import order_journal


def test_two_active_claims_same_old_trade_date_read_once(monkeypatch):
    from trader.us.db import repos
    calls = []

    def read_events(trade_date, **kwargs):
        calls.append(str(trade_date))
        return []  # Both claims remain blocked: no submit evidence.
    monkeypatch.setattr(order_journal, "load_order_events", read_events)
    monkeypatch.setattr(
        repos, "load_active_execution_claim_attempts",
        lambda: [
            {"trade_date": "2026-10-07", "client_order_key": "K1",
             "active_attempt_id": "ATT1"},
            {"trade_date": "2026-10-07", "client_order_key": "K2",
             "active_attempt_id": "ATT2"},
        ],
    )
    result = order_journal.replay_order_journal(
        "2026-10-16", provider=None,
        include_active_claims=True, active_claims_only=True,
    )
    assert calls == ["2026-10-16", "2026-10-07"]
    assert result["status"] == "ERROR"
    assert result["failed_count"] == 2
    assert result["unresolved_count"] == 2


def test_different_historical_dates_still_get_independent_journal_reads(monkeypatch):
    from trader.us.db import repos
    calls = []
    monkeypatch.setattr(
        order_journal, "load_order_events",
        lambda trade_date, **kwargs: (calls.append(str(trade_date)) or []),
    )
    monkeypatch.setattr(
        repos, "load_active_execution_claim_attempts",
        lambda: [
            {"trade_date": "2026-10-07", "client_order_key": "K1",
             "active_attempt_id": "ATT1"},
            {"trade_date": "2026-10-08", "client_order_key": "K2",
             "active_attempt_id": "ATT2"},
        ],
    )
    result = order_journal.replay_order_journal(
        "2026-10-16", provider=None,
        include_active_claims=True, active_claims_only=True,
    )
    assert calls == ["2026-10-16", "2026-10-07", "2026-10-08"]
    assert result["unresolved_count"] == 2
