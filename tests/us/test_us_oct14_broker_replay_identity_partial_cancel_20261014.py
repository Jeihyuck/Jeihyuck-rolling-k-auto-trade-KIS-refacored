"""Broker replay memo identity, clock bridge and partial-cancel regression."""
from __future__ import annotations

import pytest

from trader.us.execution.order_journal import _replay_order_row_matches


@pytest.fixture
def proof():
    journal = {
        "symbol": "MCK", "side": "SELL", "exchange": "NASDAQ",
        "market": "US", "session": "am", "session_run_id": "session-1",
        "session_generation": 1, "account_id": "account-1",
        "trading_epoch_id": "epoch-1", "strategy_owner": "US_STANDARD",
    }
    order = {
        "order_no": "0000043251", "symbol": "MCK", "side": "SELL",
        "requested_qty": 2, "filled_qty": 1, "remaining_qty": 0,
        "status": "CANCELLED", "normalization_result": "normalized",
        "exchange": "NASD", "market": "US", "session": "am",
        "session_run_id": "session-1", "session_generation": 1,
        "account_id": "account-1", "trading_epoch_id": "epoch-1",
        "strategy_owner": "US_STANDARD",
        "trade_date": "20261014", "us_trade_date": "2026-10-13",
    }
    return journal, order


def _matches(journal, order):
    return _replay_order_row_matches(
        order, order_no="0000043251", symbol="MCK", side="SELL",
        requested_qty=2, source_trade_date="2026-10-13", journal_event=journal,
    )


def test_valid_partial_fill_then_cancel_and_kst_date_bridge(proof):
    journal, order = proof
    assert _matches(journal, order)


@pytest.mark.parametrize("field,value", [
    ("requested_qty", 3), ("filled_qty", 3), ("remaining_qty", 2),
    ("account_id", "other-account"), ("market", "KR"),
    ("session", "afternoon"), ("session_run_id", "different-session"),
    ("session_generation", 2), ("trading_epoch_id", "other-epoch"),
    ("strategy_owner", "TQQQ_INFINITE"), ("exchange", "NYSE"),
    ("us_trade_date", "2026-10-12"), ("trade_date", "20261015"),
])
def test_conflicting_or_overflowing_broker_identity_forces_fallback(proof, field, value):
    journal, order = proof
    order[field] = value
    assert not _matches(journal, order)


@pytest.mark.parametrize("field", ["requested_qty", "filled_qty"])
def test_incomplete_quantity_proof_is_never_reused(proof, field):
    journal, order = proof
    del order[field]
    assert not _matches(journal, order)


def test_submit_cancel_and_late_fill_require_fresh_replay_snapshot(monkeypatch):
    import trader.us.db.repos as repos
    import trader.us.execution.fills as fills
    import trader.us.execution.order_journal as journal

    trade_date = "2026-10-13"
    events = [
        {"event_type": "BROKER_SUBMIT_STARTED", "event_id": "a",
         "trade_date": trade_date, "client_order_key": "key-MCK",
         "submit_attempt_id": "attempt-1", "symbol": "MCK",
         "side": "SELL", "exchange": "NASDAQ", "session": "am", "qty": 2},
        {"event_type": "BROKER_ACK_RECEIVED", "event_id": "b",
         "trade_date": trade_date, "client_order_key": "key-MCK",
         "submit_attempt_id": "attempt-1", "symbol": "MCK",
         "side": "SELL", "exchange": "NASDAQ", "session": "am",
         "qty": 2, "broker_order_no": "0000043251"},
    ]
    monkeypatch.setattr(journal, "load_order_events", lambda date, session_run_id=None: events)
    monkeypatch.setattr(journal, "append_order_event", lambda *a, **kw: {})
    monkeypatch.setattr(repos, "save_order_ack", lambda *a, **kw: True)
    monkeypatch.setattr(repos, "load_active_execution_claim_attempts", lambda: [])
    monkeypatch.setattr(fills, "get_fills_today",
                        lambda provider, trade_date: {"status": "OK", "fills": []})
    observations = []
    def save_broker(**kwargs):
        observations.append(kwargs)
        return {"status": "OK"}
    monkeypatch.setattr(repos, "apply_broker_order_observation", save_broker)

    class Provider:
        stage = 0
        reads = 0
        def get_balance(self, force_refresh=False):
            return {"positions": []}
        def get_order_lookup_provenance(self, trade_date):
            return {"verified": True, "source": "kis_authenticated_order_book",
                    "query_trade_date": trade_date, "market": "US",
                    "account_id": "verified-practice-account", "session": "am"}

        def get_today_orders(self, *, trade_date):
            self.reads += 1
            return [{
                "order_no": "0000043251", "symbol": "MCK", "side": "SELL",
                "exchange": "NASD", "trade_date": "20261013",
                "requested_qty": 2, "filled_qty": (0 if self.stage == 0 else 1),
                "remaining_qty": (2 if self.stage == 0 else 0),
                "status": ("OPEN" if self.stage == 0 else "CANCELLED"),
                "normalization_result": "normalized", "avg_price": 100.0,
            }]
        def get_fills_by_order_no(self, **kwargs):
            pytest.fail("validated KIS order book must not need a duplicate request")
    provider = Provider()
    first = journal.replay_order_journal(trade_date, provider=provider)
    assert first["broker_unfilled_ack_count"] == 1
    assert provider.reads == 1
    provider.stage = 1   # Broker cancel/partial fill arrived after previous observation.
    second = journal.replay_order_journal(trade_date, provider=provider)
    assert provider.reads == 2
    assert second["broker_partial_fill_count"] == 1
    assert observations[-1]["requested_qty"] == 2
    assert observations[-1]["filled_qty"] == 1
    assert observations[-1]["remaining_qty"] == 0
    assert observations[-1]["broker_status"] == "CANCELLED"


def test_absent_kis_row_account_session_and_date_require_proven_scope(proof):
    journal, row = proof
    # A row containing order number/symbol/quantity but no context is NOT a
    # verified account/session/date match for reusing a KIS snapshot.
    for name in ("account_id", "session", "trade_date", "us_trade_date"):
        row.pop(name, None)
    assert not _matches(journal, row)
    proven = {"verified": True, "source": "kis_authenticated_order_book",
              "query_trade_date": "2026-10-13", "market": "US",
              "account_id": "account-1", "session": "am",
              "session_run_id": "session-1", "session_generation": 1}
    assert _replay_order_row_matches(
        row, order_no="0000043251", symbol="MCK", side="SELL",
        requested_qty=2, source_trade_date="2026-10-13",
        journal_event=journal, lookup_context=proven)
    for field, value in (("account_id", "wrong-account"),
                         ("session", "afternoon"),
                         ("query_trade_date", "2026-10-12")):
        wrong = dict(proven, **{field: value})
        assert not _replay_order_row_matches(
            row, order_no="0000043251", symbol="MCK", side="SELL",
            requested_qty=2, source_trade_date="2026-10-13",
            journal_event=journal, lookup_context=wrong)
