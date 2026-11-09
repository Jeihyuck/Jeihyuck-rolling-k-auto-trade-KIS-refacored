from datetime import date

import pytest

from trader.us.db import repos
from trader.us.execution import order_journal

TODAY = "2026-11-06"
PRIOR = "2026-11-05"


def test_current_replay_does_not_load_prior_journals(monkeypatch, tmp_path):
    monkeypatch.setenv("US_ORDER_JOURNAL_DIR", str(tmp_path))
    monkeypatch.setattr(repos, "load_active_execution_claim_attempts", lambda: [
        {"trade_date": date.fromisoformat(PRIOR), "client_order_key": "old", "active_attempt_id": "old-attempt"},
    ])

    def read_events(day, **_kwargs):
        assert day == TODAY, "an old journal must not be opened by intraday replay"
        return []

    monkeypatch.setattr(order_journal, "load_order_events", read_events)
    result = order_journal.replay_order_journal(
        TODAY, include_active_claims=True, active_claims_only=True,
        active_claim_date_scope="current",
    )
    assert result["status"] == "OK"
    assert result["unresolved_count"] == 0


@pytest.mark.parametrize("phase", ["AM_START", "CLOSE"])
def test_boundary_reports_prior_claim_failure_and_skips_current(monkeypatch, caplog, phase):
    monkeypatch.setattr(repos, "load_active_execution_claim_attempts", lambda: [
        {"trade_date": TODAY, "client_order_key": "current", "active_attempt_id": "current-attempt"},
        {"trade_date": PRIOR, "client_order_key": "old", "active_attempt_id": "old-attempt"},
    ])

    def read_events(day, **_kwargs):
        assert day == PRIOR
        raise OSError("historical journal unavailable")

    monkeypatch.setattr(order_journal, "load_order_events", read_events)
    result = order_journal.report_historical_order_recovery(
        provider=object(), trade_date=TODAY, phase=phase,
    )
    assert result["unresolved_count"] == 1
    assert f"phase={phase}" in caplog.text
    assert "status=RECOVERY_UNRESOLVED" in caplog.text


def test_buy_ack_survives_db_outage_and_is_restored_only_for_matching_attempt(monkeypatch, tmp_path):
    monkeypatch.setenv("US_ORDER_JOURNAL_DIR", str(tmp_path))
    writes = []

    def append(event):
        if event["event_type"] == "BROKER_ACK_RECEIVED" and not writes:
            return False
        writes.append(event)
        return True

    monkeypatch.setattr(repos, "append_us_order_event", append)
    intent = {
        "trade_date": TODAY, "client_order_key": "buy", "submit_attempt_id": "attempt",
        "symbol": "TEST", "side": "BUY", "qty": 2, "broker_account_hash": "account",
        "strategy_owner": "US_STANDARD", "trading_epoch_id": "epoch",
    }
    with pytest.raises(OSError, match="local_ack_preserved"):
        order_journal.append_order_event(
            "BROKER_ACK_RECEIVED", intent, broker_order_no="000123", require_db=True,
        )
    assert order_journal._restore_local_buy_acks(TODAY, []) == []
    started = order_journal.append_order_event("BROKER_SUBMIT_STARTED", intent, require_db=True)
    restored = order_journal._restore_local_buy_acks(TODAY, [started])
    assert restored[-1]["broker_order_no"] == "000123"
    assert writes[-1]["event_type"] == "BROKER_ACK_RECEIVED"
    assert order_journal._restore_local_buy_acks(TODAY, restored) == restored
    assert len(writes) == 2


def test_required_buy_db_write_disallows_test_and_offline_fallback(monkeypatch, tmp_path):
    monkeypatch.setenv("US_ORDER_JOURNAL_DIR", str(tmp_path))
    monkeypatch.setenv("US_OFFLINE", "1")
    monkeypatch.setattr(repos, "append_us_order_event", lambda _event: False)
    with pytest.raises(OSError, match="ledger_unavailable"):
        order_journal.append_order_event(
            "BROKER_SUBMIT_STARTED",
            {"trade_date": TODAY, "side": "BUY", "symbol": "TEST", "qty": 2},
            require_db=True,
        )
    assert not order_journal.journal_path(TODAY).exists()
