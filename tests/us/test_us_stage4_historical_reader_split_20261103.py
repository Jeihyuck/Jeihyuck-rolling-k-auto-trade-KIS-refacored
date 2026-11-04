"""Stage4 old-order KIS GET is independent; a single writer applies proof.

All providers are offline stubs: NO actual KIS, cancellation or DB writes.
"""
from __future__ import annotations

import copy
import threading
import time

import pytest

from trader.us.execution.stage4_historical_reader import (
    HistoricalReadOnlyReader, SnapshotOnlyProvider,
    active_claim_signature,
)


SCOPE = {"env": "practice", "account_id": "a" * 64, "trading_epoch_id": "epoch-test"}
CLAIM = {
    "trade_date": "2026-10-30", "client_order_key": "tmo-original",
    "active_attempt_id": "tmo-attempt",
}
SOURCE_DATE = "2026-10-30"


class ReadOnlyKIS:
    def __init__(self, scope=None, rows=None):
        self.scope = scope or SCOPE
        self.rows = rows if rows is not None else [{
            "trade_date": SOURCE_DATE, "order_no": "0000036019", "symbol": "TMO",
            "side": "SELL", "requested_qty": 1, "filled_qty": 0,
            "remaining_qty": 1, "status": "OPEN",
            "normalization_result": "normalized",
            "filled_qty_present": True,
        }]
        self.calls = []

    def get_today_orders(self, trade_date):
        self.calls.append(("GET_ORDER_BOOK", trade_date))
        return copy.deepcopy(self.rows)

    def get_order_lookup_provenance(self, trade_date):
        return {
            "verified": True, "source": "kis_authenticated_order_book",
            "query_trade_date": trade_date, "market": "US", "env": "practice",
            "account_id": self.scope["account_id"],
            "account_product_code": "01",
        }

    def get_balance(self, force_refresh=False):
        self.calls.append(("GET_BALANCE", bool(force_refresh)))
        return {
            "balance_complete": True, "balance_authoritative": True,
            "balance_parse_status": "OK", "positions": [],
        }


def _reader(*, kis=None, active=None, debts=None, debt_dates=None,
            clock=None, **kwargs):
    provider = kis or ReadOnlyKIS()
    rows = active if active is not None else [CLAIM]
    debt_rows = debts if debts is not None else []
    return HistoricalReadOnlyReader(
        scope=SCOPE, generation="generation-1",
        provider_factory=lambda: provider,
        load_claims=lambda: list(rows),
        load_debts=lambda **kw: list(debt_rows),
        load_debt_dates=lambda _debts, **kw: set(debt_dates or ()),
        load_scope=lambda: dict(SCOPE),
        clock=clock or time.monotonic,
        poll_sec=1.0, ttl_sec=15.0,
        **kwargs,
    )


def test_prefetched_original_day_is_immutable_and_never_calls_kis_on_writer():
    kis = ReadOnlyKIS()
    reader = _reader(kis=kis)
    reader._poll_once(kis)
    evidence = reader.snapshot_for_claims([CLAIM])
    proxy = reader.provider_for(evidence)
    assert kis.calls == [("GET_ORDER_BOOK", SOURCE_DATE)]
    rows = proxy.get_today_orders(SOURCE_DATE)
    assert rows[0]["order_no"] == "0000036019"
    assert proxy.get_order_lookup_provenance(SOURCE_DATE)["account_id"] == SCOPE["account_id"]
    assert proxy.get_fills_by_order_no(
        order_no="0000036019", symbol="TMO", trade_date=SOURCE_DATE,
    )["status"] == "OPEN"
    rows[0]["status"] = "FILLED"
    assert proxy.get_today_orders(SOURCE_DATE)[0]["status"] == "OPEN"
    with pytest.raises(RuntimeError, match="BALANCE_NOT_PREFETCHED"):
        proxy.get_balance(force_refresh=True)
    with pytest.raises(RuntimeError, match="RAW_KIS_IO_FORBIDDEN"):
        proxy._get_client()
    assert kis.calls == [("GET_ORDER_BOOK", SOURCE_DATE)]
    assert not hasattr(proxy, "place_us_sell_order")
    assert not hasattr(proxy, "cancel_us_order")


def test_missing_original_book_blocks_ready_until_successful_read():
    clock = [100.0]
    kis = ReadOnlyKIS()
    reader = _reader(kis=kis, clock=lambda: clock[0])
    with pytest.raises(RuntimeError, match="READONLY_UNAVAILABLE"):
        reader.snapshot_for_claims([CLAIM])
    reader._poll_once(kis)
    snapshot = reader.snapshot_for_claims([CLAIM])
    reader.assert_still_valid(snapshot, [CLAIM])
    clock[0] += 16.0
    with pytest.raises(RuntimeError, match="BROKER_EVIDENCE_STALE"):
        reader.snapshot_for_claims([CLAIM])
    with pytest.raises(RuntimeError, match="BROKER_EVIDENCE_STALE"):
        reader.assert_still_valid(snapshot, [CLAIM])


def test_claim_change_between_proof_and_ready_is_fenced():
    kis = ReadOnlyKIS()
    reader = _reader(kis=kis)
    reader._poll_once(kis)
    snapshot = reader.snapshot_for_claims([CLAIM])
    new_claim = {**CLAIM, "active_attempt_id": "other-submission"}
    with pytest.raises(RuntimeError, match="CLAIM_SET_CHANGED"):
        reader.assert_still_valid(snapshot, [new_claim])
    with pytest.raises(RuntimeError, match="CLAIM_SET_CHANGED"):
        reader.snapshot_for_claims([CLAIM, new_claim])


@pytest.mark.parametrize("broken_scope", [
    {"verified": False},
    {"env": "real"},
    {"account_id": "b" * 64},
    {"query_trade_date": "2026-10-29"},
    {"account_product_code": ""},
])
def test_incorrect_original_kis_scope_never_becomes_usable_proof(broken_scope):
    class Broken(ReadOnlyKIS):
        def get_order_lookup_provenance(self, trade_date):
            return {**super().get_order_lookup_provenance(trade_date), **broken_scope}
    kis = Broken()
    reader = _reader(kis=kis)
    with pytest.raises(RuntimeError, match="BROKER_SCOPE_UNVERIFIED"):
        reader._poll_once(kis)
    with pytest.raises(RuntimeError, match="READONLY_UNAVAILABLE"):
        reader.snapshot_for_claims([CLAIM])


def test_quarantined_normalization_never_promotes_incomplete_pagination():
    kis = ReadOnlyKIS(rows=[{
        "trade_date": SOURCE_DATE, "order_no": "0000036019",
        "symbol": "TMO", "side": "SELL",
        "normalization_result": "quarantined",
    }])
    reader = _reader(kis=kis)
    with pytest.raises(RuntimeError, match="NORMALIZATION_INCOMPLETE"):
        reader._poll_once(kis)
    with pytest.raises(RuntimeError, match="READONLY_UNAVAILABLE"):
        reader.snapshot_for_claims([CLAIM])


def test_background_get_cannot_delay_main_writer_and_stays_fail_closed():
    entered = threading.Event()
    release = threading.Event()

    class SlowKIS(ReadOnlyKIS):
        def get_today_orders(self, trade_date):
            entered.set()
            assert release.wait(timeout=4.0)
            return super().get_today_orders(trade_date)

    kis = SlowKIS()
    reader = _reader(kis=kis)
    reader.start()
    try:
        assert entered.wait(timeout=2.0)
        started = time.monotonic()
        with pytest.raises(RuntimeError, match="READONLY_UNAVAILABLE"):
            reader.snapshot_for_claims([CLAIM])
        assert time.monotonic() - started < 0.3
        release.set()
        deadline = time.monotonic() + 2.0
        while True:
            try:
                reader.snapshot_for_claims([CLAIM])
                break
            except RuntimeError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.01)
    finally:
        release.set()
        reader.stop()


def test_only_deferred_debts_fetch_prefetched_balance_off_writer():
    kis = ReadOnlyKIS()
    debt = {
        "account_id": "practice:stub", "trading_epoch_id": "epoch-test",
        "symbol": "TMO", "lifecycle_id": "life",
    }
    reader = _reader(kis=kis, active=[], debts=[debt],
                     debt_dates={SOURCE_DATE})
    reader._poll_once(kis)  # original-day book GET
    reader._poll_once(kis)  # independent balance GET
    snapshot = reader.snapshot_for_claims([])
    proxy = reader.provider_for(snapshot)
    assert kis.calls == [("GET_ORDER_BOOK", SOURCE_DATE), ("GET_BALANCE", True)]
    assert proxy.get_balance(force_refresh=True)["positions"] == []
    assert kis.calls == [("GET_ORDER_BOOK", SOURCE_DATE), ("GET_BALANCE", True)]


def test_stop_debt_without_matching_original_tp_skips_3_exchange_balance():
    debt = {
        "account_id": "practice:stub", "trading_epoch_id": "epoch-test",
        "symbol": "ELV", "lifecycle_id": "life",
    }
    kis = ReadOnlyKIS()
    reader = _reader(kis=kis, active=[], debts=[debt], debt_dates=set())
    reader._poll_once(kis)
    snapshot = reader.snapshot_for_claims([])
    assert kis.calls == []
    with pytest.raises(RuntimeError, match="BALANCE_NOT_PREFETCHED"):
        reader.provider_for(snapshot).get_balance(force_refresh=True)


def test_prefetch_active_claims_identity_must_be_complete():
    with pytest.raises(RuntimeError, match="IDENTITY_MISSING"):
        active_claim_signature([{"trade_date": "2026-10-30"}])


def test_writer_uses_prefetched_historical_book_but_critical_fills_remain_kis(monkeypatch):
    from trader.us.execution import authoritative_reconcile_worker as worker
    from trader.us.execution import fills as fill_source
    from trader.us.db import repos
    from trader.us.execution import order_journal, reconcile
    from trader.us.execution import reservation_reconcile_worker as reservation
    from trader.us.runner import trade_tick_runner as ticks

    main_kis = object()
    kis = ReadOnlyKIS()
    reader = _reader(kis=kis)
    reader._poll_once(kis)
    snapshot = reader.snapshot_for_claims([CLAIM])
    historical_provider = reader.provider_for(snapshot)
    observed = []

    def fetch_critical(**kwargs):
        assert kwargs["provider"] is main_kis
        observed.append("CURRENT_KIS_FILL")
        return {"status": "OK", "fills": []}

    def replay(*args, **kwargs):
        assert kwargs["provider"] is historical_provider
        # The writer may read the frozen GET-only original source day; it
        # cannot make additional original KIS network requests.
        assert kwargs["provider"].get_today_orders(SOURCE_DATE)[0]["status"] == "OPEN"
        observed.append("ORIGINAL_DATE_REPLAY")
        return {"status": "UNRESOLVED", "unresolved_count": 1}

    def ack(**kwargs):
        assert kwargs["provider"] is main_kis
        observed.append("CURRENT_ACK")
        return {"status": "OK", "unresolved_count": 0, "pending_count": 0}

    monkeypatch.setattr(worker, "_STAGE4_FILL_MEMOS", {})
    monkeypatch.setattr(fill_source, "get_fills_today", fetch_critical)
    monkeypatch.setattr(repos, "save_fills_with_result",
                        lambda *a, **k: {"status": "OK"})
    monkeypatch.setattr(order_journal, "replay_order_journal", replay)
    monkeypatch.setattr(reconcile, "reconcile_ack_orders_with_balance", ack)
    monkeypatch.setattr(reservation, "reconcile_proven_reservations_once",
                        lambda **k: {"unresolved": 0})
    monkeypatch.setattr(ticks, "_mark_trend_stages_from_records",
                        lambda *a, **k: None)
    monkeypatch.setattr(ticks, "_mark_soft_stop_stages_from_records",
                        lambda *a, **k: None)
    result = worker.run_reconcile_once(
        provider=main_kis, trade_date="2026-11-02",
        session_run_id="offline-split", writer_generation="g",
        conn=None, scope={}, historical_provider=historical_provider,
    )
    assert result["reconcile_status"] == "OK"
    assert result["replay_status"] == "UNRESOLVED"
    assert result["unresolved_count"] == 1
    assert observed == ["CURRENT_KIS_FILL", "ORIGINAL_DATE_REPLAY", "CURRENT_ACK"]
    assert kis.calls == [("GET_ORDER_BOOK", SOURCE_DATE)]


def test_writer_snapshot_get_rechecks_expiration_at_exact_use_boundary():
    now = [50.0]
    kis = ReadOnlyKIS()
    reader = _reader(kis=kis, clock=lambda: now[0])
    reader._poll_once(kis)
    snapshot = reader.snapshot_for_claims([CLAIM])
    historical_provider = reader.provider_for(snapshot)
    now[0] = 66.0
    with pytest.raises(RuntimeError, match="BROKER_EVIDENCE_STALE"):
        historical_provider.get_today_orders(SOURCE_DATE)
    with pytest.raises(RuntimeError, match="BROKER_EVIDENCE_STALE"):
        historical_provider.get_fills_by_order_no(
            order_no="0000036019", symbol="TMO", trade_date=SOURCE_DATE,
        )


def test_source_day_limit_fails_closed_without_truncating_old_claims():
    claims = [
        {"trade_date": f"2026-10-{day:02d}",
         "client_order_key": f"key-{day}",
         "active_attempt_id": f"attempt-{day}"}
        for day in range(20, 25)
    ]
    kis = ReadOnlyKIS()
    reader = _reader(kis=kis, active=claims)
    with pytest.raises(RuntimeError, match="SOURCE_DATES_OVER_BUDGET"):
        reader._poll_once(kis)
    assert kis.calls == []


def test_epoch_mismatch_after_read_before_publish_is_fenced():
    reader = _reader(kis=ReadOnlyKIS())
    kis = ReadOnlyKIS()
    reader._poll_once(kis)
    snapshot = reader.snapshot_for_claims([CLAIM])
    reader.load_scope = lambda: {**SCOPE, "trading_epoch_id": "new-epoch"}
    with pytest.raises(RuntimeError, match="ACCOUNT_OR_EPOCH_CHANGED"):
        reader.assert_still_valid(snapshot, [CLAIM])
    with pytest.raises(RuntimeError, match="ACCOUNT_OR_EPOCH_CHANGED"):
        reader.snapshot_for_claims([CLAIM])


def test_identical_newer_readonly_snapshot_does_not_starve_single_writer_ready():
    now = [60.0]
    kis = ReadOnlyKIS()
    reader = _reader(kis=kis, clock=lambda: now[0])
    reader._poll_once(kis)
    snapshot = reader.snapshot_for_claims([CLAIM])
    now[0] = 62.0
    reader._poll_once(kis)   # refreshed with same KIS status and qty
    reader.assert_still_valid(snapshot, [CLAIM])


def test_newer_kis_fill_cannot_be_overwritten_by_old_snapshot():
    now = [60.0]
    kis = ReadOnlyKIS()
    reader = _reader(kis=kis, clock=lambda: now[0])
    reader._poll_once(kis)
    original = reader.snapshot_for_claims([CLAIM])
    kis.rows[0]["status"] = "FILLED"
    kis.rows[0]["filled_qty"] = 1
    kis.rows[0]["remaining_qty"] = 0
    now[0] = 62.0
    reader._poll_once(kis)
    with pytest.raises(RuntimeError, match="NEWER_BROKER_FACTS_CHANGED"):
        reader.assert_still_valid(original, [CLAIM])


def test_same_attempt_cumulative_fill_change_fences_old_broker_snapshot():
    kis = ReadOnlyKIS()
    original = {
        **CLAIM, "strategy_owner": "US_STANDARD",
        "trading_epoch_id": SCOPE["trading_epoch_id"],
        "requested_qty": 1, "cumulative_filled_qty": 0,
        "attempt_state": "ACK_PENDING",
    }
    reader = _reader(kis=kis, active=[original])
    reader._poll_once(kis)
    snapshot = reader.snapshot_for_claims([original])
    new_fill = {**original, "cumulative_filled_qty": 1}
    with pytest.raises(RuntimeError, match="CLAIM_SET_CHANGED"):
        reader.assert_still_valid(snapshot, [new_fill])
    new_state = {**original, "attempt_state": "PARTIALLY_SATISFIED"}
    with pytest.raises(RuntimeError, match="CLAIM_SET_CHANGED"):
        reader.assert_still_valid(snapshot, [new_state])


def test_zero_filled_and_missing_cumulative_are_different_broker_facts():
    now = [60.0]
    kis = ReadOnlyKIS()
    reader = _reader(kis=kis, clock=lambda: now[0])
    reader._poll_once(kis)
    original = reader.snapshot_for_claims([CLAIM])
    # Treat "filled_qty=0" as actual KIS zero, not an absent unknown.
    kis.rows[0].pop("filled_qty")
    kis.rows[0].pop("filled_qty_present")
    now[0] = 61.0
    reader._poll_once(kis)
    with pytest.raises(RuntimeError, match="NEWER_BROKER_FACTS_CHANGED"):
        reader.assert_still_valid(original, [CLAIM])


def test_new_claim_set_refreshes_original_book_before_deferred_balance():
    # Stale identity must not authorize READY while the reader spends its
    # next GET on an unrelated three-exchange balance.
    kis = ReadOnlyKIS()
    debt = {"symbol": "TMO", "lifecycle_id": "life"}
    reader = _reader(
        kis=kis, active=[CLAIM], debts=[debt], debt_dates={SOURCE_DATE},
    )
    reader._poll_once(kis)  # original-day proof
    updated = {**CLAIM, "active_attempt_id": "tmo-new-attempt"}
    reader.load_claims = lambda: [updated]
    with pytest.raises(RuntimeError, match="CLAIM_SET_CHANGED"):
        reader.snapshot_for_claims([updated])
    reader._poll_once(kis)  # MUST refresh book before balance
    assert kis.calls == [
        ("GET_ORDER_BOOK", SOURCE_DATE),
        ("GET_ORDER_BOOK", SOURCE_DATE),
    ]
    reader.snapshot_for_claims([updated])
    reader._poll_once(kis)  # balance can proceed after fresh book proof
    assert kis.calls[-1] == ("GET_BALANCE", True)


def test_expired_historical_book_is_refetched_without_extending_evidence_ttl():
    now = [100.0]
    kis = ReadOnlyKIS()
    debt = {"symbol": "TMO", "lifecycle_id": "life"}
    reader = _reader(
        kis=kis, active=[CLAIM], debts=[debt], debt_dates={SOURCE_DATE},
        clock=lambda: now[0],
    )
    reader._poll_once(kis)
    now[0] += 16.0
    with pytest.raises(RuntimeError, match="BROKER_EVIDENCE_STALE"):
        reader.snapshot_for_claims([CLAIM])
    reader._poll_once(kis)
    assert kis.calls == [
        ("GET_ORDER_BOOK", SOURCE_DATE),
        ("GET_ORDER_BOOK", SOURCE_DATE),
    ]
    reader.snapshot_for_claims([CLAIM])


def test_stage4_unverified_replay_reports_exact_failure_class_without_ready(monkeypatch, caplog):
    from trader.us.execution import authoritative_reconcile_worker as worker
    from trader.us.execution import fills as fill_source
    from trader.us.db import repos
    from trader.us.execution import order_journal, reconcile
    from trader.us.execution import reservation_reconcile_worker as reservation
    from trader.us.runner import trade_tick_runner as ticks

    monkeypatch.setattr(worker, "_STAGE4_FILL_MEMOS", {})
    monkeypatch.setattr(
        fill_source, "get_fills_today",
        lambda **kw: {"status": "OK", "fills": []},
    )
    monkeypatch.setattr(
        repos, "save_fills_with_result",
        lambda *a, **kw: {"status": "OK"},
    )
    monkeypatch.setattr(
        order_journal, "replay_order_journal",
        lambda *a, **kw: {
            "status": "ERROR", "failed_count": 1,
            "identity_mismatch_count": 0, "broker_unknown_count": 0,
            "unresolved_count": 1,
        },
    )
    monkeypatch.setattr(
        reconcile, "reconcile_ack_orders_with_balance",
        lambda **kw: {"status": "OK", "unresolved_count": 0},
    )
    monkeypatch.setattr(
        reservation, "reconcile_proven_reservations_once",
        lambda **kw: {"unresolved": 0},
    )
    monkeypatch.setattr(
        ticks, "_mark_trend_stages_from_records",
        lambda *a, **kw: None,
    )
    monkeypatch.setattr(
        ticks, "_mark_soft_stop_stages_from_records",
        lambda *a, **kw: None,
    )
    with pytest.raises(RuntimeError, match="US_STAGE4_BROKER_RECOVERY_NOT_VERIFIED"):
        worker.run_reconcile_once(
            provider=object(), trade_date="2026-11-03",
            session_run_id="offline", writer_generation="g", conn=None,
            scope={}, historical_provider=object(),
        )
    assert "replay_status=ERROR ack_status=OK" in caplog.text
    assert "action=FAIL_CLOSED" in caplog.text
