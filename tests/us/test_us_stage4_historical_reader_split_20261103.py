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


def _reader(*, kis=None, active=None, debts=None, clock=None, **kwargs):
    provider = kis or ReadOnlyKIS()
    rows = active if active is not None else [CLAIM]
    debt_rows = debts if debts is not None else []
    return HistoricalReadOnlyReader(
        scope=SCOPE, generation="generation-1",
        provider_factory=lambda: provider,
        load_claims=lambda: list(rows),
        load_debts=lambda **kw: list(debt_rows),
        load_debt_dates=lambda _debts, **kw: set(),
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
    reader = _reader(kis=kis, active=[], debts=[debt])
    reader._poll_once(kis)
    snapshot = reader.snapshot_for_claims([])
    proxy = reader.provider_for(snapshot)
    assert kis.calls == [("GET_BALANCE", True)]
    assert proxy.get_balance(force_refresh=True)["positions"] == []
    assert kis.calls == [("GET_BALANCE", True)]


def test_prefetch_active_claims_identity_must_be_complete():
    with pytest.raises(RuntimeError, match="IDENTITY_MISSING"):
        active_claim_signature([{"trade_date": "2026-10-30"}])
