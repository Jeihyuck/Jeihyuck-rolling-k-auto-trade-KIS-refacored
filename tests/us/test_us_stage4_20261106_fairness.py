"""2026-11-06 Stage4 original-date budget and immutable evidence regressions.

No broker submission, cancellation, production DB access or network traffic.
"""
from __future__ import annotations

import copy
import pytest
from trader.us.execution.stage4_historical_reader import HistoricalReadOnlyReader

SCOPE = {"env": "practice", "account_id": "a" * 64, "trading_epoch_id": "epoch-1"}
CLAIM = {
    "trade_date": "2026-10-30", "client_order_key": "TMO-original",
    "active_attempt_id": "original-attempt",
}
DEBT = {"symbol": "SYY", "lifecycle_id": "original-stop",
        "trading_epoch_id": "epoch-1", "account_id": "practice:offline"}


class StubClockKIS:
    def __init__(self, clock, *, latency=0):
        self.clock = clock
        self.latency = latency
        self.calls = []

    def get_today_orders(self, trade_date):
        self.calls.append(("BOOK", trade_date))
        self.clock[0] += self.latency
        return [{"trade_date": trade_date, "order_no": "0000036019",
                 "symbol": "TMO", "side": "SELL", "requested_qty": 1,
                 "filled_qty": 0, "remaining_qty": 1,
                 "status": "OPEN", "normalization_result": "normalized"}]

    def get_order_lookup_provenance(self, trade_date):
        return {"verified": True, "source": "kis_authenticated_order_book",
                "market": "US", "env": "practice",
                "account_id": SCOPE["account_id"], "account_product_code": "01",
                "query_trade_date": trade_date}

    def get_balance(self, force_refresh=False):
        self.calls.append(("BALANCE", force_refresh))
        self.clock[0] += self.latency
        return {"balance_complete": True, "balance_authoritative": True,
                "balance_parse_status": "OK", "positions": []}


def _reader(kis, clock, *, debts=None, dates=None, claims=None):
    return HistoricalReadOnlyReader(
        scope=SCOPE, generation="offline-g",
        provider_factory=lambda: kis,
        load_claims=lambda: list([CLAIM] if claims is None else claims),
        load_debts=lambda **kw: list([] if debts is None else debts),
        load_debt_dates=lambda _debts, **kw: set([] if dates is None else dates),
        load_scope=lambda: copy.deepcopy(SCOPE),
        clock=lambda: clock[0],
        poll_sec=2.0, ttl_sec=15.0, max_dates=4,
    )


def test_three_real_source_days_cannot_starve_historical_balance():
    """An expiring original claim book must not consume every GET forever."""
    clock = [100.0]
    kis = StubClockKIS(clock, latency=16.0)
    reader = _reader(
        kis, clock, debts=[DEBT], dates={"2026-11-02", "2026-11-03"}
    )
    for _ in range(4):
        reader._poll_once(kis)
    assert ("BOOK", "2026-10-30") in kis.calls
    assert ("BALANCE", True) in kis.calls
    assert kis.calls.index(("BALANCE", True)) < 4
    # A completed balance GET does NOT magically refresh stale old orders.
    # After the newest claim proof ages out, readiness must fail closed.
    clock[0] += 16.0
    with pytest.raises(RuntimeError, match="BROKER_EVIDENCE_STALE"):
        reader.snapshot_for_claims([CLAIM])


def test_expired_adopted_book_cannot_be_renewed_by_identical_later_get():
    """READY cannot be committed using a different observation's fresh TTL."""
    clock = [100.0]
    kis = StubClockKIS(clock)
    reader = _reader(kis, clock)
    reader._poll_once(kis)
    original = reader.snapshot_for_claims([CLAIM])
    clock[0] = 116.0
    reader._poll_once(kis)  # Same broker facts, but new observation.
    reader.snapshot_for_claims([CLAIM])
    with pytest.raises(RuntimeError, match="BROKER_EVIDENCE_STALE"):
        reader.assert_still_valid(original, [CLAIM])


def test_claim_key_change_preempts_balance_without_adopting_old_book():
    clock = [100.0]
    kis = StubClockKIS(clock)
    state = [dict(CLAIM)]
    reader = HistoricalReadOnlyReader(
        scope=SCOPE, generation="offline-g",
        provider_factory=lambda: kis,
        load_claims=lambda: list(state),
        load_debts=lambda **kw: [DEBT],
        load_debt_dates=lambda _debts, **kw: {"2026-10-30"},
        load_scope=lambda: copy.deepcopy(SCOPE),
        clock=lambda: clock[0], ttl_sec=15.0,
    )
    reader._poll_once(kis)
    state[0] = {**CLAIM, "active_attempt_id": "another-attempt"}
    with pytest.raises(RuntimeError, match="CLAIM_SET_CHANGED"):
        reader.snapshot_for_claims(state)
    reader._poll_once(kis)
    assert kis.calls == [("BOOK", "2026-10-30"), ("BOOK", "2026-10-30")]
    reader.snapshot_for_claims(state)
    reader._poll_once(kis)
    assert kis.calls[-1] == ("BALANCE", True)


def test_no_budget_bypass_for_five_source_days():
    clock = [100.0]
    kis = StubClockKIS(clock)
    claims = [{**CLAIM, "trade_date": f"2026-10-{day:02d}",
               "client_order_key": f"k-{day}"} for day in range(20, 25)]
    reader = _reader(kis, clock, claims=claims)
    with pytest.raises(RuntimeError, match="SOURCE_DATES_OVER_BUDGET"):
        reader._poll_once(kis)
    assert kis.calls == []


def test_deferred_settlement_with_no_balance_keeps_fence_without_io(monkeypatch):
    from trader.us.execution import authoritative_reconcile_worker as worker
    from trader.us import protective_stop_obligation as stops
    from trader.us.execution import protective_stop_settlement_worker as settlement

    monkeypatch.setattr(worker, "_DEFERRED_PHASE_CURSOR", 1)
    monkeypatch.setattr(
        stops, "load_pending_stop_obligations",
        lambda *, env: [{"symbol": "SYY", "status": "PENDING"}],
    )
    calls = []
    monkeypatch.setattr(
        settlement, "settle_verified_stop_debts_once",
        lambda **kw: calls.append(kw),
    )
    result = worker.run_deferred_recovery_once(
        provider=object(), trade_date="2026-11-05",
        historical_balance_available=False,
    )
    assert result["phase"] == "settlement"
    assert result["settlement"]["status"] == "DEFERRED_NO_FRESH_EVIDENCE"
    assert result["settlement"]["settled"] == 0
    assert calls == []


def test_deferred_settlement_with_balance_preserves_authoritative_writer(monkeypatch):
    from trader.us.execution import authoritative_reconcile_worker as worker
    from trader.us import protective_stop_obligation as stops
    from trader.us.execution import protective_stop_settlement_worker as settlement

    monkeypatch.setattr(worker, "_DEFERRED_PHASE_CURSOR", 1)
    monkeypatch.setattr(
        stops, "load_pending_stop_obligations",
        lambda *, env: [{"symbol": "SYY", "status": "PENDING"}],
    )
    provider = object()
    calls = []
    def settle_verified(*, provider, trade_date, limit):
        calls.append((provider, trade_date, limit))
        return {"status": "OK", "settled": 0}
    monkeypatch.setattr(
        settlement, "settle_verified_stop_debts_once", settle_verified,
    )
    result = worker.run_deferred_recovery_once(
        provider=provider, trade_date="2026-11-05",
        historical_balance_available=True,
    )
    assert result["settlement"]["status"] == "OK"
    assert calls == [(provider, "2026-11-05", 1)]


def test_slow_claim_book_cannot_starve_debt_only_original_day_books():
    """Original stop-debt dates must eventually receive bounded GET turns."""
    clock = [200.0]
    kis = StubClockKIS(clock, latency=16.0)
    reader = _reader(
        kis, clock, debts=[DEBT], dates={"2026-11-02", "2026-11-03"},
    )
    for _ in range(10):
        reader._poll_once(kis)
    assert ("BALANCE", True) in kis.calls
    assert ("BOOK", "2026-10-30") in kis.calls
    assert ("BOOK", "2026-11-02") in kis.calls
    assert ("BOOK", "2026-11-03") in kis.calls
    # This is progress only, NOT a valid READY claim on expired originals.
    clock[0] += 16.0
    with pytest.raises(RuntimeError, match="BROKER_EVIDENCE_STALE"):
        reader.snapshot_for_claims([CLAIM])
