"""US v3: immutable legacy broker evidence caching is strictly scope-bound.

These are read-only proof tests. No KIS order submission or credentials.
"""
import pytest

from trader.us.execution import us_legacy_nine_active_sell as legacy


@pytest.fixture(autouse=True)
def clear_previous_day_terminal_cache():
    legacy._HISTORICAL_TERMINAL_CACHE.clear()
    yield
    legacy._HISTORICAL_TERMINAL_CACHE.clear()


def _probe(*, current_day="2026-11-16", source_day="2026-11-04",
           account="a" * 64, epoch="epoch-1", digest="c" * 64,
           symbol="XYZ", side="SELL", classification="PROVEN_CANCELLED",
           qty=10, filled=4):
    calls = {"get": 0, "classify": 0}
    def get(day):
        calls["get"] += 1
        return [{"order_no": "0000000001", "trade_date": day}]
    def classify(rows, **kwargs):
        calls["classify"] += 1
        return {
            "classification": classification,
            "cumulative_filled_qty": filled,
            "order_no": kwargs["order_no"],
        }
    def run():
        return legacy._classify_immutable_source_order(
            classifier=classify, get_order_day=get,
            day=source_day, current_trade_date=current_day,
            source_account_hash=account, epoch=epoch,
            contract_sha=digest, symbol=symbol, side=side,
            order_no="0000000001", requested_qty=qty,
            cumulative_filled_qty=filled,
        )
    return run, calls


def test_exact_previous_day_cancelled_partial_sell_cached_but_not_unsafe_open():
    run, calls = _probe()
    assert run()["classification"] == "PROVEN_CANCELLED"
    assert run()["cumulative_filled_qty"] == 4
    assert calls == {"get": 1, "classify": 1}
    assert run()["classification"] == "PROVEN_CANCELLED"
    assert calls == {"get": 1, "classify": 1}


@pytest.mark.parametrize("classification", [
    "UNKNOWN", "OPEN", "SUBMITTED", "PROVEN_UNRESOLVED",
])
def test_no_cache_for_unknown_open_or_unverified_previous_orders(classification):
    run, calls = _probe(classification=classification)
    assert run()["classification"] == classification
    assert run()["classification"] == classification
    assert calls == {"get": 2, "classify": 2}


def test_current_day_terminal_always_fetches_fresh_kis_order_evidence():
    run, calls = _probe(current_day="2026-11-16",
                        source_day="2026-11-16")
    run()
    run()
    assert calls == {"get": 2, "classify": 2}


def test_broker_proof_cannot_cross_account_epoch_contract_or_symbol():
    account = "a" * 64
    a, a_calls = _probe(account=account)
    b, b_calls = _probe(account="b" * 64)
    c, c_calls = _probe(account=account, epoch="epoch-2")
    d, d_calls = _probe(account=account, digest="d" * 64)
    e, e_calls = _probe(account=account, symbol="NEW_TICKER")
    for runner in (a,b,c,d,e):
        runner()
    assert [c["get"] for c in (a_calls,b_calls,c_calls,d_calls,e_calls)] == [1] * 5
    for runner in (a,b,c,d,e):
        runner()
    assert [c["get"] for c in (a_calls,b_calls,c_calls,d_calls,e_calls)] == [1] * 5


def test_original_buy_proof_requires_terminal_fill_not_cancelled():
    run, calls = _probe(side="BUY", classification="PROVEN_FILLED", qty=10, filled=10)
    run()
    run()
    assert calls["get"] == 1
    rejected, other_calls = _probe(side="BUY", classification="PROVEN_CANCELLED", qty=10, filled=0)
    rejected()
    rejected()
    assert other_calls["get"] == 2


def test_cumulative_quantity_disagreement_never_cached():
    fetches = {"count": 0}
    def get(day):
        fetches["count"] += 1
        return [day]
    def classify(rows, **kwargs):
        return {"classification": "PROVEN_FILLED", "cumulative_filled_qty": 9}
    for _ in range(2):
        legacy._classify_immutable_source_order(
            classifier=classify, get_order_day=get,
            day="2026-10-10", current_trade_date="2026-11-16",
            source_account_hash="a" * 64, epoch="epoch", contract_sha="b" * 64,
            symbol="XYZ", side="BUY", order_no="11", requested_qty=10,
            cumulative_filled_qty=10,
        )
    assert fetches["count"] == 2


def test_terminal_cache_bounded_and_preserves_current_trade_day_no_proof():
    legacy._HISTORICAL_TERMINAL_MAX = 4
    try:
        for i in range(8):
            run, _ = _probe(symbol=f"ANY{i:02d}")
            run()
        assert len(legacy._HISTORICAL_TERMINAL_CACHE) == 4
    finally:
        legacy._HISTORICAL_TERMINAL_MAX = 256
