"""Bounded opt-in quote I/O regression, no broker orders or DB mutation."""
from threading import Barrier, BrokenBarrierError

from trader.us.pb1.us_exit_engine import prepare_exit_position_snapshots


def _positions():
    return [
        {"symbol": "AAA", "exchange": "NASDAQ", "strategy_owner": "US_STANDARD",
         "entry_price": 0, "position_lifecycle_id": "A"},
        {"symbol": "BBB", "exchange": "NASDAQ", "strategy_owner": "US_STANDARD",
         "entry_price": 0, "position_lifecycle_id": "B"},
    ]


def test_quote_prefetch_parallel_only_with_explicit_safe_provider(monkeypatch):
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("US_EXIT_QUOTE_PREFETCH_ENABLED", "1")
    monkeypatch.setenv("US_EXIT_QUOTE_PREFETCH_WORKERS", "2")
    start_together = Barrier(2, timeout=5)

    class ConcurrentProvider:
        concurrent_price_reads_safe = True

        def get_current_price(self, symbol, exchange):
            assert exchange == "NASDAQ"
            start_together.wait()  # cannot pass if called serially
            return {"last": 91 if symbol == "AAA" else 103}

    observed = []
    result = prepare_exit_position_snapshots(
        _positions(), ConcurrentProvider(),
        on_snapshot_ready=lambda position: observed.append(position["symbol"]),
    )
    assert [x["symbol"] for x in result] == ["AAA", "BBB"]
    assert [x["resolved_current_price"] for x in result] == [91, 103]
    assert observed == ["AAA", "BBB"]


def test_quote_prefetch_requires_provider_thread_safety_and_practice(monkeypatch):
    class UnsafeProvider:
        def __init__(self):
            self.calls = []

        def get_current_price(self, symbol, exchange):
            self.calls.append(symbol)
            return {"last": "100"}

    for env, enabled in (("practice", "1"), ("live", "1"), ("practice", "0")):
        monkeypatch.setenv("KIS_ENV", env)
        monkeypatch.setenv("US_EXIT_QUOTE_PREFETCH_ENABLED", enabled)
        provider = UnsafeProvider()
        result = prepare_exit_position_snapshots(_positions(), provider)
        assert [x["symbol"] for x in result] == ["AAA", "BBB"]
        assert provider.calls == ["AAA", "BBB"]
