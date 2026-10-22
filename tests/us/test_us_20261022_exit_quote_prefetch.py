"""Bounded opt-in quote I/O regression, no broker orders or DB mutation."""
from threading import Barrier, BrokenBarrierError
import time
import pytest

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


def test_stalled_parallel_quote_does_not_block_exit_snapshot_batch(monkeypatch):
    import time
    from threading import Event

    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("US_EXIT_QUOTE_PREFETCH_ENABLED", "1")
    monkeypatch.setenv("US_EXIT_QUOTE_PREFETCH_DEADLINE_SEC", "0.2")
    monkeypatch.setenv("US_EXIT_QUOTE_PREFETCH_WORKERS", "2")
    release_slow_io = Event()

    class IsolatedReadOnlyProvider:
        concurrent_price_reads_safe = True

        def get_current_price(self, symbol, exchange):
            if symbol == "AAA":
                release_slow_io.wait(timeout=2.0)
                return {"last": "94"}
            return {"last": "99"}

    start = time.monotonic()
    try:
        snapshots = prepare_exit_position_snapshots(
            _positions(), IsolatedReadOnlyProvider(),
        )
    finally:
        release_slow_io.set()  # finish test-only outstanding IO cleanly
    elapsed = time.monotonic() - start
    # Slow symbol cannot stall the rest of the Tick; it is safely skipped,
    # never assigned an assumed price or mutated into a false stop.
    assert elapsed < 1.0, elapsed
    assert [x["symbol"] for x in snapshots] == ["BBB"]



def test_isolated_production_provider_keeps_session_broker_state_separate(monkeypatch):
    from trader.us.data_provider import USDataProvider
    from trader.us.execution.kis_us_client import KisUSTemporaryError

    parents = []
    clients = []
    class FakeReadOnlyClient:
        pass

    def fake_get_client(instance):
        if instance._client is None:
            instance._client = FakeReadOnlyClient()
            clients.append(instance._client)
        return instance._client

    def fake_get_price(instance, symbol, exchange):
        parents.append(instance)
        assert instance._tick_context is None
        assert instance._client is clients[-1]
        assert isinstance(instance._client._stage_deadline, float)
        return {"last": "103", "source": "FAKE_PRICE_ONLY"}

    monkeypatch.setattr(USDataProvider, "_get_client", fake_get_client)
    monkeypatch.setattr(USDataProvider, "get_current_price", fake_get_price)
    main = USDataProvider(env="practice")
    main._tick_context = object()
    main._client = FakeReadOnlyClient()
    result = main.get_isolated_exit_quote(
        "AAA", "NASDAQ", stage_deadline=time.monotonic() + 5,
    )
    assert result["last"] == "103"
    assert parents and parents[0] is not main
    assert main._client not in clients
    assert main._tick_context is not None
    with pytest.raises(KisUSTemporaryError):
        main.get_isolated_exit_quote(
            "AAA", "NASDAQ", stage_deadline=time.monotonic() - 1,
        )


def test_isolated_reader_is_preferred_over_shared_unsafe_provider(monkeypatch):
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("US_EXIT_QUOTE_PREFETCH_ENABLED", "1")
    monkeypatch.setenv("US_EXIT_QUOTE_PREFETCH_WORKERS", "2")
    both = Barrier(2, timeout=5)

    class IsolatedProvider:
        concurrent_price_reads_safe = False
        def get_current_price(self, symbol, exchange):
            raise AssertionError("unsafe shared price method must not run")

        def get_isolated_exit_quote(self, symbol, exchange, *, stage_deadline):
            assert stage_deadline > time.monotonic()
            both.wait()
            return {"last": "100", "source": "ISOLATED_SAFE"}

    snapshots = prepare_exit_position_snapshots(_positions(), IsolatedProvider())
    assert [x["symbol"] for x in snapshots] == ["AAA", "BBB"]
