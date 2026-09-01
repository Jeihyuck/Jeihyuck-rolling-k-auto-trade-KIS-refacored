import os
import time

import pytest

from trader.us.execution.tick_context import TickExecutionContext
from trader.us.runner.tick_process import TickProcessTimeout, run_tick_in_process
from trader.us.data_provider import USDataProvider


def _hung_tick(**kwargs):
    path = kwargs["marker"]
    while True:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write("write\n")
        time.sleep(.02)


def test_deadline_and_selective_invalidation():
    context = TickExecutionContext("2026-08-31", "am", "run", 1, "tick", deadline=time.monotonic() + .1)
    context.price_cache[("JNJ", "NYSE")] = 266.66
    context.price_cache[("TQQQ", "NASDAQ")] = 71.34
    context.invalidate_after_order("JNJ")
    assert ("JNJ", "NYSE") not in context.price_cache
    assert context.price_cache[("TQQQ", "NASDAQ")] == 71.34
    assert context.has_budget(.01)


def test_hung_child_is_dead_and_cannot_write_after_timeout(tmp_path):
    marker = tmp_path / "writes"
    with pytest.raises(TickProcessTimeout) as caught:
        run_tick_in_process(_hung_tick, kwargs={"marker": str(marker)}, timeout_sec=.08, terminate_grace_sec=.1)
    assert caught.value.result["process_alive"] is False
    size = os.path.getsize(marker)
    time.sleep(.08)
    assert os.path.getsize(marker) == size


class _CountingClient:
    def __init__(self, delay=0.0):
        self.balance = self.quote = self.cash = 0
        self.delay = delay

    def get_us_balance(self, force_refresh=False):
        time.sleep(self.delay)
        self.balance += 1
        return {"output1": [], "output2": {}}

    def get_us_price(self, symbol, exchange):
        time.sleep(self.delay)
        self.quote += 1
        return {"output": {"last": "266.66"}}

    def get_us_orderable_cash(self, **kwargs):
        time.sleep(self.delay)
        self.cash += 1
        return {"output": {"ord_psbl_cash": "5000"}}


def test_real_provider_paths_use_tick_scoped_caches_and_counters():
    ctx = TickExecutionContext("2026-08-31", "afternoon", "run", 1, "tick")
    provider = USDataProvider(offline=False).bind_tick_context(ctx)
    client = _CountingClient()
    provider._client = client
    assert provider.get_balance(force_refresh=True) == provider.get_balance(force_refresh=True)
    assert provider.get_current_price("JNJ", "NYSE") == provider.get_current_price("JNJ", "NYSE")
    assert provider.get_orderable_cash("JNJ", "NYSE", 266.66) == provider.get_orderable_cash("JNJ", "NYSE", 266.66)
    assert (client.balance, client.quote, client.cash) == (1, 1, 1)
    assert ctx.counters["balance_logical_calls"] == 2 and ctx.counters["balance_http_calls"] == 1
    assert ctx.counters["quote_logical_calls"] == 2 and ctx.counters["quote_http_calls"] == 1
    assert ctx.counters["psamount_logical_calls"] == 2 and ctx.counters["psamount_http_calls"] == 1


def test_slow_provider_duplicate_requests_finish_inside_scaled_deadline():
    ctx = TickExecutionContext("2026-08-31", "afternoon", "run", 1, "slow", deadline=time.monotonic() + .14)
    provider = USDataProvider(offline=False).bind_tick_context(ctx)
    client = _CountingClient(delay=.03)
    provider._client = client
    started = time.monotonic()
    for _ in range(2):
        provider.get_balance(force_refresh=True)
        provider.get_current_price("JNJ", "NYSE")
        provider.get_orderable_cash("JNJ", "NYSE", 266.66)
    elapsed = time.monotonic() - started
    assert elapsed < .14
    assert ctx.has_budget(0)
    assert (client.balance, client.quote, client.cash) == (1, 1, 1)
