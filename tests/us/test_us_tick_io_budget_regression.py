import os
import time

import pytest

from trader.us.execution.tick_context import TickExecutionContext
from trader.us.runner.tick_process import TickProcessTimeout, run_tick_in_process


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
