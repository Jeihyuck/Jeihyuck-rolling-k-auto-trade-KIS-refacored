"""Parent SIGINT must never release the trading lock with child tick still active."""
from __future__ import annotations

import multiprocessing as mp
import time

import pytest

from trader.us.runner.tick_process import run_tick_in_process


def _forever(tick_cancellation_event=None):
    while True:
        time.sleep(0.02)


class _InterruptReceiver:
    def __init__(self, real, exc):
        self._real = real
        self.exc = exc

    def poll(self, timeout=0):
        raise self.exc

    def close(self):
        self._real.close()


class _InterruptContext:
    def __init__(self, exc):
        self.real = mp.get_context("fork" if "fork" in mp.get_all_start_methods() else "spawn")
        self.exc = exc
        self.child = None

    def Pipe(self, duplex=False):
        receiver, sender = self.real.Pipe(duplex=duplex)
        return _InterruptReceiver(receiver, self.exc), sender

    def Event(self):
        return self.real.Event()

    def Process(self, **kwargs):
        self.child = self.real.Process(**kwargs)
        return self.child


@pytest.mark.parametrize("exc", [KeyboardInterrupt(), SystemExit(130)])
def test_parent_interrupt_stops_tick_before_unwinding(exc):
    ctx = _InterruptContext(exc)
    with pytest.raises(type(exc)):
        run_tick_in_process(
            _forever, kwargs={}, timeout_sec=3,
            terminate_grace_sec=0.1, mp_context=ctx,
        )
    assert ctx.child is not None
    assert not ctx.child.is_alive()
    assert ctx.child.exitcode is not None
