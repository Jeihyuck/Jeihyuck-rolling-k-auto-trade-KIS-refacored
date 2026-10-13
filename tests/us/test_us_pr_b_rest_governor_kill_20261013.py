"""US KIS practice REST interprocess token/lock cannot remain held by a killed tick."""
from __future__ import annotations

import multiprocessing as mp
import os
import signal
import time

import pytest

from trader.us import kis_http_governor as governor


def _lock_and_wait(lock_path: str, signal_path: str):
    import fcntl
    with open(lock_path, "a+") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        with open(signal_path, "w") as signal_file:
            signal_file.write("LOCK_HELD")
        while True:
            time.sleep(0.01)


@pytest.mark.skipif(os.name != "posix", reason="WSL/Linux flock semantics")
def test_killed_tick_does_not_leave_rest_rate_governor_locked(tmp_path, monkeypatch):
    ctx = mp.get_context("fork")
    lock_path = tmp_path / "shared-practice.lock"
    last_path = tmp_path / "shared-practice.last"
    ready = tmp_path / "locked"
    child = ctx.Process(target=_lock_and_wait, args=(str(lock_path), str(ready)))
    child.start()
    try:
        for _ in range(100):
            if ready.exists():
                break
            time.sleep(0.01)
        assert ready.exists(), "child must own the lock before termination"
        os.kill(child.pid, signal.SIGKILL)
        child.join(timeout=1.5)
        assert not child.is_alive()

        monkeypatch.setattr(governor, "_state_paths", lambda: (lock_path, last_path))
        monkeypatch.setattr(governor, "_enabled", lambda: True)
        monkeypatch.setattr(governor, "_is_kis_vts_url", lambda url: True)
        monkeypatch.setattr(governor, "_min_interval_sec", lambda: 0.04)
        started = time.monotonic()
        governor._wait_for_global_slot("GET", "https://openapivts.koreainvestment.com:29443/uapi/test")
        assert time.monotonic() - started < 0.5
        assert last_path.exists()
    finally:
        if child.is_alive():
            child.kill()
        child.join(timeout=1)
