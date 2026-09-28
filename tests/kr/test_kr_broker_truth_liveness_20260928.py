from __future__ import annotations

import os
import signal
import time
from datetime import datetime, timedelta, timezone

import pytest

from trader.kr.runtime_integrity_20260928 import (
    _build_balance_cache_guard,
    _build_hard_timeout_runner,
    _build_reconcile_guard,
    _build_run_once_precheck_guard,
)


class _DummyBalance:
    def __init__(self, age_sec: float) -> None:
        self._balance_cache = {"output1": [{"pdno": "000660", "hldg_qty": "1"}], "output2": {}}
        self._balance_cache_at = datetime.now(timezone.utc) - timedelta(seconds=age_sec)
        self.invalidations: list[str] = []

    def invalidate_balance_cache(self, *, reason: str, codes=None) -> None:
        del codes
        self.invalidations.append(reason)
        self._balance_cache = None
        self._balance_cache_at = None


def test_balance_cache_179_seconds_remains_cache_eligible(monkeypatch):
    monkeypatch.setenv("KIS_BALANCE_SNAPSHOT_TTL_SEC", "180")
    monkeypatch.setenv("KR_BALANCE_CACHE_MAX_AGE_SEC", "180")
    calls = []

    def original(self, force=False, *, return_source=False, return_raw=False):
        del self, return_source, return_raw
        calls.append(bool(force))
        return {"ok": True}

    guarded = _build_balance_cache_guard(original)
    dummy = _DummyBalance(179)
    guarded(dummy)

    assert calls == [False]
    assert dummy.invalidations == []


def test_balance_cache_181_seconds_forces_fresh_fetch(monkeypatch):
    monkeypatch.setenv("KIS_BALANCE_SNAPSHOT_TTL_SEC", "180")
    monkeypatch.setenv("KR_BALANCE_CACHE_MAX_AGE_SEC", "180")
    calls = []

    def original(self, force=False, *, return_source=False, return_raw=False):
        del self, return_source, return_raw
        calls.append(bool(force))
        return {"ok": True}

    guarded = _build_balance_cache_guard(original)
    dummy = _DummyBalance(181)
    guarded(dummy)

    assert calls == [True]
    assert dummy.invalidations == ["kr_p0_balance_cache_ttl_expired"]


def test_unresolved_broker_activity_discards_injected_balance_snapshot():
    observed = {}

    def original(*args, **kwargs):
        del args
        observed.update(kwargs)
        return {"ok": True}

    class Kis:
        def __init__(self):
            self.invalidations = []

        def invalidate_balance_cache(self, *, reason: str, codes=None):
            del codes
            self.invalidations.append(reason)

    kis = Kis()
    guarded = _build_reconcile_guard(original, unresolved_probe=lambda engine, env: True)
    stale = {"output1": [{"pdno": "000660", "hldg_qty": "1"}], "output2": {}}
    guarded(
        engine=object(),
        kis=kis,
        env="practice",
        run_id="r1",
        strategy="pb1_pullback_close",
        tick_ts=datetime.now(timezone.utc),
        balance_snapshot=stale,
    )

    assert observed["balance_snapshot"] is None
    assert kis.invalidations == ["kr_p0_unresolved_broker_activity"]


def test_no_unresolved_activity_keeps_validated_snapshot():
    observed = {}

    def original(*args, **kwargs):
        del args
        observed.update(kwargs)
        return {"ok": True}

    class Kis:
        def invalidate_balance_cache(self, **kwargs):
            raise AssertionError("must not invalidate without unresolved broker activity")

    stale = {"output1": [{"pdno": "000660", "hldg_qty": "1"}], "output2": {}}
    guarded = _build_reconcile_guard(original, unresolved_probe=lambda engine, env: False)
    guarded(
        engine=object(),
        kis=Kis(),
        env="practice",
        balance_snapshot=stale,
    )

    assert observed["balance_snapshot"] is stale


def test_session_start_balance_precheck_is_one_shot(monkeypatch):
    calls = []

    def original(*args, **kwargs):
        calls.append((args, kwargs))
        return "ok"

    guarded = _build_run_once_precheck_guard(original)
    monkeypatch.setenv("KR_BALANCE_PRECHECK_PATH", "/tmp/balance_precheck.json")
    monkeypatch.setenv("PB1_LOOP_TICK_INDEX", "1")
    assert guarded(loop_mode=True) == "ok"
    assert os.getenv("KR_BALANCE_PRECHECK_PATH") == "/tmp/balance_precheck.json"

    monkeypatch.setenv("PB1_LOOP_TICK_INDEX", "2")
    assert guarded(loop_mode=True) == "ok"
    assert os.getenv("KR_BALANCE_PRECHECK_PATH") is None
    assert len(calls) == 2


@pytest.mark.skipif(not hasattr(signal, "setitimer"), reason="SIGALRM watchdog requires POSIX setitimer")
def test_watchdog_cannot_be_swallowed_by_except_exception(monkeypatch):
    class TickTimeoutError(TimeoutError):
        pass

    monkeypatch.setenv("PB1_LAST_STAGE", "account_reconcile")
    hardened = _build_hard_timeout_runner(TickTimeoutError)
    swallowed = []

    def inner_fail_soft_loop():
        while True:
            try:
                time.sleep(0.01)
            except Exception as exc:  # mirrors the production fail-soft pattern
                swallowed.append(type(exc).__name__)

    started = time.monotonic()
    with pytest.raises(TickTimeoutError, match="tick_hard_timeout"):
        hardened(timeout_sec=0.05, call=inner_fail_soft_loop)

    assert time.monotonic() - started < 1.0
    assert swallowed == []
