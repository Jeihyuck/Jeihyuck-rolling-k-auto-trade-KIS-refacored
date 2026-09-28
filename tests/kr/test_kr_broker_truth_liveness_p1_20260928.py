from __future__ import annotations

import signal
import time

import pytest

from trader.kr.runtime_integrity_20260928 import (
    _build_balance_cache_guard,
    _build_hard_timeout_runner,
    _build_reconcile_guard,
    _build_runs_finish_guard,
    _build_runs_start_guard,
)


def test_post_order_force_true_balance_is_reused_by_unresolved_reconcile(monkeypatch):
    """Replay broker_truth_hardening: force=True balance -> reconcile_kis(snapshot)."""
    monkeypatch.setenv("KR_BROKER_TRUTH_FORCED_SNAPSHOT_MAX_AGE_SEC", "10")
    snapshot = {
        "output1": [{"pdno": "028050", "hldg_qty": "21"}],
        "output2": {},
    }
    balance_calls: list[bool] = []
    observed: dict = {}

    class Kis:
        def __init__(self):
            self._balance_cache = None
            self._balance_cache_at = None
            self.invalidations: list[str] = []

        def invalidate_balance_cache(self, *, reason: str, codes=None):
            del codes
            self.invalidations.append(reason)
            self._balance_cache = None
            self._balance_cache_at = None

    def original_balance(self, force=False, *, return_source=False, return_raw=False):
        del self, return_source, return_raw
        balance_calls.append(bool(force))
        return snapshot

    def original_reconcile(*args, **kwargs):
        del args
        observed.update(kwargs)
        return {"ok": True}

    kis = Kis()
    guarded_balance = _build_balance_cache_guard(original_balance)
    fresh = guarded_balance(kis, force=True)
    guarded_reconcile = _build_reconcile_guard(
        original_reconcile,
        unresolved_probe=lambda engine, env: True,
    )
    guarded_reconcile(
        engine=object(),
        kis=kis,
        env="practice",
        balance_snapshot=fresh,
    )

    assert balance_calls == [True]
    assert observed["balance_snapshot"] is snapshot
    assert kis.invalidations == []


def test_unresolved_reconcile_still_discards_unproven_snapshot():
    """Only the proven force=True object may bypass the stale-snapshot fence."""
    stale = {
        "output1": [{"pdno": "028050", "hldg_qty": "0"}],
        "output2": {},
    }
    observed: dict = {}

    class Kis:
        def __init__(self):
            self.invalidations: list[str] = []

        def invalidate_balance_cache(self, *, reason: str, codes=None):
            del codes
            self.invalidations.append(reason)

    def original_reconcile(*args, **kwargs):
        del args
        observed.update(kwargs)
        return {"ok": True}

    kis = Kis()
    guarded_reconcile = _build_reconcile_guard(
        original_reconcile,
        unresolved_probe=lambda engine, env: True,
    )
    guarded_reconcile(
        engine=object(),
        kis=kis,
        env="practice",
        balance_snapshot=stale,
    )

    assert observed["balance_snapshot"] is None
    assert kis.invalidations == ["kr_p0_unresolved_broker_activity"]


@pytest.mark.skipif(not hasattr(signal, "setitimer"), reason="SIGALRM watchdog requires POSIX setitimer")
def test_watchdog_timeout_finalizes_exact_per_tick_run_before_recovery(monkeypatch):
    """A BaseException watchdog must not leave RunsRepo.start_run() at STARTED."""
    monkeypatch.setenv("PB1_LAST_STAGE", "account_reconcile.positions_lookup.start")

    class TickTimeoutError(TimeoutError):
        pass

    class FakeRunsRepo:
        def __init__(self):
            self.finished: list[tuple[str, str, str]] = []

        def start_run(self, *args, **kwargs):
            del args, kwargs
            return "tick-run-147"

        def finish_run(self, run_id, *, status, notes=None, **kwargs):
            del kwargs
            self.finished.append((str(run_id), str(status), str(notes or "")))

    FakeRunsRepo.start_run = _build_runs_start_guard(FakeRunsRepo.start_run)
    FakeRunsRepo.finish_run = _build_runs_finish_guard(FakeRunsRepo.finish_run)
    repo = FakeRunsRepo()
    hardened = _build_hard_timeout_runner(TickTimeoutError)
    swallowed: list[str] = []

    def timed_tick():
        repo.start_run(strategy="pb1_pullback_close")
        while True:
            try:
                time.sleep(0.01)
            except Exception as exc:  # production fail-soft shape
                swallowed.append(type(exc).__name__)

    with pytest.raises(TickTimeoutError, match="tick_hard_timeout"):
        hardened(timeout_sec=0.05, call=timed_tick)

    assert swallowed == []
    assert repo.finished == [
        ("tick-run-147", "RECOVERABLE_DB_TIMEOUT", "TICK_TIMEOUT_KRX")
    ]
