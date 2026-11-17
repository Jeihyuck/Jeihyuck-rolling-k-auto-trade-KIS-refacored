"""Cross-process observer REST budget: never delay broker order/balance calls."""
from __future__ import annotations

import fcntl
import os
from multiprocessing import get_context
import time

import pytest

from trader.kr import kis_observer_budget as budget


@pytest.fixture
def budget_path(tmp_path, monkeypatch):
    monkeypatch.setattr(budget, "runtime_path", lambda *parts: tmp_path.joinpath(*parts))
    return tmp_path


def test_observer_quota_is_account_scoped_and_fail_closed_if_not_due(budget_path):
    yes, reason = budget.acquire_observer_slot(
        environment="practice", account_key="scoped", now=100.0, rps=0.5,
    )
    assert (yes, reason) == (True, "GRANTED")
    assert budget.acquire_observer_slot(
        environment="practice", account_key="scoped", now=100.1, rps=0.5,
    ) == (False, "OBSERVER_RPS_LIMIT")
    assert budget.acquire_observer_slot(
        environment="practice", account_key="other-account", now=100.1, rps=0.5,
    ) == (True, "GRANTED")
    assert budget.acquire_observer_slot(
        environment="practice", account_key="scoped", now=102.1, rps=0.5,
    ) == (True, "GRANTED")


def test_protected_broker_notice_is_not_delayed_and_pauses_observer(budget_path):
    assert budget.note_critical_kr_call(
        environment="practice", account_key="scoped", now=104.0,
    )
    assert budget.acquire_observer_slot(
        environment="practice", account_key="scoped", now=105.0, rps=0.5,
    ) == (False, "PRIORITY_BROKER_TRAFFIC")
    assert budget.acquire_observer_slot(
        environment="practice", account_key="scoped", now=106.1, rps=0.5,
    ) == (True, "GRANTED")
    assert budget.acquire_observer_slot(
        environment="practice", account_key="scoped", now=107.0, rps=-1.0,
    ) == (False, "INVALID_OBSERVER_BUDGET")


def _reserve_in_child(path, out):
    from trader.kr import kis_observer_budget as module
    from pathlib import Path
    module.runtime_path = lambda *parts: Path(path).joinpath(*parts)
    out.put(module.acquire_observer_slot(
        environment="practice", account_key="scoped",
        now=100.5, rps=0.5,
    ))


@pytest.mark.skipif(os.name != "posix", reason="WSL/Linux flock contract")
def test_observer_quota_survives_process_boundary(budget_path):
    assert budget.acquire_observer_slot(
        environment="practice", account_key="scoped", now=100.0, rps=0.5,
    )[0]
    ctx = get_context("fork")
    q = ctx.Queue()
    child = ctx.Process(target=_reserve_in_child, args=(str(budget_path), q))
    child.start()
    child.join(timeout=5)
    assert child.exitcode == 0
    assert q.get(timeout=2) == (False, "OBSERVER_RPS_LIMIT")


@pytest.mark.skipif(os.name != "posix", reason="WSL/Linux flock contract")
def test_critical_notice_never_waits_on_collector_lock(budget_path):
    file_path = budget._path("scoped", "practice")
    with budget._secured_file(file_path) as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        t = time.perf_counter()
        ok = budget.note_critical_kr_call(
            environment="practice", account_key="scoped",
        )
        elapsed = time.perf_counter() - t
        assert not ok
        assert elapsed < 0.1
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
