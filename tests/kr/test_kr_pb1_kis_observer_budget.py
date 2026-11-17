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



def test_same_symbol_minute_then_vwap_has_bounded_slot_wait(budget_path, monkeypatch):
    # The real observer makes TWO REST calls per hot candidate. The second
    # must not be permanently denied by its own low-priority 0.5RPS quota.
    monkeypatch.setenv("KR_PB1_OBSERVER_KIS_MAX_RPS", "1.5")
    ok, reason = budget.acquire_observer_slot(
        environment="practice", account_key="minute-and-quote",
    )
    assert ok and reason == "GRANTED"
    start = time.monotonic()
    granted, second_reason, waited = budget.acquire_observer_slot_bounded(
        environment="practice", account_key="minute-and-quote",
        max_wait_sec=1.5, poll_sec=0.05,
    )
    assert granted and second_reason == "GRANTED"
    assert time.monotonic() - start >= 0.5
    assert 0.4 <= waited <= 1.5


def test_priority_notice_preempts_even_a_queued_observer_quote(budget_path):
    budget.note_critical_kr_call(
        environment="practice", account_key="priority-preempts",
    )
    started = time.monotonic()
    granted, reason, waited = budget.acquire_observer_slot_bounded(
        environment="practice", account_key="priority-preempts",
        max_wait_sec=2.5,
    )
    assert not granted and reason == "PRIORITY_BROKER_TRAFFIC"
    assert waited < 0.2
    assert time.monotonic() - started < 0.2


def test_am_launcher_wires_observer_before_broker_session():
    from pathlib import Path
    am = Path("scripts/wsl/run-kr-am.sh").read_text(encoding="utf-8")
    pm = Path("scripts/wsl/run-kr-afternoon.sh").read_text(encoding="utf-8")
    activation = Path(
        "scripts/wsl/kr-entry-timing-activation.sh",
    ).read_text(encoding="utf-8")
    assert "source scripts/wsl/kr-entry-timing-activation.sh" in am
    assert "source scripts/wsl/kr-entry-timing-activation.sh" in pm
    assert am.index("kr_pb1_opening_observer_start") < am.index(
        "python -m trader.kr.runner.trade_session_runner"
    )
    assert pm.index("kr_pb1_opening_observer_start") < pm.index(
        "python -m trader.kr.runner.trade_session_runner"
    )
    assert "KR_PB1_OBSERVER_ONLY=1" in activation
    assert "KR_PB1_KIS_OBSERVER_BUDGET_ENABLED" in activation
