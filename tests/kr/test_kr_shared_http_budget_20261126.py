"""Offline cross-process REST admission and priority/cooldown contracts."""
from __future__ import annotations

import multiprocessing as mp
import time
import uuid

import pytest
from trader.kr import kis_shared_http_budget as budget


def _acquire(key, start, results):
    start.wait(5)
    granted, reason, waited = budget.acquire(
        environment="practice", appkey=key, priority=False,
        observer=False, max_wait_sec=2.0,
    )
    results.put((granted, reason, time.monotonic()))


def test_two_processes_share_one_credential_budget(monkeypatch):
    monkeypatch.setenv("KR_SHARED_HTTP_RPS_PRACTICE", "2")
    key = "test-" + uuid.uuid4().hex
    ctx = mp.get_context("spawn")
    start, results = ctx.Event(), ctx.Queue()
    children = [ctx.Process(target=_acquire, args=(key, start, results)) for _ in range(2)]
    try:
        for p in children:
            p.start()
        start.set()
        rows = [results.get(timeout=10) for _ in children]
        assert all(row[0] for row in rows), rows
        assert abs(rows[0][2]-rows[1][2]) >= .4, rows
    finally:
        for p in children:
            p.join(5)
            if p.is_alive():
                p.terminate()
                p.join(5)
        budget._path("practice", key).unlink(missing_ok=True)


def test_priority_observer_and_cooldown_are_independent_of_order_outcome(monkeypatch):
    monkeypatch.setenv("KR_SHARED_HTTP_RPS_PRACTICE", "4")
    key = "test-" + uuid.uuid4().hex
    try:
        assert budget.acquire(environment="practice", appkey=key, priority=True,
                              observer=False, max_wait_sec=.5)[0]
        granted, reason, _ = budget.acquire(environment="practice", appkey=key,
                                             priority=False, observer=True, max_wait_sec=.3)
        assert not granted and reason == "PRIORITY_TRAFFIC"
        assert budget.penalize(environment="practice", appkey=key, seconds=.5)
        granted, reason, _ = budget.acquire(environment="practice", appkey=key,
                                             priority=True, observer=False, max_wait_sec=.05)
        assert not granted and reason == "DEADLINE"
        assert budget.acquire(environment="practice", appkey=key, priority=True,
                              observer=False, max_wait_sec=1.5)[0]
    finally:
        budget._path("practice", key).unlink(missing_ok=True)


def test_bad_configuration_fails_closed(monkeypatch):
    monkeypatch.setenv("KR_SHARED_HTTP_RPS_PRACTICE", "nan")
    granted, reason, _ = budget.acquire(environment="practice", appkey="fake",
                                         priority=False, observer=True, max_wait_sec=.2)
    assert not granted and reason == "STATE_OR_CONFIG_UNAVAILABLE"


def test_disabled_real_environment(monkeypatch):
    monkeypatch.setenv("KR_SHARED_HTTP_BUDGET_ENABLED", "1")
    assert budget.enabled("practice")
    assert not budget.enabled("real")
