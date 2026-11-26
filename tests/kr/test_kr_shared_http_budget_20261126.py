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


def test_preflight_caps_rate_to_existing_policy_and_checks_state(monkeypatch):
    key = "fixture-" + uuid.uuid4().hex
    monkeypatch.setenv("KIS_GLOBAL_RPS_PRACTICE", "1.5")
    monkeypatch.setenv("KR_SHARED_HTTP_RPS_PRACTICE", "1.5")
    try:
        assert budget.preflight(environment="practice", appkey=key) == (True, "LOCAL_CONFIG_READY")
        assert not budget.preflight(environment="practice", appkey="")[0]
        assert not budget.preflight(environment="real", appkey=key)[0]
        monkeypatch.setenv("KR_SHARED_HTTP_RPS_PRACTICE", "2")
        assert budget.preflight(environment="practice", appkey=key) == (False, "RATE_EXCEEDS_LOCAL_CAP_OR_INVALID")
        monkeypatch.setenv("KR_SHARED_HTTP_RPS_PRACTICE", "1.5")
        budget._path("practice", key).write_text("invalid state")
        assert not budget.preflight(environment="practice", appkey=key)[0]
    finally:
        budget._path("practice", key).unlink(missing_ok=True)


def test_practice_shell_defaults_inherit_existing_cap_and_observer_profile(monkeypatch):
    import os
    import subprocess
    import sys
    from pathlib import Path
    key = "fixture-" + uuid.uuid4().hex
    script = (
        'source scripts/wsl/kr-entry-timing-activation.sh; kr_pb1_entry_timing_defaults; '
        'printf "RESULT:%s|%s|%s\\n" "$KR_WS_SHARED_QUOTE_ENABLED" '
        '"$KR_SHARED_HTTP_BUDGET_ENABLED" "$KR_SHARED_HTTP_RPS_PRACTICE"'
    )
    # Resolve the current Python environment; no KIS call is possible in
    # the pure preflight command. The key is synthetic.
    base = dict(PATH=str(Path(sys.executable).parent)+":"+os.environ.get("PATH","/usr/bin:/bin"),
                STRATEGY_ENV="practice", KIS_ENV="practice", KIS_APP_KEY=key,
                KIS_GLOBAL_RPS_PRACTICE="1.5")
    def run(**overrides):
        return subprocess.run(["bash", "-c", script], env={**base, **overrides},
                              text=True, capture_output=True, check=True)
    try:
        assert "RESULT:1|1|1.5" in run().stdout
        assert "RESULT:0|0|1.5" in run(KR_WS_SHARED_QUOTE_ENABLED="0",
                                          KR_SHARED_HTTP_BUDGET_ENABLED="0").stdout
        assert "RESULT:1|0|2" in run(KR_SHARED_HTTP_RPS_PRACTICE="2").stdout
        assert "DEGRADED" in run(KIS_APP_KEY="").stderr
        assert "RESULT:0|0|" in run(KIS_ENV="real", STRATEGY_ENV="real",
                                    KR_WS_SHARED_QUOTE_ENABLED="1",
                                    KR_SHARED_HTTP_BUDGET_ENABLED="1").stdout
    finally:
        budget._path("practice", key).unlink(missing_ok=True)
