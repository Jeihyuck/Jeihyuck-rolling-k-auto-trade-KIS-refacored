"""Offline Stage4 startup/shutdown regression tests for 2026-10-26 AM failure.

No broker IO, KIS credentials, real database, or live trading is required.
"""
from __future__ import annotations

import subprocess
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from trader.us.execution.stage4_lifecycle import (
    stage4_error_code, stop_stage4_worker, wait_for_stage4_ready,
    read_stage4_tick_health_with_bounded_retry,
)


class Clock:
    def __init__(self):
        self.now = 0.0

    def read(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class Child:
    def __init__(self, exit_code=None):
        self.exit_code = exit_code
        self.terminated = False
        self.killed = False

    def poll(self):
        return self.exit_code

    def terminate(self):
        self.terminated = True
        self.exit_code = 0

    def kill(self):
        self.killed = True
        self.exit_code = -9

    def wait(self, timeout):
        return self.exit_code


READY = {"worker_state": "READY", "reconcile_status": "OK",
         "replay_status": "OK", "filled_rows": 0}


def test_stage4_startup_waits_for_initial_reconcile_and_current_generation():
    timeline = Clock()
    statuses = iter([
        "US_STAGE4_WRITER_NOT_READY",
        "US_STAGE4_WRITER_GENERATION_MISMATCH",
        "US_STAGE4_WRITER_STALE",
        READY,
    ])

    def health(**kwargs):
        assert kwargs["env"] == "practice"
        item = next(statuses)
        if isinstance(item, str):
            raise RuntimeError(item)
        return item

    result = wait_for_stage4_ready(
        Child(), health_reader=health, timeout_sec=6, poll_sec=0.5,
        clock=timeline.read, sleeper=timeline.sleep,
    )
    assert result == READY
    assert timeline.now == 1.5


def test_stage4_startup_timeouts_are_bounded_and_keep_trades_closed():
    timeline = Clock()

    def not_ready(**kwargs):
        raise RuntimeError("US_STAGE4_WRITER_NOT_READY")

    with pytest.raises(RuntimeError, match="US_STAGE4_STARTUP_TIMEOUT"):
        wait_for_stage4_ready(
            Child(), health_reader=not_ready, timeout_sec=1.0,
            poll_sec=0.25, clock=timeline.read, sleeper=timeline.sleep,
        )
    assert timeline.now == 1.0


def test_stage4_startup_rejects_worker_exit_even_with_old_ready_evidence():
    called = []

    def old_ready(**kwargs):
        called.append(1)
        return READY

    with pytest.raises(RuntimeError, match="US_STAGE4_STARTUP_WORKER_EXITED"):
        wait_for_stage4_ready(Child(exit_code=0), health_reader=old_ready)
    assert not called


@pytest.mark.parametrize("failure", [
    "US_STAGE4_WRITER_POLICY_NOT_ARMED",
    "US_STAGE4_WRITER_PRACTICE_ONLY",
    "US_STAGE4_STARTUP_WORKER_EXITED",
])
def test_stage4_startup_nonretryable_errors_are_not_silently_bypassed(failure):
    clock = Clock()
    with pytest.raises(RuntimeError, match="US_STAGE4_STARTUP_FATAL_"):
        wait_for_stage4_ready(
            Child(), health_reader=lambda **_: (_ for _ in ()).throw(
                RuntimeError(failure)),
            clock=clock.read, sleeper=clock.sleep,
        )
    assert clock.now == 0


def test_stage4_reconcile_unverified_waits_until_new_safe_evidence():
    timeline = Clock()
    calls = 0

    def health(**_):
        nonlocal calls
        calls += 1
        return READY if calls == 2 else {
            **READY, "reconcile_status": "ERROR",
        }

    assert wait_for_stage4_ready(
        Child(), health_reader=health, timeout_sec=3, poll_sec=1,
        clock=timeline.read, sleeper=timeline.sleep,
    )["reconcile_status"] == "OK"
    assert timeline.now == 1


def test_stage4_tick_recovers_only_after_authoritative_evidence_is_valid():
    timeline = Clock()
    states = iter([
        "US_STAGE4_WRITER_NOT_READY",
        "US_STAGE4_WRITER_STALE",
        READY,
    ])
    def health(**kwargs):
        assert kwargs["max_age_seconds"] == 30
        state = next(states)
        if isinstance(state, str):
            raise RuntimeError(state)
        return state
    assert read_stage4_tick_health_with_bounded_retry(
        health_reader=health, retry_sec=2.0, poll_sec=0.25,
        clock=timeline.read, sleeper=timeline.sleep,
    ) == READY
    assert timeline.now == .5


def test_stage4_tick_retry_exhausted_stays_fail_closed():
    timeline = Clock()
    def always_stale(**_):
        raise RuntimeError("US_STAGE4_WRITER_STALE")
    with pytest.raises(RuntimeError, match="US_STAGE4_TICK_HEALTH_RETRY_EXHAUSTED"):
        read_stage4_tick_health_with_bounded_retry(
            health_reader=always_stale, retry_sec=1.0, poll_sec=0.25,
            clock=timeline.read, sleeper=timeline.sleep,
        )
    assert timeline.now == 1.0


def test_stage4_tick_policy_not_armed_does_not_retry_or_bypass():
    timeline = Clock()
    with pytest.raises(RuntimeError, match="US_STAGE4_WRITER_POLICY_NOT_ARMED"):
        read_stage4_tick_health_with_bounded_retry(
            health_reader=lambda **_: (_ for _ in ()).throw(
                RuntimeError("US_STAGE4_WRITER_POLICY_NOT_ARMED")),
            retry_sec=3.0, clock=timeline.read, sleeper=timeline.sleep,
        )
    assert timeline.now == 0


def test_stage4_startup_nonpractice_is_rejected_before_health_or_http():
    with pytest.raises(RuntimeError, match="PRACTICE_ONLY"):
        wait_for_stage4_ready(Child(), env="real",
                              health_reader=lambda **_: pytest.fail("should not read"))


def test_stage4_shutdown_marks_stopped_only_after_confirmed_os_exit():
    child = Child()
    witnessed = []

    def marker(**kwargs):
        assert child.poll() is not None
        witnessed.append(kwargs)
        return True

    generation = str(uuid.uuid4())
    assert stop_stage4_worker(child, expected_generation=generation,
                              marker=marker)
    assert child.terminated
    assert not child.killed
    assert witnessed == [{"expected_generation": generation, "env": "practice"}]


def test_stage4_shutdown_kills_blocked_worker_only_after_grace_expiry():
    class HungOnce(Child):
        def wait(self, timeout):
            if not self.killed:
                self.exit_code = None
                raise subprocess.TimeoutExpired("stage4", timeout)
            return -9

    child = HungOnce()
    assert stop_stage4_worker(
        child, expected_generation=str(uuid.uuid4()),
        marker=lambda **_: True,
    )
    assert child.terminated and child.killed


def test_stage4_shutdown_unreaped_never_marks_database_stopped():
    class NeverExits(Child):
        def poll(self):
            return None

        def wait(self, timeout):
            raise subprocess.TimeoutExpired("stage4", timeout)

    marked = []
    child = NeverExits()
    with pytest.raises(RuntimeError, match="US_STAGE4_WORKER_STOP_UNVERIFIED"):
        stop_stage4_worker(
            child, expected_generation=str(uuid.uuid4()),
            marker=lambda **kw: marked.append(kw) or True,
        )
    assert child.killed
    assert marked == []


def test_stage4_shutdown_does_not_claim_other_generation_stopped():
    assert stop_stage4_worker(
        Child(exit_code=0), expected_generation=str(uuid.uuid4()),
        marker=lambda **_: False,
    ) is False


def test_stage4_shutdown_sql_updates_only_matching_account_epoch_generation(monkeypatch):
    from trader.us.execution import authoritative_reconcile_worker as worker
    from trader.us.db import repos

    observed = []
    generation = str(uuid.uuid4())
    scope = {
        "env": "practice", "account_id": "a" * 64,
        "trading_epoch_id": str(uuid.uuid4()),
    }

    class Connection:
        def execute(self, sql, params):
            observed.append((str(sql), params))
            return SimpleNamespace(rowcount=1)

    class Transaction:
        def __enter__(self):
            return Connection()

        def __exit__(self, exc_type, exc, tb):
            return False

    class Engine:
        def begin(self):
            return Transaction()

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: Engine())
    monkeypatch.setattr(repos, "_us_execution_claim_scope", lambda: scope)

    assert worker.mark_stopped_after_confirmed_exit(
        expected_generation=generation, env="practice",
    )
    sql, params = observed[0]
    assert "worker_state='STOPPED'" in sql
    assert "evidence_at=NULL" in sql
    assert "account_fingerprint=:account" in sql
    assert "trading_epoch_id=:epoch" in sql
    assert "writer_generation=CAST(:generation AS uuid)" in sql
    assert params == {
        "env": "practice", "account": scope["account_id"],
        "epoch": scope["trading_epoch_id"], "generation": generation,
    }


def test_stage4_shutdown_refuses_malformed_generation_without_db_write(monkeypatch):
    from trader.us.execution import authoritative_reconcile_worker as worker
    from trader.us.db import repos

    monkeypatch.setattr(repos, "_us_execution_claim_scope", lambda: {
        "env": "practice", "account_id": "a" * 64,
        "trading_epoch_id": str(uuid.uuid4()),
    })
    monkeypatch.setattr(repos, "_get_engine_or_none",
                        lambda: pytest.fail("must not touch DB"))
    with pytest.raises(RuntimeError, match="GENERATION_NOT_CONFIGURED"):
        worker.mark_stopped_after_confirmed_exit(expected_generation="not-uuid")


def test_stage4_log_code_never_leaks_unstructured_exceptions():
    assert stage4_error_code(RuntimeError("US_STAGE4_WRITER_STALE")) == (
        "US_STAGE4_WRITER_STALE"
    )
    assert stage4_error_code(RuntimeError("secret: 123")) == "RuntimeError"
    assert stage4_error_code(RuntimeError("US_STAGE4_WRITER_STALE secret")) == "RuntimeError"


def test_stage4_session_source_gates_first_tick_and_shutdown():
    src = (Path(__file__).resolve().parents[2]
           / "trader/us/runner/trade_session_runner.py").read_text()
    after_child_start = src.index('_shadow_reconcile_child = subprocess.Popen(')
    readiness = src.index('wait_for_stage4_ready(', after_child_start)
    tick_loop = src.index('while not _stage4_startup_failed:', readiness)
    tick_run = src.index('run_tick_in_process(', tick_loop)
    assert after_child_start < readiness < tick_loop < tick_run
    assert 'final_reason = "stage4_worker_startup_unavailable"' in src
    assert 'stop_stage4_worker(' in src
    assert 'mark_us_session_child_unverified_fence(' in src


def test_stage4_tick_preserves_order_fence_and_diagnostic_error_code():
    src = (Path(__file__).resolve().parents[2]
           / "trader/us/runner/trade_tick_runner.py").read_text()
    assert 'stage4_error_code(_stage4_exception)' in src
    assert '"stage4_error_code":_stage4_code' in src
    assert '"exit_can_proceed":False' in src
    assert '"orders_sent":0' in src



@pytest.mark.skipif(not Path("/proc/self/stat").exists(),
                    reason="Linux /proc process identity proof required")
def test_stage4_worker_real_os_pid_birth_identity_fences_dead_and_reused_pid(monkeypatch):
    import sys
    from trader.us.execution import authoritative_reconcile_worker as worker

    child = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(30)"])
    try:
        birth = worker.stage4_process_start_ticks(child.pid)
        monkeypatch.setenv("US_RECONCILE_WORKER_MODE", "WORKER")
        monkeypatch.setenv("US_STAGE4_PROCESS_PROOF_REQUIRED", "1")
        monkeypatch.setenv("US_STAGE4_WORKER_PID", str(child.pid))
        monkeypatch.setenv("US_STAGE4_WORKER_START_TICKS", birth)
        worker.assert_stage4_worker_process_alive()

        # A recycled PID cannot impersonate the expected writer.
        monkeypatch.setenv("US_STAGE4_WORKER_START_TICKS", str(int(birth) + 1))
        with pytest.raises(RuntimeError, match="US_STAGE4_WORKER_PROCESS_DEAD"):
            worker.assert_stage4_worker_process_alive()

        monkeypatch.setenv("US_STAGE4_WORKER_START_TICKS", birth)
        child.terminate()
        child.wait(timeout=5)
        with pytest.raises(RuntimeError, match="US_STAGE4_WORKER_PROCESS_DEAD"):
            worker.assert_stage4_worker_process_alive()
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=5)


def test_stage4_writer_required_pid_identity_missing_fails_closed(monkeypatch):
    from trader.us.execution import authoritative_reconcile_worker as worker
    monkeypatch.setenv("US_RECONCILE_WORKER_MODE", "WORKER")
    monkeypatch.setenv("US_STAGE4_PROCESS_PROOF_REQUIRED", "1")
    monkeypatch.delenv("US_STAGE4_WORKER_PID", raising=False)
    monkeypatch.delenv("US_STAGE4_WORKER_START_TICKS", raising=False)
    with pytest.raises(RuntimeError, match="US_STAGE4_WORKER_PROCESS_IDENTITY_MISSING"):
        worker.assert_stage4_worker_process_alive()


def test_stage4_worker_dead_is_restartable_in_parent_but_never_at_tick_order_gate():
    from trader.us.execution import stage4_lifecycle as lifecycle
    assert "US_STAGE4_WORKER_PROCESS_DEAD" in lifecycle._RETRYABLE_STARTUP_CODES
    assert "US_STAGE4_WORKER_PROCESS_DEAD" not in lifecycle._RETRYABLE_TICK_CODES
    clock = Clock()
    with pytest.raises(RuntimeError, match="US_STAGE4_WORKER_PROCESS_DEAD"):
        read_stage4_tick_health_with_bounded_retry(
            health_reader=lambda **_: (_ for _ in ()).throw(
                RuntimeError("US_STAGE4_WORKER_PROCESS_DEAD")),
            clock=clock.read, sleeper=clock.sleep,
        )
    assert clock.now == 0


def test_session_supervisor_requires_os_reaping_before_new_generation_and_no_tick_while_paused():
    source = (Path(__file__).resolve().parents[2] /
              "trader/us/runner/trade_session_runner.py").read_text()
    assert 'US_STAGE4_PROCESS_PROOF_REQUIRED' in source
    assert 'US_STAGE4_WORKER_START_TICKS' in source
    assert 'US_STAGE4_MAX_RESTARTS' in source
    assert 'US_STAGE4_RESTART_AFTER_UNHEALTHY_SEC' in source
    assert 'generation_changed=1 orders_authorized=0' in source
    assert 'if not stop_stage4_worker(' in source
    assert 'new_generation = str(_stage4_new_uuid.uuid4())' in source
    assert 'old.args, env=new_env' in source
    assert '[US_STAGE4][PAUSED]' in source
    assert 'orders_authorized=0 action=SUPERVISE' in source
    assert 'action=RETURN_TO_SUPERVISOR orders_authorized=0' in source
    assert 'stage4_restart_budget_exhausted' in source
    assert 'stage4_evidence_unavailable_at_deadline' in source


def test_pre_http_uses_same_authoritative_process_identity_gate():
    src = (Path(__file__).resolve().parents[2] /
           "trader/us/execution/order_router.py").read_text()
    reader = (Path(__file__).resolve().parents[2] /
              "trader/us/execution/authoritative_reconcile_worker.py").read_text()
    assert src.count("read_worker_health(env=") >= 2
    assert reader.count("assert_stage4_worker_process_alive()") >= 2
