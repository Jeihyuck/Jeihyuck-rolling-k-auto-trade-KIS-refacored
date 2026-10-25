"""2026-10-23 Stage4 startup regression: never race the first trading Tick.

All cases are offline. No broker requests, database writes, epoch changes or
scheduler actions are permitted in this suite.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from trader.us.runner.trade_session_runner import wait_for_stage4_startup


class FakeClock:
    def __init__(self):
        self.value = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.value

    def sleep(self, seconds):
        assert seconds > 0
        self.sleeps.append(seconds)
        self.value += seconds


class Child:
    def __init__(self, code=None):
        self.code = code

    def poll(self):
        return self.code


def ready_health(**_kwargs):
    return {
        "worker_state": "READY",
        "reconcile_status": "OK",
        "replay_status": "UNRESOLVED",
    }


def test_stage4_waits_for_real_ready_evidence_before_first_tick():
    clock = FakeClock()
    reads = []

    def read_health(**kwargs):
        reads.append(kwargs)
        if len(reads) < 3:
            raise RuntimeError("US_STAGE4_WRITER_NOT_READY")
        return ready_health()

    ready, reason = wait_for_stage4_startup(
        child=Child(), env="practice", timeout_sec=5,
        read_health=read_health, monotonic=clock.monotonic,
        sleep=clock.sleep,
    )
    assert (ready, reason) == (True, "READY")
    assert len(reads) == 3
    assert clock.value == 2
    assert all(call["env"] == "practice" for call in reads)


@pytest.mark.parametrize("status", [
    {"worker_state": "STARTING", "reconcile_status": "OK", "replay_status": "OK"},
    {"worker_state": "FAILED", "reconcile_status": "OK", "replay_status": "OK"},
    {"worker_state": "READY", "reconcile_status": "ERROR", "replay_status": "OK"},
    {"worker_state": "READY", "reconcile_status": "OK", "replay_status": "ERROR"},
])
def test_stage4_nonready_health_never_authorizes_first_tick(status):
    clock = FakeClock()
    ready, reason = wait_for_stage4_startup(
        child=Child(), env="practice", timeout_sec=2,
        read_health=lambda **_: status,
        monotonic=clock.monotonic, sleep=clock.sleep,
    )
    assert ready is False
    assert reason.startswith("US_STAGE4_STARTUP_TIMEOUT:")
    assert clock.value == 2


def test_stage4_missing_database_row_is_not_treated_as_ready():
    clock = FakeClock()

    def missing(**_kwargs):
        raise RuntimeError("US_STAGE4_WRITER_NOT_READY")

    ready, reason = wait_for_stage4_startup(
        child=Child(), env="practice", timeout_sec=3,
        read_health=missing, monotonic=clock.monotonic,
        sleep=clock.sleep,
    )
    assert ready is False
    assert reason == "US_STAGE4_STARTUP_TIMEOUT:US_STAGE4_WRITER_NOT_READY"
    assert clock.value == 3


def test_stage4_dead_child_fails_immediately_without_sleep_or_health_lookup():
    clock = FakeClock()

    def must_not_read(**_kwargs):
        pytest.fail("Dead worker must never be trusted")

    ready, reason = wait_for_stage4_startup(
        child=Child(code=1), env="practice", timeout_sec=75,
        read_health=must_not_read,
        monotonic=clock.monotonic, sleep=clock.sleep,
    )
    assert (ready, reason) == (False, "US_STAGE4_WORKER_PROCESS_EXITED")
    assert clock.sleeps == []


def test_stage4_wrong_generation_never_downgrades_to_legacy():
    clock = FakeClock()

    def mismatch(**_kwargs):
        raise RuntimeError("US_STAGE4_WRITER_GENERATION_MISMATCH")

    ready, reason = wait_for_stage4_startup(
        child=Child(), env="practice", timeout_sec=1,
        read_health=mismatch, monotonic=clock.monotonic,
        sleep=clock.sleep,
    )
    assert ready is False
    assert "GENERATION_MISMATCH" in reason


def test_stage4_wait_budget_is_bounded_and_zero_budget_fails_closed():
    clock = FakeClock()

    def missing(**_kwargs):
        raise RuntimeError("US_STAGE4_WRITER_NOT_READY")

    ready, reason = wait_for_stage4_startup(
        child=Child(), env="practice", timeout_sec=0,
        read_health=missing, monotonic=clock.monotonic,
        sleep=clock.sleep,
    )
    assert ready is False
    assert reason.startswith("US_STAGE4_STARTUP_TIMEOUT:")
    assert clock.sleeps == []


def test_session_handoff_occurs_before_first_trading_tick_and_retains_http_fence():
    source = (
        Path(__file__).resolve().parents[2]
        / "trader/us/runner/trade_session_runner.py"
    ).read_text(encoding="utf-8")
    handoff = source.index("_stage4_ready, _stage4_reason = wait_for_stage4_startup(")
    first_tick = source.index("tick_count += 1", handoff)
    assert handoff < first_tick
    assert 'final_reason = "stage4_worker_startup_unverified"' in source
    assert "action=NO_TICK_NO_ORDER" in source
    assert "FIRST_TICK_ALLOWED_WITH_CONTINUOUS_FENCING" in source


def test_close_claim_health_is_scoped_to_new_account_and_epoch(monkeypatch):
    """Old account/epoch conflicts must not leak into next day's new account."""
    from trader.us.db import repos

    scope = {
        "env": "practice", "account_id": "new-account-fingerprint",
        "market": "US", "trading_epoch_id": "new-epoch",
    }
    captured = []

    class FakeClaims:
        def health(self, **kwargs):
            captured.append(kwargs)
            return {
                "unresolved_execution_actions": 0,
                "execution_claim_conflicts": 0,
            }

    monkeypatch.setattr(repos, "_us_execution_claim_scope", lambda: scope)
    monkeypatch.setattr(repos, "_execution_claim_repo", lambda: FakeClaims())
    assert repos.load_execution_claim_health() == {
        "unresolved_execution_actions": 0,
        "execution_claim_conflicts": 0,
    }
    assert captured == [scope]


def test_authoritative_worker_publishes_non_authorizing_starting_before_kis():
    source = (
        Path(__file__).resolve().parents[2]
        / "trader/us/execution/authoritative_reconcile_worker.py"
    ).read_text(encoding="utf-8")
    start = source.index('generation=generation, state="STARTING"')
    provider = source.index('provider=USDataProvider(offline=False,env="practice")', start)
    assert start < provider
    assert 'str(row["worker_state"])!="READY"' in source


@pytest.mark.parametrize("message,expected", [
    ("US_STAGE4_WRITER_GENERATION_MISMATCH", "US_STAGE4_WRITER_GENERATION_MISMATCH"),
    ("US_STAGE4_WRITER_NOT_READY", "US_STAGE4_WRITER_NOT_READY"),
    ("KIS_APP_SECRET=supersecret", "US_STAGE4_HEALTH_READ_ERROR"),
    ("US_STAGE4_WRITER_NOT_READY KIS_APP_SECRET=supersecret",
     "US_STAGE4_HEALTH_READ_ERROR"),
    ("DB failed postgresql://user:password@db.internal:5432/db",
     "US_STAGE4_HEALTH_READ_ERROR"),
    ("US_STAGE4_WRITER_NOT_READY\\npassword=supersecret",
     "US_STAGE4_HEALTH_READ_ERROR"),
])
def test_stage4_untrusted_exception_does_not_leak(message, expected):
    from trader.us.execution.authoritative_reconcile_worker import stage4_diagnostic_code

    assert stage4_diagnostic_code(RuntimeError(message)) == expected


def test_stage4_startup_failure_redacts_broker_and_database_error_messages():
    clock = FakeClock()

    def read_failed(**_kwargs):
        raise RuntimeError("postgresql://user:supersecret@db.internal/db")

    ready, reason = wait_for_stage4_startup(
        child=Child(), env="practice", timeout_sec=1,
        read_health=read_failed, monotonic=clock.monotonic,
        sleep=clock.sleep,
    )
    assert ready is False
    assert reason == "US_STAGE4_STARTUP_TIMEOUT:US_STAGE4_HEALTH_READ_ERROR"
    assert "supersecret" not in reason
    assert "db.internal" not in reason
