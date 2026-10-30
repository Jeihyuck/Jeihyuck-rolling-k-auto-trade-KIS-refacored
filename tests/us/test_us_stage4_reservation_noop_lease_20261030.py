"""Stage4 reservation sweeps: no-op leases must not contend with live BUYs.

Regression for 2026-10-29 22:32/23:03 KST US_ACCOUNT_AUTHORITY_LEASE_HELD.
Broker-derived terminal candidates must still be fenced and independently proven.
"""
from __future__ import annotations

import pytest

from trader.us.execution import reservation_reconcile_worker as worker
from trader.us.execution.account_authority import AuthorityUnavailable
from trader.us.db import repos


class _Rows:
    def __init__(self, rows):
        self.rows = rows

    def scalars(self):
        return self

    def mappings(self):
        return self

    def all(self):
        return self.rows


class _ReadOnlyConnection:
    def __init__(self, terminal_candidates, abandoned_candidates):
        self.terminal_candidates = terminal_candidates
        self.abandoned_candidates = abandoned_candidates

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, statement, params):
        sql = str(statement)
        if "SELECT DISTINCT trading_epoch_id" in sql:
            return _Rows(["epoch-current"])
        if "NULL::DATE AS source_trade_date" in sql:
            return _Rows(self.abandoned_candidates)
        if "SELECT r.action_key AS attempt_id" in sql:
            return _Rows(self.terminal_candidates)
        raise AssertionError("unexpected SQL in no-op lease regression")


class _Engine:
    def __init__(self, terminal_candidates=(), abandoned_candidates=()):
        self.conn = _ReadOnlyConnection(
            list(terminal_candidates), list(abandoned_candidates),
        )

    def connect(self):
        return self.conn


def _run(monkeypatch, *, terminal=(), abandoned=(), lease_blocked=False):
    events = []
    engine = _Engine(terminal, abandoned)
    monkeypatch.setenv("US_ACCOUNT_RESERVATION_MODE", "ENFORCE")
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: engine)
    monkeypatch.setattr(
        repos,
        "_us_execution_claim_scope",
        lambda: {
            "env": "practice",
            "account_id": "a" * 64,
            "trading_epoch_id": "epoch-current",
        },
    )

    class _Authority:
        def __init__(self, _engine, **scope):
            assert _engine is engine
            assert scope["trading_epoch_id"] == "epoch-current"

        def acquire_lease(self, **kwargs):
            events.append("lease_attempt")
            if lease_blocked:
                raise AuthorityUnavailable("US_ACCOUNT_AUTHORITY_LEASE_HELD")
            return object()

        def release_lease(self, *, lease):
            events.append("lease_released")

        def terminalize_verified_attempt(self, **kwargs):
            events.append("terminal_proof")
            assert kwargs["attempt_id"] == "verified-1"
            assert kwargs["client_order_key"] == "client-1"
            return {"state": "CONSUMED", "idempotent": False}

        def release_proven_pre_submit_abort(self, **kwargs):
            events.append("pre_submit_proof")
            return True

    monkeypatch.setattr(worker, "AccountExecutionAuthority", _Authority)
    return events, lambda: worker.reconcile_proven_reservations_once(
        env="practice", trade_date="2026-10-29",
    )


def test_no_terminal_candidate_does_not_acquire_live_order_lease(monkeypatch):
    events, run = _run(monkeypatch, lease_blocked=True)
    result = run()
    assert result["attempted"] == 0
    assert result["released"] == 0
    assert result["consumed"] == 0
    assert result["unresolved"] == 0
    assert events == []


def test_proven_candidate_lease_conflict_stays_fail_closed(monkeypatch):
    candidate = {
        "attempt_id": "verified-1",
        "client_order_key": "client-1",
        "source_trade_date": "2026-10-29",
        "attempt_state": "FILLED",
    }
    events, run = _run(
        monkeypatch, terminal=[candidate], lease_blocked=True,
    )
    with pytest.raises(AuthorityUnavailable, match="LEASE_HELD"):
        run()
    assert events == ["lease_attempt"]


def test_proven_candidate_is_not_skipped_or_unfenced(monkeypatch):
    candidate = {
        "attempt_id": "verified-1",
        "client_order_key": "client-1",
        "source_trade_date": "2026-10-29",
        "attempt_state": "FILLED",
    }
    events, run = _run(monkeypatch, terminal=[candidate])
    result = run()
    assert result["attempted"] == 1
    assert result["consumed"] == 1
    assert result["unresolved"] == 0
    assert events == ["lease_attempt", "terminal_proof", "lease_released"]


def test_verified_pre_submit_abort_remains_fenced(monkeypatch):
    candidate = {
        "attempt_id": "verified-1",
        "client_order_key": "client-1",
        "source_trade_date": None,
        "attempt_state": "ABANDONED_PRE_SUBMIT",
    }
    events, run = _run(monkeypatch, abandoned=[candidate])
    result = run()
    assert result["attempted"] == 1
    assert result["released"] == 1
    assert events == ["lease_attempt", "pre_submit_proof", "lease_released"]
