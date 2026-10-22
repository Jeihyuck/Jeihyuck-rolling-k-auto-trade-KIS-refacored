"""No-write shadow observer and session Worker-mode safety contract."""
import json

import pytest

from trader.us.execution import reconcile_shadow_worker as worker


def test_shadow_only_explicit_mode_and_invalid_modes_fail_closed():
    assert worker.parse_mode("legacy") == "LEGACY"
    assert worker.parse_mode("SHADOW") == "SHADOW"
    assert worker.parse_mode("RESERVATION_ONLY") == "RESERVATION_ONLY"
    with pytest.raises(ValueError, match="not yet implemented"):
        worker.parse_mode("WORKER")
    with pytest.raises(ValueError):
        worker.parse_mode("invalid")
    assert worker.safe_scope_token("../../private/account") == ".._.._private_account"


def test_shadow_diagnostic_reads_only_and_redacts_identity(monkeypatch):
    from trader.us.db import repos
    calls = []

    def active():
        calls.append("claim_read")
        return [{"account_id": "PRIVATE", "active_attempt_id": "a-1"}]

    def health():
        calls.append("health_read")
        return {"unresolved_execution_actions": 1, "other": 2}

    def pending(*a, **kw):
        calls.append("pending_read")
        return {"status": "OK", "orders": [{"order_no": "SECRET"}]}

    monkeypatch.setattr(repos, "load_active_execution_claim_attempts", active)
    monkeypatch.setattr(repos, "load_execution_claim_health", health)
    monkeypatch.setattr(
        repos, "count_us_terminal_order_claim_mismatches",
        lambda **kwargs: calls.append("scoped_mismatch_read") or 1,
    )
    monkeypatch.setattr(repos, "load_pending_ack_orders_result", pending)
    result = worker.observe_once(
        trade_date="2026-10-20", session_run_id="../../MY_SESSION",
        env="practice",
    )
    assert calls == [
        "claim_read", "health_read", "pending_read", "scoped_mismatch_read",
    ]
    assert result["terminal_order_active_claim_mismatch_count"] == 1
    assert result["active_claim_count"] == 1
    assert result["pending_order_count"] == 1
    assert result["execution_claim_health"]["unresolved_execution_actions"] == 1
    assert result["write_authority"] == result["broker_access"] == "NONE"
    assert "SECRET" not in json.dumps(result)
    assert "PRIVATE" not in json.dumps(result)


def test_shadow_missing_db_is_error_not_empty_orders(monkeypatch):
    from trader.us.db import repos
    monkeypatch.setattr(repos, "load_active_execution_claim_attempts", lambda: [])
    monkeypatch.setattr(repos, "load_execution_claim_health", lambda: {})
    monkeypatch.setattr(
        repos, "count_us_terminal_order_claim_mismatches", lambda **kwargs: 0,
    )
    monkeypatch.setattr(
        repos, "load_pending_ack_orders_result",
        lambda *a, **kw: {"status": "DB_UNAVAILABLE"},
    )
    with pytest.raises(RuntimeError, match="shadow_order_read_failed"):
        worker.observe_once(trade_date="2026-10-20", session_run_id="S", env="practice")


def test_observer_writes_only_separate_local_diagnostics(tmp_path, monkeypatch):
    from trader.us.db import repos
    monkeypatch.setenv("US_SHADOW_RECONCILE_DIR", str(tmp_path))
    monkeypatch.setattr(repos, "load_active_execution_claim_attempts", lambda: [])
    monkeypatch.setattr(repos, "load_execution_claim_health",
                        lambda: {"unresolved_execution_actions": 0})
    monkeypatch.setattr(
        repos, "count_us_terminal_order_claim_mismatches", lambda **kwargs: 0,
    )
    monkeypatch.setattr(
        repos, "load_pending_ack_orders_result",
        lambda *a, **kw: {"status": "OK", "orders": []},
    )
    worker._STOP = False
    assert worker.run_shadow(
        trade_date="2026-10-20", session_run_id="US-AM", env="practice",
        max_cycles=1,
    ) == 0
    report = worker.observer_report_path("2026-10-20", "US-AM")
    data = json.loads(report.read_text(encoding="utf-8"))
    assert data["read_status"] == "OK"
    assert data["write_authority"] == "NONE"
    assert data["terminal_order_active_claim_mismatch_count"] == 0
    assert list(tmp_path.rglob("*.json")) == [report]


def test_shadow_rejects_production_account_and_path_escape(tmp_path, monkeypatch):
    from trader.us.db import repos
    with pytest.raises(RuntimeError, match="practice"):
        worker.observe_once(trade_date="2026-10-20", session_run_id="a", env="real")
    with pytest.raises(ValueError, match="ISO"):
        worker.observer_report_path("../../secrets", "session")


def test_session_runner_reaches_actual_worker_lifecycle():
    # Verify production entrypoint callsites, not just an unused worker module.
    import inspect
    from trader.us.runner import trade_session_runner
    source = inspect.getsource(trade_session_runner.run_trade_session)
    assert "subprocess.Popen(" in source
    assert "trader.us.execution.reconcile_shadow_worker" in source
    assert "_shadow_reconcile_child.terminate()" in source
    assert "_shadow_reconcile_child.wait(timeout=5)" in source
    assert 'parse_mode(os.getenv("US_RECONCILE_WORKER_MODE", "LEGACY"))' in source


def test_reservation_worker_is_brokerless_and_has_separate_health_file(
    tmp_path, monkeypatch,
):
    from trader.us.execution import reservation_reconcile_worker as reserve_worker
    monkeypatch.setenv("US_RESERVATION_RECONCILE_DIR", str(tmp_path))
    calls = []
    def fake_sweep(**kwargs):
        calls.append(kwargs)
        return {
            "mode": "RESERVATION_ONLY", "released": 1,
            "consumed": 0, "attempted": 1, "unresolved": 0,
            "write_authority": "FENCED_ACCOUNT_RESERVATIONS_ONLY",
        }
    import hashlib
    from trader.us.execution.account_authority import AuthorityUnavailable
    _account = "a" * 64
    _epoch = "test-epoch"
    _expected_digest = hashlib.sha256(
        ("practice|" + _account + "|" + _epoch).encode("utf-8")
    ).hexdigest()
    _old_fake = fake_sweep
    def fake_sweep(**kwargs):
        return {
            **_old_fake(**kwargs),
            "account_scope_fingerprint": _expected_digest,
        }
    monkeypatch.setattr(
        reserve_worker, "reconcile_proven_reservations_once", fake_sweep,
    )
    assert reserve_worker.run_reservation_loop(
        trade_date="2026-10-20", session_run_id="R-123",
        env="practice", max_cycles=1,
    ) == 0
    assert calls == [{"env": "practice", "trade_date": "2026-10-20"}]
    report = reserve_worker.reservation_report_path("2026-10-20", "R-123")
    data = json.loads(report.read_text(encoding="utf-8"))
    assert data["released"] == 1 and data["read_status"] == "OK"
    assert data["write_authority"] == "FENCED_ACCOUNT_RESERVATIONS_ONLY"
    assert len(list(tmp_path.rglob("*.json"))) == 1
    reserve_worker.assert_fresh_reservation_worker(
        trade_date="2026-10-20", session_run_id="R-123",
        env="practice", account_fingerprint=_account,
        trading_epoch_id=_epoch,
    )
    with pytest.raises(AuthorityUnavailable, match="SCOPE_MISMATCH"):
        reserve_worker.assert_fresh_reservation_worker(
            trade_date="2026-10-20", session_run_id="R-123",
            env="practice", account_fingerprint="b" * 64,
            trading_epoch_id=_epoch,
        )
    with pytest.raises(ValueError, match="ISO"):
        reserve_worker.reservation_report_path("../secrets", "session")


def test_reservation_worker_session_activation_is_explicitly_gated():
    import inspect
    from trader.us.runner.trade_session_runner import run_trade_session
    source = inspect.getsource(run_trade_session)
    assert "US_RESERVATION_WORKER_REQUIRES_ACCOUNT_ATOMIC_GATE" in source
    assert "US_ACCOUNT_RESERVATION_MODE" in source
    assert "US_ATOMIC_CLAIM_OUTBOX_ENABLED" in source
    assert "trader.us.execution.reservation_reconcile_worker" in source
    assert "_shadow_reconcile_child.terminate()" in source
