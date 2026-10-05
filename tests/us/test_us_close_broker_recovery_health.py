from __future__ import annotations

import pytest


def _clean_health():
    return {
        "available": True,
        "recovery_health_error_count": 0,
        "unattributed_broker_fills": 0,
        "broker_fill_rebound_failure_count": 0,
        "broker_local_cumulative_fill_mismatch_count": 0,
        "unresolved_execution_actions": 0,
        "filled_sell_missing_cost_basis_count": 0,
        "broker_fill_rebound_success_count": 1,
    }


def _configure_close(monkeypatch, tmp_path, health, *, report_errors=None, pending_open=0,
                     active_unresolved=0):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GITHUB_RUN_ID", f"close-health-{tmp_path.name}")

    class Provider:
        def get_balance(self, force_refresh=False):
            return {"positions": [], "balance_parse_status": "OK"}

    monkeypatch.setattr("trader.us.data_provider.USDataProvider", lambda offline=False: Provider())
    monkeypatch.setattr(
        "trader.us.execution.fills.get_fills_today",
        lambda **kwargs: {"status": "OK", "fills": []},
    )
    monkeypatch.setattr(
        "trader.us.execution.order_journal.replay_order_journal",
        lambda *args, **kwargs: {"status": "OK", "unresolved_count": active_unresolved},
    )
    monkeypatch.setattr(
        "trader.us.execution.reconcile.reconcile_ack_orders_with_balance",
        lambda **kwargs: {"status": "OK", "unresolved_count": 0, "failed_count": 0},
    )
    monkeypatch.setattr(
        "trader.us.execution.reconcile.reconcile_positions",
        lambda **kwargs: {
            "status": "OK",
            "balance_fetch_status": "OK",
            "balance_parse_status": "OK",
            "authoritative_positions": True,
            "preserve_previous_positions": False,
            "positions": [],
        },
    )
    monkeypatch.setattr("trader.us.db.repos.save_fills_with_result", lambda *a, **k: {"status": "OK"})
    monkeypatch.setattr("trader.us.db.repos.save_position_snapshot", lambda *a, **k: True)
    monkeypatch.setattr("trader.us.db.repos.save_reconcile_log", lambda *a, **k: True)
    monkeypatch.setattr(
        "trader.us.db.repos.load_broker_recovery_health",
        lambda trade_date: dict(health),
    )
    monkeypatch.setattr(
        "trader.us.execution.reconcile.classify_ack_orders_with_final_balance",
        lambda **kwargs: {
            "status": "OK",
            "orders": [],
            "counts": {},
            "pending_order_count": 0,
            "open_order_pending_count": pending_open,
        },
    )
    received_health = {}

    def daily_report(**kwargs):
        received_health.update(kwargs["broker_recovery_health"])
        errors = list(report_errors or [])
        return {
            "status": "FAILED_RECONCILE" if errors else "OK",
            "report": {
                "report_consistency": "FAILED" if errors else "OK",
                "errors": errors,
            },
        }

    monkeypatch.setattr("trader.us.runner.daily_report_runner.run_daily_report", daily_report)
    from trader.us.runner.trade_close_runner import run_trade_close

    result = run_trade_close(
        env="practice", offline=False, force_now="2026-10-01T16:05:00-04:00",
    )
    return result, received_health


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("available", False),
        ("recovery_health_error_count", 1),
        ("unattributed_broker_fills", 1),
        ("broker_fill_rebound_failure_count", 1),
        ("broker_local_cumulative_fill_mismatch_count", 1),
        ("unresolved_execution_actions", 1),
    ],
)
def test_close_fails_closed_for_broker_recovery_integrity_health(
    monkeypatch, tmp_path, field, value,
):
    health = _clean_health()
    health[field] = value

    result, report_health = _configure_close(monkeypatch, tmp_path, health)

    assert result["status"] == "ERROR"
    assert result["report_consistency"] == "FAILED"
    assert result["manual_reconcile_required"] is True
    assert result["reconcile_required"] is True
    assert result["broker_recovery_integrity_errors"]
    assert report_health == result["broker_recovery_health"]


def test_close_requires_manual_reconcile_for_active_claim_recovery_unresolved(
    monkeypatch, tmp_path,
):
    result, _ = _configure_close(
        monkeypatch, tmp_path, _clean_health(), active_unresolved=1,
    )

    assert result["status"] == "ERROR"
    assert result["broker_recovery_health"]["unresolved_execution_actions"] == 1
    assert result["manual_reconcile_required"] is True
    assert result["reconcile_required"] is True


def test_close_fails_when_authoritative_sell_lacks_recoverable_cost_basis(
    monkeypatch, tmp_path,
):
    health = _clean_health()
    health["filled_sell_missing_cost_basis_count"] = 1
    result, _ = _configure_close(
        monkeypatch,
        tmp_path,
        health,
        report_errors=["ERROR_MISSING_SELL_COST_BASIS symbol=QXYZ"],
    )

    assert result["status"] == "ERROR"
    assert result["report_consistency"] == "FAILED"
    assert any(
        "FILLED_SELL_COST_BASIS_MISSING" in error
        for error in result["broker_recovery_integrity_errors"]
    )
    assert result["manual_reconcile_required"] is True
    assert result["reconcile_required"] is True


def test_clean_recovered_broker_truth_can_close_normally(monkeypatch, tmp_path):
    result, report_health = _configure_close(monkeypatch, tmp_path, _clean_health())

    assert result["status"] == "OK"
    assert report_health["broker_fill_rebound_success_count"] == 1
    assert result["manual_reconcile_required"] is False
    assert result["reconcile_required"] is False


def test_known_open_order_warning_is_preserved_with_healthy_broker_truth(
    monkeypatch, tmp_path,
):
    result, _ = _configure_close(
        monkeypatch, tmp_path, _clean_health(), pending_open=1,
    )

    assert result["status"] == "OK_WITH_WARNINGS"
    assert result["manual_reconcile_required"] is False
    assert result["reconcile_required"] is False


def test_recovery_health_requires_cost_basis_for_authoritative_sell(monkeypatch):
    from trader.us.db import repos

    monkeypatch.setattr(repos, "load_today_fills", lambda trade_date: [{
        "client_order_key": "local-sell-key",
        "order_no": "broker-sell",
        "symbol": "QXYZ",
        "side": "SELL",
        "qty": 2,
        "meta": {"fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL"},
    }])
    monkeypatch.setattr(repos, "load_us_daily_orders_for_report", lambda trade_date: [{
        "client_order_key": "local-sell-key",
        "qty_filled": 2,
        "meta": {"strategy_owner": "US_STANDARD"},
    }])
    monkeypatch.setattr(
        "trader.us.execution.order_journal.load_order_events",
        lambda trade_date: [],
    )
    monkeypatch.setattr(repos, "load_execution_claim_health", lambda: {
        "unresolved_execution_actions": 0,
        "execution_claim_conflicts": 0,
    })

    health = repos.load_broker_recovery_health("2026-10-01")

    assert health["available"] is True
    assert health["filled_sell_missing_cost_basis_count"] == 1

    monkeypatch.setattr(repos, "load_today_fills", lambda trade_date: [{
        "client_order_key": "local-sell-key",
        "order_no": "broker-sell",
        "symbol": "QXYZ",
        "side": "SELL",
        "qty": 2,
        "meta": {
            "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
            "avg_cost": 41.25,
        },
    }])
    health = repos.load_broker_recovery_health("2026-10-01")
    assert health["filled_sell_missing_cost_basis_count"] == 0


def test_recovery_health_sums_distinct_execution_rows_not_cumulative_snapshots(monkeypatch):
    from trader.us.db import repos

    execution_one = {
        "trade_date": "2026-10-01",
        "client_order_key": "execution-order",
        "order_no": "execution-order-no",
        "symbol": "HEALTH_TEST",
        "side": "SELL",
        "qty": 1,
        "fill_idempotency_key": "health-exec-1",
        "meta": {
            "fill_evidence_type": "KIS_EXECUTION_ACTUAL",
            "broker_execution_id": "exec-1",
        },
    }
    execution_two = {
        **execution_one,
        "fill_idempotency_key": "health-exec-2",
        "meta": {
            "fill_evidence_type": "KIS_ACTUAL",
            "broker_execution_id": "exec-2",
        },
    }
    cumulative_snapshot = {
        **execution_one,
        "qty": 2,
        "fill_idempotency_key": "health-cumulative",
        "meta": {
            "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
            "cumulative_filled_qty": 2,
        },
    }
    monkeypatch.setattr(
        repos, "load_today_fills",
        lambda trade_date: [
            execution_one, execution_two, execution_one, cumulative_snapshot,
        ],
    )
    monkeypatch.setattr(repos, "load_us_daily_orders_for_report", lambda trade_date: [{
        "client_order_key": "execution-order",
        "qty_filled": 2,
        "meta": {},
    }])
    monkeypatch.setattr(
        "trader.us.execution.order_journal.load_order_events",
        lambda trade_date: [],
    )
    monkeypatch.setattr(repos, "load_execution_claim_health", lambda: {
        "unresolved_execution_actions": 0,
        "execution_claim_conflicts": 0,
    })

    health = repos.load_broker_recovery_health("2026-10-01")

    assert health["broker_local_cumulative_fill_mismatch_count"] == 0


def test_rebound_supersedes_historical_replay_failure_but_unresolved_attempt_fails_close(
    monkeypatch, tmp_path,
):
    from trader.us.db import repos

    recovered_identity = {
        "client_order_key": "recovered-order",
        "submit_attempt_id": "recovered-attempt",
    }
    unresolved_identity = {
        "client_order_key": "unresolved-order",
        "submit_attempt_id": "unresolved-attempt",
    }
    events = [
        {"event_type": "JOURNAL_REPLAY_UNRESOLVED", **recovered_identity},
        {
            "event_type": "BROKER_ACK_RECOVERED",
            "meta": {"broker_recovery_status": "REBOUND"},
            **recovered_identity,
        },
        {"event_type": "JOURNAL_REPLAY_FAILED", **unresolved_identity},
    ]
    monkeypatch.setattr(repos, "load_today_fills", lambda trade_date: [])
    monkeypatch.setattr(repos, "load_us_daily_orders_for_report", lambda trade_date: [])
    monkeypatch.setattr(
        "trader.us.execution.order_journal.load_order_events",
        lambda trade_date: events,
    )
    monkeypatch.setattr(repos, "load_execution_claim_health", lambda: {
        "unresolved_execution_actions": 0,
        "execution_claim_conflicts": 0,
    })

    health = repos.load_broker_recovery_health("2026-10-01")
    events[:] = events[:2]
    clean_health = repos.load_broker_recovery_health("2026-10-01")

    assert health["broker_fill_rebound_failure_count"] == 1
    assert clean_health["broker_fill_rebound_failure_count"] == 0
    result, _ = _configure_close(monkeypatch, tmp_path, health)
    assert result["status"] == "ERROR"
    assert result["manual_reconcile_required"] is True
    result, _ = _configure_close(monkeypatch, tmp_path, clean_health)
    assert result["status"] == "OK"
