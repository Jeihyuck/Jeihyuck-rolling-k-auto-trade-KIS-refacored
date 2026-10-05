import pytest

from trader.us.runner.daily_report_runner import worsen_consistency


def test_failed_consistency_maps_to_failed_reconcile(monkeypatch):
    from trader.us.runner import daily_report_runner as d
    r=d.run_daily_report(env="practice",session="close",trade_date="2026-07-16",offline=True,final_balance={"positions":[{"symbol":"AMD","qty":1,"current_px":0}],"total_pvs":0},final_positions=[{"symbol":"AMD","qty":1,"current_px":0}],kis_fills=[])
    assert r["report"]["report_consistency"] == "REPORT_INCONSISTENT_POSITION_VALUE"
    assert r["status"] == "FAILED_RECONCILE"


def test_worsen_consistency_never_downgrades_failed():
    assert worsen_consistency("FAILED","OK") == "FAILED"


def test_daily_report_fails_closed_on_unresolved_submit_missing_from_orders_db(monkeypatch, tmp_path):
    from trader.us.db import repos
    from trader.us.execution import order_journal
    from trader.us.runner.daily_report_runner import run_daily_report

    class Provider:
        def get_balance(self, force_refresh=False):
            return {"positions": [], "balance_parse_status": "OK"}

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("US_DAILY_REPORT_BASE", str(tmp_path / "reports"))
    monkeypatch.setattr("trader.us.data_provider.USDataProvider", lambda offline=False: Provider())
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    monkeypatch.setattr(repos, "load_us_daily_orders_for_report", lambda *_a, **_k: [])
    monkeypatch.setattr(repos, "load_today_fills", lambda *_a, **_k: [])
    monkeypatch.setattr(order_journal, "order_audit_timelines", lambda *_a, **_k: {})
    monkeypatch.setattr(order_journal, "aggregate_order_events", lambda *_a, **_k: {
        "orders_sent_total": 1, "buy_orders_count": 1, "sell_orders_count": 0,
        "orders_ack_total": 0, "orders_reject_total": 0, "orders_fill_confirmed": 0,
        "unresolved_submit_attempt_count": 1,
    })

    result = run_daily_report(
        env="practice", session="close", trade_date="2026-10-01",
        final_balance={"positions": []}, final_positions=[], kis_fills=[],
        close_order_classification={
            "status": "OK", "pending_order_count": 0, "open_order_pending_count": 0,
            "unresolved_error_count": 0, "orders": [], "counts": {},
        },
    )

    assert result["status"] == "FAILED_RECONCILE"
    assert result["report"]["close_integrity_status"] == "RECONCILE_REQUIRED"
    assert result["report"]["unresolved_submit_attempt_count"] == 1


def test_broker_recovery_failure_preserves_specific_report_consistency():
    assert (
        worsen_consistency("REPORT_INCONSISTENT_POSITION_VALUE", "FAILED")
        == "REPORT_INCONSISTENT_POSITION_VALUE"
    )


@pytest.mark.parametrize(
    ("field", "value", "expected_integrity"),
    [
        ("available", False, "RECONCILE_REQUIRED"),
        ("recovery_health_error_count", 1, "RECONCILE_REQUIRED"),
        ("unresolved_execution_actions", 1, "RECONCILE_REQUIRED"),
        ("unattributed_broker_fills", 1, "RECONCILE_REQUIRED"),
        ("broker_fill_rebound_failure_count", 1, "RECONCILE_REQUIRED"),
        ("duplicate_semantic_submit_detection_count", 1, "INTEGRITY_DEGRADED"),
        ("broker_local_cumulative_fill_mismatch_count", 1, "INTEGRITY_DEGRADED"),
        ("filled_sell_missing_cost_basis_count", 1, "INTEGRITY_DEGRADED"),
        ("stale_pending_exit_stage_after_fill_count", 1, "INTEGRITY_DEGRADED"),
        ("closed_lifecycle_open_state_count", 1, "INTEGRITY_DEGRADED"),
    ],
)
def test_close_daily_report_never_returns_ok_for_broker_recovery_failures(
    monkeypatch, tmp_path, field, value, expected_integrity,
):
    from trader.us.db import repos
    from trader.us.execution import order_journal
    from trader.us.runner.daily_report_runner import run_daily_report

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("US_DAILY_REPORT_BASE", str(tmp_path / "reports"))
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    monkeypatch.setattr(repos, "load_us_daily_orders_for_report", lambda *_a, **_k: [])
    monkeypatch.setattr(repos, "load_today_fills", lambda *_a, **_k: [])
    monkeypatch.setattr(repos, "load_execution_claim_health", lambda: {
        "unresolved_execution_actions": 0,
        "execution_claim_conflicts": 0,
    })
    monkeypatch.setattr(order_journal, "order_audit_timelines", lambda *_a, **_k: {})
    monkeypatch.setattr(order_journal, "aggregate_order_events", lambda *_a, **_k: {
        "orders_sent_total": 0,
        "buy_orders_count": 0,
        "sell_orders_count": 0,
        "orders_ack_total": 0,
        "orders_reject_total": 0,
        "orders_fill_confirmed": 0,
        "unresolved_submit_attempt_count": 0,
    })

    health = {
        "available": True,
        "recovery_health_error_count": 0,
        "unattributed_broker_fills": 0,
        "broker_fill_rebound_failure_count": 0,
        "broker_local_cumulative_fill_mismatch_count": 0,
        "duplicate_semantic_submit_detection_count": 0,
        "unresolved_execution_actions": 0,
        "filled_sell_missing_cost_basis_count": 0,
        "stale_pending_exit_stage_after_fill_count": 0,
        "closed_lifecycle_open_state_count": 0,
    }
    health[field] = value
    result = run_daily_report(
        env="practice",
        session="close",
        trade_date="2026-10-01",
        offline=False,
        final_balance={"positions": []},
        final_positions=[],
        kis_fills=[],
        close_order_classification={
            "status": "OK",
            "pending_order_count": 0,
            "open_order_pending_count": 0,
            "unresolved_error_count": 0,
            "orders": [],
            "counts": {},
        },
        broker_recovery_health=health,
    )

    assert result["status"] == "FAILED_RECONCILE"
    assert result["report"]["close_integrity_status"] == expected_integrity
    assert result["report"]["broker_recovery_integrity_errors"]
