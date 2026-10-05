from __future__ import annotations

import pytest


def _clean_health():
    return {
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
        "broker_fill_rebound_success_count": 1,
    }


def _configure_close(monkeypatch, tmp_path, health, *, report_errors=None, pending_open=0,
                     active_unresolved=0, attributable_fills=None, pnl_summary=None):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GITHUB_RUN_ID", f"close-health-{tmp_path.name}")

    class Provider:
        def get_balance(self, force_refresh=False):
            return {"positions": [], "balance_parse_status": "OK"}

    monkeypatch.setattr("trader.us.data_provider.USDataProvider", lambda offline=False: Provider())
    monkeypatch.setattr(
        "trader.us.execution.fills.get_fills_today",
        lambda **kwargs: {"status": "OK", "fills": list(attributable_fills or [])},
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
    persisted_fills = []

    def persist_fills(fills, **_kwargs):
        persisted_fills.extend(fills)
        return {"status": "OK", "inserted_count": len(fills), "updated_count": 0}

    monkeypatch.setattr("trader.us.db.repos.save_fills_with_result", persist_fills)
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
        if attributable_fills is not None:
            received_health["kis_fills"] = list(kwargs["kis_fills"])
            received_health["persisted_fills"] = list(persisted_fills)
        if pnl_summary is not None:
            received_health["trade_pnl_analysis"] = pnl_summary
        errors = list(report_errors or [])
        return {
            "status": "FAILED_RECONCILE" if errors else "OK",
            "report": {
                "report_consistency": "FAILED" if errors else "OK",
                "errors": errors,
                **({"trade_pnl_analysis": pnl_summary} if pnl_summary is not None else {}),
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
        ("duplicate_semantic_submit_detection_count", 1),
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


def test_recovery_health_detects_filled_pending_exit_stages_and_closed_lifecycle(
    monkeypatch,
):
    from trader.us.db import repos

    fills = [
        {
            "symbol": "QXYZ",
            "side": "SELL",
            "client_order_key": key,
            "order_no": f"broker-{key}",
            "qty": 2,
            "meta": {
                "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
                "cumulative_filled_qty": 2,
                "avg_cost": 10,
            },
        }
        for key in ("tp-key", "trend-key")
    ]
    orders = [
        {
            "symbol": "QXYZ",
            "side": "SELL",
            "client_order_key": key,
            "order_no": f"broker-{key}",
            "qty": 2,
            "qty_filled": 2,
            "status": "FILLED",
            "meta": {},
        }
        for key in ("tp-key", "trend-key")
    ]
    monkeypatch.setattr(repos, "load_today_fills", lambda _trade_date: fills)
    monkeypatch.setattr(repos, "load_us_daily_orders_for_report", lambda _trade_date: orders)
    monkeypatch.setattr(repos, "load_positions", lambda **_kwargs: [{
        "symbol": "QXYZ",
        "qty": 2,
        "position_lifecycle_id": "life-1",
        "meta": {"position_lifecycle_id": "life-1"},
    }])
    monkeypatch.setattr(repos, "load_latest_us_position_risk_state", lambda *_args: {
        "state": {
            "lifecycle": {"is_open": False},
            "trend": {"trend_trim_pending": True, "trend_trim_order_key": "trend-key"},
        },
    })
    monkeypatch.setattr(repos, "load_us_profit_capture_state", lambda *_args: {
        "QXYZ": {
            "tp1_pending": True,
            "tp1_order_key": "tp-key",
            "meta": {"tp1_qty": 2, "tp1_order_keys": ["tp-key"]},
        },
    })
    monkeypatch.setattr(
        "trader.us.execution.order_journal.load_order_events",
        lambda _trade_date: [],
    )
    monkeypatch.setattr(repos, "load_execution_claim_health", lambda: {
        "unresolved_execution_actions": 0,
        "execution_claim_conflicts": 0,
    })

    health = repos.load_broker_recovery_health("2026-10-01")

    assert health["stale_pending_exit_stage_after_fill_count"] == 2
    assert health["closed_lifecycle_open_state_count"] == 1


def test_recovery_health_sums_distinct_replacement_order_fills_without_duplicate_cumulative_count(
    monkeypatch,
):
    from trader.us.db import repos

    fills = [
        {
            "symbol": "QXYZ",
            "side": "SELL",
            "client_order_key": "tp-original",
            "order_no": "broker-original",
            "qty": 2,
            "meta": {
                "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
                "cumulative_filled_qty": 2,
                "avg_cost": 10,
            },
        },
        {
            "symbol": "QXYZ",
            "side": "SELL",
            "client_order_key": "tp-original",
            "order_no": "broker-original",
            "qty": 2,
            "meta": {
                "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
                "cumulative_filled_qty": 2,
                "avg_cost": 10,
            },
        },
        {
            "symbol": "QXYZ",
            "side": "SELL",
            "client_order_key": "tp-replacement",
            "order_no": "broker-replacement",
            "qty": 3,
            "meta": {
                "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
                "cumulative_filled_qty": 3,
                "avg_cost": 10,
            },
        },
    ]
    orders = [
        {
            "symbol": "QXYZ",
            "side": "SELL",
            "client_order_key": key,
            "order_no": f"broker-{key.removeprefix('tp-')}",
            "qty": qty,
            "qty_filled": qty,
            "status": "FILLED",
            "meta": {},
        }
        for key, qty in (("tp-original", 2), ("tp-replacement", 3))
    ]
    monkeypatch.setattr(repos, "load_today_fills", lambda _trade_date: fills)
    monkeypatch.setattr(repos, "load_us_daily_orders_for_report", lambda _trade_date: orders)
    monkeypatch.setattr(repos, "load_positions", lambda **_kwargs: [{
        "symbol": "QXYZ",
        "qty": 5,
        "position_lifecycle_id": "life-1",
        "meta": {"position_lifecycle_id": "life-1"},
    }])
    monkeypatch.setattr(repos, "load_latest_us_position_risk_state", lambda *_args: {
        "state": {"lifecycle": {"is_open": True}, "trend": {}},
    })
    monkeypatch.setattr(repos, "load_us_profit_capture_state", lambda *_args: {
        "QXYZ": {
            "tp1_pending": True,
            "tp1_order_key": "tp-replacement",
            "meta": {
                "tp1_qty": 5,
                "tp1_filled_qty": 2,
                "tp1_order_keys": ["tp-original", "tp-replacement", "tp-original"],
            },
        },
    })
    monkeypatch.setattr(
        "trader.us.execution.order_journal.load_order_events",
        lambda _trade_date: [],
    )
    monkeypatch.setattr(repos, "load_execution_claim_health", lambda: {
        "unresolved_execution_actions": 0,
        "execution_claim_conflicts": 0,
    })

    health = repos.load_broker_recovery_health("2026-10-01")

    assert health["stale_pending_exit_stage_after_fill_count"] == 1


def test_replacement_fill_completes_profit_capture_stage_and_close_health(
    monkeypatch,
):
    from trader.us.db import repos

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos.reset_memory_stores()
    trade_date = "2026-10-01"
    lifecycle = "life-replacement-fill"
    repos.mark_us_profit_capture_stage(
        trade_date,
        "QXYZ",
        "tp1",
        position_lifecycle_id=lifecycle,
        order_key="tp-original",
        qty=5,
        status="PARTIALLY_FILLED",
        filled_qty=2,
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
    )
    repos.mark_us_profit_capture_stage(
        trade_date,
        "QXYZ",
        "tp1",
        position_lifecycle_id=lifecycle,
        order_key="tp-replacement",
        qty=5,
        status="FILLED",
        filled_qty=3,
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
    )

    stage = repos.load_us_profit_capture_state(
        trade_date, ["QXYZ"], {"QXYZ": lifecycle}
    )["QXYZ"]
    assert stage["tp1_done"] is True
    assert stage["tp1_pending"] is False
    assert stage["meta"]["tp1_filled_qty"] == 5

    fills = [
        {
            "symbol": "QXYZ",
            "side": "SELL",
            "client_order_key": key,
            "order_no": order_no,
            "qty": qty,
            "meta": {
                "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
                "cumulative_filled_qty": qty,
                "avg_cost": 10,
            },
        }
        for key, order_no, qty in (
            ("tp-original", "broker-original", 2),
            ("tp-replacement", "broker-replacement", 3),
        )
    ]
    orders = [
        {
            "symbol": "QXYZ",
            "side": "SELL",
            "client_order_key": key,
            "order_no": order_no,
            "qty": qty,
            "qty_filled": qty,
            "status": "FILLED",
            "meta": {},
        }
        for key, order_no, qty in (
            ("tp-original", "broker-original", 2),
            ("tp-replacement", "broker-replacement", 3),
        )
    ]
    monkeypatch.setattr(repos, "load_today_fills", lambda _trade_date: fills)
    monkeypatch.setattr(repos, "load_us_daily_orders_for_report", lambda _trade_date: orders)
    monkeypatch.setattr(repos, "load_positions", lambda **_kwargs: [{
        "symbol": "QXYZ",
        "qty": 5,
        "position_lifecycle_id": lifecycle,
        "meta": {"position_lifecycle_id": lifecycle},
    }])
    monkeypatch.setattr(repos, "load_latest_us_position_risk_state", lambda *_args: {
        "state": {"lifecycle": {"is_open": True}, "trend": {}},
    })
    monkeypatch.setattr(
        "trader.us.execution.order_journal.load_order_events",
        lambda _trade_date: [],
    )
    monkeypatch.setattr(repos, "load_execution_claim_health", lambda: {
        "unresolved_execution_actions": 0,
        "execution_claim_conflicts": 0,
    })

    health = repos.load_broker_recovery_health(trade_date)

    assert health["stale_pending_exit_stage_after_fill_count"] == 0
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
