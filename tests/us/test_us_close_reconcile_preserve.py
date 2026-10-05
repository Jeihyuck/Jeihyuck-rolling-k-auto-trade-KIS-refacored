from __future__ import annotations


def _patch_close_common(monkeypatch):
    class DummyProvider:
        def get_balance(self):
            return {"positions": []}

    monkeypatch.setattr("trader.us.data_provider.USDataProvider", lambda offline=False: DummyProvider())
    monkeypatch.setattr("trader.us.execution.fills.get_fills_today", lambda **kwargs: {"status": "OK", "fills": []})
    monkeypatch.setattr("trader.us.db.repos.save_fills_with_result", lambda fills, trade_date=None: {"status":"OK","inserted_count":0,"updated_count":0,"unchanged_count":0,"regression_count":0})
    monkeypatch.setattr("trader.us.db.repos.save_reconcile_log", lambda payload, trade_date=None: None)
    monkeypatch.setattr("trader.us.db.repos.load_broker_recovery_health", lambda trade_date: {
        "available": True,
        "recovery_health_error_count": 0,
        "unattributed_broker_fills": 0,
        "broker_fill_rebound_failure_count": 0,
        "broker_local_cumulative_fill_mismatch_count": 0,
        "unresolved_execution_actions": 0,
        "filled_sell_missing_cost_basis_count": 0,
    })
    monkeypatch.setattr(
        "trader.us.execution.order_journal.replay_order_journal",
        lambda *args, **kwargs: {"status": "OK", "unresolved_count": 0},
    )
    monkeypatch.setattr(
        "trader.us.execution.reconcile.reconcile_ack_orders_with_balance",
        lambda **kwargs: {"status": "OK", "unresolved_count": 0, "failed_count": 0},
    )
    monkeypatch.setattr(
        "trader.us.execution.reconcile.classify_ack_orders_with_final_balance",
        lambda **kwargs: {"status": "OK", "orders": [], "counts": {}, "pending_order_count": 0},
    )
    monkeypatch.setattr("trader.us.runner.daily_report_runner.run_daily_report", lambda **kwargs: None)


def test_close_does_not_save_empty_positions_when_reconcile_raises(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _patch_close_common(monkeypatch)
    saved = {"called": False, "positions": None}

    def fake_save_position_snapshot(positions, **kwargs):
        saved["called"] = True
        saved["positions"] = positions

    def fake_reconcile_positions(provider, trade_date=None):
        raise RuntimeError("balance fetch failed")

    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_positions", fake_reconcile_positions)
    monkeypatch.setattr("trader.us.db.repos.save_position_snapshot", fake_save_position_snapshot)

    from trader.us.runner.trade_close_runner import run_trade_close

    result = run_trade_close(env="practice", offline=False, force_now="2026-06-22T16:05:00-04:00")

    assert saved["called"] is False
    assert result["reconcile_status"] == "TEMP_ERROR"
    assert result["status"] == "ERROR"
    assert result["manual_reconcile_required"] is True
    assert result["reconcile_required"] is True
    assert result["report_consistency"] == "FAILED"
    assert result["daily_report_status"] == "FAILED_RECONCILE"


def test_close_saves_authoritative_empty_positions(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _patch_close_common(monkeypatch)
    saved = {"called": False, "positions": None, "trade_date": None}

    def fake_save_position_snapshot(positions, **kwargs):
        saved["called"] = True
        saved["positions"] = positions
        saved["trade_date"] = kwargs.get("trade_date")

    def fake_reconcile_positions(provider, trade_date=None):
        return {
            "status": "OK",
            "balance_fetch_status": "OK",
            "authoritative_positions": True,
            "preserve_previous_positions": False,
            "positions": [],
            "position_count": 0,
        }

    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_positions", fake_reconcile_positions)
    monkeypatch.setattr("trader.us.db.repos.save_position_snapshot", fake_save_position_snapshot)

    from trader.us.runner.trade_close_runner import run_trade_close

    result = run_trade_close(env="practice", offline=False, force_now="2026-06-22T16:05:00-04:00")

    assert saved == {"called": True, "positions": [], "trade_date": "2026-06-22"}
    assert result["reconcile_status"] == "OK"
