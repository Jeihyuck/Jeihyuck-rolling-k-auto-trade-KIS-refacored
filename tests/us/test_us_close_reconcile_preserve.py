from __future__ import annotations


def _patch_close_common(monkeypatch):
    class DummyProvider:
        def get_balance(self):
            return {"positions": []}

    monkeypatch.setattr("trader.us.data_provider.USDataProvider", lambda offline=False: DummyProvider())
    monkeypatch.setattr("trader.us.execution.fills.get_fills_today", lambda **kwargs: {"status": "OK", "fills": []})
    monkeypatch.setattr("trader.us.db.repos.save_fills", lambda fills: None)
    monkeypatch.setattr("trader.us.db.repos.save_reconcile_log", lambda payload: None)
    monkeypatch.setattr("trader.us.runner.daily_report_runner.run_daily_report", lambda **kwargs: None)


def test_close_does_not_save_empty_positions_when_reconcile_raises(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _patch_close_common(monkeypatch)
    saved = {"called": False, "positions": None}

    def fake_save_position_snapshot(positions):
        saved["called"] = True
        saved["positions"] = positions

    def fake_reconcile_positions(provider):
        raise RuntimeError("balance fetch failed")

    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_positions", fake_reconcile_positions)
    monkeypatch.setattr("trader.us.db.repos.save_position_snapshot", fake_save_position_snapshot)

    from trader.us.runner.trade_close_runner import run_trade_close

    result = run_trade_close(env="practice", offline=False, force_now="2026-06-22T16:05:00-04:00")

    assert saved["called"] is False
    assert result["reconcile_status"] == "TEMP_ERROR"
    assert result["status"] == "OK_WITH_WARNINGS"


def test_close_saves_authoritative_empty_positions(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _patch_close_common(monkeypatch)
    saved = {"called": False, "positions": None}

    def fake_save_position_snapshot(positions):
        saved["called"] = True
        saved["positions"] = positions

    def fake_reconcile_positions(provider):
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

    assert saved == {"called": True, "positions": []}
    assert result["reconcile_status"] == "OK"
