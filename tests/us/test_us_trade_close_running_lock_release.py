from __future__ import annotations


def test_close_running_lock_released_when_save_fills_raises(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    class DummyProvider:
        def get_balance(self):
            return {"positions": []}

    monkeypatch.setattr("trader.us.data_provider.USDataProvider", lambda offline=False: DummyProvider())
    monkeypatch.setattr("trader.us.db.repos.save_fills", lambda fills: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr("trader.us.db.repos.save_position_snapshot", lambda positions: None)
    monkeypatch.setattr("trader.us.db.repos.save_reconcile_log", lambda payload: None)
    monkeypatch.setattr("trader.us.runner.daily_report_runner.run_daily_report", lambda **kwargs: None)

    from trader.us.runner.trade_close_runner import run_trade_close

    result = run_trade_close(env="practice", offline=True, force_now="2026-06-22T16:05:00-04:00")

    running = tmp_path / "runtime/session_guard/us/2026-06-22/close.running"
    assert result["status"] == "OK"
    assert not running.exists()
