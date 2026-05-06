# -*- coding: utf-8 -*-
"""US trade close contract tests."""

from pathlib import Path


def test_trade_close_does_not_mask_fills_contract_error_as_ok():
    """Close가 fills contract error를 OK로 덮지 않는지 확인."""
    text = Path("trader/us/runner/trade_close_runner.py").read_text(encoding="utf-8")
    assert "fills_status" in text
    assert "CONTRACT_ERROR" in text
    assert "final_status=ERROR" in text or 'status = "ERROR"' in text


def test_trade_close_tracks_fills_status():
    """Close가 fills status를 추적하는지 확인."""
    text = Path("trader/us/runner/trade_close_runner.py").read_text(encoding="utf-8")
    assert "fills_status" in text
    assert "fills_error" in text


def test_trade_close_returns_error_on_contract_error(monkeypatch):
    """fills CONTRACT_ERROR 시 status=ERROR 반환하는지 확인."""
    def fake_get_fills_today(**kwargs):
        return {
            "status": "CONTRACT_ERROR",
            "error": "all schemas failed",
            "fills": [],
        }
    
    def fake_reconcile_positions(**kwargs):
        return {"status": "OK", "positions": []}
    
    def fake_run_daily_report(**kwargs):
        pass
    
    monkeypatch.setattr(
        "trader.us.execution.fills.get_fills_today",
        fake_get_fills_today,
    )
    monkeypatch.setattr(
        "trader.us.execution.reconcile.reconcile_positions",
        fake_reconcile_positions,
    )
    monkeypatch.setattr(
        "trader.us.runner.daily_report_runner.run_daily_report",
        fake_run_daily_report,
    )
    
    # Fake repos
    def fake_save(*args, **kwargs):
        pass
    
    monkeypatch.setattr("trader.us.db.repos.save_fills", fake_save)
    monkeypatch.setattr("trader.us.db.repos.save_position_snapshot", fake_save)
    monkeypatch.setattr("trader.us.db.repos.save_reconcile_log", fake_save)
    
    from trader.us.runner.trade_close_runner import run_trade_close
    result = run_trade_close(env="practice", offline=False)
    
    assert result["status"] == "ERROR"
    assert result["fills_status"] == "CONTRACT_ERROR"
