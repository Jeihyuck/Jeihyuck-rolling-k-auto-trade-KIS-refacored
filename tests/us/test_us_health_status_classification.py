import json
from trader.us.runner.trade_session_runner import _write_us_schedule_health


def test_health_is_failed_for_fill_persistence_failure(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _write_us_schedule_health({"trade_date": "2026-07-20", "final_status": "FAILED", "reason": "fill_persistence_failed"}, "am")
    health = json.loads((tmp_path / "runtime/health/us-2026-07-20.json").read_text())
    assert health["ok"] is False
    assert health["status"] == "FAILED"
    assert health["reason"] == "fill_persistence_failed"


def test_health_rejects_broker_fills_that_were_not_persisted(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _write_us_schedule_health({"trade_date": "2026-07-20", "final_status": "OK", "broker_fills_fetched": 8, "fills_count": 0}, "close")
    health = json.loads((tmp_path / "runtime/health/us-2026-07-20.json").read_text())
    assert health["ok"] is False
    assert health["reason"] == "broker_fills_not_persisted"
