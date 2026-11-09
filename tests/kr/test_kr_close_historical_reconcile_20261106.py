"""CLOSE isolated full recovery must stay outside AM/PM SELL-first lane."""
from datetime import datetime
from zoneinfo import ZoneInfo


def test_close_owner_invokes_original_full_broker_recovery_once(monkeypatch):
    import trader.reconcile_kis as reconcile
    from trader.kr.close_historical_reconcile import reconcile_close_historical_once

    observed = []
    def fake_reconcile(**kwargs):
        observed.append(kwargs)
        return {"ok": True, "historical_recovery_executed": True}

    monkeypatch.setattr(reconcile, "reconcile_kis", fake_reconcile)
    stamp = datetime(2026, 11, 6, 15, 35, tzinfo=ZoneInfo("Asia/Seoul"))
    result = reconcile_close_historical_once(
        engine="engine", kis="kis", env="practice", run_id="close-run",
        tick_ts=stamp, runtime_root_dir="/tmp/kr-close",
    )
    assert result["historical_recovery_executed"] is True
    assert len(observed) == 1
    assert observed[0]["strategy"] == "pb1_pullback_close"
    assert observed[0]["historical_recovery"] is True
    assert observed[0]["tick_ts"] == stamp
