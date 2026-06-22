from datetime import datetime
from zoneinfo import ZoneInfo


def test_get_live_gate_status_fresh_uses_passed_intraday_time(monkeypatch):
    monkeypatch.setenv("STRATEGY_MODE", "LIVE")
    monkeypatch.setenv("DRY_RUN", "0")
    from trader.config import get_live_gate_status_fresh

    gate = get_live_gate_status_fresh(
        now_kst=datetime(2026, 6, 22, 9, 0, 12, tzinfo=ZoneInfo("Asia/Seoul")),
        reason="test_order",
    )
    assert gate.window in {"intraday", "regular", "open", "morning", "day"}
    assert gate.reason != "WINDOW=preopen"
