from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from trader import config


def test_force_run_dry_route_returns_diag(monkeypatch) -> None:
    monkeypatch.setenv("FORCE_RUN", "1")
    monkeypatch.setenv("DRY_RUN", "1")
    monkeypatch.setenv("FORCE_BLOCK_LIVE", "1")
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "0")

    mode, trading_day, window, source = config.resolve_strategy_mode(
        now_kst=datetime(2026, 4, 4, 18, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    assert mode == "DIAG"
    assert trading_day is True
    assert window == "day"
    assert source == "force_run_dry"