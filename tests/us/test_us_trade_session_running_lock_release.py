from __future__ import annotations

import json
from datetime import datetime
from zoneinfo import ZoneInfo


def _ny_dt() -> datetime:
    return datetime(2026, 6, 22, 9, 35, tzinfo=ZoneInfo("America/New_York"))


def test_session_file_guard_skip_releases_running_lock(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    done = tmp_path / "runtime/session_guard/us/2026-06-22/am.done"
    done.parent.mkdir(parents=True)
    done.write_text(json.dumps({"status": "OK", "ticks": 1}), encoding="utf-8")

    import trader.us.runner.trade_session_runner as m
    monkeypatch.setattr(m, "_now_ny", lambda force_now=None: _ny_dt())

    result = m.run_trade_session(session="am", env="practice", offline=False, max_ticks=0)

    running = tmp_path / "runtime/session_guard/us/2026-06-22/am.running"
    assert result["status"] == "SKIP"
    assert not running.exists()


def test_session_prep_guard_block_releases_running_lock(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    import trader.us.runner.trade_session_runner as m

    monkeypatch.setattr(m, "_now_ny", lambda force_now=None: _ny_dt())
    monkeypatch.setattr("trader.us.market_calendar.is_us_trading_day", lambda d: True)
    monkeypatch.setattr(
        "trader.us.prep_contract.check_us_prep_guard",
        lambda trade_date: {"ok": False, "reason": "test_block"},
    )

    result = m.run_trade_session(session="am", env="practice", offline=False, max_ticks=0)

    running = tmp_path / "runtime/session_guard/us/2026-06-22/am.running"
    assert result["status"] == "FAILED_PREP_GUARD"
    assert not running.exists()


def test_session_tick_exception_releases_running_lock(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    import trader.us.runner.trade_session_runner as m

    monkeypatch.setattr(m, "_now_ny", lambda force_now=None: _ny_dt())
    monkeypatch.setattr("trader.us.market_calendar.is_us_trading_day", lambda d: True)
    monkeypatch.setattr("trader.us.runner.trade_tick_runner.run_trade_tick", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("tick boom")))

    result = m.run_trade_session(
        session="am",
        env="practice",
        offline=True,
        force_now="2026-06-22T09:35:00-04:00",
        max_ticks=1,
        interval_sec=1,
    )

    running = tmp_path / "runtime/session_guard/us/2026-06-22/am.running"
    assert result["status"] in {"FAILED", "ERROR", "OK_WITH_WARNINGS"}
    assert not running.exists()
