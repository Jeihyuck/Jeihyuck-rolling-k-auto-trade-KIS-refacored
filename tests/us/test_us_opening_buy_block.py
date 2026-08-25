from datetime import datetime
from zoneinfo import ZoneInfo

from trader.us.runner.trade_tick_runner import _is_us_opening_buy_blocked


def test_us_buy_block_boundaries(monkeypatch):
    monkeypatch.setenv("US_OPENING_BUY_BLOCK_ENABLED", "1")
    tz = ZoneInfo("America/New_York")
    assert _is_us_opening_buy_blocked(datetime(2026, 8, 24, 9, 45, tzinfo=tz)) == (True, "10:00:00")
    assert _is_us_opening_buy_blocked(datetime(2026, 8, 24, 10, 0, tzinfo=tz))[0] is False


def test_us_opening_gate_does_not_change_exit_permission():
    blocked, _ = _is_us_opening_buy_blocked(
        datetime(2026, 8, 24, 9, 45, tzinfo=ZoneInfo("America/New_York"))
    )
    entry_can_proceed, exit_can_proceed = not blocked, True
    assert entry_can_proceed is False and exit_can_proceed is True


def test_us_tick_opening_gate_prevents_entry_routing_but_keeps_exit_enabled():
    from tests.us.test_us_trade_tick_runner import _setup_env
    from trader.us.runner.trade_tick_runner import run_trade_tick

    _setup_env()
    result = run_trade_tick(
        session="am", env="practice", offline=True,
        force_now="2026-01-02T09:45:00-05:00",
    )

    assert result["opening_buy_blocked"] is True
    assert result["entry_can_proceed"] is False
    assert result["entry_intents"] == 0
    assert result["exit_can_proceed"] is True


def test_us_buy_start_env_takes_precedence(monkeypatch):
    tz = ZoneInfo("America/New_York")
    monkeypatch.setenv("US_BUY_START_ET", "10:15")
    assert _is_us_opening_buy_blocked(datetime(2026, 8, 24, 10, 5, tzinfo=tz)) == (True, "10:15:00")


def test_us_tick_passes_opening_permissions_to_tqqq_sleeve(monkeypatch):
    from tests.us.test_us_trade_tick_runner import _setup_env
    from trader.us.runner.trade_tick_runner import run_trade_tick

    _setup_env()
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    captured: dict = {}

    def fake_run_sleeve(**kwargs):
        captured.update(kwargs["overlay"])
        return {"status": "WAIT", "orders": []}

    monkeypatch.setattr("trader.us.infinite.integration.run_sleeve", fake_run_sleeve)
    run_trade_tick(
        session="am", env="practice", offline=True,
        force_now="2026-08-24T09:45:00-04:00",
    )

    assert captured["opening_buy_blocked"] is True
    assert captured["opening_buy_start_et"] == "10:00:00"
    assert captured["entry_can_proceed"] is False
    assert captured["exit_can_proceed"] is True
    assert captured["now_et"] == "2026-08-24T09:45:00-04:00"
