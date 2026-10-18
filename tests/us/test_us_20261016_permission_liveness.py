"""2026-10-16 US protection SELL permission incident (offline/no orders)."""
from pathlib import Path

from scripts.us_order_permission_diagnostic import diagnose
from trader.us.runner.status_contract import is_permission_blocked_sell_route

ALLOWED = {
    "STRATEGY_ENV": "practice", "KIS_ENV": "practice",
    "DRY_RUN": "0", "DISABLE_LIVE_TRADING": "0",
    "DISABLE_REAL_TRADING": "0", "LIVE_TRADING_ENABLED": "1",
    "US_LIVE_TRADING_ENABLED": "1", "US_ORDER_ARMED": "1",
    "US_RUN_MODE": "TRADE", "US_AGENT_ENABLED": "1",
}


def test_oct16_permission_disabled_and_sanitized():
    env = {**ALLOWED, "US_LIVE_TRADING_ENABLED": "0",
           "KIS_US_APP_SECRET": "secret-never-print",
           "RUN_MODE": "TRADE"}
    result = diagnose("afternoon", env, "US_LIVE_TRADING_ENABLED=0\nKIS_US_APP_SECRET=secret-never-print")
    assert not result["permission_allowed"]
    assert "us_live_trading_disabled" in result["permission_reasons"]
    assert result["effective_safe_flags"]["US_LIVE_TRADING_ENABLED"] == "0"
    assert result["env_file_declares_flags"] == ["US_LIVE_TRADING_ENABLED"]
    assert "secret-never-print" not in str(result)
    assert not result["origin_verified"]


def test_am_and_afternoon_equal_order_policy():
    assert diagnose("am", ALLOWED)["permission_allowed"]
    assert diagnose("afternoon", ALLOWED)["permission_allowed"]


def test_explicit_stop_and_signal_only_never_auto_enabled():
    for updates in (
        {"US_LIVE_TRADING_ENABLED": "0"},
        {"US_ORDER_ARMED": "0"},
        {"DISABLE_LIVE_TRADING": "1"},
        {"DRY_RUN": "1"},
        {"US_SIGNAL_ONLY": "1"},
        {"US_RUN_MODE": "SIGNAL_ONLY"},
    ):
        assert not diagnose("am", {**ALLOWED, **updates})["permission_allowed"]


def test_wrapper_emits_diagnostic_without_changing_flags():
    for name in ("run-us-am.sh", "run-us-afternoon.sh"):
        wrapper = Path("scripts/wsl") / name
        src = wrapper.read_text(encoding="utf-8")
        assert "scripts/us_order_permission_diagnostic.py" in src
        assert "US_LIVE_TRADING_ENABLED=1" not in src
        assert 'US_LIVE_TRADING_ENABLED="${US_LIVE_TRADING_ENABLED:-1}"' in src


def test_sell_permission_denial_degrades_liveness_but_duplicate_does_not():
    denied = [{"status": "BLOCKED", "side": "SELL",
               "symbol": "META", "reason": "us_live_trading_disabled"}]
    assert is_permission_blocked_sell_route(denied)
    assert not is_permission_blocked_sell_route([
        {"status": "WARN_DUPLICATE_EXIT_BLOCKED",
         "side": "SELL", "reason": "pending_sell_order_exists"}
    ])
    assert not is_permission_blocked_sell_route([
        {"status": "BLOCKED", "side": "SELL", "reason": "dry_run_enabled"}
    ])
    assert "is_permission_blocked_sell_route(exit_route_orders)" in Path(
        "trader/us/runner/trade_tick_runner.py"
    ).read_text(encoding="utf-8")
