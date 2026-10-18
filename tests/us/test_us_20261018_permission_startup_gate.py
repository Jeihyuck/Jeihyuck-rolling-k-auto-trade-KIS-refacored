"""No-network AM/PM startup permission fail-fast contract."""
from pathlib import Path
import pytest

from scripts.us_order_permission_diagnostic import diagnose, classify_trade_start

ENABLED = {
    "STRATEGY_ENV": "practice", "KIS_ENV": "practice",
    "DRY_RUN": "0", "DISABLE_LIVE_TRADING": "0",
    "DISABLE_REAL_TRADING": "0", "LIVE_TRADING_ENABLED": "1",
    "US_LIVE_TRADING_ENABLED": "1", "US_ORDER_ARMED": "1",
    "US_RUN_MODE": "TRADE",
}


@pytest.mark.parametrize("session", ["am", "afternoon"])
def test_trade_start_fails_closed_when_us_live_is_disabled(session):
    env = {**ENABLED, "US_LIVE_TRADING_ENABLED": "0"}
    result = classify_trade_start(env, diagnose(session, env))
    assert result["decision"] == "TRADE_START_BLOCKED"
    assert "us_live_trading_disabled" in result["permission_reasons"]
    assert env["US_LIVE_TRADING_ENABLED"] == "0"


@pytest.mark.parametrize("patch", [
    {"DRY_RUN": "1"},
    {"US_SIGNAL_ONLY": "1"},
    {"US_RUN_MODE": "SIGNAL_ONLY"},
    {"US_OFFLINE": "1"},
])
def test_explicit_nontrading_session_keeps_running(patch):
    env = {**ENABLED, **patch}
    assert classify_trade_start(env, diagnose("am", env))["decision"] == "OBSERVATION_MODE"


@pytest.mark.parametrize("patch", [
    {"US_ORDER_ARMED": "0"},
    {"DISABLE_LIVE_TRADING": "1"},
    {"LIVE_TRADING_ENABLED": "0"},
    {"US_KIS_ORDER_ALLOWED": "0"},
])
def test_unarmed_or_explicit_stop_is_never_overridden(patch):
    env = {**ENABLED, **patch}
    result = classify_trade_start(env, diagnose("afternoon", env))
    assert result["decision"] == "TRADE_START_BLOCKED"


def test_approved_practice_trade_starts_in_both_sessions():
    for session in ("am", "afternoon"):
        env = dict(ENABLED)
        assert classify_trade_start(env, diagnose(session, env))["decision"] == "TRADE_START_ALLOWED"


def test_both_wrappers_run_prestart_gate_before_session_command():
    for name in ("run-us-am.sh", "run-us-afternoon.sh"):
        script = (Path("scripts/wsl") / name).read_text(encoding="utf-8")
        assert "--check-trade-start" in script
        assert script.index("--check-trade-start") < script.index('"${cmd[@]}" >> "$LOG_FILE" 2>&1')
        assert "FAILED_ORDER_PERMISSION" in script
        assert "US_LIVE_TRADING_ENABLED=\"1\"" not in script
