"""Practice-only ON defaults must reach AM, afternoon, observer and broker boundary."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess

from trader.us.entry_timing.opportunity import effective_mode

ROOT = Path(__file__).resolve().parents[2]
PROFILE = ROOT / "scripts/wsl/us-entry-timing-paper-defaults.sh"
VARS = (
    "US_ENTRY_OPPORTUNITY_MODE", "US_ENTRY_OPPORTUNITY_APPROVED",
    "US_OPENING_OBSERVER_ENABLED", "US_OPENING_OBSERVER_BAR_FETCH_ENABLED",
    "US_WS_COLLECTOR_ENABLED",
)


def _source_profile(overrides=None):
    # An isolated shell reproduces the WSL default-injection semantics, without
    # executing a trading script, obtaining secrets or connecting to a broker.
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "KIS_ENV": "practice", "US_PAPER_TRADING_ENABLED": "1",
        "DRY_RUN": "0", "US_RUN_MODE": "TRADE",
    }
    env.update(overrides or {})
    statement = (
        'source "$1"; for k in US_ENTRY_OPPORTUNITY_MODE '
        'US_ENTRY_OPPORTUNITY_APPROVED US_OPENING_OBSERVER_ENABLED '
        'US_OPENING_OBSERVER_BAR_FETCH_ENABLED US_WS_COLLECTOR_ENABLED; '
        'do eval "printf \'%s\\n\' \\\"\\$\\{\\$k-UNSET\\}\\\""; done'
    )
    # Use bash indirect expansion rather than eval when reading the sourced flags.
    statement = (
        'source "$1"; for k in US_ENTRY_OPPORTUNITY_MODE '
        'US_ENTRY_OPPORTUNITY_APPROVED US_OPENING_OBSERVER_ENABLED '
        'US_OPENING_OBSERVER_BAR_FETCH_ENABLED US_WS_COLLECTOR_ENABLED; '
        'do printf "%s\\n" "${!k-UNSET}"; done'
    )
    result = subprocess.run(
        ["bash", "-c", statement, "bash", str(PROFILE)],
        env=env, capture_output=True, text=True, check=True,
    )
    values = result.stdout.strip().splitlines()[-len(VARS):]
    return dict(zip(VARS, values)), result


def test_canonical_wsl_trade_has_active_not_shadow_timing():
    flags, result = _source_profile()
    assert flags == {
        "US_ENTRY_OPPORTUNITY_MODE": "ENFORCE",
        "US_ENTRY_OPPORTUNITY_APPROVED": "1",
        "US_OPENING_OBSERVER_ENABLED": "1",
        "US_OPENING_OBSERVER_BAR_FETCH_ENABLED": "1",
        "US_WS_COLLECTOR_ENABLED": "1",
    }
    assert effective_mode({"KIS_ENV": "practice", **flags}) == "ENFORCE"
    assert "[US_ENTRY_TIMING][PROFILE]" in result.stdout


def test_operator_override_not_silently_promoted_or_hidden():
    flags, result = _source_profile({"US_ENTRY_OPPORTUNITY_MODE": "SHADOW"})
    assert flags["US_ENTRY_OPPORTUNITY_MODE"] == "SHADOW"
    assert "[US_ENTRY_TIMING][OVERRIDE]" in result.stderr
    flags, result = _source_profile({"US_ENTRY_OPPORTUNITY_APPROVED": "0"})
    assert flags["US_ENTRY_OPPORTUNITY_APPROVED"] == "0"
    assert effective_mode({"KIS_ENV": "practice", **flags}) == "ENFORCE_NOT_APPROVED"
    assert "[US_ENTRY_TIMING][OVERRIDE]" in result.stderr


def test_nontrading_and_real_environment_never_auto_enforce():
    cases = (
        {"KIS_ENV": "real"},
        {"DRY_RUN": "1"},
        {"US_OFFLINE": "1"},
        {"US_SIGNAL_ONLY": "1"},
        {"US_RUN_MODE": "SIGNAL_ONLY"},
        {"US_PAPER_TRADING_ENABLED": "0"},
    )
    for case in cases:
        flags, _ = _source_profile(case)
        assert all(value == "UNSET" for value in flags.values()), case


def test_explicit_off_survives_and_nonpractice_cannot_authorize_buy():
    flags, _ = _source_profile({
        "US_ENTRY_OPPORTUNITY_MODE": "OFF",
        "US_OPENING_OBSERVER_ENABLED": "0",
    })
    assert flags["US_ENTRY_OPPORTUNITY_MODE"] == "OFF"
    assert flags["US_OPENING_OBSERVER_ENABLED"] == "0"
    assert effective_mode({"KIS_ENV": "practice", **flags}) == "OFF"
    assert effective_mode({
        "KIS_ENV": "real", "US_ENTRY_OPPORTUNITY_MODE": "ENFORCE",
        "US_ENTRY_OPPORTUNITY_APPROVED": "1",
    }) == "ENFORCE_NOT_APPROVED"


def test_both_scheduled_wsl_sessions_load_identical_profile():
    for script in ("run-us-am.sh", "run-us-afternoon.sh"):
        source = (ROOT / "scripts/wsl" / script).read_text(encoding="utf-8")
        assert "source scripts/wsl/us-stage123-paper-defaults.sh" in source
        assert "source scripts/wsl/us-entry-timing-paper-defaults.sh" in source
        assert source.index("source scripts/wsl/us-stage123-paper-defaults.sh") < source.index(
            "source scripts/wsl/us-entry-timing-paper-defaults.sh"
        )
