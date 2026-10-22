"""Stage123 Windows-owned WSL defaults, no broker HTTP or orders."""
import os
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path("scripts/wsl/us-stage123-paper-defaults.sh")


def _source(env):
    base = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), **env}
    proc = subprocess.run(
        ["bash", "-c", 'source "$1" || exit $?; echo "RESULT:${US_STAGE123_PAPER_PROFILE}:${US_ATOMIC_CLAIM_OUTBOX_ENABLED:-unset}:${US_ACCOUNT_RESERVATION_MODE:-unset}:${US_RECONCILE_WORKER_MODE:-unset}:${US_SHARED_KIS_LIMITER_ENABLED:-unset}:${US_EXIT_QUOTE_PREFETCH_ENABLED:-unset}:${US_EARLY_HARD_STOP_PILOT_ENABLED:-unset}:${US_EARLY_MULTI_HARD_STOP_ENABLED:-unset}:${US_EARLY_SOFT_STOP_PILOT_ENABLED:-unset}"', "shell", str(SCRIPT)],
        env=base, capture_output=True, text=True,
    )
    return proc


PAPER = {
    "KIS_ENV": "practice", "US_PAPER_TRADING_ENABLED": "1",
    "DRY_RUN": "0", "US_RUN_MODE": "TRADE",
}


def test_canonical_paper_trade_defaults_stage123_all_supported_flags_on():
    result = _source(PAPER)
    assert result.returncode == 0, result.stderr
    assert "RESULT:ON:1:ENFORCE:RESERVATION_ONLY:1:1:1:1:1" in result.stdout


@pytest.mark.parametrize("override", [
    {"KIS_ENV": "live"}, {"DRY_RUN": "1"},
    {"US_OFFLINE": "1"}, {"US_SIGNAL_ONLY": "1"},
    {"US_PAPER_TRADING_ENABLED": "0"}, {"US_RUN_MODE": "NON_TRADING_SIGNAL_ONLY"},
])
def test_no_auto_activation_for_nonpractice_or_dry_run(override):
    result = _source({**PAPER, **override})
    assert result.returncode == 0, result.stderr
    assert "RESULT:OFF:" in result.stdout


def test_explicit_all_off_override_fences_paper_profile():
    result = _source({**PAPER, "US_STAGE123_PAPER_PROFILE": "OFF"})
    assert result.returncode == 0, result.stderr
    assert "RESULT:OFF:0:OFF:LEGACY:0:0:0:0:0" in result.stdout


def test_partial_unsafe_flags_never_silently_mix_with_paper_on():
    result = _source({**PAPER, "US_SHARED_KIS_LIMITER_ENABLED": "0"})
    assert result.returncode == 43
    assert "CONFIG_ERROR" in result.stderr


def test_manual_on_profile_cannot_arm_real_broker():
    result = _source({**PAPER, "KIS_ENV": "live", "US_STAGE123_PAPER_PROFILE": "ON"})
    assert result.returncode == 43
    assert "practice" in result.stderr


def test_missing_ddl_preflight_fails_closed_without_postgres_connection():
    from scripts.us_stage123_paper_preflight import validate_stage123_prerequisites
    from sqlalchemy import create_engine
    with pytest.raises(RuntimeError, match="postgres_required"):
        validate_stage123_prerequisites(
            engine=create_engine("sqlite:///:memory:"),
            scope={"env": "practice", "account_id": "a" * 64, "trading_epoch_id": "e"},
            env={
                **PAPER,
                "US_STAGE123_PAPER_PROFILE": "ON",
                "US_ATOMIC_CLAIM_OUTBOX_ENABLED": "1",
                "US_ACCOUNT_RESERVATION_MODE": "ENFORCE",
                "US_RECONCILE_WORKER_MODE": "RESERVATION_ONLY",
                "US_SHARED_KIS_LIMITER_ENABLED": "1",
                "US_EXIT_QUOTE_PREFETCH_ENABLED": "1",
                "US_EARLY_HARD_STOP_PILOT_ENABLED": "1",
                "US_EARLY_MULTI_HARD_STOP_ENABLED": "1",
                "US_EARLY_SOFT_STOP_PILOT_ENABLED": "1",
            },
        )
