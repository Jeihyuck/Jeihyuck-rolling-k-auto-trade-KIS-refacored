"""Canonical WSL US paper startup must accept *authorized* Stage4 WORKER only."""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts/wsl/us-stage123-paper-defaults.sh"


def _source_profile(worker_mode: str | None, *, authorized: str = "1",
                    ledger_mode: str = "ENFORCE") -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.update({
        "KIS_ENV": "practice",
        "US_PAPER_TRADING_ENABLED": "1",
        "DRY_RUN": "0",
        "US_OFFLINE": "0",
        "US_SIGNAL_ONLY": "0",
        "US_RUN_MODE": "TRADE",
        "US_STAGE123_PAPER_PROFILE": "ON",
        "US_STAGE4_WRITER_AUTHORIZED": authorized,
        "US_PROTECTIVE_STOP_LEDGER_MODE": ledger_mode,
        "US_ATOMIC_CLAIM_OUTBOX_ENABLED": "1",
        "US_ACCOUNT_RESERVATION_MODE": "ENFORCE",
        "US_SHARED_KIS_LIMITER_ENABLED": "1",
        "US_EXIT_QUOTE_PREFETCH_ENABLED": "1",
        "US_EARLY_HARD_STOP_PILOT_ENABLED": "1",
        "US_EARLY_MULTI_HARD_STOP_ENABLED": "1",
        "US_EARLY_SOFT_STOP_PILOT_ENABLED": "1",
    })
    if worker_mode is None:
        env.pop("US_RECONCILE_WORKER_MODE", None)
    else:
        env["US_RECONCILE_WORKER_MODE"] = worker_mode
    return subprocess.run(
        ["bash", "-c", 'source "$1"', "test-stage4-profile", str(SCRIPT)],
        env=env, capture_output=True, text=True, check=False,
    )


def test_stage4_worker_profile_accepted_only_when_explicitly_authorized():
    result = _source_profile("WORKER")
    assert result.returncode == 0, result.stderr
    assert "worker=WORKER" in result.stdout


def test_stage4_worker_profile_refuses_missing_writer_authorization():
    result = _source_profile("WORKER", authorized="0")
    assert result.returncode == 43
    assert "[US_STAGE123][CONFIG_ERROR]" in result.stderr


def test_stage4_worker_profile_refuses_protective_ledger_shadow():
    result = _source_profile("WORKER", ledger_mode="SHADOW")
    assert result.returncode == 43


def test_default_reservation_only_mode_remains_unchanged():
    result = _source_profile(None)
    assert result.returncode == 0, result.stderr
    assert "worker=RESERVATION_ONLY" in result.stdout
