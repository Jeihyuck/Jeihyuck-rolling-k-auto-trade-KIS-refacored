from pathlib import Path

import pytest

from trader.us.prep_effective import is_effective_prep_contract, is_effective_prep_status


@pytest.mark.parametrize(
    "status",
    [
        "OK",
        "OK_WITH_WARNINGS",
        "OK_WITH_WARNINGS_ENTRY_BLOCKED_CLUSTER_CAP",
        "OK_WITH_WARNINGS_CLUSTER_INCOMPLETE",
        "OK_WITH_WARNINGS_ENTRY_BLOCKED_UNDERFILLED",
    ],
)
def test_completed_ok_status_family_is_effective(status):
    assert is_effective_prep_status(status) is True


@pytest.mark.parametrize("status", ["", "STARTED", "ERROR", "DEGRADED", "FAILED_CLUSTER_CAP_CONTRACT"])
def test_non_completed_or_fatal_status_is_not_effective(status):
    assert is_effective_prep_status(status) is False


def _contract(status="OK_WITH_WARNINGS_ENTRY_BLOCKED_CLUSTER_CAP", **extra):
    return {
        "trade_date": "2026-09-29",
        "status": status,
        "contract_ok": True,
        "trade_can_proceed": 1,
        "entry_can_proceed": 0,
        "exit_can_proceed": 1,
        "close_can_proceed": 1,
        "final30_scored_count": 28,
        "score_nonzero_count": 28,
        **extra,
    }


def test_entry_blocked_warning_contract_still_prevents_recovery_rerun():
    rows = [{"symbol": f"S{i}", "score_final": 1.0} for i in range(28)]
    assert is_effective_prep_contract(
        _contract(), rows, trade_date="2026-09-29", min_rows=10
    ) is True


def test_effective_contract_rejects_score_or_date_mismatch():
    rows = [{"symbol": f"S{i}", "score_final": 1.0} for i in range(28)]
    assert not is_effective_prep_contract(
        _contract(score_nonzero_count=27), rows, trade_date="2026-09-29"
    )
    assert not is_effective_prep_contract(
        _contract(trade_date="2026-09-28"), rows, trade_date="2026-09-29"
    )


def test_recovery_and_preflight_use_shared_effective_contract_helper():
    recovery = Path("scripts/wsl/run-us-prep-recovery.sh").read_text(encoding="utf-8")
    preflight = Path("scripts/wsl/check-us-prep-before-am.sh").read_text(encoding="utf-8")
    prep = Path("scripts/wsl/run-us-prep.sh").read_text(encoding="utf-8")
    assert "is_effective_prep_contract" in recovery
    assert "is_effective_prep_contract" in preflight
    assert "is_effective_prep_contract" in prep
    assert "status in {\"OK\", \"OK_WITH_WARNINGS\"}" not in recovery
    assert "status in {'OK', 'OK_WITH_WARNINGS'}" not in preflight
    assert "exec bash scripts/wsl/run-us-prep.sh" in recovery
    assert "CLEAR_STALE_EXIT_ONLY" in prep
