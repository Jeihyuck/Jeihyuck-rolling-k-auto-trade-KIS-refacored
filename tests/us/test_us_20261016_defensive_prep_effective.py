"""2026-10-13/16 defensive PREP incident — no broker/DB access."""
from pathlib import Path

import pytest

from trader.us.prep_effective import (
    is_effective_prep_status,
    is_effective_prep_contract,
    locked_rows_match_prep_run,
)

DATE = "2026-10-16"


def rows(run_id="defensive-prep", count=30):
    return [{"symbol": f"T{i}", "score_final": 80, "run_id": run_id} for i in range(count)]


def contract(status="DEFENSE_CRASH_ENTRY_BLOCKED", **overrides):
    return {
        "run_id": "defensive-prep",
        "trade_date": DATE,
        "status": status,
        "trade_can_proceed": 1,
        "contract_ok": True,
        "entry_can_proceed": 0,
        "exit_can_proceed": 1,
        "close_can_proceed": 1,
        "final30_scored_count": 30,
        "score_nonzero_count": 30,
        **overrides,
    }


@pytest.mark.parametrize("status", [
    "DEFENSE_CRASH_ENTRY_BLOCKED", "RISK_OFF_ENTRY_BLOCKED",
])
def test_defensive_prep_is_complete_but_not_buy_enabled(status):
    c = contract(status)
    assert is_effective_prep_status(status)
    assert is_effective_prep_contract(c, rows(), trade_date=DATE)
    assert c["entry_can_proceed"] == 0
    assert locked_rows_match_prep_run(c, rows())


@pytest.mark.parametrize("status", [
    "STARTED", "ERROR", "DEGRADED", "FAILED_CLUSTER_CAP_CONTRACT",
    "UNKNOWN_ENTRY_BLOCKED", "DEFENSE_CRASH_ENTRY_BLOCKED_ERROR",
])
def test_failure_and_unknown_blocked_statuses_remain_ineligible(status):
    assert not is_effective_prep_status(status)
    assert not is_effective_prep_contract(contract(status), rows(), trade_date=DATE)


def test_defense_must_not_hide_broken_exit_contract():
    assert not is_effective_prep_contract(
        contract(exit_can_proceed=0), rows(), trade_date=DATE
    )
    assert not is_effective_prep_contract(
        contract(contract_ok=False), rows(), trade_date=DATE
    )


def test_completed_contract_cannot_clear_exit_only_with_other_run_rows():
    assert not locked_rows_match_prep_run(contract(), rows("another-run"))
    assert not locked_rows_match_prep_run(contract(), rows(""))
    assert not is_effective_prep_contract(
        contract(score_nonzero_count=29), rows(), trade_date=DATE
    )


def test_preflight_requires_artifact_db_locked_watchlist_same_run_id():
    code = Path("scripts/wsl/check-us-prep-before-am.sh").read_text()
    assert "check_effective_prep_sources(trade_date)" in code
