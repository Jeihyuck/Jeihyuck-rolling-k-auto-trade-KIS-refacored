"""Cross-process KIS shared pacing contract (no KIS requests)."""
from concurrent.futures import ThreadPoolExecutor
import time

import pytest

from trader.us.execution.shared_kis_limiter import (
    SharedLimiterUnavailable, wait_for_shared_slot,
)


def _budget_sleep(delay):
    time.sleep(delay)
    return True


def test_shared_pacing_serializes_same_account_across_client_instances(tmp_path):
    scope = "hashed-synthetic-account"
    def reserve_slot(_):
        wait_for_shared_slot(
            scope, min_interval_sec=0.085,
            root=str(tmp_path), sleep_with_budget=_budget_sleep,
        )
        return time.monotonic()
    with ThreadPoolExecutor(max_workers=3) as executor:
        completed = list(executor.map(reserve_slot, range(3)))
    ordered = sorted(completed)
    # File lock reservation and monotonic times must serialize all callers.
    assert ordered[1] - ordered[0] >= 0.060, ordered
    assert ordered[2] - ordered[1] >= 0.060, ordered
    files = list(tmp_path.glob("*.slot"))
    assert len(files) == 1
    assert "hashed-synthetic-account" not in files[0].name


def test_shared_pacing_fails_closed_when_deadline_budget_exhausted(tmp_path):
    wait_for_shared_slot(
        "budget-test-account", min_interval_sec=1.0,
        root=str(tmp_path), sleep_with_budget=_budget_sleep,
    )
    with pytest.raises(SharedLimiterUnavailable, match="budget_exceeded"):
        wait_for_shared_slot(
            "budget-test-account", min_interval_sec=1.0,
            root=str(tmp_path), sleep_with_budget=lambda _: False,
        )


def test_shared_pacing_requires_valid_scope_and_interval(tmp_path):
    with pytest.raises(SharedLimiterUnavailable, match="scope_missing"):
        wait_for_shared_slot(
            "", min_interval_sec=1.0,
            root=str(tmp_path), sleep_with_budget=_budget_sleep,
        )
    with pytest.raises(SharedLimiterUnavailable, match="interval_invalid"):
        wait_for_shared_slot(
            "test", min_interval_sec=0,
            root=str(tmp_path), sleep_with_budget=_budget_sleep,
        )
