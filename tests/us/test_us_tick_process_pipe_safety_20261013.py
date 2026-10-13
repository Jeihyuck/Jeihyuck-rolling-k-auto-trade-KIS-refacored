"""PR-C: process IPC and broker-submission-uncertainty invariants."""
from __future__ import annotations

import os
import time

import pytest

from trader.us.runner.tick_process import run_tick_in_process, TickProcessTimeout


def _large_result(tick_cancellation_event=None):
    return {"status": "OK", "payload": "X" * 4_000_000}


def _wait_after_submit_marker(marker, tick_cancellation_event=None):
    with open(marker, "w", encoding="utf-8") as handle:
        handle.write("BROKER_SUBMIT_STARTED;ACK_UNKNOWN")
    while True:
        time.sleep(0.02)


def _small_result(tick_cancellation_event=None):
    return {"status": "OK", "qty": 1}


def _eligibility_and_mock_ack(tick, tick_cancellation_event=None):
    """Test broker-free strict signal→risk→route→ACK boundary for 12 ticks."""
    signal = {"family": "MOMENTUM", "eligible": True, "score": 82}
    risk = {
        "cash_confirmed": 5000, "position_count": 0,
        "max_positions": 5, "duplicate_claim": False,
    }
    assert signal["eligible"] and signal["score"] >= 80
    assert risk["cash_confirmed"] > 500 and risk["position_count"] < risk["max_positions"]
    assert not risk["duplicate_claim"] and not tick_cancellation_event.is_set()
    return {
        "tick": tick, "status": "OK",
        "entry_evaluated": 1, "orders_sent": 1,
        "mock_broker": {"ACK": True, "qty": 1},
    }


def test_large_child_result_does_not_deadlock_join():
    started = time.monotonic()
    result = run_tick_in_process(_large_result, kwargs={}, timeout_sec=5)
    assert result.result["status"] == "OK"
    assert len(result.result["payload"]) == 4_000_000
    assert time.monotonic() - started < 5


def test_normal_child_returns_structured_result():
    assert run_tick_in_process(_small_result, kwargs={}, timeout_sec=2).result == {
        "status": "OK", "qty": 1,
    }


def test_timeout_after_submit_start_never_claims_no_order(tmp_path):
    marker = tmp_path / "submitted"
    with pytest.raises(TickProcessTimeout) as caught:
        run_tick_in_process(
            _wait_after_submit_marker, kwargs={"marker": str(marker)},
            timeout_sec=0.25, terminate_grace_sec=0.1,
        )
    assert marker.exists()
    assert caught.value.result["broker_submission_truth"] == "UNVERIFIED"
    assert "NO_ORDER" not in caught.value.result["status"]
    assert not caught.value.result["process_alive"]


def test_twelve_delayed_tick_results_keep_buy_to_ack_contract_without_shortcut():
    for tick in range(1, 13):
        result = run_tick_in_process(
            _eligibility_and_mock_ack, kwargs={"tick": tick}, timeout_sec=2
        ).result
        assert result["tick"] == tick
        assert result["entry_evaluated"] == 1
        assert result["orders_sent"] == 1
        assert result["mock_broker"] == {"ACK": True, "qty": 1}


def _crash_without_result(tick_cancellation_event=None):
    os._exit(83)


def test_child_that_exits_without_result_does_not_imply_no_broker_order():
    result = run_tick_in_process(_crash_without_result, kwargs={}, timeout_sec=2)
    assert result.result["status"] == "ERROR"
    assert result.result["broker_submission_truth"] == "UNVERIFIED"
    assert "without_result" in result.result["reason"]
