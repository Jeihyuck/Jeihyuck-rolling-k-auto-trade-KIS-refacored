"""Cross-day phantom OPEN from KIS practice CCNL is not live cancel evidence."""
from __future__ import annotations

from trader.us.protective_tp_recovery import (
    classify_stale_tp_conflict, request_protective_tp_cancel,
)


class _ForbiddenBroker:
    def cancel_us_order(self, **kwargs):
        raise AssertionError("historical OPEN must never authorize KIS cancel")


class _ForbiddenProvider:
    def get_fills_by_order_no(self, **kwargs):
        raise AssertionError("historical status query is not active cancellation proof")


def test_msft_cross_day_inflight_keeps_fence_without_phantom_cancel(monkeypatch):
    monkeypatch.setenv("US_PROTECTIVE_TP_CANCEL_RECOVERY_ENABLED", "1")
    order = {
        "symbol": "MSFT", "trade_date": "2026-10-12",
        "side": "SELL", "order_no": "0000044796",
        "claim_state": "IN_FLIGHT", "qty_requested": 1, "qty_filled": 0,
        "meta": {"strategy_owner": "US_STANDARD", "position_lifecycle_id": "msft-life",
                 "profit_capture_stage": "tp1"},
    }
    intent = {
        "symbol": "MSFT", "side": "SELL", "exit_type": "hard_stop_loss",
        "strategy": "us_pb1_exit", "strategy_owner": "US_STANDARD",
        "position_lifecycle_id": "msft-life",
    }
    result = request_protective_tp_cancel(
        intents=[intent], provider=_ForbiddenProvider(), kis_client=_ForbiddenBroker(),
        trade_date="2026-10-13", env="practice",
        find_open=lambda *a, **kw: [order],
        reserve=lambda x: (_ for _ in ()).throw(AssertionError("never reserve")),
    )
    assert result == [{
        "symbol": "MSFT", "status": "FENCED",
        "reason": "historical_open_not_current_cancel_proof",
        "stale_classification": "STALE_TP_BROKER_TERMINAL_PROOF_REQUIRED",
    }]
    assert order["claim_state"] == "IN_FLIGHT"


def test_amd_and_mrvl_are_cross_day_not_terminal_proof():
    for sym, no in [("AMD", "0000035281"), ("MRVL", "0000034237")]:
        classification = classify_stale_tp_conflict({
            "symbol": sym, "order_no": no, "trade_date": "2026-10-06",
            "claim_state": "IN_FLIGHT",
        }, current_trade_date="2026-10-12")
        assert classification == "STALE_TP_BROKER_TERMINAL_PROOF_REQUIRED"


def test_same_day_open_recovery_remains_eligible_for_broker_proof_checks():
    assert classify_stale_tp_conflict({
        "trade_date": "2026-10-12", "claim_state": "IN_FLIGHT",
    }, current_trade_date="2026-10-12") == "SAME_DAY_TP_CLAIM"
