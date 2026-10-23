"""Cross-day phantom OPEN from KIS practice CCNL is not live cancel evidence."""
from __future__ import annotations

from trader.us.protective_tp_recovery import (
    classify_stale_tp_conflict, request_protective_tp_cancel,
)


class _ForbiddenBroker:
    def cancel_us_order(self, **kwargs):
        raise AssertionError("historical OPEN must never authorize KIS cancel")


class _HistoricalReadOnlyProvider:
    def get_fills_by_order_no(self, **kwargs):
        # Read-only original-date query: OPEN never grants cancel authority
        # and must not release the execution claim.
        return {
            "order_no": "44796", "requested_qty": 1, "filled_qty": 0,
            "remaining_qty": 1, "filled_qty_present": True, "status": "OPEN",
            "normalization_result": "normalized",
        }


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
        intents=[intent], provider=_HistoricalReadOnlyProvider(), kis_client=_ForbiddenBroker(),
        trade_date="2026-10-13", env="practice",
        find_open=lambda *a, **kw: [order],
        reserve=lambda x: (_ for _ in ()).throw(AssertionError("never reserve")),
    )
    assert result == [{
        "symbol": "MSFT", "status": "FENCED",
        "reason": "historical_order_terminal_proof_missing",
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


def test_cross_day_terminal_proof_settles_old_tp_without_cancel_request(monkeypatch):
    """A broker-proven terminal old TP can unlock reconciliation without inventing a cancel ACK."""
    monkeypatch.setenv("US_PROTECTIVE_TP_CANCEL_RECOVERY_ENABLED", "1")
    order = {
        "id": "msft-order", "symbol": "MSFT", "trade_date": "2026-10-12",
        "side": "SELL", "order_no": "0000044796", "exchange": "NASDAQ",
        "claim_state": "IN_FLIGHT", "qty_requested": 1, "qty_filled": 0,
        "client_order_key": "old-msft-tp", "trading_epoch_id": "epoch",
        "meta": {"strategy_owner":"US_STANDARD","position_lifecycle_id":"msft-life","profit_capture_stage":"tp1"},
    }
    intent = {
        "symbol": "MSFT", "side": "SELL", "exit_type": "hard_stop_loss",
        "strategy": "us_pb1_exit", "strategy_owner": "US_STANDARD",
        "position_lifecycle_id": "msft-life",
    }
    class Provider:
        def get_fills_by_order_no(self, **kwargs):
            assert kwargs["trade_date"] == "2026-10-12"
            return {"order_no":"44796","status":"CANCELLED",
                    "requested_qty":1,"filled_qty":0,"remaining_qty":0,
                    "filled_qty_present":True,"normalization_result":"normalized"}
    observed=[]
    def apply_observation(**kwargs):
        observed.append(kwargs)
        return {"status":"OK","authoritative":True,"requires_reconcile":False}
    result=request_protective_tp_cancel(
        intents=[intent],provider=Provider(),kis_client=_ForbiddenBroker(),
        trade_date="2026-10-13",env="practice",
        find_open=lambda *args,**kwargs:[order],
        reserve=lambda _: (_ for _ in ()).throw(AssertionError("no cancel reservation on terminal")),
        apply_observation=apply_observation,
    )
    assert result[0]["status"]=="CANCEL_TERMINAL_RECONCILED"
    assert observed[0]["trade_date"]=="2026-10-12"
    assert observed[0]["evidence_type"]=="KIS_TERMINAL_CANCEL"
