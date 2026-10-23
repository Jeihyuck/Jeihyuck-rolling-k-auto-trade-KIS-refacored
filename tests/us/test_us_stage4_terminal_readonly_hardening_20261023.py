"""Regression for Stage4 no-cancel TP proof, true health counts and debt-free fast path."""
from __future__ import annotations

import hashlib

from trader.us.execution import authoritative_reconcile_worker as worker
from trader.us.protective_tp_recovery import request_protective_tp_cancel


def _intent() -> dict:
    return {
        "symbol": "AMD", "side": "SELL", "exit_type": "hard_stop_loss",
        "strategy": "us_pb1_exit", "strategy_owner": "US_STANDARD",
        "position_lifecycle_id": "life-1", "account_id": "practice-account",
    }


def _historical_tp() -> dict:
    return {
        "id": "original", "symbol": "AMD", "side": "SELL",
        "trade_date": "2026-10-06", "order_no": "0000035281",
        "exchange": "NASDAQ", "qty_requested": 1, "qty_filled": 0,
        "client_order_key": "tp-original", "trading_epoch_id": "epoch",
        "claim_state": "IN_FLIGHT",
        "claim_account_id": hashlib.sha256(b"practice-account").hexdigest(),
        "meta": {"strategy_owner": "US_STANDARD", "profit_capture_stage": "tp1",
                 "position_lifecycle_id": "life-1"},
    }


class TerminalProvider:
    def __init__(self, status="CANCELLED"):
        self.status = status
        self.queries = []

    def get_fills_by_order_no(self, **kwargs):
        self.queries.append(kwargs)
        return {"order_no": "35281", "status": self.status,
                "requested_qty": 1, "filled_qty": 0,
                "remaining_qty": 0 if self.status == "CANCELLED" else 1,
                "filled_qty_present": True, "normalization_result": "normalized"}


def _forbid_cancel(*args, **kwargs):
    raise AssertionError("read-only terminal repair must not reserve/cancel")


def test_original_cancel_feature_stays_opt_in(monkeypatch):
    monkeypatch.setenv("US_PROTECTIVE_TP_CANCEL_RECOVERY_ENABLED", "0")
    result = request_protective_tp_cancel(
        intents=[_intent()], provider=object(), kis_client=object(),
        trade_date="2026-10-23", env="practice",
        find_open=_forbid_cancel,
    )
    assert result == []


def test_broker_terminal_reconciles_original_date_read_only_with_cancel_off(monkeypatch):
    monkeypatch.setenv("US_PROTECTIVE_TP_CANCEL_RECOVERY_ENABLED", "0")
    provider = TerminalProvider()
    observations = []
    result = request_protective_tp_cancel(
        intents=[_intent()], provider=provider, kis_client=None,
        trade_date="2026-10-23", env="practice",
        read_only_terminal_reconcile=True,
        find_open=lambda *_args, **_kw: [_historical_tp()],
        reserve=_forbid_cancel,
        apply_observation=lambda **kwargs:
            observations.append(kwargs) or
            {"status": "OK", "authoritative": True, "requires_reconcile": False},
    )
    assert result == [{"symbol": "AMD", "status": "CANCEL_TERMINAL_RECONCILED"}]
    assert provider.queries[0]["trade_date"] == "2026-10-06"
    assert observations[0]["broker_status"] == "CANCELLED"
    assert observations[0]["evidence_type"] == "KIS_TERMINAL_CANCEL"


def test_readonly_nonterminal_does_not_cancel_reserve_or_release_fence(monkeypatch):
    monkeypatch.setenv("US_PROTECTIVE_TP_CANCEL_RECOVERY_ENABLED", "1")
    provider = TerminalProvider(status="OPEN")
    result = request_protective_tp_cancel(
        intents=[_intent()], provider=provider, kis_client=None,
        trade_date="2026-10-23", env="practice",
        read_only_terminal_reconcile=True,
        find_open=lambda *_args, **_kw: [_historical_tp()],
        reserve=_forbid_cancel, apply_observation=_forbid_cancel,
    )
    assert result == [{
        "symbol": "AMD", "status": "FENCED",
        "reason": "historical_order_terminal_proof_missing",
        "stale_classification": "STALE_TP_BROKER_TERMINAL_PROOF_REQUIRED",
    }]


def test_readonly_same_day_open_never_reserves_or_sends_cancel(monkeypatch):
    monkeypatch.setenv("US_PROTECTIVE_TP_CANCEL_RECOVERY_ENABLED", "1")
    order = _historical_tp()
    order["trade_date"] = "2026-10-23"
    result = request_protective_tp_cancel(
        intents=[_intent()], provider=TerminalProvider(status="OPEN"),
        kis_client=None, trade_date="2026-10-23", env="practice",
        read_only_terminal_reconcile=True,
        find_open=lambda *_args, **_kw: [order],
        reserve=_forbid_cancel, apply_observation=_forbid_cancel,
    )
    assert result == [{"symbol": "AMD", "status": "FENCED",
                       "reason": "read_only_terminal_proof_missing"}]


def test_stage4_delegates_to_read_only_mode_when_cancel_not_authorized(monkeypatch):
    from trader.us import protective_stop_obligation as debt_mod
    from trader.us.execution import protective_stop_recovery_worker as recovery_mod
    from trader.us import protective_tp_recovery as tp
    monkeypatch.setenv("US_PROTECTIVE_TP_CANCEL_RECOVERY_ENABLED", "0")
    monkeypatch.setenv("US_STOP_RECOVERY_AUTHORIZED", "0")
    monkeypatch.setattr(debt_mod, "load_pending_stop_obligations",
                        lambda **kwargs: [{"symbol": "AMD", "account_id": "practice-account",
                                           "lifecycle_id": "life-1", "severity": 3}])
    monkeypatch.setattr(recovery_mod, "recovery_candidates",
                        lambda debts: [_intent()])
    got = []
    monkeypatch.setattr(tp, "request_protective_tp_cancel",
                        lambda **kwargs: got.append(kwargs) or [])
    result = worker.recover_stop_tp_conflicts_once(
        provider=object(), trade_date="2026-10-23",
    )
    assert result["checked"] == 1
    assert result["cancel_authorized"] is False
    assert got[0]["read_only_terminal_reconcile"] is True
    assert got[0]["reserve"]({"id": "anything"}) is False


def test_stage4_health_publishes_actual_zero_unresolved():
    captured = {}
    class Conn:
        def execute(self, _sql, params):
            captured.update(params)
    worker.publish_health(
        Conn(), scope={"env": "practice", "account_id": "a" * 64,
                       "trading_epoch_id": "epoch"},
        generation="11111111-1111-1111-1111-111111111111",
        state="READY", result={"reconcile_status": "OK",
                               "replay_status": "OK",
                               "pending_count": 0, "unresolved_count": 0,
                               "filled_rows": 0},
    )
    assert captured["unresolved"] == 0
    assert captured["pending"] == 0


def test_stage4_no_stop_debt_skips_expensive_broker_work(monkeypatch):
    from trader.us import protective_stop_obligation as debt_mod
    monkeypatch.setattr(debt_mod, "load_pending_stop_obligations",
                        lambda **kwargs: [])
    report = worker.run_deferred_recovery_once(
        provider=object(), trade_date="2026-10-23",
    )
    assert report["tp"]["checked"] == 0
    assert report["settlement"]["total_pending"] == 0


def test_settlement_without_debt_does_not_fetch_broker_balance(monkeypatch):
    from trader.us.execution.protective_stop_settlement_worker import (
        settle_verified_stop_debts_once,
    )
    from trader.us import protective_stop_obligation as debt_mod
    import trader.account_state as account_state
    monkeypatch.setattr(account_state, "resolve_env_name", lambda: "practice")
    monkeypatch.setattr(debt_mod, "load_pending_stop_obligations",
                        lambda **kwargs: [])
    assert settle_verified_stop_debts_once(
        provider=object(), trade_date="2026-10-23"
    ) == {"checked": 0, "settled": 0, "blocked": 0, "total_pending": 0}


def test_stage4_never_arms_cancel_with_only_one_operator_flag(monkeypatch):
    from trader.us import protective_stop_obligation as debt_mod
    from trader.us.execution import protective_stop_recovery_worker as recovery_mod
    from trader.us import protective_tp_recovery as tp
    monkeypatch.setenv("US_PROTECTIVE_TP_CANCEL_RECOVERY_ENABLED", "1")
    monkeypatch.setenv("US_STOP_RECOVERY_AUTHORIZED", "0")
    monkeypatch.setattr(debt_mod, "load_pending_stop_obligations",
                        lambda **kwargs: [{"symbol": "AMD", "account_id": "practice-account",
                                           "lifecycle_id": "life-1", "severity": 3}])
    monkeypatch.setattr(recovery_mod, "recovery_candidates",
                        lambda debts: [_intent()])
    got = []
    monkeypatch.setattr(tp, "request_protective_tp_cancel",
                        lambda **kwargs: got.append(kwargs) or [])
    class ReadOnlyProvider:
        def _get_client(self):
            raise AssertionError("read-only mode must not create a cancel client")
    report = worker.recover_stop_tp_conflicts_once(
        provider=ReadOnlyProvider(), trade_date="2026-10-23",
    )
    assert report["cancel_authorized"] is False
    assert got[0]["read_only_terminal_reconcile"] is True
    assert got[0]["reserve"]({"id": "anything"}) is False
