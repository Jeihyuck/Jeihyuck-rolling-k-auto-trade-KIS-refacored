"""2026-10-06 US incident regressions: TP final-share, broker-open and fence."""
from datetime import datetime, timezone

import pytest

from trader.us.market_state_overlay import resolve_profit_capture_stage_quantity
from trader.us.execution.order_router import (
    _validate_take_profit_with_fresh_broker_position,
    _tp_unsubmitted_attempt_may_release_stage,
)
from trader.us.execution.reconcile import classify_ack_orders_with_final_balance
from trader.us.db import repos


NOW = datetime(2026, 10, 6, 19, 30, tzinfo=timezone.utc)
DATE = "2026-10-06"


def _tp_intent(*, symbol="MSFT", qty=1, stage="tp2"):
    return {
        "symbol": symbol, "side": "SELL", "qty": qty,
        "limit_price": 106.0, "trade_date": DATE,
        "reason": f"TAKE_PROFIT_{stage.upper()}",
        "client_order_key": f"US_PC_{DATE}_{symbol}_lc-{symbol}_{stage}",
        "position_lifecycle_id": f"lc-{symbol}",
        "meta": {
            "reason": f"TAKE_PROFIT_{stage.upper()}",
            "profit_capture_stage": stage,
            "position_lifecycle_id": f"lc-{symbol}",
            "tp_threshold_fraction": 0.05 if stage == "tp2" else 0.03,
            "profit_capture_sell_fraction": "0.25",
            "profit_capture_runner_min_remain_pct": "0.40",
            "profit_capture_stage_target_qty": 0,
            "profit_capture_stage_filled_qty": 0,
            "profit_capture_decision_holding_qty": 2,
        },
    }


def _broker_position(*, symbol="MSFT", qty=1, orderable=None):
    return {
        "symbol": symbol, "qty": qty,
        "orderable_qty": qty if orderable is None else orderable,
        "avg_price_usd": 100.0, "balance_source": "kis_balance_authoritative",
    }


def test_msft_stale_two_fresh_last_share_blocks_tp2():
    intent = _tp_intent()
    result = _validate_take_profit_with_fresh_broker_position(
        intent, _broker_position(qty=1), now=NOW
    )
    assert result["ok"] is False
    assert result["reason"] == "fresh_tp_stage_quantity_or_runner_limit"
    assert result["stage_max_qty"] == 0


def test_tp_intent_and_router_share_runner_and_rounding():
    assert resolve_profit_capture_stage_quantity(
        holding_qty=1, sell_fraction=.25, runner_min_remain_pct=.40,
    ) == 0
    assert resolve_profit_capture_stage_quantity(
        holding_qty=2, sell_fraction=.25, runner_min_remain_pct=.40,
    ) == 1
    assert resolve_profit_capture_stage_quantity(
        holding_qty=100, sell_fraction=.25, runner_min_remain_pct=.40,
        stage_target_qty=25, stage_filled_qty=10,
    ) == 15


def test_tp_fresh_orderable_excludes_open_broker_reservations():
    intent = _tp_intent(qty=2, stage="tp1")
    result = _validate_take_profit_with_fresh_broker_position(
        intent, _broker_position(qty=10, orderable=1), now=NOW
    )
    assert result["ok"] is False
    assert result["orderable_qty"] == 1


def test_tp_stale_qty_does_not_resize_and_submit_new_amount():
    intent = _tp_intent(qty=3)
    result = _validate_take_profit_with_fresh_broker_position(
        intent, _broker_position(qty=7), now=NOW
    )
    assert result["ok"] is False
    assert intent["qty"] == 3


def test_tp_pre_submit_fence_release_requires_all_sources_clean(monkeypatch):
    from trader.us.execution import order_journal
    monkeypatch.setattr(repos, "load_pending_ack_orders_result",
                        lambda td: {"status": "OK", "orders": []})
    monkeypatch.setattr(repos, "load_active_execution_claim_attempts", lambda: [])
    monkeypatch.setattr(order_journal, "load_order_events", lambda td: [])
    intent = _tp_intent()
    assert _tp_unsubmitted_attempt_may_release_stage(
        intent, DATE, _broker_position(qty=2)
    )
    assert not _tp_unsubmitted_attempt_may_release_stage(
        intent, DATE, _broker_position(qty=2, orderable=1)
    )
    monkeypatch.setattr(repos, "load_pending_ack_orders_result",
                        lambda td: {"status": "DB_ERROR", "error": "database timeout", "orders": []})
    assert not _tp_unsubmitted_attempt_may_release_stage(
        intent, DATE, _broker_position(qty=2)
    )


def test_tp_existing_open_order_keeps_stage_fenced(monkeypatch):
    from trader.us.execution import order_journal
    intent = _tp_intent()
    monkeypatch.setattr(repos, "load_active_execution_claim_attempts", lambda: [])
    monkeypatch.setattr(order_journal, "load_order_events", lambda td: [])
    monkeypatch.setattr(repos, "load_pending_ack_orders_result",
                        lambda td: {"status": "OK", "orders": [
                            {"symbol": "MSFT", "side": "SELL", "client_order_key": "other-open-order"}
                        ]})
    assert not _tp_unsubmitted_attempt_may_release_stage(
        intent, DATE, _broker_position(qty=2)
    )
    monkeypatch.setattr(repos, "load_pending_ack_orders_result",
                        lambda td: {"status": "OK", "orders": []})
    monkeypatch.setattr(order_journal, "load_order_events",
                        lambda td: [{"event_type": "BROKER_SUBMIT_STARTED",
                                     "client_order_key": intent["client_order_key"]}])
    assert not _tp_unsubmitted_attempt_may_release_stage(
        intent, DATE, _broker_position(qty=2)
    )


class CloseProvider:
    def __init__(self, broker_rows=None):
        self.broker_rows = broker_rows or []

    def get_balance(self, *, force_refresh=False):
        return {
            "balance_parse_status": "OK", "balance_complete": True,
            "balance_authoritative": True,
            "positions": [
                {"symbol": "MRVL", "qty": 9, "orderable_qty": 7},
                {"symbol": "AMD", "qty": 4, "orderable_qty": 3},
            ],
        }

    def get_today_orders(self, trade_date):
        assert trade_date == DATE
        return list(self.broker_rows)


def _broker_row(symbol, number, *, requested, filled=0, remaining=None,
                status="OPEN", fill_present=True):
    return {
        "symbol": symbol, "side": "SELL", "order_no": number,
        "requested_qty": requested, "filled_qty": filled,
        "remaining_qty": requested - filled if remaining is None else remaining,
        "filled_qty_present": fill_present, "status": status,
        "normalization_result": "normalized",
    }


def _order(symbol, number, *, qty=10, filled=0, status="ACK", observation=None):
    row = {
        "symbol": symbol, "side": "SELL", "order_no": number,
        "client_order_key": f"KEY_{number}", "qty_requested": qty,
        "qty_filled": filled, "status": status,
    }
    if observation is not None:
        row["_broker_observation"] = observation
    return row


def test_close_open_mrvl_and_amd_survive_empty_local_rows(monkeypatch):
    from trader.us.execution import order_journal
    monkeypatch.setattr(repos, "load_pending_ack_orders_result",
                        lambda td, env="practice": {"status": "OK", "orders": []})
    monkeypatch.setattr(repos, "load_active_execution_claim_attempts", lambda: [])
    monkeypatch.setattr(order_journal, "load_order_events", lambda td: [])
    provider = CloseProvider([
        _broker_row("MRVL", "34237", requested=2),
        _broker_row("AMD", "35281", requested=1),
    ])
    result = classify_ack_orders_with_final_balance(provider=provider, trade_date=DATE)
    assert result["status"] == "WARNING_OPEN_ORDER_PENDING"
    assert result["open_order_pending_count"] == 2
    assert {row["order_no"] for row in result["orders"]} == {"34237", "35281"}
    assert all(row["final_status"] == "ack_open_order_pending" for row in result["orders"])


def test_partial_fill_remaining_stays_pending():
    row = _order("MRVL", "123", qty=10, filled=3, status="PARTIALLY_FILLED",
                 observation=_broker_row("MRVL", "123", requested=10, filled=3, remaining=7))
    result = classify_ack_orders_with_final_balance(
        provider=CloseProvider(), trade_date=DATE, orders=[row],
    )
    assert result["open_order_pending_count"] == 1
    assert result["orders"][0]["final_status"] == "partial_fill_open"
    assert result["orders"][0]["broker_remaining_qty"] == 7


def test_broker_open_overrides_local_filled():
    row = _order("MRVL", "124", qty=10, filled=10, status="FILLED",
                 observation=_broker_row("MRVL", "124", requested=10, filled=3, remaining=7))
    result = classify_ack_orders_with_final_balance(
        provider=CloseProvider(), trade_date=DATE, orders=[row],
    )
    assert result["orders"][0]["final_status"] == "partial_fill_open"
    assert result["manual_reconcile_required"] == 1
    assert result["orders"][0]["broker_local_fill_qty_conflict"] is True


def test_partially_filled_then_cancelled_preserves_three_shares():
    row = _order("MRVL", "125", qty=10, filled=3, status="PARTIALLY_FILLED",
                 observation=_broker_row("MRVL", "125", requested=10, filled=3,
                                          remaining=0, status="CANCELLED"))
    result = classify_ack_orders_with_final_balance(
        provider=CloseProvider(), trade_date=DATE, orders=[row],
    )
    assert result["status"] == "OK"
    assert result["orders"][0]["final_status"] == "partial_fill_cancelled"
    assert result["orders"][0]["broker_filled_qty"] == 3


def test_cancel_without_explicit_fill_evidence_is_unresolved():
    row = _order("MRVL", "126", qty=10, filled=0, status="ACK",
                 observation=_broker_row("MRVL", "126", requested=10, filled=None,
                                          remaining=0, status="CANCELLED",
                                          fill_present=False))
    result = classify_ack_orders_with_final_balance(
        provider=CloseProvider(), trade_date=DATE, orders=[row],
    )
    assert result["pending_order_count"] == 1
    assert result["orders"][0]["final_status"] == "ack_unresolved_error"


def test_close_pending_db_failure_is_not_zero_orders(monkeypatch):
    monkeypatch.setattr(repos, "load_pending_ack_orders_result",
                        lambda td, env="practice": {"status": "DB_ERROR", "orders": [], "error": "timeout"})
    result = classify_ack_orders_with_final_balance(
        provider=CloseProvider(), trade_date=DATE,
    )
    assert result["status"] == "ERROR"
    assert result["pending_order_count"] > 0
    assert result["manual_reconcile_required"] == 1


def test_close_same_semantic_action_two_broker_attempts_not_collapsed(monkeypatch):
    from trader.us.execution import order_journal
    monkeypatch.setattr(repos, "load_pending_ack_orders_result",
                        lambda td, env="practice": {"status": "OK", "orders": []})
    monkeypatch.setattr(repos, "load_active_execution_claim_attempts", lambda: [])
    monkeypatch.setattr(order_journal, "load_order_events", lambda td: [
        {"event_type": "BROKER_ACK_RECEIVED", "symbol": "MRVL", "side": "SELL",
         "broker_order_no": "111", "client_order_key": "a", "submit_attempt_id": "attempt-a",
         "semantic_action": "US_STANDARD_TP2"},
        {"event_type": "BROKER_ACK_RECEIVED", "symbol": "MRVL", "side": "SELL",
         "broker_order_no": "222", "client_order_key": "b", "submit_attempt_id": "attempt-b",
         "semantic_action": "US_STANDARD_TP2"},
    ])
    provider = CloseProvider([
        _broker_row("MRVL", "111", requested=2, remaining=2),
        _broker_row("MRVL", "222", requested=2, remaining=2),
    ])
    result = classify_ack_orders_with_final_balance(provider=provider, trade_date=DATE)
    assert result["open_order_pending_count"] == 2
    assert {row["submit_attempt_id"] for row in result["orders"]} == {"attempt-a", "attempt-b"}


def test_real_router_pre_submit_msft_tp2_does_not_call_kis_sell(monkeypatch):
    """Exercise the production risk/router path rather than the TP helper alone."""
    from unittest.mock import patch
    from trader.us.execution.order_router import route_order

    for name, value in {
        "KIS_ENV": "practice", "STRATEGY_ENV": "practice", "DRY_RUN": "0",
        "TRADING_REGION": "US", "US_AGENT_ENABLED": "1",
        "US_PAPER_TRADING_ENABLED": "1", "US_SESSION_WINDOW_VALID": "1",
        "US_PREP_CONTRACT_OK": "1", "US_BALANCE_AVAILABLE": "1",
    }.items():
        monkeypatch.setenv(name, value)

    class KIS:
        sell_calls = []

        def get_balance(self, force_refresh=False):
            return {"positions": [{
                "symbol": "MSFT", "qty": 1, "orderable_qty": 1,
                "avg_price_usd": 100.0, "currency": "USD",
            }]}

        def place_us_sell_order(self, symbol, exchange, qty, price):
            self.sell_calls.append((symbol, exchange, qty, price))
            return {"rt_cd": "0", "output": {"ODNO": "UNEXPECTED"}}

    kis = KIS()
    intent = _tp_intent()
    intent.update({
        "exchange": "NASDAQ", "notional_usd": 106.0,
        "strategy_owner": "US_STANDARD", "sleeve_id": "US_STANDARD",
        "position_action": "PARTIAL_EXIT_SELL",
    })
    with (
        patch("trader.us.execution.order_router.same_day_semantic_sell_exists", return_value=False),
        patch("trader.us.db.repos.save_order_intent", return_value=True),
        patch("trader.us.db.repos.load_today_order_keys", return_value=set()),
        patch("trader.us.db.repos.mark_order_intent_blocked"),
        patch("trader.us.execution.order_router.resolve_dry_run_for_us_order", return_value=False),
        patch("trader.us.execution.order_router._tp_unsubmitted_attempt_may_release_stage", return_value=False),
        patch("trader.us.execution.risk_gate.check_pending_sell_order_hard"),
    ):
        result = route_order(
            intent, kis_client=kis, current_position_symbols={"MSFT"},
            total_portfolio_usd=10000.0, available_cash_usd=10000.0,
        )
    assert result["status"] == "BLOCKED", result
    assert result["guard_reason"] == "fresh_tp_stage_quantity_or_runner_limit", result
    assert result["safe_to_release_tp_pending"] is False
    assert kis.sell_calls == []
