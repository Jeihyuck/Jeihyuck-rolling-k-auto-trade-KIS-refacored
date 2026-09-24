from __future__ import annotations

import json

import pytest


class _BudgetContext:
    def __init__(self, remaining: float):
        self._remaining = remaining

    def remaining_sec(self) -> float:
        return self._remaining


def test_entry_budget_preserves_execution_tail(monkeypatch):
    from trader.us.runner.trade_tick_runner import (
        budgeted_entry_timeout_sec,
        resolve_us_execution_tail_reserve_sec,
    )

    monkeypatch.setenv("US_EXECUTION_TAIL_RESERVE_SEC", "70")
    assert resolve_us_execution_tail_reserve_sec() == 70.0
    assert budgeted_entry_timeout_sec(_BudgetContext(105.0), 60.0) == 35.0
    assert budgeted_entry_timeout_sec(_BudgetContext(200.0), 60.0) == 60.0
    assert budgeted_entry_timeout_sec(_BudgetContext(65.0), 60.0) == 0.0


def test_fill_poll_policy_is_event_driven_and_periodic():
    from trader.us.runner.trade_tick_runner import should_fetch_fills_for_tick

    assert should_fetch_fills_for_tick(
        tick_index=1, pending_order_count=0, reconcile_only_until_clean=False, interval_ticks=3
    )
    assert not should_fetch_fills_for_tick(
        tick_index=2, pending_order_count=0, reconcile_only_until_clean=False, interval_ticks=3
    )
    assert should_fetch_fills_for_tick(
        tick_index=2, pending_order_count=1, reconcile_only_until_clean=False, interval_ticks=3
    )
    assert should_fetch_fills_for_tick(
        tick_index=3, pending_order_count=0, reconcile_only_until_clean=False, interval_ticks=3
    )
    assert should_fetch_fills_for_tick(
        tick_index=2, pending_order_count=0, reconcile_only_until_clean=True, interval_ticks=3
    )


def test_profit_capture_sync_persists_requested_and_actual_filled(monkeypatch):
    import trader.us.db.repos as repos
    from trader.us.profit_capture import sync_profit_capture_stage_from_order

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos.reset_memory_stores()

    sync_profit_capture_stage_from_order(
        trade_date="2026-09-23",
        symbol="PLTR",
        position_lifecycle_id="PLTR-L1",
        client_order_key="PLTR-TP1",
        broker_order_no="32192",
        profit_capture_stage="tp1",
        order_status="FILLED",
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        requested_qty=4,
        filled_qty=4,
    )

    state = repos.load_us_profit_capture_state(
        "2026-09-23", ["PLTR"], {"PLTR": "PLTR-L1"}
    )["PLTR"]
    assert state["tp1_done"] is True
    assert state["tp1_pending"] is False
    assert state["meta"]["tp1_qty"] == 4
    assert state["meta"]["tp1_filled_qty"] == 4


def test_profit_capture_state_flattens_recursive_meta():
    import trader.us.db.repos as repos

    state = {
        "position_lifecycle_id": "L1",
        "tp1_done": True,
        "meta": {
            "last_stage": "tp1",
            "meta": {
                "tp1_qty": 4,
                "meta": {
                    "tp1_filled_qty": 4,
                },
            },
        },
    }
    normalized = repos._normalize_profit_capture_state("2026-09-23", "PLTR", state, "L1")
    assert "meta" not in normalized["meta"]
    assert normalized["meta"]["tp1_qty"] == 4
    assert normalized["meta"]["tp1_filled_qty"] == 4
    first = json.dumps(normalized, sort_keys=True)
    again = repos._normalize_profit_capture_state("2026-09-23", "PLTR", normalized, "L1")
    second = json.dumps(again, sort_keys=True)
    assert first == second


def test_authoritative_sell_fill_carries_cost_basis_and_realized_pnl(monkeypatch):
    import trader.us.db.repos as repos

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos.reset_memory_stores()
    repos._MEM_ORDERS.append({
        "trade_date": "2026-09-23",
        "order_no": "32192",
        "client_order_key": "PLTR-TP1",
        "symbol": "PLTR",
        "exchange": "NASDAQ",
        "side": "SELL",
        "qty_requested": 4,
        "qty_filled": 0,
        "status": "ACK",
        "meta": {
            "pre_sell_avg_cost": 183.42,
            "position_lifecycle_id": "PLTR-L1",
        },
    })

    result = repos.mark_order_filled_by_reconcile(
        order_no="32192",
        client_order_key="PLTR-TP1",
        symbol="PLTR",
        side="SELL",
        filled_qty=4,
        requested_qty=4,
        cumulative_filled_qty=4,
        avg_price_usd=191.48,
        source="fills_by_order_no",
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        trade_date="2026-09-23",
    )
    assert result["status"] == "OK"
    actual = [f for f in repos._MEM_FILLS if not repos.is_synthetic_fill_meta(f.get("meta"))]
    assert len(actual) == 1
    meta = actual[0]["meta"]
    assert meta["cost_basis_price_usd"] == pytest.approx(183.42)
    assert meta["realized_pnl_usd"] == pytest.approx(32.24)
    assert meta["realized_pnl_pct"] == pytest.approx((191.48 / 183.42 - 1) * 100, abs=0.001)


def test_trade_reason_pnl_summary_includes_sell_realized_and_buy_eod():
    from trader.us.runner.daily_report_runner import _build_trade_reason_pnl_summary

    summary = _build_trade_reason_pnl_summary(
        [
            {
                "symbol": "PLTR",
                "side": "SELL",
                "reason": "TAKE_PROFIT_TP1",
                "gross_realized_pnl": 32.24,
                "filled_qty": 4,
                "fill_price": 191.48,
            },
            {
                "symbol": "TEAM",
                "side": "BUY",
                "entry_reason": "ENTRY_PULLBACK",
                "strategy_owner": "US_STANDARD",
                "filled_qty": 17,
                "fill_price": 192.3156,
            },
        ],
        [
            {
                "symbol": "TEAM",
                "qty": 17,
                "avg_cost": 192.3156,
                "current_px": 195.17,
                "meta": {"entry_reason": "ENTRY_PULLBACK", "entry_strategy": "us_pb1"},
            }
        ],
    )
    assert summary["realized_pnl_usd"] == pytest.approx(32.24)
    assert summary["position_pnl_snapshot"]["TEAM"]["unrealized_pnl_usd"] == pytest.approx(
        (195.17 - 192.3156) * 17, abs=0.001
    )
    buy = next(x for x in summary["trade_details"] if x["symbol"] == "TEAM")
    assert buy["eod_unrealized_pnl_pct"] == pytest.approx((195.17 / 192.3156 - 1) * 100, abs=0.001)
