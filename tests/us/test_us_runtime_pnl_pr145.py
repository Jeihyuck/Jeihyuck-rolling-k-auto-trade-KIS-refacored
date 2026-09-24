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
    assert buy["same_day_buy_lot_unrealized_pnl_pct"] == pytest.approx((195.17 / 192.3156 - 1) * 100, abs=0.001)


def test_execution_quantity_provenance_distinguishes_strategy_and_broker_resize():
    from trader.us.execution.order_router import annotate_execution_quantity_provenance

    intent = {"meta": {"broker_cash_resized": True}}
    out = annotate_execution_quantity_provenance(
        intent, strategy_requested_qty=18, final_qty=17, price=193.9549
    )
    meta = out["meta"]
    assert meta["strategy_requested_qty"] == 18
    assert meta["execution_final_qty"] == 17
    assert meta["execution_qty_changed"] is True
    assert meta["execution_qty_change_reason"] == "BROKER_ORDERABLE_CASH_RESIZE"


def test_authoritative_close_zeroes_stale_unrealized_pnl(monkeypatch):
    import trader.us.db.repos as repos

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos.reset_memory_stores()
    repos._MEM_POSITIONS.append({
        "as_of": "2026-09-23",
        "symbol": "PLTR",
        "exchange": "NASDAQ",
        "qty": 10,
        "avg_cost": 183.42,
        "current_px": 191.87,
        "unrealized_pnl_usd": 84.5,
        "meta": {},
    })
    repos.save_position_snapshot(
        [],
        trade_date="2026-09-23",
        balance_fetch_status="OK",
        balance_parse_status="OK",
        authoritative_positions=True,
        preserve_previous_positions=False,
    )
    row = repos._MEM_POSITIONS[0]
    assert row["qty"] == 0
    assert row["unrealized_pnl_usd"] == 0.0


def test_authoritative_partial_fill_recomputes_realized_pnl_on_cumulative_growth(monkeypatch):
    import trader.us.db.repos as repos

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos.reset_memory_stores()
    repos._MEM_ORDERS.append({
        "trade_date": "2026-09-23",
        "order_no": "PLTR-PARTIAL",
        "client_order_key": "PLTR-PARTIAL",
        "symbol": "PLTR",
        "exchange": "NASDAQ",
        "side": "SELL",
        "qty_requested": 4,
        "qty_filled": 0,
        "status": "ACK",
        "meta": {"pre_sell_avg_cost": 183.42},
    })
    first = repos.mark_order_filled_by_reconcile(
        order_no="PLTR-PARTIAL", client_order_key="PLTR-PARTIAL",
        symbol="PLTR", side="SELL", filled_qty=2, requested_qty=4,
        cumulative_filled_qty=2, avg_price_usd=191.48,
        source="fills_by_order_no", evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        trade_date="2026-09-23",
    )
    assert first["order_status"] == "PARTIALLY_FILLED"
    assert repos._MEM_FILLS[0]["meta"]["realized_pnl_usd"] == pytest.approx(16.12)

    second = repos.mark_order_filled_by_reconcile(
        order_no="PLTR-PARTIAL", client_order_key="PLTR-PARTIAL",
        symbol="PLTR", side="SELL", filled_qty=4, requested_qty=4,
        cumulative_filled_qty=4, avg_price_usd=191.48,
        source="fills_by_order_no", evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        trade_date="2026-09-23",
    )
    assert second["order_status"] == "FILLED"
    assert len(repos._MEM_FILLS) == 1
    assert repos._MEM_FILLS[0]["qty"] == 4
    assert repos._MEM_FILLS[0]["meta"]["realized_pnl_usd"] == pytest.approx(32.24)


def test_entry_timeout_returns_without_waiting_for_stubborn_worker():
    import threading
    import time

    from trader.us.runner.trade_tick_runner import run_entry_eval_with_timeout

    cancel = threading.Event()

    def stubborn():
        time.sleep(0.25)
        return ["late"]

    started = time.monotonic()
    with pytest.raises(TimeoutError):
        run_entry_eval_with_timeout(stubborn, timeout_sec=0.02, cancel_event=cancel)
    elapsed = time.monotonic() - started

    assert cancel.is_set()
    assert elapsed < 0.12


def test_stage_provider_uses_isolated_kis_client_and_deadline():
    import time

    from trader.us.data_provider import USDataProvider

    provider = USDataProvider(offline=True, cache_enabled=True, env="practice")
    original_client = provider._get_client()
    deadline = time.monotonic() + 1.0
    fork = provider.fork_for_stage(stage_deadline=deadline)

    assert fork is not provider
    assert fork._get_client() is not original_client
    assert fork._get_client()._stage_deadline == pytest.approx(deadline)
    assert fork._get_client()._stage_max_attempts == 1


def test_add_to_existing_reports_only_same_day_buy_lot_pnl():
    from trader.us.runner.daily_report_runner import _build_trade_reason_pnl_summary

    summary = _build_trade_reason_pnl_summary(
        [
            {
                "symbol": "MSFT",
                "side": "BUY",
                "client_order_key": "MSFT-ADD-1",
                "position_action": "ADD_TO_EXISTING_BUY",
                "filled_qty": 2,
                "fill_price": 100.0,
            },
        ],
        [
            {
                "symbol": "MSFT",
                "qty": 12,
                "avg_cost": 90.0,
                "current_px": 110.0,
                "unrealized_pnl_usd": 240.0,
                "meta": {"entry_reason": "ENTRY_MOMENTUM"},
            },
        ],
        [
            {
                "symbol": "MSFT",
                "side": "BUY",
                "qty": 2,
                "price_usd": 100.0,
                "client_order_key": "MSFT-ADD-1",
                "meta": {
                    "is_synthetic": False,
                    "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
                },
            },
        ],
    )

    assert summary["position_pnl_snapshot"]["MSFT"]["unrealized_pnl_usd"] == pytest.approx(240.0)
    assert summary["same_day_buy_lot_unrealized_pnl_usd"] == pytest.approx(20.0)
    assert summary["gross_trade_day_impact_usd"] == pytest.approx(20.0)

    trade = summary["trade_details"][0]
    assert trade["eod_position_unrealized_pnl_usd"] == pytest.approx(240.0)
    assert trade["same_day_buy_lot_qty"] == pytest.approx(2.0)
    assert trade["same_day_buy_lot_unrealized_pnl_usd"] == pytest.approx(20.0)
    assert trade["same_day_buy_lot_pnl_source"] == "AUTHORITATIVE_KIS_BUY_FILL"


def test_cancelled_entry_engine_stops_before_provider_lookup(monkeypatch):
    import threading

    from trader.us.pb1.us_entry_engine import generate_entry_intents

    cancel = threading.Event()
    cancel.set()

    class Provider:
        def get_current_price(self, *args, **kwargs):
            raise AssertionError("provider lookup must not run after cancellation")

    diagnostics = {}
    result = generate_entry_intents(
        tickers=None,
        provider=Provider(),
        sold_today=set(),
        available_cash_usd=10000.0,
        position_count=0,
        capital_usd_cap=10000.0,
        watchlist_entries=[{
            "symbol": "MSFT",
            "score_final": 0.8,
            "exchange": "NASDAQ",
            "entry_reason": "ENTRY_MOMENTUM",
            "entry_style_selected": "pb1_momentum",
        }],
        current_position_symbols=set(),
        diagnostics=diagnostics,
        cancel_event=cancel,
    )

    assert result == []
    assert diagnostics["cancelled"] is True


def test_stage_provider_blocks_lookup_after_cancel():
    import threading
    import time

    from trader.us.data_provider import USDataProvider
    from trader.us.execution.kis_us_client import KisUSTemporaryError

    cancel = threading.Event()
    provider = USDataProvider(offline=True, cache_enabled=True, env="practice")
    fork = provider.fork_for_stage(
        stage_deadline=time.monotonic() + 10.0,
        cancel_event=cancel,
    )
    cancel.set()

    with pytest.raises(KisUSTemporaryError, match="entry stage cancelled"):
        fork.get_current_price("MSFT", "NASDAQ")


def test_same_day_buy_lot_pnl_caps_to_net_added_open_qty_after_sell():
    from trader.us.runner.daily_report_runner import _build_trade_reason_pnl_summary

    summary = _build_trade_reason_pnl_summary(
        [{
            "symbol": "MSFT",
            "side": "BUY",
            "client_order_key": "MSFT-ADD-SELL",
            "position_action": "ADD_TO_EXISTING_BUY",
            "pre_order_holding_qty": 10,
            "filled_qty": 2,
            "fill_price": 100.0,
        }],
        [{
            "symbol": "MSFT",
            "qty": 11,
            "avg_cost": 92.0,
            "current_px": 110.0,
            "unrealized_pnl_usd": 198.0,
        }],
        [{
            "symbol": "MSFT",
            "side": "BUY",
            "qty": 2,
            "price_usd": 100.0,
            "client_order_key": "MSFT-ADD-SELL",
            "meta": {
                "is_synthetic": False,
                "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
                "pre_order_holding_qty": 10,
            },
        }],
    )

    trade = summary["trade_details"][0]
    assert trade["same_day_buy_filled_qty"] == pytest.approx(2.0)
    assert trade["same_day_buy_lot_qty"] == pytest.approx(1.0)
    assert trade["same_day_buy_lot_unrealized_pnl_usd"] == pytest.approx(10.0)
    assert summary["same_day_buy_lot_unrealized_pnl_usd"] == pytest.approx(10.0)


def test_same_symbol_multiple_buys_partial_sell_allocates_eod_lots_once_fifo():
    from trader.us.runner.daily_report_runner import _build_trade_reason_pnl_summary

    summary = _build_trade_reason_pnl_summary(
        [
            {
                "symbol": "XYZ", "side": "BUY",
                "client_order_key": "XYZ-B1",
                "filled_qty": 2, "fill_price": 100.0,
            },
            {
                "symbol": "XYZ", "side": "BUY",
                "client_order_key": "XYZ-B2",
                "filled_qty": 3, "fill_price": 105.0,
            },
            {
                "symbol": "XYZ", "side": "SELL",
                "client_order_key": "XYZ-S1",
                "filled_qty": 3, "fill_price": 110.0,
                "gross_realized_pnl": 21.0,
            },
        ],
        [{
            "symbol": "XYZ",
            "qty": 2,
            "avg_cost": 105.0,
            "current_px": 120.0,
            "unrealized_pnl_usd": 30.0,
        }],
        [
            {
                "symbol": "XYZ", "side": "BUY", "qty": 2, "price_usd": 100.0,
                "client_order_key": "XYZ-B1",
                "filled_at": "2026-09-23T14:00:00+00:00",
                "meta": {
                    "is_synthetic": False,
                    "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
                    "pre_order_position_qty": 0,
                },
            },
            {
                "symbol": "XYZ", "side": "BUY", "qty": 3, "price_usd": 105.0,
                "client_order_key": "XYZ-B2",
                "filled_at": "2026-09-23T14:05:00+00:00",
                "meta": {
                    "is_synthetic": False,
                    "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
                    "pre_order_position_qty": 2,
                },
            },
            {
                "symbol": "XYZ", "side": "SELL", "qty": 3, "price_usd": 110.0,
                "client_order_key": "XYZ-S1",
                "filled_at": "2026-09-23T14:10:00+00:00",
                "realized_pnl_usd": 21.0,
                "meta": {
                    "is_synthetic": False,
                    "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
                    "pre_order_holding_qty": 5,
                },
            },
        ],
    )

    buys = {
        row["client_order_key"]: row
        for row in summary["trade_details"]
        if row["side"] == "BUY"
    }
    assert buys["XYZ-B1"]["same_day_buy_lot_qty"] is None
    assert buys["XYZ-B1"]["same_day_buy_lot_unrealized_pnl_usd"] is None
    assert buys["XYZ-B2"]["same_day_buy_lot_qty"] == pytest.approx(2.0)
    assert buys["XYZ-B2"]["same_day_buy_lot_cost_basis_usd"] == pytest.approx(210.0)
    assert buys["XYZ-B2"]["same_day_buy_lot_unrealized_pnl_usd"] == pytest.approx(30.0)

    assert summary["same_day_buy_lot_cost_basis_usd"] == pytest.approx(210.0)
    assert summary["same_day_buy_lot_unrealized_pnl_usd"] == pytest.approx(30.0)
    assert summary["realized_pnl_usd"] == pytest.approx(21.0)
    assert summary["gross_trade_day_impact_usd"] == pytest.approx(51.0)
    assert summary["buy_lot_attribution"] == "SYMBOL_CHRONOLOGICAL_FIFO"


def test_same_symbol_buy_sell_buy_respects_fill_time_order():
    from trader.us.runner.daily_report_runner import _build_trade_reason_pnl_summary

    summary = _build_trade_reason_pnl_summary(
        [
            {"symbol": "XYZ", "side": "BUY", "client_order_key": "B1", "filled_qty": 2, "fill_price": 100.0},
            {"symbol": "XYZ", "side": "SELL", "client_order_key": "S1", "filled_qty": 1, "fill_price": 110.0},
            {"symbol": "XYZ", "side": "BUY", "client_order_key": "B2", "filled_qty": 2, "fill_price": 105.0},
        ],
        [{"symbol": "XYZ", "qty": 3, "current_px": 120.0, "avg_cost": 103.3333}],
        [
            {
                "symbol": "XYZ", "side": "BUY", "qty": 2, "price_usd": 100.0,
                "client_order_key": "B1", "filled_at": "2026-09-23T14:00:00+00:00",
                "meta": {"is_synthetic": False, "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL", "pre_order_position_qty": 0},
            },
            {
                "symbol": "XYZ", "side": "SELL", "qty": 1, "price_usd": 110.0,
                "client_order_key": "S1", "filled_at": "2026-09-23T14:02:00+00:00",
                "meta": {"is_synthetic": False, "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL", "pre_order_holding_qty": 2},
            },
            {
                "symbol": "XYZ", "side": "BUY", "qty": 2, "price_usd": 105.0,
                "client_order_key": "B2", "filled_at": "2026-09-23T14:04:00+00:00",
                "meta": {"is_synthetic": False, "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL", "pre_order_position_qty": 1},
            },
        ],
    )

    buys = {
        row["client_order_key"]: row
        for row in summary["trade_details"]
        if row["side"] == "BUY"
    }
    assert buys["B1"]["same_day_buy_lot_qty"] == pytest.approx(1.0)
    assert buys["B1"]["same_day_buy_lot_unrealized_pnl_usd"] == pytest.approx(20.0)
    assert buys["B2"]["same_day_buy_lot_qty"] == pytest.approx(2.0)
    assert buys["B2"]["same_day_buy_lot_unrealized_pnl_usd"] == pytest.approx(30.0)
    assert summary["same_day_buy_lot_unrealized_pnl_usd"] == pytest.approx(50.0)
