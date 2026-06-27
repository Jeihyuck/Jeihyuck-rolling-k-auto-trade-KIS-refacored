from __future__ import annotations

import time

from trader.us.runner.trade_tick_runner import route_exit_orders_immediately


def test_exit_route_immediate_sell_does_not_use_watchlist_allowed_symbols(monkeypatch):
    calls = []

    def fake_route(intent, **kwargs):
        calls.append((intent, kwargs))
        return {"status": "ACK", "side": "SELL", "symbol": intent["symbol"], "intent": intent}

    monkeypatch.setattr("trader.us.execution.order_router.route_order", fake_route)
    result = route_exit_orders_immediately(
        [{"symbol": "BE", "side": "SELL", "qty": 1, "notional_usd": 2000}],
        buy_daily_notional=0.0,
        position_count=1,
        effective_budget=1000.0,
        signal_only=False,
        kis_order_allowed=True,
        current_position_symbols={"BE"},
    )
    assert len(result["orders"]) == 1 and result["orders"][0]["status"] == "ACK"
    assert result["sell_notional_routed"] == 2000
    assert calls[0][1]["current_daily_notional_usd"] == 0.0
    assert calls[0][1]["allowed_symbols"] is None
    assert calls[0][1]["current_position_symbols"] == {"BE"}


def _patch_tick_basics(monkeypatch, calls: list):
    monkeypatch.setenv("DRY_RUN", "0")
    monkeypatch.setenv("US_KIS_ORDER_ALLOWED", "1")
    monkeypatch.setenv("US_WATCHLIST_LOAD_TIMEOUT_SEC", "0")
    monkeypatch.setenv("US_ENTRY_EVAL_TIMEOUT_SEC", "1")
    monkeypatch.setattr("trader.us.market_calendar.is_us_trading_day", lambda d: True)
    monkeypatch.setattr("trader.us.market_calendar.market_phase", lambda now: "REGULAR_MID")
    monkeypatch.setattr("trader.us.budget.resolve_us_order_budget", lambda cash: {"effective_order_budget_usd": 5000.0})

    class _Provider:
        def __init__(self, offline=False):
            self.offline = offline
        def get_orderable_cash(self, symbol, exchange, price):
            return 10000.0
        def _get_client(self):
            return type("C", (), {"stats": {}})()

    monkeypatch.setattr("trader.us.data_provider.USDataProvider", _Provider)
    positions = [{"symbol": "BE", "exchange": "NASDAQ", "qty": 1, "orderable_qty": 1, "entry_price": 100.0}]
    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_positions", lambda provider=None: {"status": "OK", "positions": positions, "position_count": 1, "position_symbols": ["BE"]})
    monkeypatch.setattr("trader.us.execution.fills.get_fills_today", lambda provider=None, signal_only=False, trade_date=None: {"status": "OK", "fills": []})
    monkeypatch.setattr("trader.us.db.repos.load_today_symbols_sold", lambda trade_date=None: set())
    monkeypatch.setattr("trader.us.db.repos.save_fills", lambda fills: 0)
    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_ack_orders_with_balance", lambda provider=None, trade_date=None, env="practice": {"status": "OK", "pending_count": 0, "confirmed_count": 0, "balance_reconcile_count": 0, "unresolved_count": 0, "symbols_by_status": {}})
    monkeypatch.setattr("trader.us.db.repos.save_position_snapshot", lambda positions: 0)
    monkeypatch.setattr("trader.us.db.repos.save_reconcile_log", lambda log: True)
    monkeypatch.setattr("trader.us.db.repos.load_positions", lambda: positions)
    monkeypatch.setattr("trader.us.pb1.us_exit_position_resolver.enrich_us_positions_for_exit", lambda positions, trade_date, env, provider: (positions, {"total": 1, "ok": 1, "missing": 0, "sources": {}, "missing_symbols": []}))
    monkeypatch.setattr("trader.us.db.repos.get_today_buy_orders_count", lambda trade_date, env="practice": 0)
    monkeypatch.setattr("trader.us.db.repos.load_latest_us_prep_status", lambda trade_date: {"status": "OK"})

    class _Engine:
        def evaluate_exits(self, positions, provider, now):
            return [{"symbol": "BE", "exchange": "NASDAQ", "side": "SELL", "qty": 1, "available_qty": 1, "limit_price": 100, "notional_usd": 2000, "client_order_key": "sell-be", "trade_date": "2026-06-05"}]
        def evaluate_entries(self, *args, **kwargs):
            raise AssertionError("entry evaluation must not run when watchlist fallback fails")

    monkeypatch.setattr("trader.us.runner.trade_tick_runner._get_strategy_engine", lambda env, offline: _Engine())
    monkeypatch.setattr("trader.us.runner.trade_tick_runner.load_watchlist_from_artifact", lambda trade_date: (_ for _ in ()).throw(FileNotFoundError("no fallback")))

    def slow_watchlist(*args, **kwargs):
        calls.append(("watchlist_load", None, {}))
        time.sleep(2)
        return []

    monkeypatch.setattr("trader.us.db.repos.load_locked_us_watchlist", slow_watchlist)


def test_run_trade_tick_routes_exit_before_watchlist_timeout(monkeypatch):
    from trader.us.runner.trade_tick_runner import run_trade_tick

    calls = []
    _patch_tick_basics(monkeypatch, calls)

    def fake_route(intent, **kwargs):
        calls.append(("route_order", intent, kwargs))
        return {"status": "ACK", "side": intent["side"], "symbol": intent["symbol"], "intent": intent}

    monkeypatch.setattr("trader.us.execution.order_router.route_order", fake_route)
    reconcile_calls = []
    def fake_reconcile_ack(provider=None, trade_date=None, env="practice"):
        reconcile_calls.append("ack_reconcile")
        return {"status": "OK", "pending_count": 0, "confirmed_count": len(reconcile_calls), "balance_reconcile_count": 0, "unresolved_count": 0, "symbols_by_status": {}}
    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_ack_orders_with_balance", fake_reconcile_ack)

    result = run_trade_tick(session="am", env="practice", offline=False, force_now="2026-06-05T10:00:00-04:00")

    route_idx = next(i for i, c in enumerate(calls) if c[0] == "route_order")
    watch_idx = next(i for i, c in enumerate(calls) if c[0] == "watchlist_load")
    route_call = calls[route_idx]
    assert route_idx < watch_idx
    assert route_call[1]["side"] == "SELL"
    assert route_call[2]["allowed_symbols"] is None
    assert "BE" in route_call[2]["current_position_symbols"]
    assert result["entry_degraded"] == 1
    assert result["entry_intents"] == 0
    assert result["orders_ack"] == 1
    assert result["status"] != "FAILED"
    assert result["orders"][0]["status"] == "ACK"
    assert result["exit_routed_before_entry"] == 1
    assert len(reconcile_calls) == 2
    assert result["ack_reconcile_before_route_confirmed_count"] == 1
    assert result["ack_reconcile_after_route_confirmed_count"] == 2
    assert result["pending_order_count"] == 0


def test_sell_notional_does_not_consume_buy_daily_notional(monkeypatch):
    calls = []

    def fake_route(intent, **kwargs):
        calls.append((intent, kwargs))
        return {"status": "ACK", "side": intent["side"], "symbol": intent["symbol"], "intent": intent}

    monkeypatch.setattr("trader.us.execution.order_router.route_order", fake_route)
    result = route_exit_orders_immediately(
        [{"symbol": "BE", "side": "SELL", "qty": 1, "notional_usd": 2000}],
        buy_daily_notional=0.0,
        position_count=1,
        effective_budget=2500.0,
        signal_only=False,
        kis_order_allowed=True,
        current_position_symbols={"BE"},
    )
    assert result["sell_notional_routed"] == 2000
    assert calls[0][1]["current_daily_notional_usd"] == 0.0
