"""run_trade_tick-level early-hard-stop proof with a fake broker only.

When enabled, A must reach the existing route_order wrapper before B's
unrelated quote. The original path waits for B. The synthetic broker does not
send KIS orders. Reconciliation/owner gates are NOT mocked out of production.
"""
from __future__ import annotations

import pytest


@pytest.mark.parametrize("early_enabled", [False, True])
@pytest.mark.parametrize("route_status", ["ACK", "BROKER_SUBMIT_RESULT_UNKNOWN"])
def test_tick_hard_stop_early_does_not_wait_for_unrelated_quote(
    monkeypatch, early_enabled, route_status,
):
    from tests.us.test_us_exit_first_routing import _patch_tick_basics
    from trader.us.db import repos
    from trader.us.runner import trade_tick_runner
    from trader.us.pb1.us_pb1_engine import USPb1Engine

    repos.reset_memory_stores()
    calls = []
    _patch_tick_basics(monkeypatch, calls)
    monkeypatch.setenv("US_EARLY_HARD_STOP_PILOT_ENABLED", "1" if early_enabled else "0")
    monkeypatch.setenv("US_PROTECTIVE_TP_CANCEL_RECOVERY_ENABLED", "0")
    monkeypatch.setenv("US_ENTRY_ENABLED", "0")
    monkeypatch.setenv("KIS_ENV", "practice")
    # This fixture explicitly models clean, available durable-order evidence.
    # Strict early routing must refuse to submit if either lookup fails.
    monkeypatch.setattr(
        "trader.us.protective_tp_recovery.load_crossday_unresolved_tp_lifecycles",
        lambda *args, **kwargs: set(),
    )
    monkeypatch.setattr(
        "trader.us.db.repos.has_pending_order_for_symbol_side",
        lambda **kwargs: False,
    )
    positions = [
        {"symbol": "AAA", "strategy_owner": "US_STANDARD",
         "exchange": "NASDAQ", "qty": 2, "orderable_qty": 2,
         "entry_price": 100.0, "current_price_usd": 90.0,
         "position_lifecycle_id": "A-LIFE"},
        {"symbol": "BBB", "strategy_owner": "US_STANDARD",
         "exchange": "NASDAQ", "qty": 2, "orderable_qty": 2,
         "entry_price": 100.0, "current_price_usd": 101.0,
         "position_lifecycle_id": "B-LIFE"},
    ]

    class QuoteProvider:
        stats = {"daily_http_call_count": 0}
        def __init__(self, offline=False):
            self._offline = offline

        def get_current_price(self, symbol, exchange):
            calls.append(("quote_" + symbol, None, {}))
            return {"last": "90.0" if symbol == "AAA" else "101.0"}

        def get_orderable_cash(self, symbol, exchange, price):
            return 10000.0

        def get_client_stats(self):
            return self.stats

    monkeypatch.setattr("trader.us.data_provider.USDataProvider", QuoteProvider)
    monkeypatch.setattr(
        "trader.us.execution.reconcile.reconcile_positions",
        lambda **kwargs: {
            "status": "OK", "positions": positions, "position_count": 2,
            "position_symbols": ["AAA", "BBB"], "authoritative_positions": True,
            "balance_fetch_status": "OK", "balance_parse_status": "OK",
            "preserve_previous_positions": False,
            "account_equity_usd": 10000, "account_equity_source": "KIS",
            "total_pvs": 382.0, "total_pvs_semantics": "holdings_market_value_usd",
        },
    )
    monkeypatch.setattr("trader.us.db.repos.load_positions",
                        lambda *args, **kwargs: positions)
    monkeypatch.setattr(
        "trader.us.execution.reconcile.reconcile_ack_orders_with_balance",
        lambda **kwargs: {
            "status": "OK", "pending_count": 0,
            "confirmed_count": 0, "unresolved_count": 0,
            "balance_reconcile_count": 0, "symbols_by_status": {},
        },
    )
    monkeypatch.setattr(
        "trader.us.execution.order_journal.replay_order_journal",
        lambda *args, **kwargs: {
            "status": "OK", "failed_count": 0, "unresolved_count": 0,
            "active_claims_scanned": 0,
        },
    )
    monkeypatch.setattr(
        "trader.us.pb1.us_exit_position_resolver.enrich_us_positions_for_exit",
        lambda p, **kwargs: (p, {
            "total": len(p), "ok": len(p), "missing": 0,
            "sources": {}, "missing_symbols": [],
        }),
    )
    state_updates = {"risk": [], "hwm": []}
    def update_risk(**kwargs):
        state_updates["risk"].append(kwargs["symbol"])
        return {"soft_stop_breach_count": 0}
    def update_hwm(**kwargs):
        state_updates["hwm"].append(kwargs["symbol"])
        return {
            "high_watermark": kwargs["current_price"],
            "lifecycle_id": kwargs["lifecycle_id"],
        }
    monkeypatch.setattr(
        "trader.us.db.repos.update_us_soft_stop_risk_state", update_risk,
    )
    monkeypatch.setattr(
        "trader.us.position_lifecycle_state.update_us_position_high_watermark",
        update_hwm,
    )
    monkeypatch.setattr(trade_tick_runner, "_get_strategy_engine",
                        lambda env, offline: USPb1Engine(env=env, offline=offline))
    monkeypatch.setattr("trader.us.db.price_daily_repo.load_recent_us_daily_bars",
                        lambda *a, **k: [])
    monkeypatch.setattr(
        "trader.us.market_state_overlay.evaluate_us_market_state",
        lambda **kwargs: {
            "market_state": "NORMAL", "exposure_multiplier": 1.0,
            "allow_new_buy": False, "allow_add_to_existing": False,
            "force_entry_block": False,
        },
    )
    monkeypatch.setattr("trader.us.market_state_overlay.build_profit_capture_intents",
                        lambda *args, **kwargs: [])
    monkeypatch.setattr("trader.us.market_state_overlay.build_defense_trim_intents",
                        lambda *args, **kwargs: [])
    monkeypatch.setattr(
        "trader.us.portfolio_cluster_guard.evaluate_portfolio_cluster_guard",
        lambda *args, **kwargs: {
            "portfolio_cluster_guard_status": "OK",
            "portfolio_ai_tech_weight": 0,
            "portfolio_cluster_cap_violations": [],
            "cluster_guard_trim_intents": [],
        },
    )
    def fake_route(intent, **kwargs):
        calls.append(("submit_" + intent["symbol"], intent, kwargs))
        return {
            "status": route_status, "side": intent["side"],
            "symbol": intent["symbol"], "intent": intent,
            "requires_reconcile": route_status != "ACK",
        }
    monkeypatch.setattr("trader.us.execution.order_router.route_order", fake_route)

    result = trade_tick_runner.run_trade_tick(
        session="am", env="practice", offline=False,
        force_now="2026-10-15T10:00:00-04:00",
        kis_order_allowed=True, locked_watchlist_cache=[],
        watchlist_cache_source="test",
    )
    events = [call[0] for call in calls]
    assert events.count("submit_AAA") == 1, events
    assert "quote_BBB" in events
    if early_enabled:
        assert events.index("submit_AAA") < events.index("quote_BBB"), events
    else:
        assert events.index("quote_BBB") < events.index("submit_AAA"), events
    assert result["exit_intents"] == 1
    assert result["orders"][0]["status"] == route_status
    # Safety-pass streaming may not repeat soft-stop ticks or high-watermarks.
    assert state_updates["risk"].count("AAA") == 1
    assert state_updates["risk"].count("BBB") == 1
    assert state_updates["hwm"].count("AAA") == 1
    assert state_updates["hwm"].count("BBB") == 1
