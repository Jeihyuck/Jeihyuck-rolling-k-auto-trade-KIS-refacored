"""PB1 continuous slow ticks: real tick → preflight/risk → router → stub KIS ACK.

The test intentionally keeps nonzero repeated latency on *all* ticks, unlike
the legacy test where only the first two are slow. This is a scaled replay,
not an extrapolation of real 300s wall-clock p95.
"""
from __future__ import annotations

import time

from trader.us.runner.trade_tick_runner import run_trade_tick


TICKERS = (
    "AAPL MSFT NVDA AMZN GOOGL META TSLA AMD MRVL QCOM AVGO ORCL CRM "
    "IBM INTC PYPL JPM BAC WMT COST GE CAT NKE KO PEP TMO ABBV UNH ISRG ADBE"
).split()


def test_sustained_ten_slow_ticks_reach_real_buy_router_and_mock_kis_ack(
    monkeypatch, tmp_path,
):
    from trader.us.db import repos
    from trader.us.execution import order_router
    from trader.us.runner import trade_tick_runner

    for key, value in {
        "DRY_RUN": "0", "KIS_ENV": "practice", "STRATEGY_ENV": "practice",
        "TRADING_REGION": "US", "US_AGENT_ENABLED": "1",
        "US_PAPER_TRADING_ENABLED": "1", "US_KIS_ORDER_ALLOWED": "1",
        "US_SESSION_WINDOW_VALID": "1", "US_PREP_CONTRACT_OK": "1",
        "US_BALANCE_AVAILABLE": "1", "US_TQQQ_INFINITE_ENABLED": "0",
        "US_MAX_DAILY_NOTIONAL_USD": "100000",
        "US_MAX_TOTAL_NOTIONAL_USD": "100000",
        "US_ENTRY_EVAL_TIMEOUT_SEC": "20", "US_TICK_DEADLINE_SEC": "360",
        "US_TICK_TIMEOUT_SEC": "360", "US_ALLOW_LEGACY_PREP_FOR_TEST": "1",
        "US_ORDER_JOURNAL_DIR": str(tmp_path / "journal"),
        "US_MIN_ENTRY_EVAL_BUDGET_SEC": "5",
        "US_TICK_NONCRITICAL_MIN_REMAINING_SEC": "5",
    }.items():
        monkeypatch.setenv(key, value)

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos.reset_memory_stores()
    # A real practice order is routed to this stub, never the network.
    class Broker:
        stats = {}
        def __init__(self):
            self.buy_calls = []
            self.sell_calls = []
        def place_us_buy_order(self, symbol, exchange, qty, price):
            self.buy_calls.append((symbol, exchange, qty, price))
            return {"rt_cd": "0", "output": {"ODNO": f"MOCK-BUY-{len(self.buy_calls)}"}}
        def place_us_sell_order(self, symbol, exchange, qty, price):
            self.sell_calls.append((symbol, exchange, qty, price))
            return {"rt_cd": "0", "output": {"ODNO": f"MOCK-SELL-{len(self.sell_calls)}"}}
        def get_balance(self, force_refresh=False):
            return {"positions": [], "balance_parse_status": "OK",
                    "balance_authoritative": True, "balance_complete": True}
    broker = Broker()

    delay_events = []
    class Provider:
        def __init__(self, offline=False):
            self.offline = offline
        def _get_client(self):
            return broker
        def get_orderable_cash(self, symbol, exchange, price):
            time.sleep(0.005)
            delay_events.append("cash")
            return 15000.0
        def get_current_price(self, symbol, exchange):
            return {"last": 100.0, "source": "test-fresh-quote", "quality": "ok"}
        def get_balance(self, force_refresh=False):
            return broker.get_balance(force_refresh)
    monkeypatch.setattr("trader.us.data_provider.USDataProvider", Provider)
    monkeypatch.setattr("trader.us.market_calendar.is_us_trading_day", lambda _day: True)
    monkeypatch.setattr("trader.us.market_calendar.market_phase", lambda _now: "REGULAR_MID")
    monkeypatch.setattr("trader.us.budget.resolve_us_order_budget", lambda _cash: {
        "effective_order_budget_usd": 10000.0, "capital_usd_cap": 10000.0,
    })
    def slow_balance(**_kwargs):
        time.sleep(0.005)
        delay_events.append("balance")
        return {
            "status": "OK", "positions": [], "position_count": 0,
            "position_symbols": [], "balance_fetch_status": "OK",
            "balance_parse_status": "OK", "authoritative_positions": True,
            "preserve_previous_positions": False, "total_pvs": 0.0,
            "total_pvs_semantics": "holdings_market_value_usd",
            "account_equity_usd": 15000.0,
            "account_equity_source": "kis_broker_authoritative",
        }
    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_positions", slow_balance)

    def slow_fills(**_kwargs):
        time.sleep(0.005)
        delay_events.append("fills")
        return {"status": "OK", "fills": []}
    monkeypatch.setattr("trader.us.execution.fills.get_fills_today", slow_fills)
    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_ack_orders_with_balance",
                        lambda **_kw: {"status": "OK", "pending_count": 0,
                                      "unresolved_count": 0, "confirmed_count": 0,
                                      "open_order_pending_count": 0, "symbols_by_status": {}})
    monkeypatch.setattr("trader.us.db.repos.load_today_symbols_sold", lambda **_kw: set())
    monkeypatch.setattr("trader.us.db.repos.save_position_snapshot", lambda *_a, **_kw: 0)
    monkeypatch.setattr("trader.us.db.repos.save_reconcile_log", lambda *_a, **_kw: True)
    monkeypatch.setattr("trader.us.db.repos.get_today_buy_orders_count", lambda *_a, **_kw: 0)
    monkeypatch.setattr("trader.us.db.repos.load_today_committed_buy_notional", lambda *_a, **_kw: 0.0)
    monkeypatch.setattr("trader.us.execution.order_journal.append_order_event", lambda *_a, **_kw: True)
    monkeypatch.setattr("trader.us.market_state_overlay.evaluate_us_market_state", lambda **_kw: {
        "market_state": "NORMAL", "market_regime": "RISK_ON",
        "allow_new_buy": True, "allow_add_to_existing": True,
        "exposure_multiplier": 1.0, "force_entry_block": False,
        "max_single_cluster_ratio": 1.0, "max_ai_tech_ratio": 1.0,
    })
    monkeypatch.setattr("trader.us.portfolio_cluster_guard.evaluate_portfolio_cluster_guard",
                        lambda *_a, **_kw: {
                            "portfolio_cluster_guard_status": "OK",
                            "portfolio_ai_tech_weight": 0.0,
                            "portfolio_cluster_cap_violations": [],
                            "cluster_guard_trim_intents": [],
                            "cluster_guard_trim_notional": 0.0,
                        })
    monkeypatch.setattr("trader.us.runner.trade_tick_runner.evaluate_balance_error_circuit",
                        lambda *_a, **_kw: {"entry_can_proceed": True,
                                            "balance_consecutive_failed_ticks": 0})
    monkeypatch.setattr("trader.us.execution.order_router.same_day_semantic_sell_exists",
                        lambda *_a, **_kw: False)

    rows = [
        {"symbol": name, "exchange": "NASDAQ", "score": 0.95 - i * .003,
         "score_final": 0.95 - i * .003, "trend_score": .90,
         "theme_cluster": "OTHER", "rank_final30": i + 1,
         "classification_source": "locked_final30_fixture",
         "position_state": "NOT_HELD", "position_action": "OPEN_NEW_POSITION"}
        for i, name in enumerate(TICKERS)
    ]
    prep_contract = {
        "status": "OK", "contract_version": "us_sector_rotation_v3",
        "market_regime_version": "us_leading_regime_v1",
        "entry_can_proceed": 1, "trade_can_proceed": 1,
        "final30_trade_ready": True, "cluster_contract_ok": True,
        "cap_violations": [], "trade_block_reason": "",
        "rotation_regime": "BROAD_UP", "allow_new_buy": True,
        "allow_ai_tech_buy": True, "capital_scale": 1.0,
        "max_new_positions": 3, "effective_max_new_positions": 3,
    }
    prep = {"status": "OK", "result": prep_contract}
    seen_eval = []

    class Engine:
        def __init__(self):
            self.last_entry_diagnostics = {"blocked": []}
        def evaluate_exits(self, positions, provider, now):
            return []
        def evaluate_entries(self, *_args, **kwargs):
            index = len(seen_eval)
            symbol = TICKERS[index]
            seen_eval.append(symbol)
            intent = {
                "symbol": symbol, "exchange": "NASDAQ", "trade_date": "2026-10-06",
                "side": "BUY", "qty": 1, "limit_price": 100.0,
                "notional_usd": 100.0, "client_order_key": f"pb1-sustained-{index}",
                "strategy_owner": "US_STANDARD", "strategy_name": "US_STANDARD",
                "position_action": "OPEN_NEW_POSITION",
                "position_state": "NOT_HELD",
                "theme_cluster": "OTHER",
                "classification_source": "locked_final30_fixture",
                "score_final": .95, "rank_final30": index + 1,
                "market_state": "NORMAL", "market_regime": "RISK_ON",
                "meta": {
                    "position_action": "OPEN_NEW_POSITION",
                    "position_state": "NOT_HELD",
                    "theme_cluster": "OTHER",
                    "classification_source": "locked_final30_fixture",
                    "score_final": .95, "rank_final30": index + 1,
                    "market_state": "NORMAL", "market_regime": "RISK_ON",
                },
            }
            decision = kwargs["intent_acceptor"](intent)
            self.last_entry_diagnostics = {"blocked": [], "accepted": int(decision.allowed)}
            return [intent] if decision.allowed else []
    monkeypatch.setattr(trade_tick_runner, "_get_strategy_engine", lambda **_kw: Engine())

    results = []
    for tick_index in range(1, 11):
        result = run_trade_tick(
            session="am", env="practice", offline=False,
            force_now=f"2026-10-06T10:{tick_index:02d}:00-04:00",
            tick_index=tick_index, kis_order_allowed=True,
            prep_status_cache=prep, locked_watchlist_cache=rows,
            prep_cache_source="test", watchlist_cache_source="test",
            entry_can_proceed=True, exit_can_proceed=True,
        )
        results.append(result)
    assert len(seen_eval) == 10, [(x.get("entry_degraded_reason"), x.get("last_stage")) for x in results]
    assert len(broker.buy_calls) == 10, [{
        "status": x.get("status"), "degraded": x.get("entry_degraded_reason"),
        "preflight": x.get("preflight_rejected_candidates"),
        "router": x.get("router_blocked_after_preflight"),
        "orders": x.get("orders"),
    } for x in results]
    assert all(x.get("orders_ack", 0) >= 1 for x in results)
    assert all(x.get("exit_routed_before_entry") == 1 for x in results)
    assert len(delay_events) >= 20, delay_events
    assert len({call[0] for call in broker.buy_calls}) == 10
    assert broker.sell_calls == []
