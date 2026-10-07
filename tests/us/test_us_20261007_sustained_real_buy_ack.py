"""PB1 continuous slow ticks: real tick → preflight/risk → router → stub KIS ACK.

The test intentionally keeps nonzero repeated latency on *all* ticks, unlike
the legacy test where only the first two are slow. This is a scaled replay,
not an extrapolation of real 300s wall-clock p95.
"""
from __future__ import annotations

import time
import os
import pytest
import uuid

from trader.us.runner.trade_tick_runner import run_trade_tick
from tests.us.test_us_postgres_integration_real import pg_engine


TICKERS = (
    "AAPL MSFT NVDA AMZN GOOGL META TSLA AMD MRVL QCOM AVGO ORCL CRM "
    "IBM INTC PYPL JPM BAC WMT COST GE CAT NKE KO PEP TMO ABBV UNH ISRG ADBE"
).split()


@pytest.mark.skipif(not os.getenv("PBCORE_TEST_POSTGRES_URL"), reason="requires real PostgreSQL broker-claim fixture")
@pytest.mark.parametrize("unresolved_ticks", [(), (4, 5)])
def test_sustained_ten_slow_ticks_reach_real_buy_router_and_mock_kis_ack(
    monkeypatch, tmp_path, pg_engine, unresolved_ticks,
):
    from trader.us.db import repos
    from trader.us.execution import order_router
    from trader.us.runner import trade_tick_runner

    # This acceptance is executed twice in the same PostgreSQL service (full suite
    # and the explicit metrics step), and is parametrized.  Durable order keys
    # must therefore identify this individual scenario run rather than collide
    # with evidence intentionally left by a preceding invocation.
    run_token = uuid.uuid4().hex[:12]
    protective_key = f"pb1-protective-sell-before-buy-{run_token}"

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

    # Real postgres and the production claim acquisition code are kept on.
    # A DB-offline run is expected to block BUY, never fabricate a mock ACK.
    monkeypatch.setattr(repos, "_active_us_epoch", lambda *_a, **_kw: "incident-epoch")
    repos.reset_memory_stores()
    from sqlalchemy import event
    sql_executions = {"read": 0, "write": 0}
    def record_statement(_conn, _cursor, statement, _params, _context, _many):
        command = str(statement).lstrip().split(maxsplit=1)[0].upper()
        if command in {"SELECT", "WITH"}:
            sql_executions["read"] += 1
        elif command in {"INSERT", "UPDATE", "DELETE"}:
            sql_executions["write"] += 1
    event.listen(pg_engine, "before_cursor_execute", record_statement)
    # A real practice order is routed to this stub, never the network.
    class Broker:
        stats = {}
        def __init__(self):
            self.buy_calls = []
            self.sell_calls = []
            self.submission_sequence = []
        def place_us_buy_order(self, symbol, exchange, qty, price):
            self.buy_calls.append((symbol, exchange, qty, price))
            self.submission_sequence.append(("BUY", symbol))
            return {"rt_cd": "0", "output": {"ODNO": f"MOCK-BUY-{len(self.buy_calls)}"}}
        def place_us_sell_order(self, symbol, exchange, qty, price):
            self.sell_calls.append((symbol, exchange, qty, price))
            self.submission_sequence.append(("SELL", symbol))
            return {"rt_cd": "0", "output": {"ODNO": f"MOCK-SELL-{len(self.sell_calls)}"}}
        def get_orderable_cash(self, symbol, exchange, price):
            return 15000.0
        def get_balance(self, force_refresh=False):
            positions = [] if self.sell_calls else [{
                "symbol": "BE", "qty": 2, "orderable_qty": 2,
                "avg_price_usd": 100.0, "exchange": "NASDAQ",
            }]
            return {"positions": positions, "balance_parse_status": "OK",
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
        positions = [] if broker.sell_calls else [{
            "symbol": "BE", "exchange": "NASDAQ", "qty": 2, "orderable_qty": 2,
            "entry_price": 100.0, "avg_price_usd": 100.0,
            "position_lifecycle_id": "be-first-protective",
            "strategy_owner": "US_STANDARD",
        }]
        return {
            "status": "OK", "positions": positions, "position_count": len(positions),
            "position_symbols": [p["symbol"] for p in positions], "balance_fetch_status": "OK",
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
    current_tick = {"index": 0}
    def reconcile_ack_stub(**_kw):
        unresolved = current_tick["index"] in unresolved_ticks
        return {
            "status": "WARN" if unresolved else "OK",
            "pending_count": int(unresolved),
            "unresolved_count": int(unresolved),
            "confirmed_count": 0,
            "open_order_pending_count": int(unresolved),
            "symbols_by_status": {},
        }
    monkeypatch.setattr(
        "trader.us.execution.reconcile.reconcile_ack_orders_with_balance",
        reconcile_ack_stub,
    )
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
    accepted_intents = []

    class Engine:
        def __init__(self):
            self.last_entry_diagnostics = {"blocked": []}
        def evaluate_exits(self, positions, provider, now):
            if broker.sell_calls:
                return []
            return [{
                "symbol": "BE", "exchange": "NASDAQ", "trade_date": "2026-10-06",
                "side": "SELL", "qty": 1, "limit_price": 90.0,
                "notional_usd": 90.0, "client_order_key": protective_key,
                "position_lifecycle_id": "be-first-protective",
                "strategy_owner": "US_STANDARD",
                "reason": "HARD_STOP_LOSS", "semantic_action": "HARD_STOP_LOSS",
                "meta": {"position_lifecycle_id": "be-first-protective",
                         "strategy_owner": "US_STANDARD", "reason": "HARD_STOP_LOSS",
                         "avg_cost": 100.0},
            }]
        def evaluate_entries(self, *_args, **kwargs):
            index = len(seen_eval)
            symbol = TICKERS[index]
            seen_eval.append(symbol)
            intent = {
                "symbol": symbol, "exchange": "NASDAQ", "trade_date": "2026-10-06",
                "side": "BUY", "qty": 1, "limit_price": 100.0,
                "notional_usd": 100.0, "client_order_key": f"pb1-sustained-{run_token}-{index}",
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
            if decision.allowed:
                accepted_intents.append(dict(intent))
            return [intent] if decision.allowed else []
    monkeypatch.setattr(trade_tick_runner, "_get_strategy_engine", lambda **_kw: Engine())

    results = []
    for tick_index in range(1, 11):
        current_tick["index"] = tick_index
        _prior_buy_count = len(broker.buy_calls)
        result = run_trade_tick(
            session="am", env="practice", offline=False,
            force_now=f"2026-10-06T10:{tick_index:02d}:00-04:00",
            tick_index=tick_index, kis_order_allowed=True,
            prep_status_cache=prep, locked_watchlist_cache=rows,
            prep_cache_source="test", watchlist_cache_source="test",
            entry_can_proceed=True, exit_can_proceed=True,
        )
        results.append(result)
        if tick_index in unresolved_ticks:
            assert len(broker.buy_calls) == _prior_buy_count, result
    expected_buys = 10 - len(unresolved_ticks)
    assert len(seen_eval) == expected_buys, [(x.get("entry_degraded_reason"), x.get("last_stage")) for x in results]
    assert len(broker.buy_calls) == expected_buys, [{
        "status": x.get("status"), "degraded": x.get("entry_degraded_reason"),
        "preflight": x.get("preflight_rejected_candidates"),
        "router": x.get("router_blocked_after_preflight"),
        "orders": x.get("orders"),
    } for x in results]
    assert all(x.get("orders_ack", 0) >= 1 for i, x in enumerate(results, 1) if i not in unresolved_ticks)
    assert all(x.get("exit_routed_before_entry") == 1 for x in results)
    assert len(delay_events) >= 20, delay_events
    assert len({call[0] for call in broker.buy_calls}) == expected_buys
    assert len(broker.sell_calls) == 1, broker.sell_calls
    assert broker.submission_sequence[0] == ("SELL", "BE"), broker.submission_sequence
    assert sql_executions["read"] > 0, sql_executions
    assert sql_executions["write"] > 0, sql_executions
    assert len(broker.submission_sequence) == 1 + expected_buys

    # A second attempt with the same durable client key cannot reach KIS,
    # even when replayed after all ten scheduled ticks have completed.
    from trader.us.execution.order_router import route_order
    duplicate = dict(accepted_intents[0])
    prev_kis_submits = len(broker.submission_sequence)
    duplicate_result = route_order(
        duplicate, kis_client=broker, current_daily_notional_usd=0.0,
        total_portfolio_usd=15000.0, available_cash_usd=15000.0,
        current_position_symbols={"BE"},
    )
    assert duplicate_result["status"] != "ACK", duplicate_result
    assert len(broker.submission_sequence) == prev_kis_submits
    print("US_PB1_BUY_ACK_10TICK_METRICS=" + __import__("json").dumps({
        "ticks": 10, "buy_ack": len(broker.buy_calls),
        "unresolved_ticks": list(unresolved_ticks),
        "blocked_buys_on_unresolved_ticks": len(unresolved_ticks),
        "protective_sell_ack": len(broker.sell_calls),
        "first_broker_submit": broker.submission_sequence[0][0],
        "sql_read_total": sql_executions["read"],
        "sql_write_total": sql_executions["write"],
        "kis_submit_count": len(broker.submission_sequence),
        "duplicate_replay_blocked": True,
        "entry_defer_count": sum(bool(x.get("entry_degraded")) for x in results),
        "slot_overrun_300s": sum(
            float(x.get("tick_total_ms") or x.get("latency_metrics", {}).get("tick_total_ms") or 0) > 300000
            for x in results
        ),
        "measurement": "scaled_ci_nonproduction_latency",
    }, sort_keys=True))
