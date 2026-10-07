from __future__ import annotations

import json
import time
from pathlib import Path

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
    monkeypatch.setenv("US_ALLOW_LEGACY_PREP_FOR_TEST", "1")
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
    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_positions", lambda provider=None, trade_date=None: {"status": "OK", "positions": positions, "position_count": 1, "position_symbols": ["BE"]})
    monkeypatch.setattr("trader.us.execution.fills.get_fills_today", lambda provider=None, signal_only=False, trade_date=None: {"status": "OK", "fills": []})
    monkeypatch.setattr("trader.us.db.repos.load_today_symbols_sold", lambda trade_date=None: set())
    monkeypatch.setattr("trader.us.db.repos.save_fills", lambda fills: 0)
    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_ack_orders_with_balance", lambda provider=None, trade_date=None, env="practice": {"status": "OK", "pending_count": 0, "confirmed_count": 0, "balance_reconcile_count": 0, "unresolved_count": 0, "symbols_by_status": {}})
    monkeypatch.setattr("trader.us.db.repos.save_position_snapshot", lambda positions, **kwargs: 0)
    monkeypatch.setattr("trader.us.db.repos.save_reconcile_log", lambda log, **kwargs: True)
    monkeypatch.setattr(
        "trader.us.db.repos.load_positions",
        lambda trade_date=None, **_kwargs: positions,
    )
    monkeypatch.setattr("trader.us.pb1.us_exit_position_resolver.enrich_us_positions_for_exit", lambda positions, trade_date, env, provider: (positions, {"total": 1, "ok": 1, "missing": 0, "sources": {}, "missing_symbols": []}))
    monkeypatch.setattr("trader.us.db.repos.get_today_buy_orders_count", lambda trade_date, env="practice": 0)
    monkeypatch.setattr("trader.us.db.repos.load_today_committed_buy_notional", lambda *args, **kwargs: 0.0)
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

    result = run_trade_tick(session="am", env="practice", offline=False, force_now="2026-06-05T10:00:00-04:00", kis_order_allowed=False)

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


def test_sep30_crossday_soft_sell_routes_through_tick_before_optional_entry(monkeypatch, tmp_path):
    from trader.us.db import repos
    from trader.us.pb1 import us_exit_position_resolver as resolver
    from trader.us.runner.trade_tick_runner import run_trade_tick

    fixture = json.loads(
        (Path(__file__).parent / "fixtures/runtime_integrity/incident_20260930_liveness_minhold.json.fixture")
        .read_text(encoding="utf-8")
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GITHUB_RUN_ID", fixture["session"]["run_id"])
    monkeypatch.setenv("GITHUB_SHA", fixture["code_sha"])
    for name, value in fixture["settings"].items():
        monkeypatch.setenv(name, str(value))
    calls = []
    production_enricher = resolver.enrich_us_positions_for_exit
    _patch_tick_basics(monkeypatch, calls)
    monkeypatch.setenv("US_WATCHLIST_LOAD_TIMEOUT_SEC", "0")
    identity = {
        "env": "practice",
        "account_id": "practice:test-account",
        "trading_epoch_id": "epoch-cross-day",
        "strategy_owner": fixture["initial_positions"][0]["strategy_owner"],
        "position_lifecycle_id": fixture["initial_positions"][0]["position_lifecycle_id"],
    }
    position = {
        **fixture["initial_positions"][0],
        "symbol": fixture["initial_positions"][0]["symbol"],
        "exchange": "NASDAQ",
        "entry_price": fixture["prices"]["SAMPLE_entry"],
        "entry_price_source": "us_positions_avg_cost",
        "current_px": fixture["prices"]["SAMPLE_current"],
        "min_hold_minutes": 390,
        **identity,
    }
    monkeypatch.setattr(
        "trader.us.execution.reconcile.reconcile_positions",
        lambda **_kwargs: {
            "status": "OK", "positions": [position], "position_count": 1,
            "position_symbols": [position["symbol"]], "authoritative_positions": True,
            "balance_fetch_status": "OK", "balance_parse_status": "OK",
            "preserve_previous_positions": False,
            "total_pvs": position["current_px"],
            "total_pvs_semantics": "holdings_market_value_usd",
            "account_equity_usd": fixture["initial_balance"]["account_equity_usd"],
            "account_equity_source": fixture["initial_balance"]["source"],
        },
    )
    monkeypatch.setattr(repos, "load_positions", lambda *_args, **_kwargs: [position])
    monkeypatch.setattr(
        repos,
        "load_latest_us_prep_status",
        lambda *_args, **_kwargs: {
            "status": fixture["prep"]["status"],
            "run_id": fixture["prep"]["run_id"],
        },
    )
    monkeypatch.setattr(resolver, "_identity_is_current", lambda value: value == identity)
    monkeypatch.setattr(
        resolver,
        "enrich_us_positions_for_exit",
        production_enricher,
    )
    monkeypatch.setattr(
        repos,
        "load_us_position_risk_state_candidates",
        lambda _symbol, _trade_date: [{
            "trade_date": "2026-09-22",
            "trading_epoch_id": identity["trading_epoch_id"],
            "state": {
                "lifecycle": {
                    "lifecycle_id": identity["position_lifecycle_id"],
                    "is_open": True,
                    "opened_trade_date": fixture["initial_positions"][0]["opened_at"][:10],
                    "opened_at": fixture["initial_positions"][0]["opened_at"],
                    "opened_at_source": "confirmed_buy_fill",
                    "holding_trade_days": 7,
                    **identity,
                },
                "high_watermark": 997.735,
            },
            **identity,
        }],
    )
    monkeypatch.setattr(
        "trader.us.position_lifecycle_state.reconcile_us_position_lifecycles",
        lambda **_kwargs: {
            "SAMPLE": {
                "lifecycle_id": identity["position_lifecycle_id"],
                "is_open": True,
                "opened_trade_date": fixture["initial_positions"][0]["opened_at"][:10],
                "opened_at": fixture["initial_positions"][0]["opened_at"],
                "opened_at_source": "confirmed_buy_fill",
                "holding_trade_days": 7,
                **identity,
            }
        },
    )
    from trader.us.pb1.us_exit_router import route_exit_by_book_horizon

    monkeypatch.setattr(
        "trader.us.pb1.us_exit_engine.evaluate_exit",
        lambda position, current_price, now, include_trend_time=True: {
            "symbol": position["symbol"],
            "exchange": position["exchange"],
            "side": "SELL",
            "qty": 1,
            "available_qty": 1,
            "limit_price": current_price,
            "notional_usd": current_price,
            "exit_type": "soft_stop_loss",
            "reason": "synthesized_soft_exit_signal",
            "client_order_key": "sep30-cross-day-soft-sell",
        },
    )
    enriched_seen = []

    class CrossDayExitEngine:
        def evaluate_exits(self, positions, provider, now):
            enriched_seen.extend(positions)
            return [
                intent
                for position in positions
                if (intent := route_exit_by_book_horizon(
                    position=position,
                    current_price=position["current_px"],
                    now=now,
                ))
            ]

        def evaluate_entries(self, *_args, **_kwargs):
            raise AssertionError("optional entry evaluation must not precede the valid SELL")

    monkeypatch.setattr(
        "trader.us.runner.trade_tick_runner._get_strategy_engine",
        lambda **_kwargs: CrossDayExitEngine(),
    )
    monkeypatch.setattr(
        "trader.us.execution.order_router.route_order",
        lambda intent, **_kwargs: (
            calls.append(("route_order", intent, {}))
            or {"status": "ACK", "side": intent["side"], "symbol": intent["symbol"], "intent": intent}
        ),
    )

    result = run_trade_tick(
        session="am",
        env="practice",
        offline=False,
        force_now=fixture["clock"]["timestamp"],
        kis_order_allowed=True,
    )

    assert enriched_seen, {
        key: result.get(key)
        for key in ("status", "last_stage", "exit_can_proceed", "position_count", "exit_intents", "error")
    }
    route_index = next(i for i, item in enumerate(calls) if item[0] == "route_order")
    assert enriched_seen[0]["opened_at"] == fixture["initial_positions"][0]["opened_at"]
    assert calls[route_index][1]["side"] == "SELL"
    assert result["exit_routed_before_entry"] == 1
    assert result["orders_ack"] == 1
    assert result["sell_liveness_status"] == "OK"


def test_sep30_session_services_valid_entry_after_repeated_production_ticks(monkeypatch, tmp_path):
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from trader.us.runner.trade_session_runner import run_trade_session

    fixture = json.loads(
        (Path(__file__).parent / "fixtures/runtime_integrity/incident_20260930_liveness_minhold.json.fixture")
        .read_text(encoding="utf-8")
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GITHUB_RUN_ID", fixture["session"]["run_id"])
    monkeypatch.setenv("GITHUB_SHA", fixture["code_sha"])
    monkeypatch.setenv("PBCORE_DB_URL", "")
    monkeypatch.setenv("US_DISABLE_TICK_PROCESS_ISOLATION", "1")
    monkeypatch.setenv("DRY_RUN", "1")
    monkeypatch.setenv("DISABLE_LIVE_TRADING", "0")
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "0")
    monkeypatch.setenv("US_LIVE_TRADING_ENABLED", "0")
    monkeypatch.setenv("US_ORDER_ARMED", "0")
    monkeypatch.setenv("US_MIN_ENTRY_EVAL_BUDGET_SEC", "0.05")
    monkeypatch.setenv("US_SESSION_SHUTDOWN_BUFFER_SEC", "0")
    monkeypatch.setenv("US_BALANCE_RECONCILE_INTERVAL_TICKS", "1")
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "0")
    for name, value in fixture["settings"].items():
        monkeypatch.setenv(name, str(value))

    calls = []
    _patch_tick_basics(monkeypatch, calls)
    from trader.us.db import repos
    from trader.us.execution import fills
    from trader.us.execution import reconcile

    fill_fetch_calls = 0
    db_snapshot_calls = 0
    current_tick = [0]
    delayed_fill_ticks = set()
    delayed_db_ticks = set()
    actual_get_fills_today = fills.get_fills_today
    actual_reconcile_positions = reconcile.reconcile_positions
    actual_save_position_snapshot = repos.save_position_snapshot

    def delayed_fill_fetch(*args, **kwargs):
        nonlocal fill_fetch_calls
        fill_fetch_calls += 1
        if current_tick[0] <= 2 and current_tick[0] not in delayed_fill_ticks:
            delayed_fill_ticks.add(current_tick[0])
            time.sleep(0.6)
        return actual_get_fills_today(*args, **kwargs)

    def authoritative_reconcile(*args, **kwargs):
        result = dict(actual_reconcile_positions(*args, **kwargs))
        result.update({
            "balance_fetch_status": "OK",
            "balance_parse_status": "OK",
            "authoritative_positions": True,
            "preserve_previous_positions": False,
        })
        return result

    def delayed_db_snapshot(*args, **kwargs):
        nonlocal db_snapshot_calls
        db_snapshot_calls += 1
        if current_tick[0] <= 2 and current_tick[0] not in delayed_db_ticks:
            delayed_db_ticks.add(current_tick[0])
            time.sleep(0.6)
        return actual_save_position_snapshot(*args, **kwargs)

    monkeypatch.setattr(fills, "get_fills_today", delayed_fill_fetch)
    monkeypatch.setattr(reconcile, "reconcile_positions", authoritative_reconcile)
    monkeypatch.setattr(repos, "save_position_snapshot", delayed_db_snapshot)
    monkeypatch.setenv("US_WATCHLIST_LOAD_TIMEOUT_SEC", "0")
    monkeypatch.setenv("US_KIS_ORDER_ALLOWED", "1")
    monkeypatch.setattr(
        "trader.us.market_calendar.now_ny",
        lambda: datetime.fromisoformat(fixture["session_tick_replay_clock"]["timestamp"]).astimezone(
            ZoneInfo("America/New_York")
        ),
    )
    monkeypatch.setattr("trader.us.market_calendar.is_us_trading_day", lambda _date: True)
    monkeypatch.setattr(
        "trader.us.utils.session_guard.acquire_us_session_running_lock",
        lambda *_args, **_kwargs: {"acquired": True},
    )
    monkeypatch.setattr("trader.us.utils.session_guard.release_us_session_running_lock", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "trader.us.utils.session_guard.check_us_session_file_guard",
        lambda *_args, **_kwargs: {"already_ran": False, "guard_status": "OK"},
    )
    monkeypatch.setattr(
        "trader.us.utils.session_guard.now_et_iso",
        lambda: fixture["session_tick_replay_clock"]["timestamp"],
    )
    monkeypatch.setattr("trader.us.utils.session_guard.write_us_session_done_file", lambda **_kwargs: None)
    monkeypatch.setattr("trader.us.budget.resolve_us_order_budget", lambda _cash: {
        "effective_order_budget_usd": 10_000.0, "capital_usd_cap": 10_000.0,
    })

    prep_contract = {
        "contract_version": "us_sector_rotation_v3",
        "market_regime_version": "us_leading_regime_v1",
        "entry_can_proceed": 1,
        "trade_can_proceed": 1,
        "final30_trade_ready": True,
        "cluster_contract_ok": True,
        "cap_violations": [],
        "trade_block_reason": "",
    }
    prep_guard = {
        "ok": True,
        "guard_state": "OK",
        "entry_can_proceed": True,
        "exit_can_proceed": True,
        "close_can_proceed": True,
        "trade_block_reason": "",
        "prep_run_id": fixture["prep"]["run_id"],
        "run_revision_mismatch": False,
    }
    monkeypatch.setattr("trader.us.prep_contract.check_us_prep_guard", lambda *_args, **_kwargs: {
        **prep_guard,
        "status": "OK",
        "prep_status": "OK",
        "final30_scored_count": 10,
        "score_nonzero_count": 10,
        "final30_trade_ready": True,
        "contract": {"entry_can_proceed": 1, "exit_can_proceed": 1},
    })
    monkeypatch.setattr("trader.us.run_manifest.verify_run_revision", lambda *_args, **_kwargs: {
        "ok": True, "entry_can_proceed": True, "expected_revision": fixture["code_sha"],
    })
    monkeypatch.setattr("trader.us.db.repos.load_latest_us_prep_status", lambda *_args, **_kwargs: {
        "status": "OK",
        "run_id": fixture["prep"]["run_id"],
        "result": dict(prep_contract),
    })
    watchlist = [{"symbol": f"FIX{i}", "score": 1.0 - i * 0.01} for i in range(10)]
    monkeypatch.setattr("trader.us.db.repos.load_locked_us_watchlist", lambda *_args, **_kwargs: watchlist)
    monkeypatch.setattr("trader.us.runner.trade_tick_runner.load_watchlist_from_artifact", lambda *_args, **_kwargs: watchlist)
    monkeypatch.setattr("trader.us.db.repos.load_us_daily_orders_for_report", lambda *_args, **_kwargs: [])
    monkeypatch.setattr("trader.us.db.repos.load_today_fills", lambda *_args, **_kwargs: [])
    monkeypatch.setattr("trader.us.execution.order_journal.aggregate_order_events", lambda *_args, **_kwargs: {})
    monkeypatch.setattr("trader.us.execution.order_journal.load_order_events", lambda *_args, **_kwargs: [])
    monkeypatch.setattr("trader.us.runner.trade_session_runner._write_us_session_report", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("trader.us.runner.trade_session_runner._write_us_schedule_health", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("trader.us.runner.trade_tick_runner.evaluate_balance_error_circuit", lambda *_args, **_kwargs: {
        "entry_can_proceed": True,
        "balance_reconcile_degraded": False,
        "entry_blocked_by_balance_degraded": False,
        "balance_consecutive_failed_ticks": 0,
        "entry_block_reasons": [],
    })
    monkeypatch.setattr(
        "trader.us.market_state_overlay.evaluate_us_market_state",
        lambda **_kwargs: {
            "market_state": "NORMAL",
            "market_regime": "RISK_ON",
            "entry_can_proceed": True,
            "exit_can_proceed": True,
            "intraday_market_overlay": "NORMAL",
        },
    )
    monkeypatch.setattr("trader.us.portfolio_cluster_guard.evaluate_portfolio_cluster_guard", lambda *_args, **_kwargs: {
        "portfolio_cluster_guard_status": "OK",
        "portfolio_ai_tech_weight": 0.0,
        "portfolio_cluster_cap_violations": [],
        "cluster_guard_trim_intents": [],
        "cluster_guard_trim_notional": 0.0,
    })
    monkeypatch.setattr(
        "trader.us.runner.trade_tick_runner._get_tqqq_tick_quote",
        lambda *_args, **_kwargs: (100.0, "fixture", False),
    )

    evaluated = []

    class EntryEngine:
        def evaluate_exits(self, *_args, **_kwargs):
            return []

        def evaluate_entries(self, *_args, **_kwargs):
            evaluated.append(len(evaluated) + 1)
            return [{
                "symbol": "FIX0",
                "exchange": "NASDAQ",
                "side": "BUY",
                "qty": 1,
                "limit_price": 100.0,
                "notional_usd": 100.0,
                "client_order_key": f"sep30-valid-entry-{len(evaluated)}",
                "strategy_owner": "US_STANDARD",
                "score_final": 1.0,
                "rank_final30": 1,
                "theme_cluster": "OTHER",
                "classification_source": "synthesized_fixture",
                "position_state": "NOT_HELD",
                "position_action": "OPEN_NEW_POSITION",
                "market_state": "NORMAL",
                "market_regime": "RISK_ON",
                "meta": {
                    "strategy_owner": "US_STANDARD",
                    "entry_style": "MOMENTUM",
                    "theme_cluster": "OTHER",
                    "classification_source": "synthesized_fixture",
                    "position_state": "NOT_HELD",
                    "position_action": "OPEN_NEW_POSITION",
                    "market_state": "NORMAL",
                    "market_regime": "RISK_ON",
                },
            }]

    monkeypatch.setattr("trader.us.runner.trade_tick_runner._get_strategy_engine", lambda **_kwargs: EntryEngine())
    routed = []

    from trader.us.db import repos
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    monkeypatch.setattr(repos, "load_positions", lambda *_args, **_kwargs: [])
    repos.reset_memory_stores()
    from trader.us.runner import trade_tick_runner
    monkeypatch.setattr(
        trade_tick_runner,
        "resolve_us_execution_tail_reserve_sec",
        lambda: fixture["test_timing_overrides"]["execution_tail_reserve_sec"],
    )
    production_tick = trade_tick_runner.run_trade_tick
    tick_kwargs = []
    tick_results = []

    def capture_production_tick(**kwargs):
        current_tick[0] += 1
        tick_kwargs.append(kwargs)
        tick_result = production_tick(**kwargs)
        tick_results.append(tick_result)
        return tick_result

    monkeypatch.setattr(trade_tick_runner, "run_trade_tick", capture_production_tick)

    result = run_trade_session(
        session="am",
        env="practice",
        offline=False,
        max_minutes=1,
        interval_sec=1,
        max_ticks=3,
        force_now=fixture["session_tick_replay_clock"]["timestamp"],
    )

    assert evaluated, tick_results[0].get("entry_degraded_reason")
    assert result["tick_count"] == 3
    assert len(evaluated) == 2
    assert tick_results[0]["entry_degraded_reason"] == "NEXT_TICK_ENTRY_DEFER_INSUFFICIENT_BUDGET"
    assert tick_results[0]["entry_eval_status"] == "DEGRADED"
    assert all(tick["entry_eval_status"] == "OK" for tick in tick_results[1:])
    assert tick_results[2]["entry_can_proceed"] is True
    assert all(tick["orders_ack"] == 0 for tick in tick_results)
    assert all(tick["submitted_orders"] == 0 for tick in tick_results)
    assert fill_fetch_calls >= 2
    assert db_snapshot_calls >= 3
    assert tick_results[0]["fill_fetch_ms"] >= 90
    assert tick_results[2]["fill_fetch_ms"] < tick_results[0]["fill_fetch_ms"]
    assert tick_results[0]["tick_total_ms"] >= tick_results[2]["tick_total_ms"] + 20
    assert tick_results[2]["preflight_rejected_candidates"]
    assert routed == []


def test_protective_sell_routes_before_optional_infinite_evaluation(monkeypatch):
    from trader.us.runner import trade_tick_runner
    from trader.us.runner.trade_tick_runner import run_trade_tick

    calls = []
    _patch_tick_basics(monkeypatch, calls)
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    monkeypatch.setattr(trade_tick_runner, "_get_tqqq_tick_quote", lambda *_a, **_k: (100.0, "fixture", False))

    def evaluate_infinite(**_kwargs):
        calls.append(("infinite_evaluation", None, {}))
        return {"status": "OK", "orders": []}

    monkeypatch.setattr("trader.us.infinite.integration.run_sleeve", evaluate_infinite)

    def fake_route(intent, **kwargs):
        calls.append(("route_order", intent, kwargs))
        return {"status": "ACK", "side": intent["side"], "symbol": intent["symbol"], "intent": intent}

    monkeypatch.setattr("trader.us.execution.order_router.route_order", fake_route)
    result = run_trade_tick(
        session="am", env="practice", offline=False,
        force_now="2026-06-05T10:00:00-04:00", kis_order_allowed=False,
    )

    route_idx = next(i for i, call in enumerate(calls) if call[0] == "route_order")
    infinite_idx = next(i for i, call in enumerate(calls) if call[0] == "infinite_evaluation")
    assert route_idx < infinite_idx
    assert calls[route_idx][1]["side"] == "SELL"
    assert result["exit_routed_before_entry"] == 1


def test_daily_notional_unavailable_fails_buy_closed_but_routes_sell(monkeypatch):
    from trader.us.runner.trade_tick_runner import run_trade_tick
    calls = []
    _patch_tick_basics(monkeypatch, calls)
    class BrokenOrdersEngine:
        def connect(self):
            raise RuntimeError("orders db unavailable")
    monkeypatch.setattr("trader.us.db.repos._get_engine_or_none", lambda: BrokenOrdersEngine())
    def strict_load(*args, **kwargs):
        from trader.us.db.repos import load_today_committed_buy_notional_result
        result = load_today_committed_buy_notional_result(*args, **kwargs)
        if not result.available:
            raise RuntimeError(result.error)
        return result.notional_usd
    monkeypatch.setattr("trader.us.db.repos.load_today_committed_buy_notional", strict_load)
    monkeypatch.setattr("trader.us.execution.order_router.route_order", lambda intent, **kwargs: (
        calls.append(("route_order", intent, kwargs))
        or {"status": "ACK", "side": intent["side"], "symbol": intent["symbol"], "intent": intent}
    ))
    result = run_trade_tick(
        session="am", env="practice", offline=False,
        force_now="2026-06-05T10:00:00-04:00", kis_order_allowed=True,
    )
    routed = [call[1] for call in calls if call[0] == "route_order"]
    assert [intent["side"] for intent in routed] == ["SELL"]
    assert result["entry_intents"] == 0
    assert result["entry_degraded"] == 1
    assert result["entry_degraded_reason"] == "DAILY_NOTIONAL_UNAVAILABLE"
    assert result["global_stop_reason"] == "daily_notional_unavailable"
    assert "orders db unavailable" in result["daily_notional_load_error"]


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


def _run_tick_and_capture_available_slots(monkeypatch, effective_max_new_positions):
    from trader.us.runner.trade_tick_runner import run_trade_tick

    calls = []
    _patch_tick_basics(monkeypatch, calls)
    monkeypatch.setenv("US_MAX_POSITIONS", "35")

    prep_contract = {
        "status": "OK",
        "contract_version": "us_sector_rotation_v3",
        "market_regime_version": "us_leading_regime_v1",
        "trade_can_proceed": 1,
        "trade_block_reason": "ok",
        "market_regime": "RISK_ON",
        "capital_scale": 1.0,
        "effective_capital_scale": 1.0,
        "allow_new_buy": True,
        "allow_add_to_existing": True,
        "force_entry_block": False,
        "max_new_positions": 30,
        "underfilled_tier": "severe_underfilled" if effective_max_new_positions == 5 else "degraded_underfilled",
    }
    if effective_max_new_positions is not None:
        prep_contract["effective_max_new_positions"] = effective_max_new_positions

    monkeypatch.setattr(
        "trader.us.db.repos.load_latest_us_prep_status",
        lambda trade_date, *args, **kwargs: {"status": "OK", "result": dict(prep_contract)},
    )
    monkeypatch.setattr(
        "trader.us.execution.order_router.route_order",
        lambda intent, **kwargs: {"status": "ACK", "side": intent["side"], "symbol": intent["symbol"], "intent": intent},
    )

    captured = {}

    class _Engine:
        def evaluate_exits(self, positions, provider, now):
            return []

        def evaluate_entries(self, *args, **kwargs):
            captured["available_new_slots"] = kwargs.get("available_new_slots")
            captured["allow_new_symbols"] = kwargs.get("allow_new_symbols")
            return []

    monkeypatch.setattr("trader.us.runner.trade_tick_runner._get_strategy_engine", lambda env, offline: _Engine())

    watchlist = [{"symbol": f"SYM{i}", "score": 1.0 - i * 0.01} for i in range(10)]
    result = run_trade_tick(
        session="am",
        env="practice",
        offline=False,
        force_now="2026-06-05T10:00:00-04:00",
        kis_order_allowed=False,
        prep_status_cache={"status": "OK", "result": dict(prep_contract)},
        locked_watchlist_cache=watchlist,
    )
    return captured, result


def test_underfilled_effective_max_positions_limits_entry_slots_to_5(monkeypatch):
    captured, result = _run_tick_and_capture_available_slots(monkeypatch, 5)

    assert captured["available_new_slots"] <= 5
    assert captured["available_new_slots"] == 5
    assert result["available_new_slots"] == 5


def test_underfilled_effective_max_positions_limits_entry_slots_to_24(monkeypatch):
    captured, result = _run_tick_and_capture_available_slots(monkeypatch, 24)

    assert captured["available_new_slots"] <= 24
    assert captured["available_new_slots"] == 24
    assert result["available_new_slots"] == 24


def test_missing_effective_max_positions_preserves_existing_entry_slots(monkeypatch):
    captured, result = _run_tick_and_capture_available_slots(monkeypatch, None)

    assert captured["available_new_slots"] == 34
    assert result["available_new_slots"] == 34
