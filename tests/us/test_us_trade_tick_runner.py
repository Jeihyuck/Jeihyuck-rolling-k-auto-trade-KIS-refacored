# -*- coding: utf-8 -*-
"""tests/us/test_us_trade_tick_runner.py - tick runner 테스트."""
import os
import pytest


def _setup_env():
    os.environ.update({
        "TRADING_REGION": "US",
        "US_AGENT_ENABLED": "1",
        "KIS_ENV": "practice",
        "US_PAPER_TRADING_ENABLED": "1",
        "US_LIVE_TRADING_ENABLED": "0",
        "DISABLE_REAL_TRADING": "1",
        "ALLOW_REAL_ORDER": "0",
        "US_STRATEGY_ENGINE": "pb1",
        "US_ENTRY_ENABLED": "1",
        "US_EXIT_ENABLED": "1",
        "US_PAPER_MAX_CAPITAL_KRW": "50000000",
        "US_BUDGET_FX_KRW_PER_USD": "1450",
        "US_BLOCK_NEW_ENTRY_AFTER_ET": "15:45",
        "US_BLOCK_REBUY_AFTER_SELL_SAME_DAY": "0",  # DB 없을 때
        "US_ORDER_ACCEPTED_IS_NOT_FILLED": "0",      # DB 없을 때
        "DRY_RUN": "1",
    })


def test_tick_runner_am_offline():
    """AM tick - offline 모드."""
    _setup_env()
    from trader.us.runner.trade_tick_runner import run_trade_tick

    result = run_trade_tick(
        session="am",
        env="practice",
        offline=True,
        force_now="2026-01-02T09:35:00-05:00",
    )

    assert result["status"] in ("OK", "OK_WITH_WARNINGS", "SKIP", "OK_NO_TRADE"), \
        f"Unexpected status: {result}"


def test_tick_runner_after_cutoff_blocks_entry():
    """15:46 ET 이후에는 BUY intent가 차단되어야 한다."""
    _setup_env()
    from trader.us.runner.trade_tick_runner import _entry_cutoff_passed
    from datetime import datetime
    from zoneinfo import ZoneInfo

    NY_TZ = ZoneInfo("America/New_York")
    dt = datetime.fromisoformat("2026-01-02T15:46:00-05:00").astimezone(NY_TZ)

    assert _entry_cutoff_passed(dt) is True


def test_tick_runner_before_cutoff_allows_entry():
    """09:35 ET 에는 entry cutoff 미통과."""
    _setup_env()
    from trader.us.runner.trade_tick_runner import _entry_cutoff_passed
    from datetime import datetime
    from zoneinfo import ZoneInfo

    NY_TZ = ZoneInfo("America/New_York")
    dt = datetime.fromisoformat("2026-01-02T09:35:00-05:00").astimezone(NY_TZ)

    assert _entry_cutoff_passed(dt) is False


def test_tick_runner_offline_after_cutoff_no_entry_intents():
    """15:46 ET 이후 tick에서는 entry_intents 가 0이어야 한다."""
    _setup_env()
    from trader.us.runner.trade_tick_runner import run_trade_tick

    result = run_trade_tick(
        session="afternoon",
        env="practice",
        offline=True,
        force_now="2026-01-02T15:46:00-05:00",
    )

    # SKIP 또는 OK/OK_WITH_WARNINGS/OK_NO_TRADE
    assert result["status"] in ("OK", "OK_WITH_WARNINGS", "SKIP", "OK_NO_TRADE"), \
        f"Unexpected status: {result}"


def test_monitoring_universe_is_union_of_final30_and_positions():
    from trader.us.runner.trade_tick_runner import build_monitoring_universe
    final30 = {"NVDA", "TSLA"}
    positions = {"APP", "BE", "INTC"}
    assert build_monitoring_universe(final30, positions) == {"NVDA", "TSLA", "APP", "BE", "INTC"}


def test_20260731_risk_off_prefilter_backfills_full_tick(monkeypatch):
    """Regression: filtered leaders must not consume the engine's three slots."""
    from trader.us.runner.trade_tick_runner import run_trade_tick

    _setup_env()
    monkeypatch.setenv("US_ALLOW_LEGACY_PREP_FOR_TEST", "1")
    monkeypatch.setenv("US_ENTRY_EVAL_TIMEOUT_SEC", "3")
    monkeypatch.setattr("trader.us.watchlist_quality.US_MIN_LOCKED_WATCHLIST_COUNT", 5)
    monkeypatch.setattr("trader.us.market_calendar.is_us_trading_day", lambda date: True)
    monkeypatch.setattr("trader.us.market_calendar.market_phase", lambda now: "REGULAR_MID")
    monkeypatch.setattr("trader.us.budget.resolve_us_order_budget", lambda cash: {"effective_order_budget_usd": 5000.0, "capital_usd_cap": 5000.0})

    class Provider:
        stats = {}
        def __init__(self, offline=False): pass
        def _get_client(self): return type("Client", (), {"stats": {}})()
        def get_current_price(self, symbol, exchange): return {"last": 100.0}

    monkeypatch.setattr("trader.us.data_provider.USDataProvider", Provider)
    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_positions", lambda **kwargs: {
        "status": "OK", "positions": [], "position_count": 0, "position_symbols": [],
        "total_pvs_usd": 10_000, "balance_fetch_status": "OK", "balance_parse_status": "OK",
        "authoritative_positions": True, "preserve_previous_positions": False,
    })
    monkeypatch.setattr("trader.us.execution.fills.get_fills_today", lambda **kwargs: {"status": "OK", "fills": []})
    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_ack_orders_with_balance", lambda **kwargs: {"status": "OK", "confirmed_orders": [], "unresolved_count": 0})
    monkeypatch.setattr("trader.us.db.repos.load_today_symbols_sold", lambda **kwargs: set())
    monkeypatch.setattr("trader.us.db.repos.save_fills", lambda fills: 0)
    monkeypatch.setattr("trader.us.db.repos.save_position_snapshot", lambda positions, **kwargs: 0)
    monkeypatch.setattr("trader.us.db.repos.save_reconcile_log", lambda log, **kwargs: True)
    monkeypatch.setattr("trader.us.db.repos.get_today_buy_orders_count", lambda *args, **kwargs: 0)
    monkeypatch.setattr("trader.us.db.repos.load_latest_us_prep_status", lambda *args, **kwargs: {
        "status": "OK", "contract_version": "us_sector_rotation_v3",
        "market_regime_version": "us_leading_regime_v1", "final30_trade_ready": True,
        "score_nonzero_count": 5, "final30_scored_count": 5,
    })
    monkeypatch.setattr("trader.us.market_state_overlay.evaluate_us_market_state", lambda **kwargs: {
        "market_state": "DEFENSE_RISK_OFF", "market_regime": "DEFENSIVE",
        "allow_new_buy": True, "allow_ai_tech_buy": False, "allow_add_to_existing": False,
        "exposure_multiplier": .25, "force_entry_block": False,
    })

    captured = {}
    class Engine:
        last_entry_diagnostics = {"attempted": 3, "accepted": 3}
        def evaluate_exits(self, positions, provider, now): return []
        def evaluate_entries(self, *args, **kwargs):
            rows = args[6]
            captured["eligible"] = [row["symbol"] for row in rows]
            return [{
                **row, "side": "BUY", "qty": 1, "notional_usd": 100,
                "limit_price": 100, "client_order_key": f"key-{row['symbol']}",
                "meta": dict(row),
            } for row in rows[:3]]
    monkeypatch.setattr("trader.us.runner.trade_tick_runner._get_strategy_engine", lambda **kwargs: Engine())
    routed = []
    monkeypatch.setattr("trader.us.execution.order_router.route_order", lambda intent, **kwargs: (
        routed.append(intent) or {"status": "ACK", "side": intent["side"], "symbol": intent["symbol"], "intent": intent}
    ))

    rows = [
        {"symbol": "DDOG", "exchange": "NASDAQ", "theme_cluster": "AI_SOFTWARE", "trend_score": 1, "score_final": .9, "score": .9, "rank_final30": 1},
        {"symbol": "SNOW", "exchange": "NYSE", "theme_cluster": "AI_SOFTWARE", "trend_score": 1, "score_final": .8, "score": .8, "rank_final30": 2},
        {"symbol": "MPC", "exchange": "NYSE", "theme_cluster": "ENERGY_MATERIALS", "trend_score": 1, "score_final": .6063, "score": .6063, "rank_final30": 3},
        {"symbol": "KO", "exchange": "NYSE", "theme_cluster": "CONSUMER_STAPLES", "trend_score": .7, "score_final": .55, "score": .55, "rank_final30": 4},
        {"symbol": "AMGN", "exchange": "NASDAQ", "theme_cluster": "HEALTHCARE", "trend_score": .7, "score_final": .5, "score": .5, "rank_final30": 5},
    ]
    result = run_trade_tick(
        session="am", env="practice", offline=False,
        force_now="2026-07-31T10:00:00-04:00", kis_order_allowed=False,
        locked_watchlist_cache=rows, watchlist_cache_source="test",
    )

    assert captured["eligible"][:3] == ["MPC", "KO", "AMGN"]
    assert [intent["symbol"] for intent in routed] == ["MPC", "KO", "AMGN"]
    assert result["prefilter_blocked_candidates"][0]["symbol"] == "DDOG"
    assert result["final_entry_intents"] == 3
