from __future__ import annotations

from datetime import datetime, timedelta, timezone

from trader.us.db import repos


def setup_function():
    repos.reset_memory_stores()


def _daily(end="2026-07-10", n=170, last=99.0):
    end_dt = datetime.fromisoformat(end)
    rows = []
    for i in range(n):
        d = end_dt - timedelta(days=n - i)
        close = 100.0
        if i == n - 1:
            close = last
        rows.append({"date": d.date().isoformat(), "close": close})
    rows.append({"date": end, "close": 70.0})  # incomplete trade_date bar must be ignored
    return rows


def _daily_below_ma20_above_ma50(end="2026-07-10"):
    end_dt = datetime.fromisoformat(end)
    rows = []
    vals = [90.0] * 120 + [95.0] * 30 + [100.0] * 20
    for i, close in enumerate(vals):
        d = end_dt - timedelta(days=len(vals) - i)
        rows.append({"date": d.date().isoformat(), "close": close})
    rows.append({"date": end, "close": 70.0})
    return rows


def _final30(td="2026-07-10"):
    return [{"symbol": f"S{i}", "trade_date": td, "score_final": 1.0, "score": 1.0, "trend_score": 0.8, "exchange": "NASDAQ"} for i in range(30)]


class _Provider:
    def __init__(self, daily=None, fail_daily=False):
        self.daily = daily or _daily()
        self.fail_daily = fail_daily
    def get_daily_prices(self, symbol, exchange, count=160, as_of_date=None):
        if self.fail_daily:
            raise RuntimeError("daily down")
        return self.daily
    def get_current_price(self, symbol, exchange):
        return {"last": 99.0}
    def get_orderable_cash(self, symbol, exchange, price):
        return 10000.0
    def _get_client(self):
        return type("C", (), {"stats": {}})()


def test_tick_trend_warning_blocks_add_to_existing_without_sell():
    from trader.us.runner.trade_tick_runner import _update_position_trends_for_tick
    from trader.us.position_trend_state import filter_add_to_existing_by_trend_state

    positions = [{"symbol": "AMD", "exchange": "NASDAQ", "qty": 10, "entry_price": 100, "current_price_usd": 101, "holding_trade_days": 3}]
    positions, counts, _, _ = _update_position_trends_for_tick(
        positions=positions,
        provider=_Provider(_daily(last=101.0)),
        trade_date="2026-07-10",
        now=datetime(2026, 7, 10, tzinfo=timezone.utc),
        locked_watchlist_cache=_final30(),
        watchlist_cache_source="test",
    )
    assert positions[0]["trend_state"] == "WARNING"
    assert counts["WARNING"] == 1
    kept, blocked = filter_add_to_existing_by_trend_state([{"symbol": "AMD", "side": "BUY"}, {"symbol": "NEW", "side": "BUY"}], positions)
    assert [b["block_reason"] for b in blocked] == ["POSITION_TREND_NOT_HEALTHY"]
    assert [k["symbol"] for k in kept] == ["NEW"]


def test_two_and_three_day_weakness_create_trim_then_exit():
    from trader.us.runner.trade_tick_runner import _update_position_trends_for_tick
    from trader.us.pb1.us_exit_engine import evaluate_exit

    pos = {"symbol": "AMD", "exchange": "NASDAQ", "qty": 10, "orderable_qty": 10, "entry_price": 100, "current_price_usd": 99, "holding_trade_days": 3}
    for td in ["2026-07-10", "2026-07-11"]:
        rows = _daily_below_ma20_above_ma50(end=td)
        positions, *_ = _update_position_trends_for_tick(positions=[dict(pos)], provider=_Provider(rows), trade_date=td, now=datetime.fromisoformat(td).replace(tzinfo=timezone.utc), locked_watchlist_cache=_final30(td), watchlist_cache_source="test")
        pos.update(positions[0])
    trim = evaluate_exit(pos, 99.0)
    assert trim["exit_type"] == "trend_deterioration_trim"
    assert trim["qty"] == 3

    for td in ["2026-07-12"]:
        rows = _daily_below_ma20_above_ma50(end=td)
        positions, *_ = _update_position_trends_for_tick(positions=[dict(pos, current_price_usd=99.0)], provider=_Provider(rows), trade_date=td, now=datetime.fromisoformat(td).replace(tzinfo=timezone.utc), locked_watchlist_cache=_final30(td), watchlist_cache_source="test")
        pos.update(positions[0])
        pos["trend"]["trend_state"] = "EXIT"
        pos["trend"]["weakness_signals"] = ["FINAL30_ABSENT_3D", "BELOW_MA20_3D", "RS20_NEGATIVE"]
        pos["trend"]["trend_trim_pending"] = False
        pos["trend"]["trend_trim_done"] = True
    exit_intent = evaluate_exit(pos, 99.0)
    assert exit_intent["exit_type"] == "trend_deterioration_exit"


def test_stale_final30_does_not_increment_and_daily_error_unknown_no_sell():
    from trader.us.runner.trade_tick_runner import _update_position_trends_for_tick
    from trader.us.pb1.us_exit_engine import evaluate_exit

    positions = [{"symbol": "AMD", "exchange": "NASDAQ", "qty": 10, "entry_price": 100, "current_price_usd": 99, "holding_trade_days": 3}]
    positions, *_ = _update_position_trends_for_tick(positions=positions, provider=_Provider(), trade_date="2026-07-10", now=datetime(2026, 7, 10, tzinfo=timezone.utc), locked_watchlist_cache=_final30("2026-07-09"), watchlist_cache_source="stale")
    assert positions[0]["final30_absent_streak"] == 0
    positions, *_ = _update_position_trends_for_tick(positions=positions, provider=_Provider(fail_daily=True), trade_date="2026-07-11", now=datetime(2026, 7, 11, tzinfo=timezone.utc), locked_watchlist_cache=_final30("2026-07-11"), watchlist_cache_source="test")
    assert positions[0]["trend_state"] == "UNKNOWN"
    assert evaluate_exit(positions[0], 99.0) is None


def test_stage_ack_fill_and_post_trim_nonrecovery_exit():
    from trader.us.db.repos import mark_us_position_exit_stage, load_us_position_risk_state
    from trader.us.position_trend_state import update_us_position_trend_state
    from trader.us.pb1.us_exit_engine import evaluate_exit

    repos.save_us_position_risk_state("AMD", "2026-07-10", {"state": {"lifecycle": {"lifecycle_id": "life1"}}})
    mark_us_position_exit_stage("2026-07-10", "AMD", "trend_trim", "k1", "PENDING", "life1")
    mark_us_position_exit_stage("2026-07-10", "AMD", "trend_trim", "k1", "ACK", "life1")
    tr = load_us_position_risk_state("AMD", "2026-07-10")["state"]["trend"]
    assert tr["trend_trim_pending"] is True
    mark_us_position_exit_stage("2026-07-10", "AMD", "trend_trim", "k1", "FILLED", "life1")
    tr = load_us_position_risk_state("AMD", "2026-07-10")["state"]["trend"]
    assert tr["trend_trim_pending"] is False and tr["trend_trim_done"] is True

    for td in ["2026-07-11", "2026-07-12"]:
        tr = update_us_position_trend_state(symbol="AMD", trade_date=td, now=datetime.fromisoformat(td).replace(tzinfo=timezone.utc), current_price=99, holding_trade_days=4, final30={"trade_date": td, "available": True, "score_contract_ok": True, "in_final30_today": False}, daily={"ma20": 100, "ma50": 100, "rs_20d": -0.01}, lifecycle_id="life1")
    pos = {"symbol": "AMD", "qty": 7, "orderable_qty": 7, "entry_price": 100, "holding_trade_days": 4, "trend": tr}
    assert evaluate_exit(pos, 99)["exit_type"] == "trend_deterioration_exit"


def test_run_trade_tick_injects_warning_before_exit_and_blocks_existing_buy(monkeypatch):
    from trader.us.runner.trade_tick_runner import run_trade_tick

    monkeypatch.setenv("DRY_RUN", "0")
    monkeypatch.setenv("US_KIS_ORDER_ALLOWED", "1")
    monkeypatch.setenv("US_ALLOW_LEGACY_PREP_FOR_TEST", "1")
    monkeypatch.setenv("US_ENTRY_EVAL_TIMEOUT_SEC", "3")
    monkeypatch.setattr("trader.us.market_calendar.is_us_trading_day", lambda d: True)
    monkeypatch.setattr("trader.us.market_calendar.market_phase", lambda now: "REGULAR_MID")
    monkeypatch.setattr("trader.us.budget.resolve_us_order_budget", lambda cash: {"effective_order_budget_usd": 5000.0})
    monkeypatch.setattr("trader.us.data_provider.USDataProvider", lambda offline=False: _Provider(_daily(last=101.0)))
    positions = [{"symbol": "AMD", "exchange": "NASDAQ", "qty": 10, "orderable_qty": 10, "entry_price": 100.0, "current_price_usd": 101.0}]
    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_positions", lambda provider=None: {"status": "OK", "positions": positions, "total_pvs_usd": 10000})
    monkeypatch.setattr("trader.us.execution.fills.get_fills_today", lambda provider=None, signal_only=False, trade_date=None: {"status": "OK", "fills": []})
    monkeypatch.setattr("trader.us.db.repos.load_today_symbols_sold", lambda trade_date=None: set())
    monkeypatch.setattr("trader.us.db.repos.save_fills", lambda fills: 0)
    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_ack_orders_with_balance", lambda provider=None, trade_date=None, env="practice": {"status": "OK", "confirmed_orders": []})
    monkeypatch.setattr("trader.us.db.repos.save_position_snapshot", lambda positions: 0)
    monkeypatch.setattr("trader.us.db.repos.save_reconcile_log", lambda log: True)
    monkeypatch.setattr("trader.us.db.repos.get_today_buy_orders_count", lambda trade_date, env="practice": 0)
    monkeypatch.setattr("trader.us.db.repos.load_latest_us_prep_status", lambda trade_date, *a, **k: {"status": "OK", "contract_version": "us_sector_rotation_v3", "market_regime_version": "us_leading_regime_v1", "final30_trade_ready": True, "score_nonzero_count": 30, "final30_scored_count": 30})

    monkeypatch.setattr("trader.us.portfolio_cluster_guard.filter_entry_intents_for_cluster_guard", lambda intents, guard: (intents, []))
    monkeypatch.setattr("trader.us.market_state_overlay.filter_entry_intents_for_market_state", lambda intents, overlay, positions=None: (intents, []))

    captured = {}
    class _Engine:
        def evaluate_exits(self, positions, provider, now):
            captured["exit_positions"] = [dict(p) for p in positions]
            return []
        def evaluate_entries(self, *args, **kwargs):
            return [{"symbol": "AMD", "side": "BUY", "qty": 1, "notional_usd": 100}, {"symbol": "NEW", "side": "BUY", "qty": 1, "notional_usd": 100}]
    monkeypatch.setattr("trader.us.runner.trade_tick_runner._get_strategy_engine", lambda env, offline: _Engine())
    routed = []
    monkeypatch.setattr("trader.us.execution.order_router.route_order", lambda intent, **kwargs: (routed.append(intent) or {"status": "ACK", "side": intent["side"], "symbol": intent["symbol"], "intent": intent}))

    result = run_trade_tick(session="am", env="practice", offline=False, force_now="2026-07-10T10:00:00-04:00", kis_order_allowed=False, locked_watchlist_cache=_final30("2026-07-10"), watchlist_cache_source="test")
    assert captured["exit_positions"][0]["trend_state"] == "WARNING"
    assert result["exit_intents"] == 0
    assert result["trend_add_blocked_count"] == 1
    assert [i["symbol"] for i in routed] == ["NEW"]
