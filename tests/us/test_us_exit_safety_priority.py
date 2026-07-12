from trader.us.pb1.us_exit_engine import evaluate_exit

def test_hard_stop_beats_bad_trend_and_missing_daily():
    pos={"symbol":"AMD","qty":10,"orderable_qty":10,"entry_price":100,"holding_trade_days":3,"trend_state":"EXIT","final30_absent_streak":3,"weakness_signals":["X","Y","Z"]}
    assert evaluate_exit(pos, 90)["exit_reason"] == "hard_stop_full_exit"

def test_trailing_beats_trend_warning():
    pos={"symbol":"AMD","qty":10,"orderable_qty":10,"entry_price":100,"max_price":120,"trend_state":"WARNING","weakness_signals":["FINAL30_ABSENT"]}
    assert evaluate_exit(pos, 113)["exit_type"] == "profit_trailing_stop"


def test_run_trade_tick_hard_stop_skips_trend_loader(monkeypatch):
    from trader.us.runner.trade_tick_runner import run_trade_tick
    from trader.us.db import repos
    repos.reset_memory_stores()
    monkeypatch.setenv("DRY_RUN", "0")
    monkeypatch.setenv("US_KIS_ORDER_ALLOWED", "1")
    monkeypatch.setenv("US_ALLOW_LEGACY_PREP_FOR_TEST", "1")
    monkeypatch.setattr("trader.us.market_calendar.is_us_trading_day", lambda d: True)
    monkeypatch.setattr("trader.us.market_calendar.market_phase", lambda now: "REGULAR_MID")
    monkeypatch.setattr("trader.us.budget.resolve_us_order_budget", lambda cash: {"effective_order_budget_usd": 5000.0})

    class Provider:
        stats = {"daily_http_call_count": 0}
        def get_current_price(self, symbol, exchange): return {"last": "90"}
        def get_orderable_cash(self, symbol, exchange, price): return 10000.0
        def get_client_stats(self): return self.stats
    monkeypatch.setattr("trader.us.data_provider.USDataProvider", lambda offline=False: Provider())
    positions = [{"symbol":"AMD","exchange":"NASDAQ","qty":10,"orderable_qty":10,"entry_price":100.0,"current_price_usd":90.0}]
    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_positions", lambda provider=None: {"status":"OK","positions":positions,"total_pvs_usd":10000})
    monkeypatch.setattr("trader.us.execution.fills.get_fills_today", lambda provider=None, signal_only=False, trade_date=None: {"status":"OK","fills":[]})
    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_ack_orders_with_balance", lambda provider=None, trade_date=None, env="practice": {"status":"OK","confirmed_orders":[]})
    monkeypatch.setattr("trader.us.db.repos.save_fills", lambda fills: 0)
    monkeypatch.setattr("trader.us.db.repos.load_today_symbols_sold", lambda trade_date=None: set())
    monkeypatch.setattr("trader.us.db.repos.save_position_snapshot", lambda positions: 0)
    monkeypatch.setattr("trader.us.db.repos.save_reconcile_log", lambda log: True)
    monkeypatch.setattr("trader.us.db.repos.get_today_buy_orders_count", lambda trade_date, env="practice": 0)
    monkeypatch.setattr("trader.us.db.repos.load_latest_us_prep_status", lambda trade_date, *a, **k: {"status":"OK","contract_version":"us_sector_rotation_v3","market_regime_version":"us_leading_regime_v1","final30_trade_ready":True,"score_nonzero_count":30,"final30_scored_count":30})
    def boom(*a, **k):
        raise AssertionError("trend daily loader should not run for hard-stop symbol")
    monkeypatch.setattr("trader.us.db.price_daily_repo.load_recent_us_daily_bars", boom)
    routed=[]
    monkeypatch.setattr("trader.us.execution.order_router.route_order", lambda intent, **kwargs: (routed.append(intent) or {"status":"ACK","side":intent["side"],"symbol":intent["symbol"],"intent":intent}))

    result = run_trade_tick(session="am", env="practice", offline=False, force_now="2026-07-10T10:00:00-04:00", kis_order_allowed=False, locked_watchlist_cache=[], watchlist_cache_source="test")
    assert result["exit_intents"] == 1
    assert routed[0]["exit_reason"] == "hard_stop_full_exit"
    assert result["daily_http_call_count"] == 0


def test_run_trade_tick_early_close_aftermarket_skips_orders(monkeypatch):
    from trader.us.runner.trade_tick_runner import run_trade_tick
    monkeypatch.setattr("trader.us.market_calendar.is_us_trading_day", lambda d: True)
    routed=[]
    monkeypatch.setattr("trader.us.execution.order_router.route_order", lambda intent, **kwargs: routed.append(intent))
    result = run_trade_tick(session="am", env="practice", offline=True, force_now="2026-11-27T13:30:00-05:00", kis_order_allowed=False, locked_watchlist_cache=[], watchlist_cache_source="test")
    assert result["status"] == "SKIP"
    assert result["reason"] == "market_not_open"
    assert routed == []


def test_run_trade_tick_soft_stop_state_and_price_once(monkeypatch):
    from trader.us.runner.trade_tick_runner import run_trade_tick
    from trader.us.db import repos
    repos.reset_memory_stores()
    monkeypatch.setenv("DRY_RUN", "0")
    monkeypatch.setenv("US_KIS_ORDER_ALLOWED", "1")
    monkeypatch.setenv("US_ALLOW_LEGACY_PREP_FOR_TEST", "1")
    monkeypatch.setattr("trader.us.market_calendar.is_us_trading_day", lambda d: True)
    monkeypatch.setattr("trader.us.market_calendar.market_phase", lambda now: "REGULAR_MID")
    monkeypatch.setattr("trader.us.budget.resolve_us_order_budget", lambda cash: {"effective_order_budget_usd": 5000.0})
    calls = {"price":0, "risk":0, "hwm":0}
    class Provider:
        stats = {"daily_http_call_count": 0}
        def get_current_price(self, symbol, exchange): calls["price"] += 1; return {"last": "94.9"}
        def get_orderable_cash(self, symbol, exchange, price): return 10000.0
        def get_client_stats(self): return self.stats
    monkeypatch.setattr("trader.us.data_provider.USDataProvider", lambda offline=False: Provider())
    positions = [{"symbol":"AMD","exchange":"NASDAQ","qty":10,"orderable_qty":10,"entry_price":100.0,"current_price_usd":94.9,"position_lifecycle_id":"L1"}]
    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_positions", lambda provider=None: {"status":"OK","positions":positions,"total_pvs_usd":10000})
    monkeypatch.setattr("trader.us.execution.fills.get_fills_today", lambda provider=None, signal_only=False, trade_date=None: {"status":"OK","fills":[]})
    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_ack_orders_with_balance", lambda provider=None, trade_date=None, env="practice": {"status":"OK","confirmed_orders":[]})
    monkeypatch.setattr("trader.us.db.repos.save_fills", lambda fills: 0)
    monkeypatch.setattr("trader.us.db.repos.load_today_symbols_sold", lambda trade_date=None: set())
    monkeypatch.setattr("trader.us.db.repos.save_position_snapshot", lambda positions: 0)
    monkeypatch.setattr("trader.us.db.repos.save_reconcile_log", lambda log: True)
    monkeypatch.setattr("trader.us.db.repos.get_today_buy_orders_count", lambda trade_date, env="practice": 0)
    monkeypatch.setattr("trader.us.db.repos.load_latest_us_prep_status", lambda trade_date, *a, **k: {"status":"OK","contract_version":"us_sector_rotation_v3","market_regime_version":"us_leading_regime_v1","final30_trade_ready":True,"score_nonzero_count":30,"final30_scored_count":30})
    def risk(*a, **k): calls["risk"] += 1; return {"soft_stop_breach_count": 1}
    def hwm(*a, **k): calls["hwm"] += 1; return {"high_watermark":100,"lifecycle_id":"L1"}
    monkeypatch.setattr("trader.us.db.repos.update_us_soft_stop_risk_state", risk)
    monkeypatch.setattr("trader.us.position_lifecycle_state.update_us_position_high_watermark", hwm)
    monkeypatch.setattr("trader.us.db.price_daily_repo.load_recent_us_daily_bars", lambda *a, **k: [])
    result = run_trade_tick(session="am", env="practice", offline=False, force_now="2026-07-10T10:00:00-04:00", kis_order_allowed=False, locked_watchlist_cache=[], watchlist_cache_source="test")
    assert calls == {"price":1, "risk":1, "hwm":1}
    assert result["daily_http_call_count"] == 0
