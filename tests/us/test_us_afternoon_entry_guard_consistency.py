from __future__ import annotations

import os


def test_afternoon_session_passes_conservative_entry_guard_to_tick(monkeypatch, tmp_path) -> None:
    from trader.us.runner.trade_session_runner import run_trade_session

    orig_cwd = os.getcwd()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.setenv("GITHUB_SHA", "sha")
    monkeypatch.setenv("GITHUB_WORKFLOW", "wf")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.setenv("DRY_RUN", "1")

    seen: dict = {}

    monkeypatch.setattr("trader.us.utils.session_guard.check_us_session_file_guard", lambda trade_date, session: {"already_ran": False, "guard_status": "OK"})
    monkeypatch.setattr("trader.us.utils.session_guard.now_et_iso", lambda: "2026-06-05T11:30:00-04:00")
    monkeypatch.setattr("trader.us.utils.session_guard.write_us_session_done_file", lambda **kwargs: None)
    monkeypatch.setattr("trader.us.market_calendar.is_us_trading_day", lambda d: True)
    monkeypatch.setattr("trader.us.budget.resolve_us_order_budget", lambda cash: {"capital_usd_cap": 10000.0})
    monkeypatch.setattr("trader.us.prep_contract.check_us_prep_guard", lambda trade_date: {"ok": True, "status": "OK"})
    monkeypatch.setattr("trader.us.db.repos.get_today_buy_orders_count", lambda trade_date, env="practice": 3)

    def _run_tick(**kwargs):
        seen.update(kwargs)
        return {
            "status": "OK",
            "last_stage": "done",
            "prep_status": "OK",
            "locked_watchlist_count": 30,
            "entry_eval_status": "OK",
            "entry_intents": 0,
            "exit_intents": 0,
            "orders_sent": 0,
            "orders_ack": 0,
            "orders_rejected": 0,
            "orders_error": 0,
            "fills": 0,
            "positions": 0,
        }

    monkeypatch.setattr("trader.us.runner.trade_tick_runner.run_trade_tick", _run_tick)

    try:
        run_trade_session(
            session="afternoon",
            env="practice",
            offline=False,
            max_minutes=1,
            interval_sec=1,
            max_ticks=1,
            force_now="2026-06-05T12:35:00-04:00",
        )

        assert seen["session_entry_allowed"] is True
        assert seen["session_buy_orders_count"] == 3
    finally:
        os.chdir(orig_cwd)


def test_tick_runner_logs_already_bought_today_skip(monkeypatch, caplog) -> None:
    from trader.us.runner.trade_tick_runner import run_trade_tick

    monkeypatch.setenv("DRY_RUN", "1")
    monkeypatch.setenv("US_KIS_ORDER_ALLOWED", "0")
    monkeypatch.setattr("trader.us.market_calendar.now_ny", lambda: __import__("datetime").datetime.fromisoformat("2026-06-05T12:35:00-04:00"))
    monkeypatch.setattr("trader.us.market_calendar.is_us_trading_day", lambda d: True)
    monkeypatch.setattr("trader.us.market_calendar.market_phase", lambda now: "REGULAR_MID")
    monkeypatch.setattr("trader.us.budget.resolve_us_order_budget", lambda cash: {"effective_order_budget_usd": 1000.0})

    class _Provider:
        def __init__(self, offline=False):
            pass
        def get_orderable_cash(self, symbol, exchange, price):
            return 10000.0
        def _get_client(self):
            class _Client:
                stats = {}
            return _Client()

    monkeypatch.setattr("trader.us.data_provider.USDataProvider", _Provider)
    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_positions", lambda provider=None, trade_date=None: {"status": "OK", "positions": [], "position_count": 0, "position_symbols": []})
    monkeypatch.setattr("trader.us.execution.fills.get_fills_today", lambda provider=None, signal_only=False, trade_date=None: {"status": "OK", "fills": []})
    monkeypatch.setattr("trader.us.db.repos.load_today_symbols_sold", lambda trade_date=None: set())
    monkeypatch.setattr("trader.us.db.repos.save_fills", lambda fills: 0)
    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_ack_orders_with_balance", lambda provider=None, trade_date=None, env="practice": {"status": "OK", "pending_count": 0, "confirmed_count": 0, "balance_reconcile_count": 0, "unresolved_count": 0})
    monkeypatch.setattr("trader.us.db.repos.save_position_snapshot", lambda positions, **kwargs: 0)
    monkeypatch.setattr("trader.us.db.repos.save_reconcile_log", lambda log, **kwargs: True)
    monkeypatch.setattr("trader.us.db.repos.load_positions", lambda: [])
    monkeypatch.setattr("trader.us.pb1.us_exit_position_resolver.enrich_us_positions_for_exit", lambda positions, trade_date, env, provider: (positions, {"total": 0, "ok": 0, "missing": 0, "sources": {}, "missing_symbols": []}))
    monkeypatch.setattr("trader.us.runner.trade_tick_runner._get_strategy_engine", lambda env, offline: type("E", (), {"evaluate_exits": lambda self, positions, provider, now: []})())
    monkeypatch.setattr("trader.us.db.repos.get_today_buy_orders_count", lambda trade_date, env="practice": 3)
    monkeypatch.setattr("trader.us.db.repos.load_latest_us_prep_status", lambda trade_date: {"status": "OK"})

    with caplog.at_level("INFO"):
        run_trade_tick(
            session="afternoon",
            env="practice",
            offline=False,
            force_now="2026-06-05T12:35:00-04:00",
            session_entry_allowed=False,
            session_buy_orders_count=3,
        )

    assert "[US_ENTRY][DIAGNOSTIC] already_bought_today=1 buy_orders_count=3 entry_global_block=0" in caplog.text
