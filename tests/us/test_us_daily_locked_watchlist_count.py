from __future__ import annotations

import json
import os


def test_session_report_uses_prep_guard_locked_watchlist_fallback(monkeypatch, tmp_path) -> None:
    from trader.us.runner.trade_session_runner import run_trade_session

    orig_cwd = os.getcwd()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.setenv("GITHUB_SHA", "sha")
    monkeypatch.setenv("GITHUB_WORKFLOW", "wf")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.setenv("DRY_RUN", "1")

    monkeypatch.setattr("trader.us.utils.session_guard.check_us_session_file_guard", lambda trade_date, session: {"already_ran": False, "guard_status": "OK"})
    monkeypatch.setattr("trader.us.utils.session_guard.now_et_iso", lambda: "2026-06-05T08:15:00-04:00")
    monkeypatch.setattr("trader.us.utils.session_guard.write_us_session_done_file", lambda **kwargs: None)
    monkeypatch.setattr("trader.us.market_calendar.is_us_trading_day", lambda d: True)
    monkeypatch.setattr("trader.us.budget.resolve_us_order_budget", lambda cash: {"capital_usd_cap": 10000.0})
    monkeypatch.setattr(
        "trader.us.prep_contract.check_us_prep_guard",
        lambda trade_date: {
            "ok": True,
            "status": "OK",
            "final30_scored_count": 30,
            "score_nonzero_count": 30,
            "locked_count": 30,
            "contract": {"status": "OK", "run_id": "prep-1", "score_nonzero_count": 30},
        },
    )
    monkeypatch.setattr(
        "trader.us.runner.trade_tick_runner.run_trade_tick",
        lambda **kwargs: {
            "status": "OK",
            "last_stage": "done",
            "prep_status": "UNKNOWN",
            "locked_watchlist_count": 0,
            "entry_eval_status": "OK",
            "entry_intents": 0,
            "exit_intents": 0,
            "orders_sent": 0,
            "orders_ack": 0,
            "orders_rejected": 0,
            "orders_error": 0,
            "fills": 0,
            "positions": 0,
        },
    )

    try:
        run_trade_session(
            session="am",
            env="practice",
            offline=False,
            max_minutes=1,
            interval_sec=1,
            max_ticks=1,
            force_now="2026-06-05T09:35:00-04:00",
        )

        payload = json.loads((tmp_path / "reports" / "us_daily" / "latest_us_daily_report.json").read_text())
        assert payload["locked_watchlist_count"] == 30
        assert payload["locked_watchlist_count_source"] == "prep_guard_result.final30_scored_count"
        assert payload["score_nonzero_count"] == 30
        assert payload["prep_run_id"] == "prep-1"
    finally:
        os.chdir(orig_cwd)
