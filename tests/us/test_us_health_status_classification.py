import json
from trader.us.runner.trade_session_runner import _write_us_schedule_health


def test_health_is_failed_for_fill_persistence_failure(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _write_us_schedule_health({"trade_date": "2026-07-20", "final_status": "FAILED", "reason": "fill_persistence_failed"}, "am")
    health = json.loads((tmp_path / "runtime/health/us-2026-07-20.json").read_text())
    assert health["ok"] is False
    assert health["status"] == "FAILED"
    assert health["reason"] == "fill_persistence_failed"


def test_health_rejects_broker_fills_that_were_not_persisted(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _write_us_schedule_health({"trade_date": "2026-07-20", "final_status": "OK", "broker_fills_fetched": 8, "fills_count": 0}, "close")
    health = json.loads((tmp_path / "runtime/health/us-2026-07-20.json").read_text())
    assert health["ok"] is False
    assert health["reason"] == "broker_fills_not_persisted"


def test_health_and_close_report_fail_on_sell_liveness_degradation(tmp_path, monkeypatch):
    from trader.us.runner.daily_report_runner import run_daily_report

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("US_DAILY_REPORT_BASE", str(tmp_path / "reports"))
    _write_us_schedule_health({
        "trade_date": "2026-07-20",
        "final_status": "OK",
        "runtime_integrity_status": "LIVENESS_DEGRADED",
        "sell_liveness_status": "LIVENESS_DEGRADED",
        "exit_route_liveness_failure": True,
    }, "am")
    health = json.loads((tmp_path / "runtime/health/us-2026-07-20.json").read_text())
    assert health["ok"] is False
    assert health["status"] == "FAILED_RUNTIME_INTEGRITY"

    result = run_daily_report(
        env="practice", session="close", trade_date="2026-07-20", offline=True,
        final_balance={"positions": []}, final_positions=[], kis_fills=[],
    )
    assert result["status"] == "FAILED_RECONCILE"
    assert result["report"]["close_integrity_status"] == "INTEGRITY_DEGRADED"
    assert result["report"]["runtime_liveness_failures"] == [{
        "session": "am",
        "runtime_integrity_status": "LIVENESS_DEGRADED",
        "sell_liveness_status": "LIVENESS_DEGRADED",
    }]


def test_policy_entry_block_does_not_fail_close_liveness(tmp_path, monkeypatch):
    from trader.us.runner.daily_report_runner import run_daily_report

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("US_DAILY_REPORT_BASE", str(tmp_path / "reports"))
    _write_us_schedule_health({
        "trade_date": "2026-07-20",
        "final_status": "OK",
        "runtime_integrity_status": "POLICY_ENTRY_BLOCKED",
        "sell_liveness_status": "OK",
    }, "am")

    result = run_daily_report(
        env="practice", session="close", trade_date="2026-07-20", offline=True,
        final_balance={"positions": []}, final_positions=[], kis_fills=[],
    )
    assert result["report"]["runtime_liveness_failures"] == []
    assert result["report"]["close_integrity_status"] == "OK"
    assert result["status"] == "OK"


def test_session_preserves_early_tick_liveness_failure_for_health_and_close(
    tmp_path, monkeypatch,
):
    from trader.us.runner.daily_report_runner import run_daily_report
    from trader.us.runner.trade_session_runner import run_trade_session

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GITHUB_RUN_ID", "multi-tick-integrity")
    monkeypatch.setenv("US_SESSION_SHUTDOWN_BUFFER_SEC", "0")
    monkeypatch.setenv("US_DAILY_REPORT_BASE", str(tmp_path / "reports"))
    monkeypatch.setattr("trader.us.market_calendar.is_us_trading_day", lambda _date: True)
    monkeypatch.setattr(
        "trader.us.budget.resolve_us_order_budget",
        lambda _cash: {"capital_usd_cap": 10_000.0},
    )
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    tick_results = iter([
        {
            "status": "OK",
            "orders": [],
            "runtime_integrity_status": "RECONCILE_REQUIRED",
            "sell_liveness_status": "LIVENESS_DEGRADED",
            "exit_route_liveness_failure": True,
        },
        {
            "status": "OK",
            "orders": [],
            "runtime_integrity_status": "OK",
            "sell_liveness_status": "OK",
            "exit_route_liveness_failure": False,
        },
    ])
    monkeypatch.setattr(
        "trader.us.runner.trade_tick_runner.run_trade_tick",
        lambda *_args, **_kwargs: next(tick_results),
    )

    session_result = run_trade_session(
        session="am",
        env="practice",
        offline=True,
        force_now="2026-07-20T10:00:00-04:00",
        max_ticks=2,
        interval_sec=1,
        max_minutes=1,
    )

    assert session_result["tick_count"] == 2
    assert session_result["runtime_integrity_status"] == "RECONCILE_REQUIRED"
    assert session_result["sell_liveness_status"] == "LIVENESS_DEGRADED"
    health = json.loads((tmp_path / "runtime/health/us-2026-07-20.json").read_text())
    assert health["ok"] is False
    assert health["status"] == "FAILED_RUNTIME_INTEGRITY"

    close = run_daily_report(
        env="practice",
        session="close",
        trade_date="2026-07-20",
        offline=True,
        final_balance={"positions": []},
        final_positions=[],
        kis_fills=[],
    )
    assert close["status"] == "FAILED_RECONCILE"
    assert close["report"]["close_integrity_status"] == "RECONCILE_REQUIRED"
    assert close["report"]["runtime_liveness_failures"][0]["sell_liveness_status"] == "LIVENESS_DEGRADED"


def test_opening_30min_is_expected_policy_not_entry_liveness():
    from trader.us.runner.status_contract import classify_runtime_integrity
    assert classify_runtime_integrity({
        "entry_can_proceed": False,
        "entry_block_reason": "OPENING_30MIN_BUY_BLOCK",
        "entry_degraded": False,
    }) == "POLICY_ENTRY_BLOCKED"
    # An unidentified denied entry must NOT be silently treated as policy.
    assert classify_runtime_integrity({
        "entry_can_proceed": False,
        "entry_block_reason": "",
        "entry_degraded": True,
    }) == "LIVENESS_DEGRADED"
    # Unknown ACK, execution invariant and protection failures override policy.
    assert classify_runtime_integrity({
        "entry_can_proceed": False,
        "entry_block_reason": "OPENING_30MIN_BUY_BLOCK",
        "entry_degraded": False,
        "unresolved_ack_count": 1,
    }) == "RECONCILE_REQUIRED"
    assert classify_runtime_integrity({
        "entry_can_proceed": False,
        "entry_block_reason": "OPENING_30MIN_BUY_BLOCK",
        "system_invariant_failure": "BUY_INTENT_PERSISTENCE_REJECTED",
    }) == "INTEGRITY_DEGRADED"


def test_opening_policy_close_ok_without_hiding_reconcile(tmp_path, monkeypatch):
    from trader.us.runner.daily_report_runner import run_daily_report
    from trader.us.runner.status_contract import classify_runtime_integrity

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("US_DAILY_REPORT_BASE", str(tmp_path / "reports"))
    payload = {
        "trade_date": "2026-07-20", "final_status": "OK",
        "runtime_integrity_status": classify_runtime_integrity({
            "entry_can_proceed": False,
            "entry_block_reason": "OPENING_30MIN_BUY_BLOCK",
            "entry_degraded": False,
        }),
        "entry_liveness_status": "OK",
        "entry_degraded": 0,
        "sell_liveness_status": "OK",
        "reconcile_liveness_status": "OK",
    }
    _write_us_schedule_health(payload, "am")
    report = run_daily_report(
        env="practice", session="close", trade_date="2026-07-20", offline=True,
        final_balance={"positions": []}, final_positions=[], kis_fills=[],
    )
    assert report["status"] == "OK"
    assert report["report"]["runtime_liveness_failures"] == []

    payload.update({
        "runtime_integrity_status": "RECONCILE_REQUIRED",
        "reconcile_liveness_status": "RECONCILE_REQUIRED",
    })
    _write_us_schedule_health(payload, "am")
    blocked = run_daily_report(
        env="practice", session="close", trade_date="2026-07-20", offline=True,
        final_balance={"positions": []}, final_positions=[], kis_fills=[],
    )
    assert blocked["status"] == "FAILED_RECONCILE"
