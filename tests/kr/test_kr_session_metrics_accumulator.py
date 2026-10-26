from __future__ import annotations

import json

from trader.pb1_runner import _write_session_result_file


def test_session_result_uses_accumulated_metrics_and_status(tmp_path, monkeypatch) -> None:
    out = tmp_path / "session.json"
    monkeypatch.setenv("PB1_SESSION_RESULT_PATH", str(out))

    _write_session_result_file(
        {
            "status": "OK_NO_TRADE",
            "exit_reason": "session_end",
            "ticks_total": 5,
            "buy_orders": 3,
            "buy_orders_ack": 3,
            "sell_orders": 0,
            "sell_orders_ack": 0,
            "order_candidates": 4,
            "api_submitted": 3,
        }
    )

    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["status"] == "OK_WITH_ORDERS"
    assert payload["ticks_total"] == 5
    assert payload["api_submitted"] == 3
    assert payload["buy_orders_ack"] == 3


def test_timed_out_ticks_count_as_attempts_and_warning_totals_survive_session_json(tmp_path, monkeypatch):
    from trader.pb1_runner import _count_kr_tick_attempt

    out = tmp_path / "kr-session.json"
    monkeypatch.setenv("PB1_SESSION_RESULT_PATH", str(out))
    session_metrics = {"ticks_total": 0, "ticks_completed": 0}
    for _ in range(4):
        _count_kr_tick_attempt(session_metrics)
        session_metrics["ticks_completed"] += 1
    # Fifth run_once times out and never reaches the normal completion path.
    _count_kr_tick_attempt(session_metrics)
    assert session_metrics == {"ticks_total": 5, "ticks_completed": 4}

    _write_session_result_file({
        "status": "OK_WITH_WARNINGS",
        "exit_reason": "session_end",
        "ticks_total": session_metrics["ticks_total"],
        "ticks_completed": session_metrics["ticks_completed"],
        "ticks_timeout": 1,
        "ticks_degraded": 1,
        "terminal_state": "SESSION_END_OK_WITH_WARNINGS",
        "warning_counts": {"timeout_count": 1, "tick_timeout_recoverable": 1},
    })
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["ticks_total"] == 5
    assert payload["ticks_completed"] == 4
    assert payload["ticks_timeout"] == 1
    assert payload["warning_counts"]["timeout_count"] == 1
    assert payload["terminal_state"] == "SESSION_END_OK_WITH_WARNINGS"
