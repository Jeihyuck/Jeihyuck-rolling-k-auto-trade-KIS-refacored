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


def test_terminal_final_skip_distinct_from_unknown_order_skips(tmp_path, monkeypatch):
    from trader.pb1_runner import _accumulate_kr_tick_skip_observations

    out = tmp_path / "kr-session-skips.json"
    monkeypatch.setenv("PB1_SESSION_RESULT_PATH", str(out))
    summary = {"skipped": 0, "ticks_completed": 0}
    # Engine reported FINAL_SKIP but *not* the number of broker-order skips.
    _accumulate_kr_tick_skip_observations(
        summary, {"terminal_event": "FINAL_SKIP", "order_candidates": 0},
    )
    summary["ticks_completed"] += 1
    # A second tick explicitly witnessed zero order skips.
    _accumulate_kr_tick_skip_observations(
        summary, {"terminal_event": "NO_ORDERABLE_CANDIDATES", "skipped": 0},
    )
    summary["ticks_completed"] += 1
    assert summary["terminal_final_skip_ticks"] == 1
    assert summary["skipped"] == 0
    assert summary["skipped_order_unobserved_ticks"] == 1
    assert summary["skipped_order_observed_ticks"] == 1
    _write_session_result_file({
        "status": "OK_NO_TRADE",
        "ticks_total": 2,
        "ticks_completed": 2,
        "terminal_final_skip_ticks": summary["terminal_final_skip_ticks"],
        "skipped": summary["skipped"],
        "skipped_order_unobserved_ticks": summary["skipped_order_unobserved_ticks"],
        "skipped_order_observed_ticks": summary["skipped_order_observed_ticks"],
        "skipped_order_count_complete": False,
    })
    saved = json.loads(out.read_text(encoding="utf-8"))
    assert saved["terminal_final_skip_ticks"] == 1
    assert saved["skipped"] == 0
    assert saved["skipped_order_count_complete"] is False
    # The count is not fabricated as 1; the missing observation is visible.


def test_explicit_order_skip_counter_accumulates_without_conflating_terminal_event():
    from trader.pb1_runner import _accumulate_kr_tick_skip_observations

    summary = {}
    _accumulate_kr_tick_skip_observations(
        summary, {"skipped_count": 3, "terminal_event": "FINAL_SKIP"},
    )
    _accumulate_kr_tick_skip_observations(
        summary, {"skipped": 2, "terminal_event": "OK_WITH_ORDERS"},
    )
    assert summary["skipped"] == 5
    assert summary["terminal_final_skip_ticks"] == 1
    assert summary["skipped_order_observed_ticks"] == 2
    assert summary.get("skipped_order_unobserved_ticks", 0) == 0
