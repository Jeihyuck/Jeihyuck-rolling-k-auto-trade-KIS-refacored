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


def test_unknown_terminal_event_does_not_prove_zero_final_skips():
    from trader.pb1_runner import _accumulate_kr_tick_skip_observations

    metrics = {}
    _accumulate_kr_tick_skip_observations(metrics, {})
    assert metrics.get("terminal_final_skip_ticks", 0) == 0
    assert metrics["terminal_event_unobserved_ticks"] == 1
    assert metrics["skipped_order_unobserved_ticks"] == 1
    # This zero is merely "not observed", not authoritative evidence that
    # no per-order or terminal skips happened in the engine.


def test_1126_retryable_tick_errors_survive_last_normal_wait():
    from trader.pb1_runner import (
        _accumulate_kr_tick_outcome, _resolve_session_terminal_state,
    )
    metrics, warnings = {}, {}
    groups = [
        ("OK_NO_TRADE", "OPENING_30MIN_BUY_BLOCK", 16),
        ("RETRYABLE_MARKET_DATA_ERROR", "KR_ENTRY_TIMING_DATA_UNVERIFIED", 45),
        ("RETRYABLE_ORDER_BUILD_ERROR", "ORDER_CANDIDATE_BUT_ZERO_API_SUBMIT", 14),
        ("OK_NO_TRADE", "KR_ENTRY_TIMING_POLICY_WAIT", 82),
    ]
    degraded = 0
    for status, reason, count in groups:
        for _ in range(count):
            degraded += int(_accumulate_kr_tick_outcome(
                metrics, warnings, status=status, reason=reason))
    assert sum(metrics.values()) == 157
    assert degraded == 59
    assert metrics["market_data_error_ticks"] == 45
    assert metrics["order_build_error_ticks"] == 14
    assert metrics["entry_policy_wait_ticks"] == 82
    assert metrics["opening_buy_block_ticks"] == 16
    assert _resolve_session_terminal_state(
        result_status="OK_NO_TRADE", exit_reason="session_end", warning_counts=warnings,
    ) == "SESSION_END_OK_DEGRADED"
    assert _resolve_session_terminal_state(
        result_status="OK_NO_TRADE", exit_reason="session_end", warning_counts={},
    ) == "SESSION_END_OK"


def test_1126_mail_log_records_distinguish_policy_and_errors(tmp_path):
    from scripts.wsl.kr_mail_execution_evidence import _session_order_activity

    rel = "runtime/logs/kr/2026-11-26/am/fixture.log"
    path = tmp_path / rel
    path.parent.mkdir(parents=True)
    groups = [
        ("OK_NO_TRADE", "OPENING_30MIN_BUY_BLOCK", 16),
        ("RETRYABLE_MARKET_DATA_ERROR", "KR_ENTRY_TIMING_DATA_UNVERIFIED", 21),
        ("RETRYABLE_ORDER_BUILD_ERROR", "ORDER_CANDIDATE_BUT_ZERO_API_SUBMIT", 14),
        ("OK_NO_TRADE", "KR_ENTRY_TIMING_POLICY_WAIT", 62),
    ]
    path.write_text("\n".join(
        f"[RUN_SUMMARY][RESULT] status={status} reason={reason} session=morning"
        for status, reason, count in groups for _ in range(count)
    ))
    evidence = _session_order_activity(tmp_path, "2026-11-26", "am", {"log_file": rel})
    assert evidence["tick_summary_records"] == 113
    assert evidence["market_data_error_ticks"] == 21
    assert evidence["order_build_error_ticks"] == 14
    assert evidence["entry_policy_wait_ticks"] == 62
    assert evidence["opening_buy_block_ticks"] == 16
    path.write_text("no terminal records")
    assert _session_order_activity(tmp_path, "2026-11-26", "am", {"log_file": rel})[
        "order_build_error_ticks"] is None
