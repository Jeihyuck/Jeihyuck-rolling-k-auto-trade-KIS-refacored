"""Evidence gate contract — no real order/DB/WS mutations."""
from __future__ import annotations

import json

from scripts.verify_us_merge_readiness import evaluate, nearest_rank


SHA = "a" * 40
DATE = "2026-10-13"


def _fixture(root, *, sha=SHA, tick_sec=48, sell_sec=8,
             close_status="OK", extra_close=None, buy_defer=0):
    daily = root / "reports" / "us_daily" / DATE
    daily.mkdir(parents=True)
    for session in ("am", "afternoon"):
        (daily / f"{session}_summary.json").write_text(json.dumps({
            "commit_sha": sha, "tick_latency": {
                "entry_defer_count": buy_defer,
                "kis_http_calls": {"quote_http_calls": 3},
            },
        }))
        folder = root / "runtime" / "logs" / "us" / DATE / session
        folder.mkdir(parents=True)
        body = "".join(
            f"[US_TICK][LATENCY] tick={i} total_ms={tick_sec * 1000} "
            f"balance_ms=100.0" + chr(10)
            for i in range(1, 17)
        )
        if session == "am" and sell_sec is not None:
            body += (
                "[US_EXIT][SIGNAL_TO_BROKER_BOUNDARY] symbol=MSFT "
                f"attempt=synthetic delay_ms={sell_sec * 1000}" + chr(10)
            )
        (folder / f"{session}.log").write_text(body)
    (daily / "close_summary.json").write_text(json.dumps({
        "commit_sha": sha, "status": close_status,
        "open_order_pending_total": 0, "manual_reconcile_required": 0,
        "errors": [], **(extra_close or {}),
    }))


def test_nearest_rank_math_for_integer_tick_count():
    assert nearest_rank([1, 2, 3, 4, 5], .95) == 5
    assert nearest_rank([], .95) is None


def test_good_live_shaped_evidence_passes(tmp_path):
    _fixture(tmp_path)
    report = evaluate(tmp_path, DATE, SHA)
    assert report["result"] == "PASS", report
    assert report["measured_ticks"] == 32
    assert report["tick_p95_sec"] == 48
    assert report["sell_signal_to_broker_boundary_p95_sec"] == 8
    assert report["kis_quote_rest_calls"] == 6


def test_prechange_old_sha_cannot_count_as_postmerge_evidence(tmp_path):
    _fixture(tmp_path, sha="b" * 40)
    report = evaluate(tmp_path, DATE, SHA)
    assert report["result"] == "NOT_MERGE_READY"
    assert "EXECUTION_SHA_MISMATCH_OR_UNVERIFIED" in report["reasons"]


def test_old_300_second_tick_latency_never_passes(tmp_path):
    _fixture(tmp_path, tick_sec=308, sell_sec=38, buy_defer=1)
    report = evaluate(tmp_path, DATE, SHA)
    assert "TICK_P95_ABOVE_60_SEC_OR_UNMEASURED" in report["reasons"]
    assert "PROTECTIVE_SELL_LATENCY_LIMIT_FAILED" in report["reasons"]
    assert "BUY_EVALUATION_DEFERRED_FOR_TIME" in report["reasons"]


def test_close_pending_and_manual_reconcile_fail_closed(tmp_path):
    _fixture(tmp_path, extra_close={
        "open_order_pending_total": 1,
        "manual_reconcile_required": 1,
        "errors": ["UNRESOLVED_EXECUTION_ACTIONS:3"],
    })
    report = evaluate(tmp_path, DATE, SHA)
    assert report["result"] == "NOT_MERGE_READY"
    assert "BROKER_OPEN_ORDERS_REQUIRE_CLOSE_TRUTH_REVIEW" in report["reasons"]
    assert "UNRESOLVED_EXECUTION_OR_MANUAL_RECONCILIATION" in report["reasons"]


def test_no_protective_broker_boundary_cannot_pass(tmp_path):
    _fixture(tmp_path, sell_sec=None)
    report = evaluate(tmp_path, DATE, SHA)
    assert "NO_ACTUAL_SELL_BROKER_SUBMISSION_LATENCY_EVIDENCE" in report["reasons"]


def test_clean_label_does_not_hide_claim_conflicts_or_protective_sell_failure(tmp_path):
    _fixture(tmp_path, close_status="OK", extra_close={
        "close_integrity_status": "RECONCILE_REQUIRED",
        "execution_claim_health": {
            "unresolved_execution_actions": 3,
            "execution_claim_conflicts": 223,
        },
        "runtime_liveness_failures": [
            {"session": "am", "sell_liveness_status": "RECONCILE_REQUIRED"}
        ],
    })
    report = evaluate(tmp_path, DATE, SHA)
    assert report["result"] == "NOT_MERGE_READY"
    assert "CLOSE_BROKER_CLAIM_OR_LIVENESS_NOT_CLEAN" in report["reasons"]
    assert report["close_claim_unresolved"] == 3
