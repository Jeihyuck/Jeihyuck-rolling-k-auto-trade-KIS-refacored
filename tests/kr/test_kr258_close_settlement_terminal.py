"""KR258: explicit terminal economic truth remains distinct from broker ACK."""
from trader.kr.close_settlement_state_258 import assess_close_economic_settlement


def test_ack_only_partial_fills_remain_pending_without_resubmission():
    outcome = assess_close_economic_settlement(
        accepted_policy_sells=4, postsubmit_reconcile_ok=False,
        audit_report={"status": "ECONOMIC_SETTLEMENT_PENDING",
                      "gap_count": 2, "sell_orders_checked": 4},
    )
    assert outcome["economic_settlement_state"] == "ECONOMIC_SETTLEMENT_PENDING"
    assert outcome["economic_settlement_pending"] == 1
    assert outcome["native_priced_sell_gap_count"] == 2


def test_native_priced_sell_proof_is_only_settled_on_successful_post_reconcile():
    evidence = {"status": "PRICED_SELL_EVIDENCE_MATCHED",
                "gap_count": 0, "sell_orders_checked": 4}
    settled = assess_close_economic_settlement(
        accepted_policy_sells=4, postsubmit_reconcile_ok=True, audit_report=evidence,
    )
    assert settled["economic_settlement_state"] == "ECONOMIC_SETTLED"
    assert settled["economic_settlement_pending"] == 0

    failed_reconcile = assess_close_economic_settlement(
        accepted_policy_sells=4, postsubmit_reconcile_ok=False, audit_report=evidence,
    )
    assert failed_reconcile["economic_settlement_state"] == "ECONOMIC_SETTLEMENT_PENDING"


def test_audit_missing_cannot_be_mistaken_for_zero_gaps():
    status = assess_close_economic_settlement(
        accepted_policy_sells=1, postsubmit_reconcile_ok=True, audit_report=None,
    )
    assert status["economic_settlement_pending"] == 1
    assert status["native_priced_sell_gap_count"] == -1


def test_priced_gap_from_older_order_remains_pending_even_with_no_new_sell():
    status = assess_close_economic_settlement(
        accepted_policy_sells=0, postsubmit_reconcile_ok=False,
        audit_report={"status": "ECONOMIC_SETTLEMENT_PENDING",
                      "gap_count": 2, "sell_orders_checked": 4},
    )
    assert status["economic_settlement_pending"] == 1


def test_close_run_path_persists_pending_state_without_broker_retry():
    from pathlib import Path
    source = (Path(__file__).resolve().parents[2] / "trader" / "pb1_runner.py").read_text(encoding="utf-8")
    assert "audit_close_priced_sell_evidence(" in source
    assert 'status="PENDING_SETTLEMENT" if settlement_metrics["economic_settlement_pending"] else "OK"' in source
    assert source.count("**settlement_metrics") >= 2
    assert "action=KEEP_ORDER_IDEMPOTENCY_NO_SELL_RETRY" in source


def test_close_ack_session_complete_independent_of_economic_pending():
    from trader.pb1_runner import normalize_session_result, _compute_session_marker_payload

    normalized = normalize_session_result(
        status="OK_CLOSE_POLICY", reason="ECONOMIC_SETTLEMENT_PENDING",
    )
    assert normalized.completed == 1 and normalized.retryable == 0
    marker = _compute_session_marker_payload(
        status=normalized.status, exit_reason=normalized.exit_reason,
        exit_code=0, sell_orders_ack=4,
        entry_status="DONE", entry_abort_reason=None,
    )
    assert marker["completed"] is True
    assert marker["retryable"] is False
    assert marker["sell_orders_ack"] == 4


def test_close_finalizer_cannot_overwrite_economic_pending_run_status():
    from pathlib import Path
    source = (Path(__file__).resolve().parents[2] / "trader" / "pb1_runner.py").read_text(encoding="utf-8")
    assert '"PENDING_SETTLEMENT" if close_ack_ok and close_pending' in source
    assert '"economic_settlement_state": str(metrics.get("economic_settlement_state")' in source
    assert 'main_exit_reason = close_state' in source
