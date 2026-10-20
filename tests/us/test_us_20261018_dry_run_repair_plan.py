"""Evidence-aware dry-run repair plans. Always read-only and fenced."""
from scripts.us_order_evidence_dry_run import classify


def _order(**changes):
    row = {"trade_date": "2026-10-14", "symbol": "ABBV",
           "side": "SELL", "qty_requested": 2, "qty_filled": 2,
           "order_status": "FILLED", "attempt_state": "ACKED",
           "action_state": "IN_FLIGHT", "authoritative": False,
           "synthetic_count": 1, "broker_count": 0}
    row.update(changes)
    return row


def test_all_seven_synthetic_cases_remain_fenced_and_dry_run_only():
    for symbol, qty in (("MCK", 1), ("NET", 1), ("COST", 1),
                        ("ABBV", 2), ("PG", 4), ("GILD", 4),
                        ("DHR", 2)):
        out = classify(_order(symbol=symbol, qty_requested=qty, qty_filled=qty))
        assert out["finding"] == "SYNTHETIC_ONLY_FILLED_ORDER"
        assert out["proposed_change"] == "NONE_DRY_RUN"
        assert out["proposed_change_plan"] == "VERIFY_ORIGINAL_ORDER_WITH_ACCOUNT_SCOPED_KIS_EVIDENCE"
        assert out["defer_reason"] == "BALANCE_DELTA_IS_NOT_BROKER_EXECUTION_PROOF"
        assert not out["release_fence"]


def test_missing_open_order_not_assumed_cancelled_or_expired():
    out = classify(_order(symbol="KO", qty_filled=0,
                          order_status="OPEN", synthetic_count=0))
    assert out["proposed_change_plan"] == "REQUERY_BROKER_ORIGINAL_TRADE_DATE_KEEP_CLAIM_FENCED"
    assert out["defer_reason"] == "BROKER_QUERY_ABSENCE_IS_NOT_TERMINAL_PROOF"
    assert not out["release_fence"]


def test_real_fill_is_not_automatically_attributed_to_wrong_attempt():
    out = classify(_order(symbol="NET", qty_requested=2, qty_filled=2,
                          broker_count=1, synthetic_count=0, attempt_state="ACKED"))
    assert out["proposed_change_plan"] == "VALIDATE_ORDER_ATTEMPT_IDENTITY_BEFORE_IDEMPOTENT_RECONCILIATION"
    assert out["defer_reason"] == "FILL_ROW_COUNT_ALONE_DOES_NOT_VERIFY_ATTEMPT_IDENTITY"
    assert not out["release_fence"]


def test_confirmed_satisfied_order_needs_no_repair():
    out = classify(_order(symbol="NET", qty_requested=2, qty_filled=2,
                          broker_count=1, synthetic_count=0,
                          attempt_state="FILLED", action_state="SATISFIED", authoritative=True))
    assert out["proposed_change_plan"] == "NO_STATE_MUTATION_NEEDED"
    assert out["defer_reason"] == "NONE"


def test_satisfied_label_without_authority_still_needs_reconciliation():
    out = classify(_order(broker_count=1, synthetic_count=0,
                          attempt_state="FILLED", action_state="SATISFIED"))
    assert out["proposed_change_plan"] == "VALIDATE_ORDER_ATTEMPT_IDENTITY_BEFORE_IDEMPOTENT_RECONCILIATION"
