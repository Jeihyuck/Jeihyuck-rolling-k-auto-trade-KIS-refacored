"""Pure offline acceptance checks for the operator's SELECT-only KR audit."""
from copy import deepcopy

from scripts.kr_release_acceptance_readonly import (
    TRIGGERS,
    evaluate_readiness,
)

SHA = "a" * 40
OTHER = "b" * 40


def good_snapshot():
    return {
        "entry_shas": {SHA: 15},
        "migration_0063": True,
        "triggers": {
            name: {"table": table, "enabled": True}
            for name, table in TRIGGERS.items()
        },
        "active_epoch_count": 1,
        "submitted_after_180s": 0,
        "elapsed_runs_over_180s": 0,
        "uncertain_claims": 0,
        "pending_close_runs": 0,
        "buy_skips": 3,
        "stale_quote_skips": 2,
        "timing_price_skips": 1,
    }


def test_db_only_never_promotes_unverified_kis_to_live_success():
    result = evaluate_readiness(good_snapshot(), SHA)
    assert result["release_readiness"] == "OPERATOR_KIS_ATTESTATION_REQUIRED"
    assert result["checks"]["operator_run_sha"] == "PASS"
    assert result["checks"]["signed_contract_triggers"] == "PASS"
    assert result["checks"]["original_broker_kis_evidence"] == "NOT_ATTESTED"
    assert result["checks"]["seller_first_and_restart_ack_replay"] == "NOT_ATTESTED"
    assert result["counts"]["buy_skips"] == 3


def test_operator_running_old_code_is_blocked_even_if_it_passes_ci():
    s = good_snapshot()
    s["entry_shas"] = {OTHER: 111}
    result = evaluate_readiness(s, SHA)
    assert result["release_readiness"] == "BLOCKED"
    assert result["checks"]["operator_run_sha"] == "BLOCKED"


def test_mixed_shas_are_not_falsely_treated_as_fully_deployed():
    s = good_snapshot()
    s["entry_shas"] = {SHA: 1, OTHER: 1}
    assert evaluate_readiness(s, SHA)["checks"]["operator_run_sha"] == "BLOCKED"


def test_migration_and_trigger_install_must_both_be_attested():
    s = good_snapshot()
    s["migration_0063"] = False
    assert evaluate_readiness(s, SHA)["checks"]["migration_0063"] == "BLOCKED"
    s["migration_0063"] = True
    s["triggers"]["kr_signed_buy_order_guard_0063"]["enabled"] = False
    assert evaluate_readiness(s, SHA)["checks"]["signed_contract_triggers"] == "BLOCKED"


def test_run_elapsed_is_not_misclassified_as_late_order_submission():
    s = good_snapshot()
    s["elapsed_runs_over_180s"] = 25
    result = evaluate_readiness(s, SHA)
    assert result["checks"]["submitted_after_180s"] == "PASS"
    assert result["checks"]["run_elapsed_over_180s"] == "REVIEW_PERFORMANCE"
    s["submitted_after_180s"] = 1
    result = evaluate_readiness(s, SHA)
    assert result["checks"]["submitted_after_180s"] == "BLOCKED"
    assert result["release_readiness"] == "BLOCKED"


def test_historical_uncertain_order_not_auto_satisfied_or_cleared():
    s = good_snapshot()
    s["uncertain_claims"] = 1
    s["pending_close_runs"] = 1
    result = evaluate_readiness(s, SHA)
    assert result["checks"]["historical_uncertain_claim"] == "REVIEW_REQUIRED"
    assert result["checks"]["pending_close_settlement"] == "REVIEW_REQUIRED"
    assert result["counts"]["uncertain_claims"] == 1
    assert result["counts"]["pending_close_runs"] == 1


def test_account_or_epoch_mismatch_is_release_blocker():
    s = good_snapshot()
    s["active_epoch_count"] = 0
    assert evaluate_readiness(s, SHA)["checks"]["active_account_epoch"] == "BLOCKED"
    s["active_epoch_count"] = 2
    assert evaluate_readiness(s, SHA)["checks"]["active_account_epoch"] == "BLOCKED"


def test_no_new_run_is_not_a_live_code_attestation():
    s = good_snapshot()
    s["entry_shas"] = {}
    assert evaluate_readiness(s, SHA)["checks"]["operator_run_sha"] == "BLOCKED"



def test_bad_signed_open_position_and_buy_hash_are_strict_release_blockers():
    s = good_snapshot()
    s["malformed_signed_open"] = 1
    result = evaluate_readiness(s, SHA)
    assert result["checks"]["active_signed_position_contracts"] == "BLOCKED"
    assert result["release_readiness"] == "BLOCKED"
    s["malformed_signed_open"] = 0
    s["malformed_signed_buy"] = 1
    assert evaluate_readiness(s, SHA)["checks"]["existing_signed_buy_contracts"] == "BLOCKED"


def test_five_zero_qty_closed_legacy_hash_gaps_reported_not_autorepaired():
    s = good_snapshot()
    s["historical_closed_missing_exit_sha"] = 5
    result = evaluate_readiness(s, SHA)
    assert result["checks"]["closed_historical_signed_meta"] == "REVIEW_HISTORICAL"
    assert result["checks"]["active_signed_position_contracts"] == "PASS"
    assert result["counts"]["historical_closed_missing_exit_sha"] == 5
    assert result["release_readiness"] == "OPERATOR_KIS_ATTESTATION_REQUIRED"
