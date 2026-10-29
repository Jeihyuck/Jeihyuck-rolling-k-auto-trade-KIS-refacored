from trader.us.validation.integration_acceptance import (
    REQUIRED, evaluate_operational_readiness,
)

def test_missing_proof_cannot_pass():
    result = evaluate_operational_readiness({"combined_ci_pass": True})
    assert result["status"] == "NOT_PROVEN"
    assert "close_reconcile_pass" in result["unknown"]

def test_failure_is_separated_from_unknown():
    result = evaluate_operational_readiness({"close_reconcile_pass": False})
    assert "close_reconcile_pass" in result["failed"]
    assert "claim_audit_pass" in result["unknown"]

def test_all_verified_proof_passes():
    result = evaluate_operational_readiness(dict.fromkeys(REQUIRED, True))
    assert result["status"] == "PASS"
    assert not result["not_passed"]
