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

def test_operational_envelope_rejects_unverified_flags_and_wrong_digest():
    from trader.us.validation.integration_acceptance import (
        evaluate_evidence_bundle, evidence_digest,
    )
    payload = {"source_event": "ci-run-123"}
    bundle = {
        "combined_ci_pass": {
            "status": "PASS", "source": "GITHUB_CI",
            "code_sha": "sha-1", "trade_date": "2026-10-28",
            "source_reference": "https://github.com/example/run/123",
            "payload": payload, "sha256": evidence_digest(payload),
        }
    }
    result = evaluate_evidence_bundle(bundle, expected_code_sha="sha-1",
                                      expected_trade_date="2026-10-28")
    assert result["status"] == "NOT_PROVEN"
    assert "combined_ci_pass" in result["passed"]
    assert "kis_original_fill_reconcile_pass" in result["unknown"]
    bundle["combined_ci_pass"]["sha256"] = "0" * 64
    result = evaluate_evidence_bundle(bundle, expected_code_sha="sha-1",
                                      expected_trade_date="2026-10-28")
    assert "combined_ci_pass" in result["failed"]


def test_stage4_zero_stale_is_valid_but_close_failure_never_passes():
    from trader.us.validation.integration_acceptance import (
        evaluate_evidence_bundle, evidence_digest,
    )
    p = {"ready_intervals": 20, "p95_sec": 9.0, "p99_sec": 14.0,
         "stale_gaps": 0}
    proof = {"status": "PASS", "source": "WSL_LOG", "code_sha": "sha-2",
             "trade_date": "2026-10-28", "source_reference": "log/run-1",
             "payload": p, "sha256": evidence_digest(p)}
    bad_close = {"report_consistency": "FAILED", "close_integrity_status": "RECONCILE_REQUIRED",
                 "manual_reconcile_required": True}
    result = evaluate_evidence_bundle({
        "stage4_20_ready_intervals_pass": proof,
        "close_reconcile_pass": {
            **proof, "payload": bad_close, "sha256": evidence_digest(bad_close),
        },
    }, expected_code_sha="sha-2", expected_trade_date="2026-10-28")
    assert "stage4_20_ready_intervals_pass" in result["passed"]
    assert "close_reconcile_pass" in result["failed"]
