"""Conservative proof requirements for US deployment signoff (not a broker gate)."""
from __future__ import annotations

REQUIRED = (
    "combined_ci_pass", "postgres_integration_pass", "kr_regression_pass",
    "windows_wsl_revision_verified", "prep_am_pin_verified",
    "stage4_20_ready_intervals_pass", "kis_original_fill_reconcile_pass",
    "claim_audit_pass", "close_reconcile_pass",
    "sell_priority_not_regressed", "entry_evidence_hash_link_pass",
    "shadow_never_authorizes_orders",
)

def evaluate_operational_readiness(proofs: dict) -> dict:
    """Unverified must be visible; an absent key is never inferred as PASS."""
    missing = [name for name in REQUIRED if proofs.get(name) is not True]
    failed = [name for name in REQUIRED if proofs.get(name) is False]
    unknown = [name for name in REQUIRED if proofs.get(name) is None]
    return {
        "status": "PASS" if not missing else "NOT_PROVEN",
        "passed": [name for name in REQUIRED if proofs.get(name) is True],
        "failed": failed,
        "unknown": unknown,
        "not_passed": missing,
        "total": len(REQUIRED),
    }

def evidence_digest(payload: dict) -> str:
    """Deterministic proof payload digest, rejecting nonfinite values."""
    import hashlib
    import json
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                     separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def evaluate_evidence_bundle(bundle: dict, *, expected_code_sha: str,
                             expected_trade_date: str) -> dict:
    """Do not accept a bare TRUE flag as KIS/WSL operational proof.

    This validates evidence packaging and cross-step consistency. It does
    NOT authenticate a claimed external KIS source without independent replay.
    """
    allowed_source = {
        "combined_ci_pass": {"GITHUB_CI"},
        "postgres_integration_pass": {"GITHUB_CI"},
        "kr_regression_pass": {"GITHUB_CI"},
        "windows_wsl_revision_verified": {"WSL_LOG"},
        "prep_am_pin_verified": {"WSL_LOG"},
        "stage4_20_ready_intervals_pass": {"WSL_LOG"},
        "kis_original_fill_reconcile_pass": {"KIS_READONLY"},
        "claim_audit_pass": {"KIS_READONLY", "DB_READONLY"},
        "close_reconcile_pass": {"WSL_LOG", "KIS_READONLY"},
        "sell_priority_not_regressed": {"GITHUB_CI", "WSL_LOG"},
        "entry_evidence_hash_link_pass": {"GITHUB_CI", "DB_READONLY"},
        "shadow_never_authorizes_orders": {"GITHUB_CI", "WSL_LOG"},
    }
    statuses = {}
    reasons = {}
    for key in REQUIRED:
        proof = bundle.get(key)
        if not isinstance(proof, dict):
            statuses[key] = None
            reasons[key] = "evidence_missing"
            continue
        payload = proof.get("payload")
        if not isinstance(payload, dict) or not payload:
            statuses[key] = False
            reasons[key] = "payload_missing"
            continue
        try:
            valid_digest = evidence_digest(payload) == proof.get("sha256")
        except (ValueError, TypeError):
            valid_digest = False
        checks = {
            "status": proof.get("status") == "PASS",
            "digest": valid_digest,
            "source": proof.get("source") in allowed_source[key],
            "revision": proof.get("code_sha") == expected_code_sha,
            "date": proof.get("trade_date") == expected_trade_date,
            "reference": bool(str(proof.get("source_reference") or "").strip()),
        }
        if key == "stage4_20_ready_intervals_pass":
            checks["stage4_continuity"] = (
                int(payload.get("ready_intervals") or 0) >= 20
                and float(payload.get("p95_sec") or 1e9) <= 25
                and float(payload.get("p99_sec") or 1e9) <= 30
                and int(payload.get("stale_gaps", -1)) == 0
            )
        if key == "close_reconcile_pass":
            checks["close_truth"] = (
                payload.get("report_consistency") == "OK"
                and payload.get("close_integrity_status") == "OK"
                and payload.get("manual_reconcile_required") is False
            )
        if key == "kis_original_fill_reconcile_pass":
            checks["kis_truth"] = (
                payload.get("broker") == "KIS"
                and payload.get("fills_verified") is True
                and payload.get("unmatched_orders") == 0
            )
        statuses[key] = all(checks.values())
        reasons[key] = "verified_envelope" if statuses[key] else (
            "invalid_fields:" + ",".join(k for k, ok in checks.items() if not ok)
        )
    result = evaluate_operational_readiness(statuses)
    result["evidence_reasons"] = reasons
    result["evaluation_kind"] = "EVIDENCE_ENVELOPE_ONLY_NOT_EXTERNAL_AUTHENTICATION"
    return result
