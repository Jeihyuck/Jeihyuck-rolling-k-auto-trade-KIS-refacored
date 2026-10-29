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
