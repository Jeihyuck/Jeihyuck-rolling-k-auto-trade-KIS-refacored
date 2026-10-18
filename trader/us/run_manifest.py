"""Immutable per-trade-day revision manifest used by every US session."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path


def manifest_path(trade_date: str) -> Path:
    return Path(os.getenv("US_RUN_MANIFEST_DIR", "runtime/us/run_manifests")) / f"{trade_date}.json"


def pin_run_revision(
    trade_date: str,
    revision: str,
    *,
    replace: bool = False,
    allow_recovery_upgrade: bool = False,
    replace_reason: str | None = None,
    replace_meta: dict | None = None,
) -> dict:
    path = manifest_path(trade_date)
    reason = ""
    if replace:
        if allow_recovery_upgrade:
            reason = str(replace_reason or "RECOVERY_UPGRADE_ALLOWED").strip()
        else:
            approved = os.getenv("US_REPREP_APPROVED", "0") == "1"
            reason = os.getenv("US_REPREP_AUDIT_REASON", "").strip()
            if not approved or not reason:
                raise RuntimeError("reprep_requires_operator_approval_and_audit_reason")
    if path.exists() and not replace:
        current = json.loads(path.read_text(encoding="utf-8"))
        if current.get("run_revision") != revision:
            raise RuntimeError("trade_day_revision_already_pinned")
        return current
    payload = {"trade_date": trade_date, "run_revision": revision,
               "strategy_schema_version": "us-v1", "prep_schema_version": "1",
               "generated_at": datetime.now(timezone.utc).isoformat()}
    if replace:
        payload["reprep_audit_reason"] = reason
        payload["replaced"] = True
        payload["replace_mode"] = "recovery_upgrade" if allow_recovery_upgrade else "manual_reprep"
    if isinstance(replace_meta, dict) and replace_meta:
        payload["replace_meta"] = replace_meta
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)
    return payload



def precheck_prep_revision(trade_date: str, revision: str,
                           existing_contract: dict | None = None,
                           *, environ: dict | None = None) -> dict:
    """Read-only PREP revision check *before* expensive candidate discovery.

    This is advisory for recovery upgrades only. Final pin_run_revision still
    enforces the immutable trade-day revision and order-progress policy.
    """
    flags = os.environ if environ is None else environ
    path = manifest_path(trade_date)
    if not path.exists() or not revision or revision == "unknown":
        return {"ok": True, "reason": "no_comparable_revision"}
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(manifest, dict):
            raise ValueError("invalid manifest")
        pinned = str(manifest.get("run_revision") or "")
        if not pinned or str(manifest.get("trade_date") or "") != str(trade_date):
            raise ValueError("manifest identity incomplete")
    except (ValueError, OSError, TypeError, json.JSONDecodeError):
        return {"ok": False, "reason": "existing_revision_manifest_unreadable"}
    if pinned == revision:
        return {"ok": True, "reason": "same_revision"}

    # Preserve the established manual revision upgrade path; never silently
    # set or overwrite operator approval or audit reason.
    if str(flags.get("US_REPREP_NEW_REVISION", "0")) == "1":
        if (str(flags.get("US_REPREP_APPROVED", "0")) == "1"
                and str(flags.get("US_REPREP_AUDIT_REASON", "")).strip()):
            return {"ok": True, "reason": "operator_approved_reprep"}
        return {"ok": False, "reason": "reprep_requires_operator_approval_and_audit_reason"}

    old = existing_contract if isinstance(existing_contract, dict) else {}
    old_status = str(old.get("status") or "").upper()
    old_entry = bool(int(old.get("entry_can_proceed", old.get("trade_can_proceed", 0)) or 0))
    # Old recoverable PREP may be upgraded after validating an entirely new
    # candidate/score contract. Do not preempt the already approved recovery
    # path: its later broker-order-progress fence remains mandatory.
    recoverable = (old_status in {"DEFENSE_CRASH_ENTRY_BLOCKED", "ERROR", "DEGRADED"}
                   or old_status.startswith(("DEGRADED", "OK_WITH_WARNINGS")))
    if recoverable and not old_entry:
        return {"ok": True, "reason": "defer_to_existing_recovery_upgrade_gate"}
    return {"ok": False, "reason": "trade_day_revision_already_pinned"}

def verify_run_revision(trade_date: str, current_revision: str, *, expected_revision: str | None = None,
                        source: str = "local_manifest_cache") -> dict:
    if expected_revision:
        ok = str(expected_revision) == str(current_revision)
        return {"ok": ok, "reason": "ok" if ok else "run_revision_mismatch",
                "entry_can_proceed": ok, "reconciliation_can_proceed": True,
                "source": "db_prep_contract", "expected_revision": expected_revision}
    path = manifest_path(trade_date)
    if not path.exists():
        return {"ok": False, "reason": "run_manifest_missing", "entry_can_proceed": False,
                "reconciliation_can_proceed": True}
    manifest = json.loads(path.read_text(encoding="utf-8"))
    ok = manifest.get("run_revision") == current_revision
    return {"ok": ok, "reason": "ok" if ok else "run_revision_mismatch",
            "entry_can_proceed": ok, "reconciliation_can_proceed": True, "manifest": manifest,
            "source": source}
