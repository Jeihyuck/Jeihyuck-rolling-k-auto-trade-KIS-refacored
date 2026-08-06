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
