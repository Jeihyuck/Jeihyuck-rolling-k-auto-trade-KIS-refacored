# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from trader.kr.artifacts import LEGACY_PATHS, ROOT, validate_kr_prep_artifact


def write_kr_diagnostics_manifest(*, trade_date: date, expected_as_of: date, session: str, result: dict[str, Any], env: str) -> Path:
    artifact = validate_kr_prep_artifact(trade_date=trade_date, expected_as_of=expected_as_of, env=env, allow_legacy=True)
    base = ROOT / "runtime/diagnostics/kr" / trade_date.isoformat()
    base.mkdir(parents=True, exist_ok=True)
    path = base / "manifest.json"
    existing: dict[str, Any] = {}
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            existing = {}
    session_results = dict(existing.get("session_results") or {})
    session_results[session] = str(result.get("status") or result.get("reason") or "UNKNOWN")
    balance_path = ROOT / "runtime/kr/session" / trade_date.isoformat() / session / "balance_precheck.json"
    balance: dict[str, Any] = {}
    if balance_path.exists():
        try:
            balance = json.loads(balance_path.read_text(encoding="utf-8"))
        except Exception:
            balance = {}
    am_wait_reached = bool(result.get("am_wait_reached", session != "am" or result.get("status") != "FAIL"))
    close_phase_executed = bool(session == "close" and result.get("status") != "FAIL")
    manifest = {
        "trade_date": trade_date.isoformat(),
        "expected_as_of": expected_as_of.isoformat(),
        "session_results": session_results,
        "artifact": {
            "valid": bool(artifact.ok),
            "reason": artifact.reason,
            "latest_final30_rows": artifact.final30_rows if artifact.ok else 0,
            "runtime_final30_rows": artifact.final30_rows if artifact.ok else 0,
            "payload_match": bool((artifact.details or {}).get("payload_match")) if artifact.ok else False,
            "contract_trade_date": trade_date.isoformat() if artifact.ok else None,
            "contract_expected_as_of": expected_as_of.isoformat() if artifact.ok else None,
            "legacy_artifact_present": any((ROOT / p).exists() for p in LEGACY_PATHS),
            "quarantined_count": len(list((ROOT / "runtime/quarantine/kr" / trade_date.isoformat()).glob("**/*"))) if (ROOT / "runtime/quarantine/kr" / trade_date.isoformat()).exists() else 0,
        },
        "balance": {
            "precheck_state": balance.get("state"),
            "source": balance.get("source"),
            "requery": False,
            "snapshot_available": bool(balance.get("raw_snapshot_available")),
        },
        "am": {"wait_target": "09:00:05", "wait_reached": am_wait_reached},
        "close": {"phase": "close" if session == "close" else None, "phase_executed": close_phase_executed, "skip_phase_window": False},
    }
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
