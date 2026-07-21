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
    session_results[session] = dict(result)
    balance_path = ROOT / "runtime/kr/session" / trade_date.isoformat() / session / "balance_precheck.json"
    phase_path = ROOT / "runtime/kr/session" / trade_date.isoformat() / "close" / "phase.json"
    balance: dict[str, Any] = {}
    if balance_path.exists():
        try:
            balance = json.loads(balance_path.read_text(encoding="utf-8"))
        except Exception:
            balance = {}
    phase: dict[str, Any] = {}
    if phase_path.exists():
        try:
            phase = json.loads(phase_path.read_text(encoding="utf-8"))
        except Exception:
            phase = {}
    precheck_state = balance.get("state")
    balance_warning = None
    if precheck_state == "OK" and not bool(balance.get("raw_snapshot_available")):
        precheck_state = "UNKNOWN_WITHOUT_SNAPSHOT"
        balance_warning = "OK state without snapshot was downgraded"
    positions_summary = balance.get("positions_summary") if isinstance(balance.get("positions_summary"), dict) else {}
    am_wait_reached = bool(result.get("am_wait_reached", session != "am" or result.get("status") != "FAIL"))
    close_skip_phase = str(result.get("reason") or result.get("status") or "").upper() == "SKIP_PHASE_WINDOW" or bool(phase.get("skip_phase_window"))
    close_phase_executed = bool(phase.get("phase_executed") or result.get("phase_executed"))
    if session == "close" and close_skip_phase:
        close_phase_executed = False
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
            "precheck_state": precheck_state,
            "source": balance.get("source"),
            "requery": False,
            "snapshot_available": bool(balance.get("raw_snapshot_available")),
            "cash": balance.get("cash"),
            "holdings_count": balance.get("holdings_count"),
            "positions_summary_count": len(positions_summary),
            "warning": balance_warning,
        },
        "am": {"wait_target": "09:00:05", "wait_reached": am_wait_reached},
        "close": {
            "phase": phase.get("phase") if phase else ("close" if session == "close" else None),
            "phase_executed": close_phase_executed,
            "force_phase": bool(phase.get("force_phase") or result.get("force_phase")),
            "skip_phase_window": close_skip_phase,
            "entry_enabled": bool(phase.get("entry_enabled", result.get("entry_enabled", False))),
            "exit_enabled": bool(phase.get("exit_enabled", result.get("exit_enabled", False))),
            "close_enabled": bool(phase.get("close_enabled", result.get("close_enabled", False))),
            "close_liquidation_enabled": bool(phase.get("close_liquidation_enabled", result.get("close_liquidation_enabled", False))),
        },
    }
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    # Health is a schedule-level contract, not merely a diagnostics attachment.
    # In particular a locked PB1 engine must never make a KR trading day look OK.
    sessions = session_results
    locked = any(str((value or {}).get("reason") or "") == "PB1_ADVISORY_LOCK_UNAVAILABLE" for value in sessions.values() if isinstance(value, dict))
    failed = any(str((value or {}).get("status") or "").upper() in {"FAIL", "FAILED", "SKIP_LOCKED"} for value in sessions.values() if isinstance(value, dict))
    health = {
        "ok": not (locked or failed),
        "status": "FAILED" if (locked or failed) else "OK",
        "reason": "kr_pb1_advisory_lock_unavailable" if locked else ("kr_session_failed" if failed else "KR_SESSION_DONE"),
        "trade_date": trade_date.isoformat(),
        "sessions": sessions,
    }
    for health_path in (ROOT / "runtime/health" / f"kr-{trade_date.isoformat()}.json", ROOT / "reports/kr_schedule_health" / f"{trade_date.isoformat()}.json"):
        health_path.parent.mkdir(parents=True, exist_ok=True)
        health_path.write_text(json.dumps(health, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return path
