#!/usr/bin/env python3
"""Read-only WSL Stage4 writer acceptance (no KIS submit/cancel and no DB writes).

Run from a canonical, activated US WSL environment, while a session owns the
writer. A READY heartbeat is only one of several operational proof gates.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import text

EXPECTED_FLAGS = {
    "KIS_ENV": "practice",
    "US_PAPER_TRADING_ENABLED": "1",
    "DRY_RUN": "0",
    "US_STAGE123_PAPER_PROFILE": "ON",
    "US_RECONCILE_WORKER_MODE": "WORKER",
    "US_STAGE4_WRITER_AUTHORIZED": "1",
    "US_PROTECTIVE_STOP_LEDGER_MODE": "ENFORCE",
    "US_ACCOUNT_RESERVATION_MODE": "ENFORCE",
    "US_ATOMIC_CLAIM_OUTBOX_ENABLED": "1",
    "US_SHARED_KIS_LIMITER_ENABLED": "1",
}
_ALLOWED_REPLAY = {"OK", "RECOVERED", "NO_ACTIVE_CLAIMS", "WARN", "PARTIAL", "UNRESOLVED"}


def evaluate_writer_snapshot(*, flags, scope, row, report, now=None,
                             max_age_seconds=30.0):
    now = now or datetime.now(timezone.utc)
    issues = []
    incorrect = sorted(k for k, expected in EXPECTED_FLAGS.items()
                       if flags.get(k) != expected)
    if incorrect:
        issues.append("STAGE4_FLAGS_NOT_ARMED:" + ",".join(incorrect))
    if scope.get("env") != "practice" or not scope.get("account_id") or not scope.get("trading_epoch_id"):
        issues.append("STAGE4_ACCOUNT_EPOCH_SCOPE_UNVERIFIED")
    if row is None:
        issues.append("STAGE4_DB_HEALTH_MISSING")
    if report is None:
        issues.append("STAGE4_SESSION_REPORT_MISSING")
    if row is not None:
        if row.get("worker_state") != "READY":
            issues.append("STAGE4_DB_NOT_READY")
        if row.get("reconcile_status") not in {"OK", "WARN"} or row.get("replay_status") not in _ALLOWED_REPLAY:
            issues.append("STAGE4_RECONCILIATION_NOT_READY")
        observed = row.get("evidence_at")
        if (not isinstance(observed, datetime) or observed.tzinfo is None
                or (now - observed).total_seconds() < 0
                or (now - observed).total_seconds() > max_age_seconds):
            issues.append("STAGE4_DB_HEARTBEAT_STALE")
    if report is not None:
        if report.get("read_status") != "OK" or report.get("worker_state") != "READY":
            issues.append("STAGE4_SESSION_REPORT_UNREADY")
        try:
            sampled = datetime.fromisoformat(
                str(report.get("sampled_at_utc") or "").replace("Z", "+00:00")
            )
            age = (now - sampled).total_seconds()
            if sampled.tzinfo is None or age < 0 or age > max_age_seconds:
                issues.append("STAGE4_REPORT_STALE")
        except (ValueError, TypeError):
            issues.append("STAGE4_REPORT_TIMESTAMP_INVALID")
    if row is not None and report is not None:
        try:
            if uuid.UUID(str(row.get("writer_generation"))) != uuid.UUID(
                    str(report.get("writer_generation"))):
                issues.append("STAGE4_REPORT_GENERATION_MISMATCH")
        except (ValueError, TypeError, AttributeError):
            issues.append("STAGE4_REPORT_GENERATION_UNVERIFIED")
    return {
        "writer_readiness": "PASS" if not issues else "BLOCK",
        "blocking_reasons": issues,
        "read_only": True,
        "orders_or_cancels_submitted": 0,
        "broker_original_date_terminal_evidence": "NOT_CHECKED",
        "native_order_ack_fill_completeness": "NOT_CHECKED",
        "native_http_p95_performance": "SEPARATE_LOG_AUDIT_REQUIRED",
        "full_operational_acceptance": "NOT_PROVEN",
    }


def git_code_pin(trade_date: str, *, root=Path(".")):
    def git(*args):
        return subprocess.run(
            ["git", *args], cwd=root, check=True,
            capture_output=True, text=True, timeout=8,
        ).stdout.strip()
    try:
        branch, sha = git("branch", "--show-current"), git("rev-parse", "HEAD")
        pin = root / "runtime/code-pins" / ("US-" + trade_date + ".sha")
        pinned = pin.read_text(encoding="utf-8").strip() if pin.is_file() else ""
        return {"code_pin_status": ("PASS" if branch == "dual-agent"
                and len(pinned) == 40 and sha == pinned else "BLOCK"),
                "branch": branch, "head_short": sha[:12],
                "pin_present": bool(pinned)}
    except (OSError, subprocess.SubprocessError):
        return {"code_pin_status": "BLOCK", "branch": "UNKNOWN",
                "head_short": "UNKNOWN", "pin_present": False}


def run_readonly(trade_date: str, *, session_run_id: str, root=Path(".")):
    from trader.us.db.repos import _get_engine_or_none, _us_execution_claim_scope
    from trader.us.execution.reconcile_shadow_worker import observer_report_path
    flags = dict(os.environ)
    report_path = observer_report_path(trade_date, session_run_id)
    report = None
    if report_path.is_file():
        try:
            report = json.loads(report_path.read_text(encoding="utf-8"))
        except (ValueError, OSError):
            report = None
    scope = _us_execution_claim_scope()
    engine = _get_engine_or_none()
    if engine is None:
        raise RuntimeError("STAGE4_DB_CONNECTION_UNAVAILABLE")
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT evidence_at,worker_state,reconcile_status,replay_status,
                   pending_count,unresolved_count,filled_rows,writer_generation
            FROM public.us_stage4_reconcile_health
            WHERE env=:env AND account_fingerprint=:account
              AND trading_epoch_id=:epoch
        """), {"env":scope["env"], "account":scope["account_id"],
               "epoch":scope["trading_epoch_id"]}).mappings().first()
        claims = conn.execute(text("""
            SELECT action_state, COUNT(*) AS n
            FROM public.us_execution_claims
            WHERE env='practice' AND market='US'
              AND account_id=:account
              AND action_state IN ('IN_FLIGHT','OPEN','RETRYABLE')
            GROUP BY action_state
        """), {"account":scope["account_id"]}).all()
    result = evaluate_writer_snapshot(
        flags=flags, scope=scope, row=dict(row) if row else None,
        report=report,
    )
    result.update(git_code_pin(trade_date, root=root))
    if result["code_pin_status"] != "PASS":
        result["writer_readiness"] = "BLOCK"
        result["blocking_reasons"].append("STAGE4_DEPLOY_CODE_PIN_UNVERIFIED")
    result["historical_nonterminal_local_claims"] = {
        str(state): int(count) for state, count in claims
    }
    result["historical_claims_are_not_broker_proof"] = True
    result["trade_date"] = trade_date
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--session-run-id", required=True)
    args = parser.parse_args()
    try:
        datetime.strptime(args.trade_date, "%Y-%m-%d")
        result = run_readonly(args.trade_date,
                              session_run_id=args.session_run_id)
    except Exception as exc:
        # No account IDs, environment values or exception strings in report.
        result = {"writer_readiness": "BLOCK",
                  "full_operational_acceptance": "NOT_PROVEN",
                  "read_only": True, "orders_or_cancels_submitted": 0,
                  "blocking_reasons": [type(exc).__name__]}
    print(json.dumps(result, sort_keys=True, ensure_ascii=False, indent=2))
    return 0 if result["writer_readiness"] == "PASS" else 43


if __name__ == "__main__":
    sys.exit(main())
