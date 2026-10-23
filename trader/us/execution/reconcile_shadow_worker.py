"""Read-only US reconciliation observer, owned by the AM/Afternoon session.

This is a SHADOW *diagnostic* worker, not the authoritative reconciliation
writer. It NEVER calls a KIS order/cancel API and NEVER mutates execution
claims, reservations, orders, fills or the operational journal.  WORKER
authority cannot be enabled until fencing and reservation migrations exist.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
import re
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)
_STOP = False


def parse_mode(mode: str) -> str:
    normalized = str(mode or "LEGACY").strip().upper()
    if normalized not in {"LEGACY", "SHADOW", "RESERVATION_ONLY", "WORKER"}:
        raise ValueError(
            "unsupported US_RECONCILE_WORKER_MODE; authoritative WORKER "
            "requires account fencing and reservations, not yet implemented"
        )
    if normalized == "WORKER" and os.getenv("US_STAGE4_WRITER_AUTHORIZED", "0") != "1":
        raise ValueError("WORKER requires explicit US_STAGE4_WRITER_AUTHORIZED=1")
    return normalized


def safe_scope_token(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", str(value or ""))[:90] or "unknown"


def observer_report_path(trade_date: str, session_run_id: str) -> Path:
    trade = str(trade_date)
    # Never permit arbitrary filesystem paths through the CLI.
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", trade):
        raise ValueError("observer requires exact ISO US trade date")
    root = Path(os.getenv("US_SHADOW_RECONCILE_DIR", "runtime/us/reconcile_shadow"))
    return root / trade / (safe_scope_token(session_run_id) + ".json")


def observe_once(*, trade_date: str, session_run_id: str, env: str,
                 scope: dict | None = None) -> dict[str, Any]:
    """Compute a scoped DB-only diagnostic. No broker GET or DB mutation."""
    from trader.us.db.repos import (
        load_active_execution_claim_attempts,
        load_execution_claim_health,
        load_pending_ack_orders_result,
        count_us_terminal_order_claim_mismatches,
    )
    if str(env).lower() != "practice":
        raise RuntimeError("read-only shadow observer restricted to practice")
    active_claims = load_active_execution_claim_attempts()
    health = load_execution_claim_health()
    pending = load_pending_ack_orders_result(trade_date, env=env)
    mismatch_count = count_us_terminal_order_claim_mismatches(
        trade_date=trade_date, env=env,
    )
    status = str(pending.get("status") or "").upper()
    if status != "OK":
        raise RuntimeError(f"shadow_order_read_failed:{status}")
    # Claims are already scoped by repository's authenticated account/epoch
    # context; never dump account IDs or KIS credentials into diagnostics.
    pending_orders = list(pending.get("orders") or [])
    return {
        "mode": "SHADOW", "write_authority": "NONE",
        "broker_access": "NONE", "env": "practice",
        "trade_date": trade_date,
        "session_run_id": safe_scope_token(session_run_id),
        "sampled_at_utc": datetime.now(timezone.utc).isoformat(),
        "active_claim_count": len(active_claims),
        "pending_order_count": len(pending_orders),
        "terminal_order_active_claim_mismatch_count": mismatch_count,
        "execution_claim_health": {
            k: int(v) for k, v in (health or {}).items()
            if isinstance(v, int) and not isinstance(v, bool)
        },
        "read_status": "OK",
    }


def save_diagnostic_atomic(path: Path, payload: dict) -> None:
    # This file is outside the operational journal and is not trading truth.
    path.parent.mkdir(parents=True, exist_ok=True)
    target = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        target.write_text(json.dumps(payload, sort_keys=True, default=str), encoding="utf-8")
        os.replace(target, path)
    finally:
        target.unlink(missing_ok=True)


def _stop_on_signal(signum: int, _frame: Any) -> None:
    global _STOP
    _STOP = True


def run_shadow(*, trade_date: str, session_run_id: str, env: str,
               interval_sec: float = 10.0, max_cycles: int = 0) -> int:
    if parse_mode("SHADOW") != "SHADOW" or env.lower() != "practice":
        raise RuntimeError("shadow observer requires practice")
    path = observer_report_path(trade_date, session_run_id)
    signal.signal(signal.SIGTERM, _stop_on_signal)
    signal.signal(signal.SIGINT, _stop_on_signal)
    cycles = 0
    while not _STOP:
        try:
            report = observe_once(trade_date=trade_date, session_run_id=session_run_id, env=env)
        except Exception as exc:
            # Fail closed for diagnostic reliability. Never turn a failed DB
            # read into 'zero pending orders' or alter live trading authority.
            report = {
                "mode": "SHADOW", "write_authority": "NONE",
                "broker_access": "NONE", "env": env.lower(),
                "trade_date": trade_date,
                "session_run_id": safe_scope_token(session_run_id),
                "sampled_at_utc": datetime.now(timezone.utc).isoformat(),
                "read_status": "ERROR",
                "reason": f"{type(exc).__name__}: {str(exc)[:200]}",
            }
            logger.exception("[US_SHADOW_RECONCILE][READ_FAILED]")
        save_diagnostic_atomic(path, report)
        cycles += 1
        if max_cycles and cycles >= max_cycles:
            break
        # No KIS reads, ever. Bound the DB poll frequency.
        until = time.monotonic() + max(5.0, float(interval_sec))
        while not _STOP and time.monotonic() < until:
            time.sleep(min(0.5, until - time.monotonic()))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="US read-only shadow reconciliation diagnostics")
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--session-run-id", required=True)
    parser.add_argument("--env", default="practice")
    parser.add_argument("--interval-sec", type=float, default=10.0)
    parser.add_argument("--max-cycles", type=int, default=0)
    a = parser.parse_args()
    if a.env.lower() != "practice":
        parser.error("shadow worker is practice only")
    return run_shadow(
        trade_date=a.trade_date, session_run_id=a.session_run_id,
        env=a.env, interval_sec=a.interval_sec, max_cycles=a.max_cycles,
    )


if __name__ == "__main__":
    sys.exit(main())
