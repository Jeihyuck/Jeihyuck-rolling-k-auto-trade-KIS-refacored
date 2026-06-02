#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Guard same-day US prep contract before trading session starts.

Checks:
- prep_status must be OK or OK_WITH_WARNINGS
- locked_watchlist count must be >= 10

Exits with code 1 if guard fails.
"""
from datetime import datetime
from zoneinfo import ZoneInfo
import json
import os
import sys

# Add repo to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from trader.us.db.repos import (
    load_latest_us_prep_status,
    load_locked_us_watchlist,
)
from trader.us.utils.timeout_guard import run_with_timeout

# ── Parse inputs ──────────────────────────────────────────────────────────────
session = os.getenv("SESSION", "unknown")
force_now = os.getenv("FORCE_NOW_INPUT", "").strip()

# Determine timeout (priority: US_PREP_GUARD_TIMEOUT_SEC > US_WATCHLIST_LOAD_TIMEOUT_SEC > 20)
timeout_sec = int(os.getenv("US_PREP_GUARD_TIMEOUT_SEC") or os.getenv("US_WATCHLIST_LOAD_TIMEOUT_SEC") or "20")

# Determine trade date and source
if force_now:
    trade_date = datetime.fromisoformat(force_now).date().isoformat()
    trade_date_source = "force_now"
else:
    trade_date = datetime.now(ZoneInfo("America/New_York")).date().isoformat()
    trade_date_source = "actual_ny_today"

# ── Print START immediately ───────────────────────────────────────────────────
print(
    f"[US_PREP_GUARD][START] session={session} trade_date={trade_date} "
    f"force_now={force_now or ''} trade_date_source={trade_date_source} timeout_sec={timeout_sec}",
    flush=True,
)

# ── Stage 1: Load prep status with timeout ────────────────────────────────────
print(
    f"[US_PREP_GUARD][PREP_STATUS][START] trade_date={trade_date} timeout_sec={timeout_sec}",
    flush=True,
)

prep_result = run_with_timeout(
    fn=lambda: load_latest_us_prep_status(trade_date, timeout_sec=timeout_sec),
    stage="prep_status",
    timeout_sec=timeout_sec,
)

# Helper function to write failure sidecar (defined early for timeout/error cases)
def write_failure_sidecar(reason: str, prep_status: str = "UNKNOWN", locked_count: int = 0, watchlist_load_error: bool = False):
    """Write failure result sidecar JSON for downstream report generation."""
    artifact_dir = os.getenv("GITHUB_WORKSPACE") and "artifacts" or "."
    os.makedirs(artifact_dir, exist_ok=True)
    result_path = os.path.join(artifact_dir, "us_prep_guard_result.json")
    with open(result_path, "w") as f:
        json.dump(
            {
                "ok": False,
                "session": session,
                "trade_date": trade_date,
                "trade_date_source": trade_date_source,
                "force_now": force_now or None,
                "prep_status": prep_status,
                "locked_count": locked_count,
                "min_count": 10,
                "watchlist_load_error": watchlist_load_error,
                "reason": reason,
            },
            f,
            indent=2,
        )
    print(f"[US_PREP_GUARD][FAILURE_SIDECAR] {result_path}", flush=True)

if prep_result["timeout"]:
    print(
        f"[US_PREP_GUARD][PREP_STATUS][TIMEOUT] trade_date={trade_date} timeout_sec={timeout_sec}",
        flush=True,
    )
    print(
        f"[US_PREP_GUARD][FAIL] session={session} trade_date={trade_date} reason=prep_status_timeout",
        flush=True,
    )
    write_failure_sidecar("prep_status_timeout")
    sys.exit(1)

if not prep_result["ok"]:
    print(
        f"[US_PREP_GUARD][PREP_STATUS][ERROR] trade_date={trade_date} error={prep_result['error']}",
        flush=True,
    )
    print(
        f"[US_PREP_GUARD][FAIL] session={session} trade_date={trade_date} reason=prep_status_error",
        flush=True,
    )
    write_failure_sidecar("prep_status_error")
    sys.exit(1)

prep = prep_result["value"] or {}
status = prep.get("status", "UNKNOWN")
print(
    f"[US_PREP_GUARD][PREP_STATUS][DONE] status={status} elapsed_sec={prep_result['elapsed_sec']:.2f}",
    flush=True,
)

# ── Stage 2: Load locked watchlist with timeout ───────────────────────────────
print(
    f"[US_PREP_GUARD][WATCHLIST][START] trade_date={trade_date} min_count=10 "
    f"allow_degraded=1 timeout_sec={timeout_sec}",
    flush=True,
)

watchlist_result = run_with_timeout(
    fn=lambda: load_locked_us_watchlist(
        trade_date=trade_date,
        min_count=10,
        allow_degraded=True,
        timeout_sec=timeout_sec,
    ),
    stage="watchlist_load",
    timeout_sec=timeout_sec,
)

if watchlist_result["timeout"]:
    print(
        f"[US_PREP_GUARD][WATCHLIST][TIMEOUT] trade_date={trade_date} timeout_sec={timeout_sec}",
        flush=True,
    )
    print(
        f"[US_PREP_GUARD][FAIL] session={session} trade_date={trade_date} "
        f"reason=watchlist_load_timeout prep_status={status}",
        flush=True,
    )
    write_failure_sidecar("watchlist_load_timeout", prep_status=status, locked_count=0, watchlist_load_error=True)
    sys.exit(1)

if not watchlist_result["ok"]:
    print(
        f"[US_PREP_GUARD][WATCHLIST][ERROR] trade_date={trade_date} error={watchlist_result['error']}",
        flush=True,
    )
    print(
        f"[US_PREP_GUARD][FAIL] session={session} trade_date={trade_date} reason=watchlist_load_error",
        flush=True,
    )
    write_failure_sidecar("watchlist_load_error", prep_status=status, locked_count=0, watchlist_load_error=True)
    sys.exit(1)

rows = watchlist_result["value"] or []
locked_count = len(rows)


def _score_positive(row: dict) -> bool:
    for key in ("score_final", "final_score", "score"):
        try:
            if float(row.get(key) or 0) > 0:
                return True
        except Exception:
            pass
    scores = row.get("scores")
    if isinstance(scores, dict):
        for key in ("final", "score_final", "final_score"):
            try:
                if float(scores.get(key) or 0) > 0:
                    return True
            except Exception:
                pass
    return False


score_nonzero_count = sum(1 for r in rows if _score_positive(r))

print(
    f"[US_PREP_GUARD][WATCHLIST][DONE] count={locked_count} score_nonzero={score_nonzero_count} elapsed_sec={watchlist_result['elapsed_sec']:.2f}",
    flush=True,
)

# ── Guard checks ──────────────────────────────────────────────────────────────
watchlist_load_error = False

print(
    f"[US_PREP_GUARD][CHECK] session={session} trade_date={trade_date} "
    f"prep_status={status} locked_count={locked_count} score_nonzero_count={score_nonzero_count} "
    f"watchlist_load_error={watchlist_load_error}",
    flush=True,
)

if status not in ("OK", "OK_WITH_WARNINGS"):
    print(
        f"[US_PREP_GUARD][FAIL] session={session} trade_date={trade_date} "
        f"reason=bad_prep_status prep_status={status} locked_count={locked_count}",
        flush=True,
    )
    write_failure_sidecar("bad_prep_status", prep_status=status, locked_count=locked_count, watchlist_load_error=watchlist_load_error)
    sys.exit(1)

if locked_count < 10:
    print(
        f"[US_PREP_GUARD][FAIL] session={session} trade_date={trade_date} "
        f"reason=no_locked_watchlist prep_status={status} locked_count={locked_count}",
        flush=True,
    )
    write_failure_sidecar("no_locked_watchlist", prep_status=status, locked_count=locked_count, watchlist_load_error=watchlist_load_error)
    sys.exit(1)

if score_nonzero_count <= 0:
    print(
        f"[US_PREP_GUARD][FAIL] session={session} trade_date={trade_date} "
        f"reason=locked_watchlist_score_nonzero_zero prep_status={status} locked_count={locked_count} score_nonzero_count={score_nonzero_count}",
        flush=True,
    )
    write_failure_sidecar("locked_watchlist_score_nonzero_zero", prep_status=status, locked_count=locked_count, watchlist_load_error=watchlist_load_error)
    sys.exit(1)

# ── Success ───────────────────────────────────────────────────────────────────
print(
    f"[US_PREP_GUARD][OK] session={session} trade_date={trade_date} "
    f"prep_status={status} locked_count={locked_count} score_nonzero_count={score_nonzero_count}",
    flush=True,
)

# ── Write result artifact ─────────────────────────────────────────────────────
artifact_dir = os.getenv("GITHUB_WORKSPACE") and "artifacts" or "."
os.makedirs(artifact_dir, exist_ok=True)
result_path = os.path.join(artifact_dir, "us_prep_guard_result.json")
with open(result_path, "w") as f:
    json.dump(
        {
            "status": "OK",
            "trade_date": trade_date,
            "trade_date_source": trade_date_source,
            "force_now": force_now or None,
            "prep_status": status,
            "locked_count": locked_count,
            "score_nonzero_count": score_nonzero_count,
            "timeout_sec": timeout_sec,
        },
        f,
        indent=2,
    )
print(f"[US_PREP_GUARD][ARTIFACT] {result_path}", flush=True)
