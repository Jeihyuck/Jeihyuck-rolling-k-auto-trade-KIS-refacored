"""PR257 retired off-hours reconciliation writer (P1 race containment).

A process-local/time-of-start check plus worker-only advisory lock cannot
exclude an overlapping PB1 AM/PM/CLOSE trading tick. Therefore this entrypoint
MUST NOT execute broker reconciliation or mutate economic database state.
Keep a stable fail-closed CLI for operators; original-account *read-only*
KIS audit remains at scripts.kr257_historical_kis_evidence_probe.

Any future worker re-enable requires a durable SINGLE-WRITER account/epoch
lease shared and acquired by BOTH the trading tick and worker, wall-clock
revalidation before each mutation, deadline/overrun handling and crash/restart
tests. No scheduler, KIS orders or live trading flags are changed here.
"""
from __future__ import annotations

import argparse
from datetime import datetime, time as clock_time
import json
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")
DISABLED_REASON = "KR257_WORKER_DISABLED_NO_SHARED_TICK_ACCOUNT_EPOCH_LEASE"


def offhours_allowed(now: datetime) -> bool:
    """Diagnostic only: a start-time check is NOT a writer safety lock."""
    at = now.astimezone(KST)
    return at.weekday() >= 5 or not (
        clock_time(5, 30) <= at.time() < clock_time(16, 30)
    )


def worker_permitted(*, env: str, strategy: str, endpoint: str,
                     explicit: bool, approved: bool, now: datetime) -> tuple[bool, str]:
    """No option or environment variable may enable this retired writer."""
    return False, DISABLED_REASON


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Disabled PR257 independent reconciliation writer"
    )
    parser.add_argument("--execute", action="store_true", help="retired; always blocked")
    parser.parse_args(argv)
    print(json.dumps({"status": "BLOCKED", "reason": DISABLED_REASON}))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
