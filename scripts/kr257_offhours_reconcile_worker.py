"""PR257 isolated, off-hours KR authoritative reconciliation worker.

The worker is intentionally NOT launched by trading ticks or CI.
It is a single account/epoch-scoped writer that may run only off market hours
after explicit operator enablement. KIS ORDER endpoints are never invoked.
Uncertain fills and prices remain unresolved when original broker evidence
is missing; this does not invent missing historical prices.
"""
from __future__ import annotations
import argparse
from datetime import datetime, time as clock_time
import hashlib
import json
import os
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")


def offhours_allowed(now: datetime) -> bool:
    at = now.astimezone(KST)
    # Includes early PREP, AM, PM and CLOSE, with conservative grace.
    return at.weekday() >= 5 or not (
        clock_time(5, 30) <= at.time() < clock_time(16, 30)
    )


def worker_permitted(*, env: str, strategy: str, endpoint: str,
                     explicit: bool, approved: bool, now: datetime) -> tuple[bool, str]:
    if env != "practice" or strategy != "pb1_pullback_close":
        return False, "PRACTICE_PB1_SCOPE_REQUIRED"
    if "openapivts.koreainvestment.com" not in endpoint:
        return False, "PRACTICE_KIS_ENDPOINT_REQUIRED"
    if not explicit or not approved:
        return False, "EXPLICIT_EXECUTION_APPROVAL_REQUIRED"
    if not offhours_allowed(now):
        return False, "KR_MARKET_OR_SCHEDULER_ACTIVE_WINDOW"
    return True, "OFFHOURS_AUTHORIZED"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Explicit KR off-hours reconciliation")
    parser.add_argument("--execute", action="store_true",
                        help="enable authorized off-hours DB reconciliation (NEVER orders)")
    args = parser.parse_args(argv)
    from trader.time_utils import now_kst
    allowed, reason = worker_permitted(
        env=str(os.getenv("KIS_ENV") or "").lower(),
        strategy=str(os.getenv("PB1_STRATEGY_OWNER") or "pb1_pullback_close"),
        endpoint=str(os.getenv("KIS_REST_URL") or ""),
        explicit=args.execute,
        approved=os.getenv("KR257_OFFHOURS_RECONCILE_APPROVED") == "1",
        now=now_kst(),
    )
    if not allowed:
        print(json.dumps({"status": "BLOCKED", "reason": reason}))
        return 2
    if os.getenv("STRATEGY_ENV") != "practice":
        raise SystemExit("PRACTICE_STRATEGY_ENV_REQUIRED")
    if not all(os.getenv(k) for k in ("KIS_APP_KEY", "KIS_APP_SECRET", "CANO", "ACNT_PRDT_CD")):
        raise SystemExit("PRACTICE_CREDENTIALS_UNAVAILABLE")

    import sqlalchemy as sa
    from trader.account_state import account_reset_mode, get_account_key
    from trader.db.engine import get_engine
    from trader.db.repos import active_trading_epoch_id
    from trader.kis_wrapper import KisAPI
    from trader.reconcile_kis import reconcile_kis

    if account_reset_mode():
        raise SystemExit("ACCOUNT_RESET_MODE_BLOCKS_INDEPENDENT_WRITER")
    engine = get_engine()
    if engine.dialect.name != "postgresql":
        raise SystemExit("POSTGRES_AUTHORITATIVE_LOCK_REQUIRED")
    kis = KisAPI(kis_env="practice")
    account = get_account_key(env="practice", kis=kis)
    with engine.connect() as conn:
        epoch = active_trading_epoch_id(conn, env="practice", account_id=account, required=True)
        if not epoch:
            raise SystemExit("NO_ACTIVE_PRACTICE_TRADING_EPOCH")
        # Lock identity is stable across worker processes, WITHOUT
        # printing account/epoch identifiers or retaining raw secret values.
        digest = hashlib.sha256(
            f"KR257:{account}:{epoch}:pb1_pullback_close".encode("utf-8")
        ).digest()[:8]
        lock_id = int.from_bytes(digest, "big", signed=True)
        acquired = bool(conn.execute(
            sa.text("SELECT pg_try_advisory_lock(:key)"), {"key": lock_id}
        ).scalar())
        if not acquired:
            raise SystemExit("ANOTHER_ACCOUNT_EPOCH_RECONCILE_WORKER_ACTIVE")
        try:
            snap = kis.get_balance_cached(force=True)
            if (not isinstance(snap, dict)
                    or not isinstance(snap.get("output1"), list)
                    or not snap.get("output2")):
                raise RuntimeError("KIS_BALANCE_UNPROVEN")
            snap = {**snap, "_kr_verified_fresh_at_kst": now_kst().isoformat()}
            result = reconcile_kis(
                engine=engine, kis=kis, env="practice", run_id=None,
                strategy="pb1_pullback_close", tick_ts=now_kst(),
                balance_snapshot=snap,
            )
            if not isinstance(result, dict) or result.get("ok") is not True:
                print(json.dumps({
                    "status": "UNRESOLVED",
                    "reason": str((result or {}).get("degraded") or (result or {}).get("reason") or "BROKER_EVIDENCE_UNPROVEN"),
                }))
                return 2
            print(json.dumps({
                "status": "RECONCILED",
                "orders": int(result.get("orders") or 0),
                "fills": int(result.get("fills") or 0),
                "historical_repair_fastpath": bool(result.get("historical_repair_fastpath")),
                "KIS_orders_submitted": 0,
            }))
            return 0
        finally:
            conn.execute(sa.text("SELECT pg_advisory_unlock(:key)"), {"key": lock_id})


if __name__ == "__main__":
    raise SystemExit(main())
