#!/usr/bin/env python3
"""Read-only fail-closed paper activation preflight. Never sends broker orders.

Only runs on canonical WSL AM/Afternoon after the permission diagnostic.
Checks actual DB schema and account scope before a session may spawn the
account reservation worker. Avoid DB writes, credentials in logs, or implicit
fallback to unreserved native HTTP. Exit 43 on missing prerequisites.
"""
from __future__ import annotations

import os
import sys

from sqlalchemy import text


def validate_stage123_prerequisites(*, engine=None, scope=None, env=None) -> dict:
    e = os.environ if env is None else env
    if e.get("US_STAGE123_PAPER_PROFILE", "OFF") != "ON":
        return {"status": "SKIP", "reason": "profile_off"}
    _reconcile_mode=e.get("US_RECONCILE_WORKER_MODE")
    _stage4_mode=_reconcile_mode=="WORKER"
    _worker_mode_authorized=(
        _reconcile_mode=="RESERVATION_ONLY"
        or (_stage4_mode
            and e.get("US_STAGE4_WRITER_AUTHORIZED")=="1"
            and e.get("US_PROTECTIVE_STOP_LEDGER_MODE")=="ENFORCE")
    )
    if not (
        e.get("KIS_ENV") == "practice"
        and e.get("US_PAPER_TRADING_ENABLED") == "1"
        and e.get("DRY_RUN") == "0"
        and e.get("US_OFFLINE", "0") != "1"
        and e.get("US_SIGNAL_ONLY", "0") != "1"
        and e.get("US_ATOMIC_CLAIM_OUTBOX_ENABLED") == "1"
        and e.get("US_ACCOUNT_RESERVATION_MODE") == "ENFORCE"
        and _worker_mode_authorized
        and e.get("US_SHARED_KIS_LIMITER_ENABLED") == "1"
        and e.get("US_EXIT_QUOTE_PREFETCH_ENABLED") == "1"
        and e.get("US_EARLY_HARD_STOP_PILOT_ENABLED") == "1"
        and e.get("US_EARLY_MULTI_HARD_STOP_ENABLED") == "1"
        and e.get("US_EARLY_SOFT_STOP_PILOT_ENABLED") == "1"
    ):
        raise RuntimeError("paper_stage123_inconsistent_or_nonpractice")
    if engine is None or scope is None:
        from trader.us.db.repos import _get_engine_or_none, _us_execution_claim_scope
        engine = engine or _get_engine_or_none()
        scope = scope or _us_execution_claim_scope()
    if engine is None or engine.dialect.name != "postgresql":
        raise RuntimeError("paper_stage123_postgres_required")
    if scope.get("env") != "practice" or not scope.get("account_id"):
        raise RuntimeError("paper_stage123_account_scope_invalid")
    if scope.get("trading_epoch_id") in {"", None, "legacy-unscoped-0052"}:
        raise RuntimeError("paper_stage123_explicit_trading_epoch_required")
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT c.relname, c.relrowsecurity, c.relkind
              FROM pg_class c
              JOIN pg_namespace n ON n.oid=c.relnamespace
             WHERE n.nspname='public'
               AND c.relname IN (
                   'us_execution_authority_leases',
                   'us_execution_account_reservations'
               )
        """)).all()
        if sorted(name for name, _, _ in rows) != [
            "us_execution_account_reservations", "us_execution_authority_leases"
        ] or any(not enabled or kind != "r" for _, enabled, kind in rows):
            raise RuntimeError("paper_stage123_migration_0056_missing_or_rls_off")
        # Check schema shape and access before the actual worker receives an
        # account lease. Missing RLS or grants cannot silently turn off fencing.
        columns = conn.execute(text("""
            SELECT table_name,column_name FROM information_schema.columns
             WHERE table_schema='public'
               AND table_name IN (
                    'us_execution_authority_leases',
                    'us_execution_account_reservations'
               )
        """)).all()
        have = {(table, col) for table, col in columns}
        must = {
            ("us_execution_authority_leases", "fencing_token"),
            ("us_execution_authority_leases", "expires_at"),
            ("us_execution_account_reservations", "action_key"),
            ("us_execution_account_reservations", "trading_epoch_id"),
            ("us_execution_account_reservations", "reserved_cash_usd"),
            ("us_execution_account_reservations", "reserved_shares"),
            ("us_execution_account_reservations", "state"),
        }
        if not must.issubset(have):
            raise RuntimeError("paper_stage123_migration_columns_incomplete")
        if _stage4_mode:
            # Stage4 must never start in WORKER mode when its durable
            # protection/debt tables or the singleton health table are absent.
            stage4=conn.execute(text("""
                SELECT c.relname,c.relrowsecurity,c.relkind
                FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname='public' AND c.relname IN (
                    'us_stop_policy_revisions',
                    'us_protective_stop_obligations',
                    'us_stage4_reconcile_health'
                )
            """)).all()
            expected={
                'us_stop_policy_revisions',
                'us_protective_stop_obligations',
                'us_stage4_reconcile_health',
            }
            if ({name for name,_,_ in stage4}!=expected
                    or any(not rls or kind!='r' for _,rls,kind in stage4)):
                raise RuntimeError("paper_stage4_migrations_0057_0059_missing_or_rls_off")
    return {"status": "PASS", "mode": "PAPER_ON_STAGE4" if _stage4_mode else "PAPER_ON",
            "broker_access":"NONE"}


def main() -> int:
    try:
        result = validate_stage123_prerequisites()
    except Exception as exc:
        print(
            f"[US_STAGE123][PREFLIGHT][BLOCK] reason={type(exc).__name__}:"
            f"{str(exc)[:120]} native_order_submit=0", file=sys.stderr,
        )
        return 43
    print(
        f"[US_STAGE123][PREFLIGHT][{result['status']}]"
        f" broker_access=NONE mode={result.get('mode','OFF')}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
