"""KR #359 operator acceptance: strictly read-only database and code-pin evidence.

This script NEVER imports a broker wrapper, submits an order, migrates schema,
or mutates operational records. It makes unknown broker/WSL states visible
rather than converting offline CI PASS into live trading authorization.

Use only on the credentialed practice operator WSL after *approved* code
deployment and migration, e.g.:
  NO_TRADE=1 python -m scripts.kr_release_acceptance_readonly \
      --expected-sha "$(git rev-parse HEAD)" --trade-date 2026-11-13

For original KIS proof, separately run the existing no-trade native probes
with authenticated practice credentials. A DB-only read cannot prove KIS truth.
"""
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import text

KST = ZoneInfo("Asia/Seoul")
TRIGGERS = {
    "kr_signed_buy_order_guard_0063": "orders",
    "kr_signed_position_guard_0063": "positions",
}

def evaluate_readiness(snapshot: dict[str, Any], expected_sha: str) -> dict[str, Any]:
    """Pure, evidence-aware result: NEVER return live PASS from database alone."""
    observed = snapshot.get("entry_shas") or {}
    migration = bool(snapshot.get("migration_0063"))
    trigger_rows = snapshot.get("triggers") or {}
    expected_sha = str(expected_sha or "").strip()
    sha_exact = bool(
        expected_sha and len(expected_sha) == 40
        and observed and all(k == expected_sha for k in observed)
    )
    trigger_ok = all(
        trigger_rows.get(name) == {"table": table, "enabled": True}
        for name, table in TRIGGERS.items()
    )
    active_epoch_count = int(snapshot.get("active_epoch_count") or 0)
    malformed_signed_buy = int(snapshot.get("malformed_signed_buy") or 0)
    malformed_signed_open = int(snapshot.get("malformed_signed_open") or 0)
    late_submits = int(snapshot.get("submitted_after_180s") or 0)
    checks = {
        "operator_run_sha": "PASS" if sha_exact else "BLOCKED",
        "active_account_epoch": "PASS" if active_epoch_count == 1 else "BLOCKED",
        "migration_0063": "PASS" if migration else "BLOCKED",
        "signed_contract_triggers": "PASS" if trigger_ok else "BLOCKED",
        "existing_signed_buy_contracts": "PASS" if malformed_signed_buy == 0 else "BLOCKED",
        "active_signed_position_contracts": "PASS" if malformed_signed_open == 0 else "BLOCKED",
        "submitted_after_180s": "PASS" if late_submits == 0 else "BLOCKED",
        "original_broker_kis_evidence": "NOT_ATTESTED",
        "ws_native_quote_and_30_symbol_latency": "NOT_ATTESTED",
        "seller_first_and_restart_ack_replay": "NOT_ATTESTED",
    }
    if int(snapshot.get("historical_closed_missing_exit_sha") or 0):
        # Closed zero-share historical contracts are evidence debt, not a
        # reason to backfill an immutable entry plan or relink a new cycle.
        checks["closed_historical_signed_meta"] = "REVIEW_HISTORICAL"
    # A historical unresolved claim is never reset from here and must not
    # be treated as a new verified order; AM/CLOSE should report its status.
    if int(snapshot.get("uncertain_claims") or 0):
        checks["historical_uncertain_claim"] = "REVIEW_REQUIRED"
    if int(snapshot.get("pending_close_runs") or 0):
        checks["pending_close_settlement"] = "REVIEW_REQUIRED"
    # Run elapsed time != submit elapsed time: track it without falsely
    # declaring a 180s submit violation.
    checks["run_elapsed_over_180s"] = (
        "REVIEW_PERFORMANCE" if int(snapshot.get("elapsed_runs_over_180s") or 0)
        else "PASS"
    )
    return {
        "release_readiness": "BLOCKED" if any(
            value == "BLOCKED" for value in checks.values()
        ) else "OPERATOR_KIS_ATTESTATION_REQUIRED",
        "checks": checks,
        "expected_sha": expected_sha,
        "observed_run_sha_counts": observed,
        "counts": {
            "entry_runs": sum(int(n) for n in observed.values()),
            "buy_skips": int(snapshot.get("buy_skips") or 0),
            "stale_exchange_quote_skips": int(snapshot.get("stale_quote_skips") or 0),
            "timing_or_price_skips": int(snapshot.get("timing_price_skips") or 0),
            "submitted_after_180s": late_submits,
            "elapsed_runs_over_180s": int(snapshot.get("elapsed_runs_over_180s") or 0),
            "uncertain_claims": int(snapshot.get("uncertain_claims") or 0),
            "pending_close_runs": int(snapshot.get("pending_close_runs") or 0),
            "malformed_signed_buy": malformed_signed_buy,
            "malformed_signed_open": malformed_signed_open,
            "historical_closed_missing_exit_sha": int(
                snapshot.get("historical_closed_missing_exit_sha") or 0
            ),
        },
        "broker_kis_proof_is_available": False,
        "note": "SELECT-only database evidence cannot independently attest native KIS fills.",
    }


def collect_readonly_snapshot(
    conn: Any, *, trade_date: date, account_id: str, env: str = "practice",
) -> dict[str, Any]:
    if env != "practice" or "unknown" in str(account_id):
        raise RuntimeError("KR_OPERATOR_PRACTICE_ACCOUNT_BINDING_REQUIRED")
    if not isinstance(trade_date, date):
        raise TypeError("trade_date must be datetime.date")
    start = datetime.combine(trade_date, time.min, tzinfo=KST).astimezone(timezone.utc)
    end = start + timedelta(days=1)
    params = {"start": start, "end": end, "env": env, "account": account_id}
    # The function's caller issues SET TRANSACTION READ ONLY before entering.
    rows = conn.execute(text("""
        SELECT git_sha, COUNT(*) n FROM public.runs
        WHERE started_at >= :start AND started_at < :end
          AND env=:env AND strategy='pb1_pullback_close' AND phase='entry'
        GROUP BY git_sha
    """), params).mappings().all()
    shas = {str(row["git_sha"] or "NULL"): int(row["n"]) for row in rows}
    migrations = conn.execute(text("""
        SELECT COUNT(*) FROM public.schema_migrations
        WHERE version='0063_kr_signed_buy_position_contract_immutability.sql'
    """)).scalar_one()
    trigger_rows = conn.execute(text("""
        SELECT tgname, tgrelid::regclass::text AS table_name, tgenabled
        FROM pg_trigger WHERE NOT tgisinternal
          AND tgname IN ('kr_signed_buy_order_guard_0063',
                         'kr_signed_position_guard_0063')
    """)).mappings().all()
    triggers = {
        str(x["tgname"]): {
            "table": str(x["table_name"]).split(".")[-1],
            "enabled": str(x["tgenabled"]) in {"O", "A"},
        }
        for x in trigger_rows
    }
    signed_buy_row = conn.execute(text("""
        WITH o AS (
            SELECT side, request_json::jsonb j, client_order_key,
                   position_cycle_id, portfolio_epoch_id, trading_epoch_id
            FROM public.orders
            WHERE env=:env AND strategy='pb1_pullback_close' AND side='BUY'
        )
        SELECT COUNT(*) FILTER (
            WHERE j->>'enforce_entry_contract'='true'
              AND (COALESCE(j->>'entry_contract_sha256','') !~ '^[0-9a-f]{64}
        SELECT COUNT(*) FROM public.trading_epochs
        WHERE env=:env AND account_id=:account AND status='ACTIVE'
    """), params).scalar_one()
    run_metrics = conn.execute(text("""
        SELECT
          COUNT(*) FILTER(WHERE EXTRACT(EPOCH FROM (finished_at-started_at)) > 180)
            AS elapsed_runs_over_180s,
          COUNT(*) FILTER(WHERE run_window='close'
                           AND status='PENDING_SETTLEMENT') AS pending_close_runs
        FROM public.runs WHERE env=:env
          AND strategy='pb1_pullback_close'
          AND started_at >= :start AND started_at < :end
    """), params).mappings().one()
    # Order submit clock, not a whole-run duration, determines the 180-second
    # requirement. Orders may have no matching run and are then NOT ATTESTED.
    late = conn.execute(text("""
        SELECT COUNT(*) FROM public.orders o
        JOIN public.runs r ON r.run_id=o.run_id
        WHERE o.env=:env AND o.strategy='pb1_pullback_close'
          AND o.submitted_at >= :start AND o.submitted_at < :end
          AND o.submitted_at IS NOT NULL
          AND EXTRACT(EPOCH FROM (o.submitted_at-r.started_at)) > 180
    """), params).scalar_one()
    skips = conn.execute(text("""
        SELECT COUNT(*) AS buy_skips,
          COUNT(*) FILTER(WHERE reasons ILIKE '%KR_VOLUME_PROOF_KIS_EXCHANGE_QUOTE_STALE_OR_FUTURE%')
            AS stale_quote_skips,
          COUNT(*) FILTER(WHERE reasons ILIKE '%KR_ENTRY_TIMING_NOT_READY_OR_PRICE_TOO_HIGH%')
            AS timing_price_skips
        FROM public.ledger_events
        WHERE env=:env AND event_type='ORDER_SKIP' AND side='BUY'
          AND ts::timestamptz >= :start AND ts::timestamptz < :end
    """), params).mappings().one()
    claims = conn.execute(text("""
        SELECT COUNT(*) FROM public.execution_claims
        WHERE env=:env AND market='KR'
          AND trading_epoch_id IN (
            SELECT trading_epoch_id FROM public.trading_epochs
            WHERE env=:env AND account_id=:account AND status='ACTIVE'
          )
          AND strategy_owner='PB1' AND action_state='UNCERTAIN'
    """), params).scalar_one()
    return {
        "entry_shas": shas,
        "migration_0063": int(migrations or 0) == 1,
        "triggers": triggers,
        "active_epoch_count": int(active_epoch_count or 0),
        "malformed_signed_buy": int(signed_buy_row["invalid_buy"] or 0),
        "malformed_signed_open": int(signed_position_rows["invalid_open"] or 0),
        "historical_closed_missing_exit_sha": int(
            signed_position_rows["legacy_closed_missing"] or 0
        ),
        "submitted_after_180s": int(late or 0),
        "elapsed_runs_over_180s": int(run_metrics["elapsed_runs_over_180s"] or 0),
        "pending_close_runs": int(run_metrics["pending_close_runs"] or 0),
        "uncertain_claims": int(claims or 0),
        "buy_skips": int(skips["buy_skips"] or 0),
        "stale_quote_skips": int(skips["stale_quote_skips"] or 0),
        "timing_price_skips": int(skips["timing_price_skips"] or 0),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-sha", required=True,
                        help="exact 40-character deployed git SHA")
    parser.add_argument("--trade-date", default=datetime.now(KST).date().isoformat())
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[0-9a-f]{40}", args.expected_sha):
        parser.error("--expected-sha must be the FULL deployed 40-character SHA")
    try:
        day = date.fromisoformat(args.trade_date)
    except ValueError:
        parser.error("--trade-date must be YYYY-MM-DD")
    if any(str(os.getenv(name) or "practice").lower() != "practice" for name in ("KIS_ENV", "STRATEGY_ENV")):
        raise RuntimeError("KR_RELEASE_ACCEPTANCE_PRACTICE_ONLY")
    from trader.account_state import get_account_key
    from trader.db.engine import get_engine

    account = get_account_key(env="practice")
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("SET TRANSACTION READ ONLY"))
        snapshot = collect_readonly_snapshot(
            conn, trade_date=day, account_id=account,
        )
        conn.rollback()
    report = evaluate_readiness(snapshot, args.expected_sha)
    report["trade_date"] = day.isoformat()
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    # A SELECT-only audit must NEVER return a green exit code for a live-release
    # gate; KIS originals, WSL observer and ACK/restart replay remain unproven.
    return 3 if report["release_readiness"] == "OPERATOR_KIS_ATTESTATION_REQUIRED" else 2


if __name__ == "__main__":
    raise SystemExit(main())

                   OR COALESCE(j->>'entry_contract_version','')=''
                   OR COALESCE(j->'entry_exit_plan','{}'::jsonb)='{}'::jsonb
                   OR j->>'client_order_key' IS DISTINCT FROM client_order_key
                   OR j->>'position_cycle_id' IS DISTINCT FROM position_cycle_id
                   OR j->>'portfolio_epoch_id' IS DISTINCT FROM portfolio_epoch_id
                   OR j->>'trading_epoch_id' IS DISTINCT FROM trading_epoch_id)
        ) AS invalid_buy
        FROM o
    """), params).mappings().one()
    signed_position_rows = conn.execute(text("""
        SELECT
        COUNT(*) FILTER (
          WHERE status='OPEN' AND qty>0
            AND entry_meta_json->>'entry_contract_sha256' IS NOT NULL
            AND (COALESCE(entry_meta_json->>'entry_contract_sha256','')
                     !~ '^[0-9a-f]{64}
        SELECT COUNT(*) FROM public.trading_epochs
        WHERE env=:env AND account_id=:account AND status='ACTIVE'
    """), params).scalar_one()
    run_metrics = conn.execute(text("""
        SELECT
          COUNT(*) FILTER(WHERE EXTRACT(EPOCH FROM (finished_at-started_at)) > 180)
            AS elapsed_runs_over_180s,
          COUNT(*) FILTER(WHERE run_window='close'
                           AND status='PENDING_SETTLEMENT') AS pending_close_runs
        FROM public.runs WHERE env=:env
          AND strategy='pb1_pullback_close'
          AND started_at >= :start AND started_at < :end
    """), params).mappings().one()
    # Order submit clock, not a whole-run duration, determines the 180-second
    # requirement. Orders may have no matching run and are then NOT ATTESTED.
    late = conn.execute(text("""
        SELECT COUNT(*) FROM public.orders o
        JOIN public.runs r ON r.run_id=o.run_id
        WHERE o.env=:env AND o.strategy='pb1_pullback_close'
          AND o.submitted_at >= :start AND o.submitted_at < :end
          AND o.submitted_at IS NOT NULL
          AND EXTRACT(EPOCH FROM (o.submitted_at-r.started_at)) > 180
    """), params).scalar_one()
    skips = conn.execute(text("""
        SELECT COUNT(*) AS buy_skips,
          COUNT(*) FILTER(WHERE reasons ILIKE '%KR_VOLUME_PROOF_KIS_EXCHANGE_QUOTE_STALE_OR_FUTURE%')
            AS stale_quote_skips,
          COUNT(*) FILTER(WHERE reasons ILIKE '%KR_ENTRY_TIMING_NOT_READY_OR_PRICE_TOO_HIGH%')
            AS timing_price_skips
        FROM public.ledger_events
        WHERE env=:env AND event_type='ORDER_SKIP' AND side='BUY'
          AND ts::timestamptz >= :start AND ts::timestamptz < :end
    """), params).mappings().one()
    claims = conn.execute(text("""
        SELECT COUNT(*) FROM public.execution_claims
        WHERE env=:env AND market='KR'
          AND trading_epoch_id IN (
            SELECT trading_epoch_id FROM public.trading_epochs
            WHERE env=:env AND account_id=:account AND status='ACTIVE'
          )
          AND strategy_owner='PB1' AND action_state='UNCERTAIN'
    """), params).scalar_one()
    return {
        "entry_shas": shas,
        "migration_0063": int(migrations or 0) == 1,
        "triggers": triggers,
        "active_epoch_count": int(active_epoch_count or 0),
        "submitted_after_180s": int(late or 0),
        "elapsed_runs_over_180s": int(run_metrics["elapsed_runs_over_180s"] or 0),
        "pending_close_runs": int(run_metrics["pending_close_runs"] or 0),
        "uncertain_claims": int(claims or 0),
        "buy_skips": int(skips["buy_skips"] or 0),
        "stale_quote_skips": int(skips["stale_quote_skips"] or 0),
        "timing_price_skips": int(skips["timing_price_skips"] or 0),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-sha", required=True,
                        help="exact 40-character deployed git SHA")
    parser.add_argument("--trade-date", default=datetime.now(KST).date().isoformat())
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[0-9a-f]{40}", args.expected_sha):
        parser.error("--expected-sha must be the FULL deployed 40-character SHA")
    try:
        day = date.fromisoformat(args.trade_date)
    except ValueError:
        parser.error("--trade-date must be YYYY-MM-DD")
    if any(str(os.getenv(name) or "practice").lower() != "practice" for name in ("KIS_ENV", "STRATEGY_ENV")):
        raise RuntimeError("KR_RELEASE_ACCEPTANCE_PRACTICE_ONLY")
    from trader.account_state import get_account_key
    from trader.db.engine import get_engine

    account = get_account_key(env="practice")
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("SET TRANSACTION READ ONLY"))
        snapshot = collect_readonly_snapshot(
            conn, trade_date=day, account_id=account,
        )
        conn.rollback()
    report = evaluate_readiness(snapshot, args.expected_sha)
    report["trade_date"] = day.isoformat()
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    # A SELECT-only audit must NEVER return a green exit code for a live-release
    # gate; KIS originals, WSL observer and ACK/restart replay remain unproven.
    return 3 if report["release_readiness"] == "OPERATOR_KIS_ATTESTATION_REQUIRED" else 2


if __name__ == "__main__":
    raise SystemExit(main())

                 OR COALESCE(entry_meta_json->>'entry_exit_plan_sha256','')
                     !~ '^[0-9a-f]{64}
        SELECT COUNT(*) FROM public.trading_epochs
        WHERE env=:env AND account_id=:account AND status='ACTIVE'
    """), params).scalar_one()
    run_metrics = conn.execute(text("""
        SELECT
          COUNT(*) FILTER(WHERE EXTRACT(EPOCH FROM (finished_at-started_at)) > 180)
            AS elapsed_runs_over_180s,
          COUNT(*) FILTER(WHERE run_window='close'
                           AND status='PENDING_SETTLEMENT') AS pending_close_runs
        FROM public.runs WHERE env=:env
          AND strategy='pb1_pullback_close'
          AND started_at >= :start AND started_at < :end
    """), params).mappings().one()
    # Order submit clock, not a whole-run duration, determines the 180-second
    # requirement. Orders may have no matching run and are then NOT ATTESTED.
    late = conn.execute(text("""
        SELECT COUNT(*) FROM public.orders o
        JOIN public.runs r ON r.run_id=o.run_id
        WHERE o.env=:env AND o.strategy='pb1_pullback_close'
          AND o.submitted_at >= :start AND o.submitted_at < :end
          AND o.submitted_at IS NOT NULL
          AND EXTRACT(EPOCH FROM (o.submitted_at-r.started_at)) > 180
    """), params).scalar_one()
    skips = conn.execute(text("""
        SELECT COUNT(*) AS buy_skips,
          COUNT(*) FILTER(WHERE reasons ILIKE '%KR_VOLUME_PROOF_KIS_EXCHANGE_QUOTE_STALE_OR_FUTURE%')
            AS stale_quote_skips,
          COUNT(*) FILTER(WHERE reasons ILIKE '%KR_ENTRY_TIMING_NOT_READY_OR_PRICE_TOO_HIGH%')
            AS timing_price_skips
        FROM public.ledger_events
        WHERE env=:env AND event_type='ORDER_SKIP' AND side='BUY'
          AND ts::timestamptz >= :start AND ts::timestamptz < :end
    """), params).mappings().one()
    claims = conn.execute(text("""
        SELECT COUNT(*) FROM public.execution_claims
        WHERE env=:env AND market='KR'
          AND trading_epoch_id IN (
            SELECT trading_epoch_id FROM public.trading_epochs
            WHERE env=:env AND account_id=:account AND status='ACTIVE'
          )
          AND strategy_owner='PB1' AND action_state='UNCERTAIN'
    """), params).scalar_one()
    return {
        "entry_shas": shas,
        "migration_0063": int(migrations or 0) == 1,
        "triggers": triggers,
        "active_epoch_count": int(active_epoch_count or 0),
        "submitted_after_180s": int(late or 0),
        "elapsed_runs_over_180s": int(run_metrics["elapsed_runs_over_180s"] or 0),
        "pending_close_runs": int(run_metrics["pending_close_runs"] or 0),
        "uncertain_claims": int(claims or 0),
        "buy_skips": int(skips["buy_skips"] or 0),
        "stale_quote_skips": int(skips["stale_quote_skips"] or 0),
        "timing_price_skips": int(skips["timing_price_skips"] or 0),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-sha", required=True,
                        help="exact 40-character deployed git SHA")
    parser.add_argument("--trade-date", default=datetime.now(KST).date().isoformat())
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[0-9a-f]{40}", args.expected_sha):
        parser.error("--expected-sha must be the FULL deployed 40-character SHA")
    try:
        day = date.fromisoformat(args.trade_date)
    except ValueError:
        parser.error("--trade-date must be YYYY-MM-DD")
    if any(str(os.getenv(name) or "practice").lower() != "practice" for name in ("KIS_ENV", "STRATEGY_ENV")):
        raise RuntimeError("KR_RELEASE_ACCEPTANCE_PRACTICE_ONLY")
    from trader.account_state import get_account_key
    from trader.db.engine import get_engine

    account = get_account_key(env="practice")
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("SET TRANSACTION READ ONLY"))
        snapshot = collect_readonly_snapshot(
            conn, trade_date=day, account_id=account,
        )
        conn.rollback()
    report = evaluate_readiness(snapshot, args.expected_sha)
    report["trade_date"] = day.isoformat()
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    # A SELECT-only audit must NEVER return a green exit code for a live-release
    # gate; KIS originals, WSL observer and ACK/restart replay remain unproven.
    return 3 if report["release_readiness"] == "OPERATOR_KIS_ATTESTATION_REQUIRED" else 2


if __name__ == "__main__":
    raise SystemExit(main())

                 OR entry_exit_plan_json IS NULL
                 OR entry_exit_plan_json='{}'::jsonb)
        ) AS invalid_open,
        COUNT(*) FILTER (
          WHERE status='CLOSED' AND qty=0
            AND entry_meta_json->>'entry_contract_sha256' IS NOT NULL
            AND COALESCE(entry_meta_json->>'entry_exit_plan_sha256','')
                !~ '^[0-9a-f]{64}
        SELECT COUNT(*) FROM public.trading_epochs
        WHERE env=:env AND account_id=:account AND status='ACTIVE'
    """), params).scalar_one()
    run_metrics = conn.execute(text("""
        SELECT
          COUNT(*) FILTER(WHERE EXTRACT(EPOCH FROM (finished_at-started_at)) > 180)
            AS elapsed_runs_over_180s,
          COUNT(*) FILTER(WHERE run_window='close'
                           AND status='PENDING_SETTLEMENT') AS pending_close_runs
        FROM public.runs WHERE env=:env
          AND strategy='pb1_pullback_close'
          AND started_at >= :start AND started_at < :end
    """), params).mappings().one()
    # Order submit clock, not a whole-run duration, determines the 180-second
    # requirement. Orders may have no matching run and are then NOT ATTESTED.
    late = conn.execute(text("""
        SELECT COUNT(*) FROM public.orders o
        JOIN public.runs r ON r.run_id=o.run_id
        WHERE o.env=:env AND o.strategy='pb1_pullback_close'
          AND o.submitted_at >= :start AND o.submitted_at < :end
          AND o.submitted_at IS NOT NULL
          AND EXTRACT(EPOCH FROM (o.submitted_at-r.started_at)) > 180
    """), params).scalar_one()
    skips = conn.execute(text("""
        SELECT COUNT(*) AS buy_skips,
          COUNT(*) FILTER(WHERE reasons ILIKE '%KR_VOLUME_PROOF_KIS_EXCHANGE_QUOTE_STALE_OR_FUTURE%')
            AS stale_quote_skips,
          COUNT(*) FILTER(WHERE reasons ILIKE '%KR_ENTRY_TIMING_NOT_READY_OR_PRICE_TOO_HIGH%')
            AS timing_price_skips
        FROM public.ledger_events
        WHERE env=:env AND event_type='ORDER_SKIP' AND side='BUY'
          AND ts::timestamptz >= :start AND ts::timestamptz < :end
    """), params).mappings().one()
    claims = conn.execute(text("""
        SELECT COUNT(*) FROM public.execution_claims
        WHERE env=:env AND market='KR'
          AND trading_epoch_id IN (
            SELECT trading_epoch_id FROM public.trading_epochs
            WHERE env=:env AND account_id=:account AND status='ACTIVE'
          )
          AND strategy_owner='PB1' AND action_state='UNCERTAIN'
    """), params).scalar_one()
    return {
        "entry_shas": shas,
        "migration_0063": int(migrations or 0) == 1,
        "triggers": triggers,
        "active_epoch_count": int(active_epoch_count or 0),
        "submitted_after_180s": int(late or 0),
        "elapsed_runs_over_180s": int(run_metrics["elapsed_runs_over_180s"] or 0),
        "pending_close_runs": int(run_metrics["pending_close_runs"] or 0),
        "uncertain_claims": int(claims or 0),
        "buy_skips": int(skips["buy_skips"] or 0),
        "stale_quote_skips": int(skips["stale_quote_skips"] or 0),
        "timing_price_skips": int(skips["timing_price_skips"] or 0),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-sha", required=True,
                        help="exact 40-character deployed git SHA")
    parser.add_argument("--trade-date", default=datetime.now(KST).date().isoformat())
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[0-9a-f]{40}", args.expected_sha):
        parser.error("--expected-sha must be the FULL deployed 40-character SHA")
    try:
        day = date.fromisoformat(args.trade_date)
    except ValueError:
        parser.error("--trade-date must be YYYY-MM-DD")
    if any(str(os.getenv(name) or "practice").lower() != "practice" for name in ("KIS_ENV", "STRATEGY_ENV")):
        raise RuntimeError("KR_RELEASE_ACCEPTANCE_PRACTICE_ONLY")
    from trader.account_state import get_account_key
    from trader.db.engine import get_engine

    account = get_account_key(env="practice")
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("SET TRANSACTION READ ONLY"))
        snapshot = collect_readonly_snapshot(
            conn, trade_date=day, account_id=account,
        )
        conn.rollback()
    report = evaluate_readiness(snapshot, args.expected_sha)
    report["trade_date"] = day.isoformat()
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    # A SELECT-only audit must NEVER return a green exit code for a live-release
    # gate; KIS originals, WSL observer and ACK/restart replay remain unproven.
    return 3 if report["release_readiness"] == "OPERATOR_KIS_ATTESTATION_REQUIRED" else 2


if __name__ == "__main__":
    raise SystemExit(main())

        ) AS legacy_closed_missing
        FROM public.positions
        WHERE env=:env AND strategy='pb1_pullback_close'
    """), params).mappings().one()
    active_epoch_count = conn.execute(text("""
        SELECT COUNT(*) FROM public.trading_epochs
        WHERE env=:env AND account_id=:account AND status='ACTIVE'
    """), params).scalar_one()
    run_metrics = conn.execute(text("""
        SELECT
          COUNT(*) FILTER(WHERE EXTRACT(EPOCH FROM (finished_at-started_at)) > 180)
            AS elapsed_runs_over_180s,
          COUNT(*) FILTER(WHERE run_window='close'
                           AND status='PENDING_SETTLEMENT') AS pending_close_runs
        FROM public.runs WHERE env=:env
          AND strategy='pb1_pullback_close'
          AND started_at >= :start AND started_at < :end
    """), params).mappings().one()
    # Order submit clock, not a whole-run duration, determines the 180-second
    # requirement. Orders may have no matching run and are then NOT ATTESTED.
    late = conn.execute(text("""
        SELECT COUNT(*) FROM public.orders o
        JOIN public.runs r ON r.run_id=o.run_id
        WHERE o.env=:env AND o.strategy='pb1_pullback_close'
          AND o.submitted_at >= :start AND o.submitted_at < :end
          AND o.submitted_at IS NOT NULL
          AND EXTRACT(EPOCH FROM (o.submitted_at-r.started_at)) > 180
    """), params).scalar_one()
    skips = conn.execute(text("""
        SELECT COUNT(*) AS buy_skips,
          COUNT(*) FILTER(WHERE reasons ILIKE '%KR_VOLUME_PROOF_KIS_EXCHANGE_QUOTE_STALE_OR_FUTURE%')
            AS stale_quote_skips,
          COUNT(*) FILTER(WHERE reasons ILIKE '%KR_ENTRY_TIMING_NOT_READY_OR_PRICE_TOO_HIGH%')
            AS timing_price_skips
        FROM public.ledger_events
        WHERE env=:env AND event_type='ORDER_SKIP' AND side='BUY'
          AND ts::timestamptz >= :start AND ts::timestamptz < :end
    """), params).mappings().one()
    claims = conn.execute(text("""
        SELECT COUNT(*) FROM public.execution_claims
        WHERE env=:env AND market='KR'
          AND trading_epoch_id IN (
            SELECT trading_epoch_id FROM public.trading_epochs
            WHERE env=:env AND account_id=:account AND status='ACTIVE'
          )
          AND strategy_owner='PB1' AND action_state='UNCERTAIN'
    """), params).scalar_one()
    return {
        "entry_shas": shas,
        "migration_0063": int(migrations or 0) == 1,
        "triggers": triggers,
        "active_epoch_count": int(active_epoch_count or 0),
        "submitted_after_180s": int(late or 0),
        "elapsed_runs_over_180s": int(run_metrics["elapsed_runs_over_180s"] or 0),
        "pending_close_runs": int(run_metrics["pending_close_runs"] or 0),
        "uncertain_claims": int(claims or 0),
        "buy_skips": int(skips["buy_skips"] or 0),
        "stale_quote_skips": int(skips["stale_quote_skips"] or 0),
        "timing_price_skips": int(skips["timing_price_skips"] or 0),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-sha", required=True,
                        help="exact 40-character deployed git SHA")
    parser.add_argument("--trade-date", default=datetime.now(KST).date().isoformat())
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[0-9a-f]{40}", args.expected_sha):
        parser.error("--expected-sha must be the FULL deployed 40-character SHA")
    try:
        day = date.fromisoformat(args.trade_date)
    except ValueError:
        parser.error("--trade-date must be YYYY-MM-DD")
    if any(str(os.getenv(name) or "practice").lower() != "practice" for name in ("KIS_ENV", "STRATEGY_ENV")):
        raise RuntimeError("KR_RELEASE_ACCEPTANCE_PRACTICE_ONLY")
    from trader.account_state import get_account_key
    from trader.db.engine import get_engine

    account = get_account_key(env="practice")
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("SET TRANSACTION READ ONLY"))
        snapshot = collect_readonly_snapshot(
            conn, trade_date=day, account_id=account,
        )
        conn.rollback()
    report = evaluate_readiness(snapshot, args.expected_sha)
    report["trade_date"] = day.isoformat()
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    # A SELECT-only audit must NEVER return a green exit code for a live-release
    # gate; KIS originals, WSL observer and ACK/restart replay remain unproven.
    return 3 if report["release_readiness"] == "OPERATOR_KIS_ATTESTATION_REQUIRED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
