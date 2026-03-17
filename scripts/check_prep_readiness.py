from __future__ import annotations

import argparse
import datetime as dt
import os
import sys

import sqlalchemy as sa

from trader.db.engine import make_engine
from trader.db.repos import DerivedMinerviniRepo, LedgerEventsRepo, WatchlistRepo
from trader.db.schema import schema_for_engine
from trader.runtime_paths import build_final30_scored_paths, repo_root
from trader.time_utils import now_kst, resolve_trade_readiness_as_of


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check PREP readiness from DB/ledger only.")
    parser.add_argument("--env", required=True, help="Strategy environment (e.g. practice/live)")
    parser.add_argument("--as-of", required=True, help="As-of date in YYYY-MM-DD")
    parser.add_argument("--run-date", help="Run date in YYYY-MM-DD (defaults to current KST date)")
    parser.add_argument("--window", required=True, help="Market window label")
    parser.add_argument("--exchange", default="KRX", help="Exchange name for trading-day resolution")
    return parser.parse_args()


def _to_date(value: str) -> dt.date:
    return dt.date.fromisoformat((value or "").strip())


def _count_watchlist_rows(conn: sa.Connection, schema, *, env: str, as_of: dt.date, strategy: str) -> int:
    stmt = (
        sa.select(sa.func.count())
        .select_from(schema.pb1_watchlist)
        .where(
            sa.and_(
                schema.pb1_watchlist.c.env == env,
                schema.pb1_watchlist.c.as_of == as_of,
                schema.pb1_watchlist.c.strategy == strategy,
            )
        )
    )
    return int(conn.execute(stmt).scalar() or 0)


def main() -> int:
    args = _parse_args()

    try:
        env = (args.env or "").strip().lower()
        requested_as_of = _to_date(args.as_of)
        run_date = _to_date(args.run_date) if args.run_date else now_kst().date()
        window = (args.window or "").strip().lower()
        exchange = (args.exchange or "KRX").strip().upper()
        resolved = resolve_trade_readiness_as_of(
            run_date=run_date,
            market_window=window,
            exchange=exchange,
            candidate_as_of=requested_as_of,
        )
        resolved_as_of = resolved["resolved_as_of"]
        calendar_prev = resolved["calendar_prev"]
        reason = str(resolved["reason"])

        engine = make_engine()
        schema = schema_for_engine(engine)

        ledger_repo = LedgerEventsRepo(engine)
        derived_repo = DerivedMinerviniRepo(engine)
        watchlist_repo = WatchlistRepo(engine)

        prep_done, prep_done_count = ledger_repo.prep_done_status(env=env, as_of=resolved_as_of)
        derived_count = derived_repo.count_as_of(env=env, as_of=resolved_as_of)
        repo_root_path = repo_root().resolve()
        final30_paths = build_final30_scored_paths(repo_root_path, env, resolved_as_of.isoformat())
        final30_path_stats = {
            label: {
                "path": str(path),
                "exists": int(path.exists()),
                "bytes": int(path.stat().st_size) if path.exists() else 0,
            }
            for label, path in final30_paths.items()
        }
        final30_missing = [
            label
            for label, stats in final30_path_stats.items()
            if not stats["exists"] or stats["bytes"] <= 0
        ]
        file_contract_ok = not final30_missing

        with engine.connect() as conn:
            watchlist_final_count = _count_watchlist_rows(
                conn,
                schema,
                env=env,
                as_of=resolved_as_of,
                strategy="pb1_watchlist_final",
            )
            watchlist_final_scored_count = _count_watchlist_rows(
                conn,
                schema,
                env=env,
                as_of=resolved_as_of,
                strategy="pb1_watchlist_final_scored",
            )

        scored_contract = watchlist_repo.verify_watchlist_scored_contract(
            env=env,
            as_of=resolved_as_of,
            strategy="pb1_watchlist_final_scored",
            allow_latest_fallback=False,
            log_result=False,
        )

        db_contract_ok = bool(
            prep_done
            and derived_count >= 1
            and watchlist_final_count == 30
            and watchlist_final_scored_count == 30
            and scored_contract.get("ok")
        )

        status = "ready" if db_contract_ok else "not_ready"
        grade = "ok"
        reason_code = "DB_CONTRACT_OK"
        if db_contract_ok and not file_contract_ok:
            grade = "warn"
            reason_code = "DB_CONTRACT_OK_FILE_OPTIONAL_MISSING"
        elif not db_contract_ok:
            grade = "error"
            reason_code = "DB_CONTRACT_INCOMPLETE"

        print(
            f"[PREP][READINESS][DB] env={env} as_of={resolved_as_of.isoformat()} prep_done={int(prep_done)} "
            f"prep_done_count={prep_done_count} derived_minervini={derived_count} watchlist_final={watchlist_final_count} "
            f"watchlist_final_scored={watchlist_final_scored_count} db_contract_ok={int(db_contract_ok)}"
        )
        print(
            f"[PREP][READINESS][FILES] runtime={final30_path_stats['runtime']['exists']} "
            f"ledger={final30_path_stats['ledger']['exists']} signals={final30_path_stats['signals']['exists']} "
            f"file_contract_ok={int(file_contract_ok)} warn_only=1"
        )

        if db_contract_ok:
            print(
                f"[PREP][READINESS] status={status} grade={grade} reason={reason_code} env={env} "
                f"run_date={run_date.isoformat()} window={window} requested_as_of={requested_as_of.isoformat()} "
                f"resolved_as_of={resolved_as_of.isoformat()} calendar_prev={calendar_prev.isoformat()} resolve_reason={reason}"
            )
            return 0

        print(
            f"[PREP][READINESS] status={status} grade={grade} reason={reason_code} env={env} "
            f"run_date={run_date.isoformat()} window={window} requested_as_of={requested_as_of.isoformat()} "
            f"resolved_as_of={resolved_as_of.isoformat()} calendar_prev={calendar_prev.isoformat()} resolve_reason={reason} "
            f"missing_paths={final30_missing} scored_missing_fields={scored_contract.get('missing_fields')}"
        )
        return 10
    except Exception as exc:
        print(f"[PREP][READINESS] status=error err={type(exc).__name__}: {exc}")
        return 20


if __name__ == "__main__":
    raise SystemExit(main())
