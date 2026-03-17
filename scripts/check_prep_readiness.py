from __future__ import annotations

import argparse
import datetime as dt
import os
import sys

import sqlalchemy as sa

from trader.db.engine import make_engine
from trader.db.repos import DerivedMinerviniRepo, LedgerEventsRepo
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

        prep_done, prep_done_count = ledger_repo.prep_done_status(env=env, as_of=resolved_as_of)
        derived_count = derived_repo.count_as_of(env=env, as_of=resolved_as_of)
        require_scored = os.getenv("TRADE_REQUIRE_PREP_FINAL30_SCORED", "1") == "1"
        strict_final30_file_contract = os.getenv("STRICT_FINAL30_FILE_CONTRACT", "0") == "1"
        repo_root_path = repo_root().resolve()
        cwd = os.getcwd()
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

        ready = (derived_count > 0) and (watchlist_final_count > 0)
        if require_scored:
            ready = ready and (watchlist_final_scored_count > 0)

        final30_rows_ok = watchlist_final_scored_count > 0
        file_contract_ok = final30_rows_ok and not final30_missing
        grade = "ok"
        if ready and not file_contract_ok:
            grade = "warn"
            if strict_final30_file_contract:
                ready = False
        elif ready and watchlist_final_count < 30:
            grade = "degraded"

        print(
            f"[PREP][READINESS][FILES] repo_root={repo_root_path} cwd={cwd} paths={final30_path_stats} missing={final30_missing} strict={int(strict_final30_file_contract)}"
        )

        if ready:
            print(
                f"[PREP][READINESS] status=ready env={env} run_date={run_date.isoformat()} "
                f"window={window} requested_as_of={requested_as_of.isoformat()} "
                f"resolved_as_of={resolved_as_of.isoformat()} calendar_prev={calendar_prev.isoformat()} reason={reason} "
                f"prep_done={int(prep_done)} prep_done_count={prep_done_count} "
                f"derived_minervini={derived_count} pb1_watchlist_final={watchlist_final_count} "
                f"pb1_watchlist_final_scored={watchlist_final_scored_count} final30_file_contract_ok={int(file_contract_ok)} grade={grade}"
            )
            return 0

        print(
            f"[PREP][READINESS] status=missing env={env} run_date={run_date.isoformat()} "
            f"window={window} requested_as_of={requested_as_of.isoformat()} "
            f"resolved_as_of={resolved_as_of.isoformat()} calendar_prev={calendar_prev.isoformat()} reason={reason} "
            f"prep_done={int(prep_done)} prep_done_count={prep_done_count} "
            f"derived_minervini={derived_count} pb1_watchlist_final={watchlist_final_count} "
            f"pb1_watchlist_final_scored={watchlist_final_scored_count} final30_file_contract_ok={int(file_contract_ok)} missing_paths={final30_missing}"
        )
        return 10
    except Exception as exc:
        print(f"[PREP][READINESS] status=error err={type(exc).__name__}: {exc}")
        return 20


if __name__ == "__main__":
    raise SystemExit(main())
