from __future__ import annotations

import argparse
import datetime as dt
import sys

import sqlalchemy as sa

from trader.db.engine import make_engine
from trader.db.repos import DerivedMinerviniRepo, LedgerEventsRepo
from trader.db.schema import schema_for_engine


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check PREP readiness from DB/ledger only.")
    parser.add_argument("--env", required=True, help="Strategy environment (e.g. practice/live)")
    parser.add_argument("--as-of", required=True, help="As-of date in YYYY-MM-DD")
    parser.add_argument("--window", required=True, help="Market window label")
    return parser.parse_args()


def _to_date(value: str) -> dt.date:
    return dt.date.fromisoformat((value or "").strip())


def main() -> int:
    args = _parse_args()

    try:
        env = (args.env or "").strip().lower()
        as_of = _to_date(args.as_of)
        window = (args.window or "").strip().lower()

        engine = make_engine()
        schema = schema_for_engine(engine)

        ledger_repo = LedgerEventsRepo(engine)
        derived_repo = DerivedMinerviniRepo(engine)

        prep_done, prep_done_count = ledger_repo.prep_done_status(env=env, as_of=as_of)
        derived_count = derived_repo.count_as_of(env=env, as_of=as_of)

        watchlist_stmt = (
            sa.select(sa.func.count())
            .select_from(schema.pb1_watchlist)
            .where(
                sa.and_(
                    schema.pb1_watchlist.c.env == env,
                    schema.pb1_watchlist.c.as_of == as_of,
                    schema.pb1_watchlist.c.strategy == "pb1_watchlist_final",
                )
            )
        )
        with engine.connect() as conn:
            watchlist_final_count = int(conn.execute(watchlist_stmt).scalar() or 0)

        ready = prep_done and (derived_count > 0) and (watchlist_final_count > 0)
        if ready:
            grade = "ok" if watchlist_final_count >= 30 else "degraded"
            print(
                f"[PREP][READINESS] status=ready env={env} as_of={as_of.isoformat()} window={window} "
                f"prep_done={int(prep_done)} prep_done_count={prep_done_count} "
                f"derived_minervini={derived_count} pb1_watchlist_final={watchlist_final_count} grade={grade}"
            )
            return 0

        print(
            f"[PREP][READINESS] status=missing env={env} as_of={as_of.isoformat()} window={window} "
            f"prep_done={int(prep_done)} prep_done_count={prep_done_count} "
            f"derived_minervini={derived_count} pb1_watchlist_final={watchlist_final_count}"
        )
        return 10
    except Exception as exc:
        print(f"[PREP][READINESS] status=error err={type(exc).__name__}: {exc}")
        return 20


if __name__ == "__main__":
    raise SystemExit(main())
