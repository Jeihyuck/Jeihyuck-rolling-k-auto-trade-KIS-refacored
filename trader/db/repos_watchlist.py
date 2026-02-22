from __future__ import annotations

from typing import List

import sqlalchemy as sa
from sqlalchemy import Engine, and_, select

from trader.time_coerce import to_date
from .schema import schema_for_engine


def load_watchlist_codes(engine: Engine, env: str, strategy: str, as_of: str) -> List[str]:
    """Return list of symbols; raise if missing/empty."""
    env_n = (env or "").strip().lower()
    strategy_n = (strategy or "").strip().lower()
    as_of_date = to_date(as_of)
    schema = schema_for_engine(engine)

    stmt = (
        select(schema.pb1_watchlist.c.code)
        .where(
            and_(
                schema.pb1_watchlist.c.env == env_n,
                schema.pb1_watchlist.c.strategy == strategy_n,
                schema.pb1_watchlist.c.as_of == as_of_date,
            )
        )
        .order_by(schema.pb1_watchlist.c.rank.asc())
    )

    with engine.connect() as conn:
        codes = [str(code).zfill(6) for code in conn.execute(stmt).scalars().all() if code]

    if not codes:
        raise ValueError(
            f"final30 watchlist is missing/empty: env={env_n} strategy={strategy_n} as_of={as_of_date.isoformat()}"
        )
    return codes


def load_watchlist_entries(engine: Engine, env: str, strategy: str, as_of: str) -> List[dict[str, str | None]]:
    env_n = (env or "").strip().lower()
    strategy_n = (strategy or "").strip().lower()
    as_of_date = to_date(as_of)
    schema = schema_for_engine(engine)

    stmt = (
        select(schema.pb1_watchlist.c.code, schema.pb1_watchlist.c.meta)
        .where(
            and_(
                schema.pb1_watchlist.c.env == env_n,
                schema.pb1_watchlist.c.strategy == strategy_n,
                schema.pb1_watchlist.c.as_of == as_of_date,
            )
        )
        .order_by(schema.pb1_watchlist.c.rank.asc())
    )

    entries: list[dict[str, str | None]] = []
    with engine.connect() as conn:
        for code, meta in conn.execute(stmt).all():
            if not code:
                continue
            code_n = str(code).zfill(6)
            name: str | None = None
            if isinstance(meta, dict):
                for key in ("name", "stock_name", "kor_name"):
                    val = meta.get(key)
                    if val:
                        name = str(val)
                        break
            entries.append({"code": code_n, "name": name})

    if not entries:
        raise ValueError(
            f"final30 watchlist is missing/empty: env={env_n} strategy={strategy_n} as_of={as_of_date.isoformat()}"
        )
    return entries
