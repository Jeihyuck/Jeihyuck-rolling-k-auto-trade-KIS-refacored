from __future__ import annotations

import json
from typing import Any

import sqlalchemy as sa
from sqlalchemy import Engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError


def upsert_minervini_v2(engine: Engine, env: str, as_of: str, run_id: str, rows: list[dict]) -> int:
    """Upsert into signals_minervini_daily_v2 (PK env, as_of, symbol)"""
    if not rows:
        return 0

    table = sa.Table("signals_minervini_daily_v2", sa.MetaData(), autoload_with=engine)
    env_n = (env or "").strip().lower()

    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        payload = dict(row)
        payload["env"] = env_n
        payload["as_of"] = as_of
        payload["run_id"] = run_id
        payload["vcp_reasons"] = payload.get("vcp_reasons") or []
        payload["reject_reasons"] = payload.get("reject_reasons") or []
        normalized_rows.append(payload)

    with engine.begin() as conn:
        if conn.dialect.name == "postgresql":
            stmt = pg_insert(table).values(normalized_rows)
            update_cols = {
                col.name: getattr(stmt.excluded, col.name)
                for col in table.c
                if col.name not in {"env", "as_of", "symbol", "created_at"}
            }
            stmt = stmt.on_conflict_do_update(
                index_elements=[table.c.env, table.c.as_of, table.c.symbol],
                set_=update_cols,
            )
            conn.execute(stmt)
        else:
            for payload in normalized_rows:
                payload = dict(payload)
                payload["vcp_reasons"] = json.dumps(payload.get("vcp_reasons") or [])
                payload["reject_reasons"] = json.dumps(payload.get("reject_reasons") or [])
                try:
                    conn.execute(sa.insert(table).values(**payload))
                except IntegrityError:
                    update_cols = {
                        k: v for k, v in payload.items() if k not in {"env", "as_of", "symbol", "created_at"}
                    }
                    where_clause = sa.and_(
                        table.c.env == payload["env"],
                        table.c.as_of == payload["as_of"],
                        table.c.symbol == payload["symbol"],
                    )
                    conn.execute(sa.update(table).where(where_clause).values(**update_cols))

    return len(normalized_rows)
