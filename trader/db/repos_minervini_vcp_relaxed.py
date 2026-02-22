from __future__ import annotations

import json
from typing import Any

import sqlalchemy as sa
from sqlalchemy import Engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError


def upsert_minervini_vcp_relaxed(engine: Engine, env: str, run_date: str, rows: list[dict]) -> int:
    if not rows:
        return 0

    table = sa.Table("minervini_results_vcp_relaxed", sa.MetaData(), autoload_with=engine)
    env_n = (env or "").strip().lower()

    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        payload = dict(row)
        payload["env"] = env_n
        payload["run_date"] = run_date
        payload["fail_reasons"] = payload.get("fail_reasons") or []
        payload["param_snapshot"] = payload.get("param_snapshot") or {}
        normalized_rows.append(payload)

    with engine.begin() as conn:
        if conn.dialect.name == "postgresql":
            stmt = pg_insert(table).values(normalized_rows)
            update_cols = {
                col.name: getattr(stmt.excluded, col.name)
                for col in table.c
                if col.name not in {"run_date", "env", "symbol", "created_at"}
            }
            stmt = stmt.on_conflict_do_update(
                index_elements=[table.c.run_date, table.c.env, table.c.symbol],
                set_=update_cols,
            )
            conn.execute(stmt)
        else:
            for payload in normalized_rows:
                payload = dict(payload)
                payload["fail_reasons"] = json.dumps(payload.get("fail_reasons") or [])
                payload["param_snapshot"] = json.dumps(payload.get("param_snapshot") or {})
                try:
                    conn.execute(sa.insert(table).values(**payload))
                except IntegrityError:
                    update_cols = {
                        k: v for k, v in payload.items() if k not in {"run_date", "env", "symbol", "created_at"}
                    }
                    where_clause = sa.and_(
                        table.c.run_date == payload["run_date"],
                        table.c.env == payload["env"],
                        table.c.symbol == payload["symbol"],
                    )
                    conn.execute(sa.update(table).where(where_clause).values(**update_cols))

    return len(normalized_rows)
