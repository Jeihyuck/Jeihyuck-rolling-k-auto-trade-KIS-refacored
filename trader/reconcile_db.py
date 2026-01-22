from __future__ import annotations

import logging
from datetime import datetime

import sqlalchemy as sa

logger = logging.getLogger(__name__)


def close_stale_positions(*, engine, env: str, strategy: str, reason: str, ts: datetime) -> int:
    inspector = sa.inspect(engine)
    columns = {col["name"] for col in inspector.get_columns("positions")}
    has_status = "status" in columns
    has_reason = "closed_reason" in columns
    has_closed_ts = "closed_ts" in columns
    status_value = "SOFT_CLOSED"
    if reason in {"stale_db_holdings_empty", "STALE_DB_BUT_KIS_EMPTY"}:
        status_value = "ORPHAN"
    with engine.begin() as conn:
        if has_status:
            set_parts = [
                "status = :status",
                "qty = 0",
                "avg_buy_price = NULL",
                "total_cost = 0.0",
                "updated_at = CURRENT_TIMESTAMP",
            ]
            params = {"status": status_value, "env": env, "strategy": strategy}
            if has_reason:
                set_parts.append("closed_reason = :reason")
                params["reason"] = reason
            if has_closed_ts:
                set_parts.append("closed_ts = :closed_ts")
                params["closed_ts"] = ts
            stmt = sa.text(
                f"""
                UPDATE positions
                SET {", ".join(set_parts)}
                WHERE env = :env AND strategy = :strategy AND qty > 0
                """
            )
            result = conn.execute(stmt, params)
        else:
            stmt = sa.text(
                """
                UPDATE positions
                SET qty = 0,
                    avg_buy_price = NULL,
                    total_cost = 0.0,
                    updated_at = CURRENT_TIMESTAMP
                WHERE env = :env AND strategy = :strategy AND qty > 0
                """
            )
            result = conn.execute(stmt, {"env": env, "strategy": strategy})
    count = int(result.rowcount or 0)
    logger.warning(
        "[STALE_DB][SOFT_CLOSE] env=%s strategy=%s rows=%s status=%s reason=%s",
        env,
        strategy,
        count,
        status_value,
        reason,
    )
    return count
