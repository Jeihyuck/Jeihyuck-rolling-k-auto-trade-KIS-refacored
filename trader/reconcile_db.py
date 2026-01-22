from __future__ import annotations

import logging
from datetime import datetime
import json
from pathlib import Path

import sqlalchemy as sa

logger = logging.getLogger(__name__)
RECONCILE_GUARD_FILE = Path("runtime") / "reconcile_guard.json"
EMPTY_STREAK_MIN = 2
EMPTY_STREAK_MAX = 3


def _guard_path(bot_state_dir: Path) -> Path:
    return bot_state_dir / RECONCILE_GUARD_FILE


def load_reconcile_guard(bot_state_dir: Path) -> dict:
    path = _guard_path(bot_state_dir)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("[RECONCILE][GUARD][LOAD_FAIL] path=%s", path, exc_info=True)
        return {}


def save_reconcile_guard(bot_state_dir: Path, payload: dict) -> Path:
    path = _guard_path(bot_state_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def evaluate_stale_db_guard(
    *,
    bot_state_dir: Path,
    tick_ts: datetime,
    kis_holdings_empty: bool,
    orders_count: int,
    fills_count: int,
    had_kis_error: bool,
    min_empty_ticks: int = EMPTY_STREAK_MIN,
) -> tuple[bool, str, dict]:
    guard = load_reconcile_guard(bot_state_dir)
    empty_streak = int(guard.get("empty_streak") or 0)

    if kis_holdings_empty and not had_kis_error:
        empty_streak = min(empty_streak + 1, EMPTY_STREAK_MAX)
    elif not kis_holdings_empty:
        empty_streak = 0

    guard.update(
        {
            "last_tick_ts": tick_ts.isoformat(),
            "empty_streak": empty_streak,
            "last_holdings_empty": kis_holdings_empty,
        }
    )
    if had_kis_error:
        guard["last_kis_error_ts"] = tick_ts.isoformat()
    if orders_count > 0 or fills_count > 0:
        guard["last_order_or_fill_ts"] = tick_ts.isoformat()

    save_reconcile_guard(bot_state_dir, guard)

    if had_kis_error:
        return False, "kis_error", guard
    if not kis_holdings_empty:
        return False, "kis_has_holdings", guard
    if orders_count > 0 or fills_count > 0:
        return False, "recent_orders_or_fills", guard
    if empty_streak < min_empty_ticks:
        return False, "empty_streak_insufficient", guard
    return True, "empty_streak_confirmed", guard


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
