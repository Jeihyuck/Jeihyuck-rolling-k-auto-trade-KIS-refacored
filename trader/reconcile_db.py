from __future__ import annotations

import logging
from datetime import datetime
import json
from pathlib import Path
from typing import Any, Iterable

import sqlalchemy as sa

from trader.runtime_paths import runtime_path
from trader.db.schema import schema_for_engine

logger = logging.getLogger(__name__)
RECONCILE_GUARD_FILE = runtime_path("runtime", "reconcile_guard.json")
EMPTY_STREAK_MIN = 2
EMPTY_STREAK_MAX = 3


def _guard_path(runtime_dir: Path) -> Path:
    return runtime_dir / RECONCILE_GUARD_FILE


def load_reconcile_guard(runtime_dir: Path) -> dict:
    path = _guard_path(runtime_dir)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("[RECONCILE][GUARD][LOAD_FAIL] path=%s", path, exc_info=True)
        return {}


def save_reconcile_guard(runtime_dir: Path, payload: dict) -> Path:
    path = _guard_path(runtime_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def evaluate_stale_db_guard(
    *,
    runtime_dir: Path,
    tick_ts: datetime,
    kis_holdings_empty: bool,
    orders_count: int,
    fills_count: int,
    had_kis_error: bool,
    min_empty_ticks: int = EMPTY_STREAK_MIN,
) -> tuple[bool, str, dict]:
    guard = load_reconcile_guard(runtime_dir)
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

    save_reconcile_guard(runtime_dir, guard)

    if had_kis_error:
        return False, "kis_error", guard
    if not kis_holdings_empty:
        return False, "kis_has_holdings", guard
    if orders_count > 0 or fills_count > 0:
        return False, "recent_orders_or_fills", guard
    if empty_streak < min_empty_ticks:
        return False, "empty_streak_insufficient", guard
    return True, "empty_streak_confirmed", guard


def close_stale_positions(
    *,
    engine,
    env: str,
    strategy: str,
    reason: str,
    ts: datetime,
    kis_balance: dict[str, Any] | None = None,
    sell_fill_codes: Iterable[str] | None = None,
    runtime_dir: Path | None = None,
) -> int:
    return close_stale_positions_guarded(
        engine=engine,
        env=env,
        strategy=strategy,
        reason=reason,
        ts=ts,
        kis_balance=kis_balance,
        sell_fill_codes=sell_fill_codes,
        runtime_dir=runtime_dir,
    )


def _kis_holdings_by_code(kis_balance: dict[str, Any] | None) -> dict[str, dict[str, int]]:
    rows = list((kis_balance or {}).get("output1") or [])
    holdings: dict[str, dict[str, int]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        code = str(row.get("pdno") or row.get("code") or "").zfill(6)
        if not code:
            continue
        try:
            hldg_qty = int(float(str(row.get("hldg_qty") or row.get("qty") or 0).replace(",", "")))
        except Exception:
            hldg_qty = 0
        try:
            ord_psbl_qty = int(float(str(row.get("ord_psbl_qty") or 0).replace(",", "")))
        except Exception:
            ord_psbl_qty = 0
        holdings[code] = {
            "hldg_qty": max(0, hldg_qty),
            "ord_psbl_qty": max(0, ord_psbl_qty),
        }
    return holdings


def close_stale_positions_guarded(
    *,
    engine,
    env: str,
    strategy: str,
    reason: str,
    ts: datetime,
    kis_balance: dict[str, Any] | None = None,
    sell_fill_codes: Iterable[str] | None = None,
    runtime_dir: Path | None = None,
) -> int:
    inspector = sa.inspect(engine)
    columns = {col["name"] for col in inspector.get_columns("positions")}
    has_status = "status" in columns
    has_reason = "closed_reason" in columns
    has_closed_ts = "closed_ts" in columns
    status_value = "SOFT_CLOSED"
    if reason in {"stale_db_holdings_empty", "STALE_DB_BUT_KIS_EMPTY"}:
        status_value = "ORPHAN"
    schema = schema_for_engine(engine)
    sell_fill_code_set = {str(code or "").zfill(6) for code in (sell_fill_codes or []) if str(code or "").strip()}
    holdings_by_code = _kis_holdings_by_code(kis_balance)
    guard = load_reconcile_guard(runtime_dir or Path(".")) if runtime_dir is not None else {}
    stale_confirmed = bool(guard.get("last_holdings_empty")) and int(guard.get("empty_streak") or 0) >= EMPTY_STREAK_MIN

    with engine.connect() as conn:
        open_rows = [
            dict(row)
            for row in conn.execute(
                sa.select(
                    schema.positions.c.code,
                    schema.positions.c.qty,
                ).where(
                    sa.and_(
                        schema.positions.c.env == env,
                        schema.positions.c.strategy == strategy,
                        schema.positions.c.qty > 0,
                    )
                )
            ).mappings().all()
        ]

    if not open_rows:
        logger.info("[STALE_DB][SOFT_CLOSE][SKIP] reason=no_open_positions rows_kept=0")
        return 0

    closable_codes: list[str] = []
    rows_kept = 0
    for row in open_rows:
        code = str((row or {}).get("code") or "").zfill(6)
        db_qty = int((row or {}).get("qty") or 0)
        kis_state = holdings_by_code.get(code) or {}
        kis_qty = int(kis_state.get("hldg_qty") or 0)
        ord_psbl_qty = int(kis_state.get("ord_psbl_qty") or 0)
        if kis_qty > 0 or ord_psbl_qty > 0:
            rows_kept += 1
            logger.info(
                "[STALE_DB][CHECK] code=%s db_qty=%s kis_qty=%s action=KEEP reason=KIS_HOLDING_EXISTS",
                code,
                db_qty,
                max(kis_qty, ord_psbl_qty),
            )
            continue
        if code not in sell_fill_code_set:
            rows_kept += 1
            logger.info(
                "[STALE_DB][CHECK] code=%s db_qty=%s kis_qty=0 action=KEEP reason=SELL_FILL_CONFIRM_MISSING",
                code,
                db_qty,
            )
            continue
        if not stale_confirmed:
            rows_kept += 1
            logger.info(
                "[STALE_DB][CHECK] code=%s db_qty=%s kis_qty=0 action=KEEP reason=STALE_CONFIRM_PENDING",
                code,
                db_qty,
            )
            continue
        closable_codes.append(code)

    if not closable_codes:
        # A live KIS holding only protects its own row; never use it as a
        # portfolio-wide reason to hide stale DB positions.
        skip_reason = "no_rowwise_soft_close_candidates" if rows_kept else "no_soft_close_candidates"
        logger.warning(
            "[STALE_DB][SOFT_CLOSE][SKIP] reason=%s rows_kept=%s",
            skip_reason,
            rows_kept,
        )
        return 0

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
            params["codes"] = closable_codes
            stmt = sa.text(
                f"""
                UPDATE positions
                SET {", ".join(set_parts)}
                WHERE env = :env AND strategy = :strategy AND qty > 0 AND code IN :codes
                """
            ).bindparams(sa.bindparam("codes", expanding=True))
            result = conn.execute(stmt, params)
        else:
            stmt = sa.text(
                """
                UPDATE positions
                SET qty = 0,
                    avg_buy_price = NULL,
                    total_cost = 0.0,
                    updated_at = CURRENT_TIMESTAMP
                WHERE env = :env AND strategy = :strategy AND qty > 0 AND code IN :codes
                """
            ).bindparams(sa.bindparam("codes", expanding=True))
            result = conn.execute(stmt, {"env": env, "strategy": strategy, "codes": closable_codes})
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
