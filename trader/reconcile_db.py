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
        try:
            avg_buy_price = float(str(row.get("pchs_avg_pric") or row.get("avg_prvs") or 0).replace(",", ""))
        except Exception:
            avg_buy_price = 0.0
        holdings[code] = {
            "hldg_qty": max(0, hldg_qty),
            "ord_psbl_qty": max(0, ord_psbl_qty),
            "avg_buy_price": max(0.0, avg_buy_price),
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
    holdings_by_code = _kis_holdings_by_code(kis_balance)
    guard = load_reconcile_guard(runtime_dir or Path(".")) if runtime_dir is not None else {}
    stale_confirmed = bool(guard.get("last_holdings_empty")) and int(guard.get("empty_streak") or 0) >= EMPTY_STREAK_MIN

    open_conditions = [
        schema.positions.c.env == env,
        schema.positions.c.strategy == strategy,
        schema.positions.c.qty > 0,
    ]
    if has_status:
        # Reconciliation is lifecycle-specific. Historical CLOSED/ORPHAN rows
        # must never be resurrected merely because a current broker holding for
        # the same symbol exists.
        open_conditions.append(schema.positions.c.status == "OPEN")

    with engine.connect() as conn:
        open_rows = [
            dict(row)
            for row in conn.execute(
                sa.select(
                    schema.positions.c.position_id,
                    schema.positions.c.code,
                    schema.positions.c.qty,
                    schema.positions.c.avg_buy_price,
                    schema.positions.c.total_cost,
                ).where(sa.and_(*open_conditions))
            ).mappings().all()
        ]

    # KIS exposes one aggregate holding quantity per symbol.  If DB contains
    # multiple simultaneous OPEN lifecycles for that symbol, there is no safe
    # way to assign the broker aggregate to one lifecycle without provenance.
    # Never duplicate the full KIS quantity into each row; fence the symbol and
    # let broker-truth health surface it as RED for lifecycle repair.
    open_rows_by_code: dict[str, list[dict[str, Any]]] = {}
    for row in open_rows:
        code = str((row or {}).get("code") or "").zfill(6)
        open_rows_by_code.setdefault(code, []).append(row)
    ambiguous_open_codes = {
        code for code, rows in open_rows_by_code.items() if len(rows) > 1
    }
    for code in sorted(ambiguous_open_codes):
        rows = open_rows_by_code[code]
        logger.error(
            "[POSITION_RECONCILE_ADJUST][MULTIPLE_OPEN_LIFECYCLES] code=%s count=%s position_ids=%s db_qtys=%s action=NO_AUTO_ADJUST",
            code,
            len(rows),
            [str(row.get("position_id") or "") for row in rows],
            [int(row.get("qty") or 0) for row in rows],
        )

    open_order_codes: set[str] = set()
    with engine.connect() as conn:
        order_rows = conn.execute(
            sa.select(schema.orders.c.code).where(
                sa.and_(
                    schema.orders.c.env == env,
                    schema.orders.c.strategy == strategy,
                    schema.orders.c.status.in_(["INTENT", "SUBMITTED", "ACKED", "ACCEPTED", "PARTIAL_FILLED"]),
                )
            )
        ).mappings().all()
    for row in order_rows:
        open_order_codes.add(str((row or {}).get("code") or "").zfill(6))

    if not open_rows:
        logger.info("[STALE_DB][SOFT_CLOSE][SKIP] reason=no_open_positions rows_kept=0")
        return 0

    closable_codes: list[str] = []
    # position_id is carried all the way into the UPDATE. A code-only UPDATE can
    # corrupt historical lifecycles when the same symbol has multiple rows.
    adjustable_rows: list[tuple[Any, str, int, int, float]] = []
    rows_kept = 0
    for row in open_rows:
        position_id = row.get("position_id")
        code = str((row or {}).get("code") or "").zfill(6)
        db_qty = int((row or {}).get("qty") or 0)
        db_avg = float((row or {}).get("avg_buy_price") or 0.0)

        if code in ambiguous_open_codes:
            rows_kept += 1
            logger.error(
                "[STALE_DB][CHECK] code=%s position_id=%s db_qty=%s action=KEEP reason=MULTIPLE_OPEN_LIFECYCLES",
                code,
                position_id,
                db_qty,
            )
            continue

        kis_state = holdings_by_code.get(code) or {}
        kis_qty = int(kis_state.get("hldg_qty") or 0)
        ord_psbl_qty = int(kis_state.get("ord_psbl_qty") or 0)
        canonical_kis_qty = max(kis_qty, ord_psbl_qty)
        qty_diff = canonical_kis_qty - db_qty
        if qty_diff == 0:
            rows_kept += 1
            logger.info(
                "[STALE_DB][CHECK] code=%s db_qty=%s kis_qty=%s action=KEEP reason=MATCH",
                code,
                db_qty,
                canonical_kis_qty,
            )
            continue

        # A non-zero quantity in a fresh KIS balance is positive broker evidence.
        # Never let a stale/open DB order freeze an already-observed broker fill.
        # This fixes cases such as 122630 6 -> 3 where the SELL execution changed
        # broker holdings but the execution-detail endpoint temporarily failed.
        if canonical_kis_qty > 0:
            kis_avg = float(kis_state.get("avg_buy_price") or 0.0)
            canonical_avg = kis_avg if kis_avg > 0 else db_avg
            adjustable_rows.append((position_id, code, db_qty, canonical_kis_qty, canonical_avg))
            logger.warning(
                "[STALE_DB][CHECK] code=%s position_id=%s db_qty=%s kis_qty=%s action=ADJUST reason=KIS_POSITIVE_QTY_AUTHORITATIVE qty_diff=%s open_order=%s",
                code,
                position_id,
                db_qty,
                canonical_kis_qty,
                qty_diff,
                int(code in open_order_codes),
            )
            continue

        # Zero is more dangerous because a transient/partial KIS response could
        # look like liquidation. Keep the existing multi-snapshot confirmation
        # before zeroing/closing a position.
        if code in open_order_codes:
            rows_kept += 1
            logger.warning(
                "[STALE_DB][CHECK] code=%s db_qty=%s kis_qty=%s action=KEEP reason=POSITION_QTY_ZERO_PENDING_ORDER qty_diff=%s",
                code,
                db_qty,
                canonical_kis_qty,
                qty_diff,
            )
            continue
        if stale_confirmed:
            adjustable_rows.append((position_id, code, db_qty, 0, 0.0))
            logger.warning(
                "[STALE_DB][CHECK] code=%s position_id=%s db_qty=%s kis_qty=%s action=ADJUST reason=POSITION_QTY_ZERO_CONFIRMED qty_diff=%s",
                code,
                position_id,
                db_qty,
                canonical_kis_qty,
                qty_diff,
            )
            continue
        rows_kept += 1
        logger.warning(
            "[STALE_DB][CHECK] code=%s db_qty=%s kis_qty=%s action=KEEP reason=POSITION_QTY_ZERO_AWAIT_CONFIRMATION qty_diff=%s",
            code,
            db_qty,
            canonical_kis_qty,
            qty_diff,
        )

    if adjustable_rows:
        with engine.begin() as conn:
            for position_id, code, db_qty_before, kis_qty_after, avg_buy_price in adjustable_rows:
                next_status = "OPEN" if kis_qty_after > 0 else "BROKER_RECONCILED_CLOSED"
                values: dict[str, Any] = {
                    "qty": int(kis_qty_after),
                    "avg_buy_price": float(avg_buy_price) if int(kis_qty_after) > 0 else None,
                    "total_cost": float(kis_qty_after) * float(avg_buy_price) if int(kis_qty_after) > 0 else 0.0,
                    "updated_at": sa.func.now(),
                }
                conditions = [
                    schema.positions.c.position_id == position_id,
                    schema.positions.c.env == env,
                    schema.positions.c.strategy == strategy,
                    schema.positions.c.code == code,
                    schema.positions.c.qty == db_qty_before,
                ]
                if has_status:
                    values["status"] = next_status
                    conditions.append(schema.positions.c.status == "OPEN")
                result = conn.execute(
                    sa.update(schema.positions)
                    .where(sa.and_(*conditions))
                    .values(**values)
                )
                updated = int(result.rowcount or 0)
                if updated != 1:
                    logger.error(
                        "[POSITION_RECONCILE_ADJUST][RACE_OR_IDENTITY_MISMATCH] code=%s position_id=%s expected_rows=1 updated=%s db_qty_before=%s kis_qty=%s",
                        code,
                        position_id,
                        updated,
                        db_qty_before,
                        kis_qty_after,
                    )
                    continue
                logger.warning(
                    "[POSITION_RECONCILE_ADJUST] code=%s position_id=%s db_qty_before=%s kis_qty=%s qty_after=%s total_cost_after=%s source=KIS_CANONICAL_BALANCE snapshots_confirmed=%s",
                    code,
                    position_id,
                    db_qty_before,
                    kis_qty_after,
                    kis_qty_after,
                    values["total_cost"],
                    int(stale_confirmed),
                )

    if not closable_codes:
        # A live KIS holding only protects its own row; never use it as a
        # portfolio-wide reason to hide stale DB positions.
        skip_reason = "no_rowwise_soft_close_candidates" if rows_kept or adjustable_rows else "no_soft_close_candidates"
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
