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

    open_order_codes: set[str] = set()
    if inspector.has_table("orders"):
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
    # code, db_before, kis_after, avg_price, reconcile_reason, pending_order
    adjustable_rows: list[tuple[str, int, int, float, str, bool]] = []
    sell_fill_code_set = {
        str(code or "").zfill(6)
        for code in (sell_fill_codes or [])
        if str(code or "").strip()
    }
    rows_kept = 0
    for row in open_rows:
        code = str((row or {}).get("code") or "").zfill(6)
        db_qty = int((row or {}).get("qty") or 0)
        kis_present = code in holdings_by_code
        kis_state = holdings_by_code.get(code) or {}
        # hldg_qty is the broker position quantity. ord_psbl_qty is only the
        # currently sellable subset and must never replace position quantity.
        canonical_kis_qty = int(kis_state.get("hldg_qty") or 0)
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

        pending_order = code in open_order_codes
        if kis_present:
            # A row that is actually present in a successful KIS balance is
            # authoritative for live position quantity, even while an order is
            # still ACKED.  Reconcile quantity only; TP/stage metadata remains
            # fill-driven and is deliberately untouched here.
            adjustable_rows.append((
                code,
                db_qty,
                canonical_kis_qty,
                float(kis_state.get("avg_buy_price") or 0.0),
                "KIS_PRESENT_AUTHORITATIVE",
                pending_order,
            ))
            logger.warning(
                "[STALE_DB][CHECK] code=%s db_qty=%s kis_qty=%s action=ADJUST "
                "reason=KIS_PRESENT_AUTHORITATIVE qty_diff=%s pending_order=%s stage_update=0",
                code,
                db_qty,
                canonical_kis_qty,
                qty_diff,
                int(pending_order),
            )
            continue

        # Absence from output1 means zero only when corroborated.  A confirmed
        # SELL fill is direct evidence; otherwise retain the existing empty
        # streak guard to protect against truncated/failed balance responses.
        if code in sell_fill_code_set:
            adjustable_rows.append((
                code, db_qty, 0, 0.0, "CONFIRMED_SELL_FILL_ZERO", pending_order
            ))
            logger.warning(
                "[STALE_DB][CHECK] code=%s db_qty=%s kis_qty=0 action=ADJUST "
                "reason=CONFIRMED_SELL_FILL_ZERO pending_order=%s",
                code,
                db_qty,
                int(pending_order),
            )
            continue
        if stale_confirmed:
            adjustable_rows.append((
                code, db_qty, 0, 0.0, "EMPTY_STREAK_CONFIRMED_ZERO", pending_order
            ))
            logger.warning(
                "[STALE_DB][CHECK] code=%s db_qty=%s kis_qty=0 action=ADJUST "
                "reason=EMPTY_STREAK_CONFIRMED_ZERO qty_diff=%s",
                code,
                db_qty,
                -db_qty,
            )
            continue

        rows_kept += 1
        logger.warning(
            "[STALE_DB][CHECK] code=%s db_qty=%s kis_qty=0 action=KEEP "
            "reason=POSITION_QTY_MISMATCH_PENDING qty_diff=%s pending_order=%s",
            code,
            db_qty,
            -db_qty,
            int(pending_order),
        )
        continue

    adjusted_to_zero_count = sum(1 for row in adjustable_rows if int(row[2]) == 0)
    if adjustable_rows:
        with engine.begin() as conn:
            for code, db_qty_before, kis_qty_after, avg_buy_price, reconcile_reason, pending_order in adjustable_rows:
                next_status = "OPEN" if kis_qty_after > 0 else "BROKER_RECONCILED_CLOSED"
                next_total_cost = (
                    float(avg_buy_price or 0.0) * int(kis_qty_after)
                    if int(kis_qty_after) > 0
                    else 0.0
                )
                conn.execute(
                    sa.text(
                        """
                        UPDATE positions
                        SET qty = :qty,
                            avg_buy_price = :avg_buy_price,
                            total_cost = :total_cost,
                            status = :status,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE env = :env AND strategy = :strategy AND code = :code
                        """
                    ),
                    {
                        "qty": int(kis_qty_after),
                        "avg_buy_price": float(avg_buy_price or 0.0) if int(kis_qty_after) > 0 else None,
                        "total_cost": next_total_cost,
                        "status": next_status,
                        "env": env,
                        "strategy": strategy,
                        "code": code,
                    },
                )
                logger.warning(
                    "[POSITION_RECONCILE_ADJUST] code=%s db_qty_before=%s kis_qty=%s "
                    "qty_after=%s source=KIS_CANONICAL_BALANCE reason=%s pending_order=%s "
                    "stage_update=0",
                    code,
                    db_qty_before,
                    kis_qty_after,
                    kis_qty_after,
                    reconcile_reason,
                    int(pending_order),
                )

    if not closable_codes:
        # A live KIS holding only protects its own row; never use it as a
        # portfolio-wide reason to hide stale DB positions.  Quantity
        # reconciliations to zero are real broker-confirmed closes and retain
        # the historical return contract of this function.
        skip_reason = "no_rowwise_soft_close_candidates" if rows_kept or adjustable_rows else "no_soft_close_candidates"
        logger.warning(
            "[STALE_DB][SOFT_CLOSE][SKIP] reason=%s rows_kept=%s adjusted_to_zero=%s",
            skip_reason,
            rows_kept,
            adjusted_to_zero_count,
        )
        return int(adjusted_to_zero_count)

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
