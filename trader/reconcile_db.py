from __future__ import annotations

import logging
from datetime import datetime
import json
from pathlib import Path

import sqlalchemy as sa

from trader.runtime_paths import runtime_path

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


def _safe_int(value) -> int:
    try:
        return int(float(str(value or "0").replace(",", "")))
    except Exception:
        return 0


def _kis_holding_maps(kis_balance: dict | None) -> tuple[bool, dict[str, int], dict[str, int]]:
    if not isinstance(kis_balance, dict):
        return False, {}, {}
    rows = kis_balance.get("output1") or []
    if not isinstance(rows, list):
        return False, {}, {}
    qty_by_code: dict[str, int] = {}
    ord_psbl_by_code: dict[str, int] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        code = str(row.get("pdno") or row.get("code") or row.get("stck_shrn_iscd") or "").zfill(6)
        if not code.strip("0"):
            continue
        qty_by_code[code] = _safe_int(row.get("hldg_qty") or row.get("qty"))
        ord_psbl_by_code[code] = _safe_int(row.get("ord_psbl_qty") or row.get("ord_psbl_qty1"))
    return True, qty_by_code, ord_psbl_by_code


def close_stale_positions(
    *,
    engine,
    env: str,
    strategy: str,
    reason: str,
    ts: datetime,
    kis_balance: dict | None = None,
    stale_confirmed: bool = False,
    confirmed_codes: list[str] | None = None,
) -> int:
    balance_ok, kis_qty_by_code, kis_ord_psbl_by_code = _kis_holding_maps(kis_balance)
    if not balance_ok:
        logger.warning("[STALE_DB][SOFT_CLOSE][SKIP] reason=kis_balance_unavailable rows_kept=unknown")
        return 0

    confirmed = {str(c or "").zfill(6) for c in (confirmed_codes or []) if str(c or "").strip()}
    inspector = sa.inspect(engine)
    columns = {col["name"] for col in inspector.get_columns("positions")}
    has_status = "status" in columns
    has_reason = "closed_reason" in columns
    has_closed_ts = "closed_ts" in columns
    status_value = "SOFT_CLOSED"
    if reason in {"stale_db_holdings_empty", "STALE_DB_BUT_KIS_EMPTY"}:
        status_value = "ORPHAN"

    with engine.begin() as conn:
        rows = [dict(r) for r in conn.execute(
            sa.text("SELECT code, qty FROM positions WHERE env = :env AND strategy = :strategy AND qty > 0"),
            {"env": env, "strategy": strategy},
        ).mappings().all()]

    close_codes: list[str] = []
    kept = 0
    for row in rows:
        code = str(row.get("code") or "").zfill(6)
        db_qty = _safe_int(row.get("qty"))
        kis_qty = kis_qty_by_code.get(code, 0)
        ord_psbl_qty = kis_ord_psbl_by_code.get(code, 0)
        if kis_qty > 0:
            kept += 1
            logger.info("[STALE_DB][CHECK] code=%s db_qty=%s kis_qty=%s action=KEEP reason=KIS_HOLDING_EXISTS", code, db_qty, kis_qty)
            continue
        if ord_psbl_qty > 0:
            kept += 1
            logger.info("[STALE_DB][CHECK] code=%s db_qty=%s kis_qty=%s ord_psbl_qty=%s action=KEEP reason=KIS_ORDERABLE_QTY_EXISTS", code, db_qty, kis_qty, ord_psbl_qty)
            continue
        if not (stale_confirmed or code in confirmed):
            kept += 1
            logger.info("[STALE_DB][CHECK] code=%s db_qty=%s kis_qty=0 action=KEEP reason=STALE_NOT_CONFIRMED", code, db_qty)
            continue
        close_codes.append(code)

    if kept:
        logger.warning("[STALE_DB][SOFT_CLOSE][SKIP] reason=kis_has_holdings rows_kept=%s", kept)
    if not close_codes:
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
            params = {"status": status_value, "env": env, "strategy": strategy, "codes": tuple(close_codes)}
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
                WHERE env = :env AND strategy = :strategy AND qty > 0 AND code IN :codes
                """
            ).bindparams(sa.bindparam("codes", expanding=True))
            result = conn.execute(stmt, params)
        else:
            stmt = sa.text(
                """
                UPDATE positions
                SET qty = 0, avg_buy_price = NULL, total_cost = 0.0, updated_at = CURRENT_TIMESTAMP
                WHERE env = :env AND strategy = :strategy AND qty > 0 AND code IN :codes
                """
            ).bindparams(sa.bindparam("codes", expanding=True))
            result = conn.execute(stmt, {"env": env, "strategy": strategy, "codes": tuple(close_codes)})
    count = int(result.rowcount or 0)
    logger.warning("[STALE_DB][SOFT_CLOSE] env=%s strategy=%s rows=%s status=%s reason=%s codes=%s", env, strategy, count, status_value, reason, close_codes)
    return count
