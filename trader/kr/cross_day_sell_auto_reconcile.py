"""Bounded, practice-only automatic recovery of prior-day PB1 SELL settlements.

Runs only in the existing close reconciliation lane, never in AM/PM BUY or
protective-SELL preparation. Reuses the PR244 authoritative original-day
KIS all-pages, strict ODNO/date/code/side/quantity path. No new broker orders.

A failed/ambiguous proof leaves the durable SUBMITTED/ACKED record untouched,
so the next close run tries again. No symbol-only or holdings-delta settlement.
"""
from __future__ import annotations

from datetime import date, datetime, time
import logging
import os
from typing import Any
from zoneinfo import ZoneInfo

from trader.account_state import get_account_key
from trader.db.repos import OrdersRepo
from trader.db.trading_epoch import active_trading_epoch_id

log = logging.getLogger(__name__)
_KST = ZoneInfo("Asia/Seoul")
_PENDING = frozenset({
    "SUBMITTED", "ACKED", "ACCEPTED", "PARTIAL_FILLED",
    "UNRESOLVED_ACK", "FILLED_QTY_CONFIRMED_PRICE_UNRESOLVED",
})


def _kst_date(raw: Any) -> date | None:
    if isinstance(raw, datetime):
        return raw.astimezone(_KST).date() if raw.tzinfo else None
    if isinstance(raw, date):
        return raw
    try:
        value = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        return value.astimezone(_KST).date() if value.tzinfo else None
    except (ValueError, TypeError):
        return None


def select_pending_historical_sell(
    orders: list[dict[str, Any]], *, today: date, epoch_id: str,
) -> tuple[dict[str, Any] | None, str]:
    """Oldest fully identified same-epoch pending SELL; no age cutoff.

    A missing identity or conflicting same-broker-ID row cannot be auto-repaired.
    The caller must use the *original order day* and broker's immutable ODNO.
    """
    pending = []
    for row in orders:
        if str(row.get("side") or "").upper() != "SELL":
            continue
        if str(row.get("status") or "").upper() not in _PENDING:
            continue
        day = _kst_date(row.get("submitted_at") or row.get("created_at"))
        if day is None:
            return None, "UNPROVEN_ORIGINAL_TRADE_DATE"
        if day >= today:
            continue
        if str(row.get("trading_epoch_id") or "") != epoch_id:
            continue  # a different account/epoch must never be queried as this account
        code = str(row.get("code") or "")
        odno = str(row.get("kis_odno") or row.get("broker_order_id") or "")
        if (
            len(code) != 6 or not code.isdigit()
            or len(odno) != 10 or not odno.isdigit()
            or not row.get("order_id")
            or not row.get("position_cycle_id")
            or not row.get("portfolio_epoch_id")
        ):
            return None, "MISSING_ORIGINAL_SELL_IDENTITY"
        pending.append((day, code, odno, row))
    if not pending:
        return None, "NO_PRIOR_DAY_PENDING_SELL"
    pending.sort(key=lambda x: (x[0], x[1], x[2]))
    day, code, odno, selected = pending[0]
    if sum(1 for d, c, n, _ in pending if d == day and c == code and n == odno) != 1:
        return None, "DUPLICATE_ORIGINAL_SELL_IDENTITY"
    qty = selected.get("qty")
    try:
        qty = int(qty)
    except (TypeError, ValueError):
        return None, "ORIGINAL_SELL_QTY_INVALID"
    if qty <= 0:
        return None, "ORIGINAL_SELL_QTY_INVALID"
    return selected, "READY"


def should_run_close_recovery(*, env: str, tick_ts: datetime, enabled: str | None = None) -> bool:
    """Never perform historical KIS I/O during an active trading session."""
    if env != "practice":
        return False
    value = enabled if enabled is not None else os.getenv("KR_CROSS_DAY_SELL_AUTO_RECONCILE", "1")
    if str(value).strip().lower() not in {"1", "true", "yes", "on"}:
        return False
    if tick_ts.tzinfo is None:
        return False
    now = tick_ts.astimezone(_KST)
    return time(15, 35) <= now.time() <= time(20, 0)


def run_one_historical_sell_recovery(
    *, engine: Any, kis: Any, env: str, strategy: str,
    tick_ts: datetime, ctx: Any,
) -> dict[str, Any]:
    """One original trade date per close invocation; retry automatically later."""
    if not should_run_close_recovery(env=env, tick_ts=tick_ts):
        return {"state": "NOT_CLOSE_WINDOW"}
    if strategy != "pb1_pullback_close":
        return {"state": "OTHER_OWNER"}
    try:
        from trader.db.repos import OrdersRepo
        with engine.connect() as conn:
            epoch_id = active_trading_epoch_id(
                conn, env=env, account_id=get_account_key(env=env, kis=kis),
                required=True,
            )
        if not epoch_id:
            return {"state": "EPOCH_UNPROVEN"}
        # Strict read: a DB timeout must NOT become an empty retry queue.
        orders = OrdersRepo(engine).get_open_orders(
            env, include_stale=True, fail_open=False,
        )
        rows = [o for o in orders or [] if str(o.get("strategy") or "") == strategy]
        selected, reason = select_pending_historical_sell(
            rows, today=tick_ts.astimezone(_KST).date(), epoch_id=str(epoch_id),
        )
        if not selected:
            return {"state": reason}
        day = _kst_date(selected.get("submitted_at") or selected.get("created_at"))
        code = str(selected["code"])
        odno = str(selected.get("kis_odno") or selected.get("broker_order_id"))
        qty = int(selected["qty"])
        # PR244: exhaustive original-day broker evidence is checked BEFORE a
        # single DB mutation, and the existing writer is idempotent.
        from trader.reconcile_kis import reconcile_today
        result = reconcile_today(
            engine=engine, kis=kis, ctx=ctx, trade_date=day,
            require_authoritative=True,
            strict_sell_targets={code: (odno, qty)},
        )
        # Query again: a reconciler return is not proof of economic settlement.
        from trader.kr.unpriced_sell_audit import audit_kr_pending_sell_orders
        audit = audit_kr_pending_sell_orders(engine, env=env, trade_date=day)
        unresolved = [
            entry for entry in audit.get("orders", [])
            if str(entry.get("code") or "") == code
            and str(entry.get("kis_odno") or entry.get("broker_order_id") or "") in {"", odno}
        ]
        state = "RECONCILE_ATTEMPTED_AUDIT_PENDING" if unresolved else "RECONCILE_ATTEMPTED"
        log.warning(
            "[KR_CROSS_DAY_SELL][%s] code=%s original_day=%s order_no=%s broker_orders=%s broker_fills=%s",
            state, code, day, odno, result.get("orders"), result.get("fills"),
        )
        return {"state": state, "code": code, "original_day": str(day),
                "broker_orders": int(result.get("orders") or 0),
                "broker_fills": int(result.get("fills") or 0)}
    except Exception as exc:
        # Keep original pending order and duplicate-order fences intact.
        log.exception("[KR_CROSS_DAY_SELL][RETRY_NEXT_CLOSE] error_type=%s", type(exc).__name__)
        return {"state": "RETRY_NEXT_CLOSE", "error_type": type(exc).__name__}
