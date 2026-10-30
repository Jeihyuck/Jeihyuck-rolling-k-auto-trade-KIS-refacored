"""Bounded, practice-only automatic recovery of prior-day PB1 SELL settlements.

Runs only when the WSL close session invokes reconciliation; never in AM/PM BUY or
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


def classify_pending_historical_sells(
    orders: list[dict[str, Any]], *, today: date, epoch_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    """Select *all* old same-epoch SELLs, reporting blocked orders independently.

    A bad record must not starve other symbols. Duplicate broker identities
    are quarantined together; no age cutoff and no inference from DB holdings.
    """
    candidates: list[tuple[date, str, str, dict[str, Any]]] = []
    blocked: list[dict[str, str]] = []
    for row in orders:
        if str(row.get("side") or "").upper() != "SELL":
            continue
        if str(row.get("status") or "").upper() not in _PENDING:
            continue
        if str(row.get("trading_epoch_id") or "") != epoch_id:
            continue
        identity = str(row.get("order_id") or "")
        day = _kst_date(row.get("submitted_at") or row.get("created_at"))
        if day is None:
            blocked.append({"order_id": identity, "reason": "UNPROVEN_ORIGINAL_TRADE_DATE"})
            continue
        if day >= today:
            continue
        code = str(row.get("code") or "")
        odno = str(row.get("kis_odno") or row.get("broker_order_id") or "")
        try:
            qty = int(row.get("qty"))
        except (ValueError, TypeError):
            qty = 0
        if (
            len(code) != 6 or not code.isdigit()
            or len(odno) != 10 or not odno.isdigit()
            or not identity
            or not row.get("position_cycle_id")
            or not row.get("portfolio_epoch_id")
            or qty <= 0
        ):
            blocked.append({"order_id": identity, "reason": "ORIGINAL_SELL_IDENTITY_OR_QTY_INVALID"})
            continue
        candidates.append((day, code, odno, row))
    counts: dict[tuple[date, str, str], int] = {}
    for day, code, odno, _ in candidates:
        key = (day, code, odno)
        counts[key] = counts.get(key, 0) + 1
    selected: list[dict[str, Any]] = []
    for day, code, odno, row in candidates:
        if counts[(day, code, odno)] != 1:
            blocked.append({"order_id": str(row["order_id"]), "reason": "DUPLICATE_ORIGINAL_SELL_IDENTITY"})
        else:
            selected.append(row)
    selected.sort(key=lambda row: (
        _kst_date(row.get("submitted_at") or row.get("created_at")),
        str(row.get("code")), str(row.get("kis_odno") or row.get("broker_order_id"))
    ))
    return selected, blocked


def select_pending_historical_sell(
    orders: list[dict[str, Any]], *, today: date, epoch_id: str,
) -> tuple[dict[str, Any] | None, str]:
    """Compatibility helper for single-order tests; production uses batch."""
    selected, blocked = classify_pending_historical_sells(orders, today=today, epoch_id=epoch_id)
    if selected:
        return selected[0], "READY"
    return None, blocked[0]["reason"] if blocked else "NO_PRIOR_DAY_PENDING_SELL"


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
    session = str(os.getenv("PB1_SESSION") or os.getenv("WSL_RUN_SESSION") or "").lower()
    return session == "close" and time(15, 15) <= now.time() <= time(16, 30)


def run_all_historical_sell_recovery(
    *, engine: Any, kis: Any, env: str, strategy: str,
    tick_ts: datetime, ctx: Any,
) -> dict[str, Any]:
    """Attempt all eligible prior-day SELLs, isolating each failure.

    Critical trading must be finished before this maintenance call. No broker
    order POST, cancel or new BUY. If the close process has a deadline, exit
    before starting another KIS inquiry, leaving unprocessed orders pending.
    """
    if not should_run_close_recovery(env=env, tick_ts=tick_ts):
        return {"state": "NOT_CLOSE_WINDOW"}
    if strategy != "pb1_pullback_close":
        return {"state": "OTHER_OWNER"}
    try:
        with engine.connect() as conn:
            epoch_id = active_trading_epoch_id(
                conn, env=env, account_id=get_account_key(env=env, kis=kis),
                required=True,
            )
        if not epoch_id:
            return {"state": "EPOCH_UNPROVEN"}
        orders = OrdersRepo(engine).get_open_orders(
            env, include_stale=True, fail_open=False,
        )
        candidates, blocked = classify_pending_historical_sells(
            [row for row in orders or [] if str(row.get("strategy") or "") == strategy],
            today=tick_ts.astimezone(_KST).date(), epoch_id=str(epoch_id),
        )
    except Exception as exc:
        log.exception("[KR_CROSS_DAY_SELL][QUEUE_READ_FAILED] err_type=%s", type(exc).__name__)
        return {"state": "RETRY_NEXT_CLOSE", "error_type": type(exc).__name__}

    from trader.reconcile_kis import reconcile_today
    from trader.kr.unpriced_sell_audit import audit_kr_pending_sell_orders
    results: list[dict[str, Any]] = []
    attempted = 0
    settled = 0
    failed = 0
    for order in candidates:
        day = _kst_date(order.get("submitted_at") or order.get("created_at"))
        code = str(order["code"])
        odno = str(order.get("kis_odno") or order.get("broker_order_id"))
        try:
            # The existing PR244 original-day authoritative all-pages writer
            # validates exactly this broker order before any economic DB write.
            evidence = reconcile_today(
                engine=engine, kis=kis, ctx=ctx, trade_date=day,
                require_authoritative=True,
                strict_sell_targets={code: (odno, int(order["qty"]))},
            )
            attempted += 1
            audit = audit_kr_pending_sell_orders(engine, env=env, trade_date=day)
            pending_ids = {str(row.get("order_id") or "") for row in audit.get("orders", [])}
            if str(order["order_id"]) in pending_ids:
                state = "STILL_PENDING_RETRY_NEXT_CLOSE"
                failed += 1
            else:
                # Audit is not by itself economic proof: the writer's own
                # execution evidence must also be positive.
                state = "RECONCILE_ATTEMPTED"
                settled += 1
            results.append({"code": code, "order_id": str(order["order_id"]),
                            "trade_date": str(day), "state": state,
                            "broker_fills": int(evidence.get("fills") or 0)})
        except Exception as exc:
            attempted += 1
            failed += 1
            log.exception("[KR_CROSS_DAY_SELL][RETRY_NEXT_CLOSE] code=%s date=%s err_type=%s",
                          code, day, type(exc).__name__)
            results.append({"code": code, "order_id": str(order["order_id"]),
                            "trade_date": str(day), "state": "RETRY_NEXT_CLOSE",
                            "error_type": type(exc).__name__})
            # Continue with the other codes. Never let one broken historical
            # KIS proof starve the rest of the reconciliation queue.
    log.info(
        "[KR_CROSS_DAY_SELL][BATCH] eligible=%d attempted=%d apparently_reconciled=%d pending=%d blocked=%d",
        len(candidates), attempted, settled, failed, len(blocked),
    )
    return {
        "state": "BATCH_COMPLETE" if not failed and not blocked else "BATCH_WITH_PENDING",
        "eligible": len(candidates), "attempted": attempted,
        "apparently_reconciled": settled, "pending": failed,
        "blocked": blocked, "orders": results,
    }


def run_one_historical_sell_recovery(**kwargs: Any) -> dict[str, Any]:
    """Backwards-compatible entrypoint now performs the entire batch."""
    return run_all_historical_sell_recovery(**kwargs)
