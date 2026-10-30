"""Bounded, practice-only automatic recovery of prior-day PB1 SELL settlements.

Runs only after the WSL close trading process returns; never inside AM/PM BUY,
CLOSE SELL or synchronous broker reconciliation. Reuses the PR244 authoritative original-day
KIS all-pages, strict ODNO/date/code/side/quantity path. No new broker orders.

A failed/ambiguous proof leaves the durable SUBMITTED/ACKED record untouched,
so the next close run tries again. No symbol-only or holdings-delta settlement.
"""
from __future__ import annotations

from datetime import date, datetime, time
from time import monotonic
import sqlalchemy as sa

from trader.db.schema import schema_for_engine
import logging
import os
import json
import hashlib
import bisect
from pathlib import Path
from uuid import uuid4
from typing import Any
from zoneinfo import ZoneInfo

from trader.account_state import get_account_key
from trader.db.trading_epoch import active_trading_epoch_id

log = logging.getLogger(__name__)
_KST = ZoneInfo("Asia/Seoul")
_PENDING = frozenset({
    "SUBMITTED", "ACKED", "ACCEPTED", "PARTIAL_FILLED",
    "UNRESOLVED_ACK", "RECONCILE_PENDING", "ACK_DB_FAILED_RECONCILE_REQUIRED",
    "PARTIALLY_FILLED", "FILLED_QTY_CONFIRMED_PRICE_UNRESOLVED",
    "RECONCILED",
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
        if str(row.get("status") or "").upper() not in _PENDING and not (
            str(row.get("status") or "").upper() in {"FILLED", "RECONCILED"}
            and row.get("_economic_repair_required") is True
        ):
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
        if not odno:
            ack = _as_obj(row.get("response_json"))
            saved_output = _as_obj(ack.get("output"))
            if str(ack.get("rt_cd") or "") == "0":
                odno = str(saved_output.get("ODNO") or "").strip()
                if len(odno) == 10 and odno.isdigit():
                    row = {**row, "_ack_only_odno": odno}
                else:
                    odno = ""
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
    return session == "close" and time(15, 15) <= now.time() <= time(23, 59, 59)


def _as_obj(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except (ValueError, TypeError):
            return {}
    return {}


def _verify_original_sell_economics(
    *, engine: Any, source: dict[str, Any],
) -> tuple[bool, str]:
    """Require durable native KIS fill AND correct original lifecycle.

    This is deliberately stricter than a terminal order status or a broker
    ACK. It prevents a FILLED-but-no-fill DB order from disappearing from the
    retry queue, and detects a stale OPEN position after forced day liquidation.
    """
    from trader.db.schema import uuid_value_for_url
    schema = schema_for_engine(engine)
    env = str(source["env"])
    strategy = str(source["strategy"])
    code = str(source["code"])
    cycle = str(source["position_cycle_id"])
    portfolio = str(source["portfolio_epoch_id"])
    original_id = uuid_value_for_url(str(engine.url), str(source["order_id"]))
    with engine.connect() as conn:
        order = conn.execute(sa.select(schema.orders).where(sa.and_(
            schema.orders.c.order_id == original_id,
            schema.orders.c.env == env,
            schema.orders.c.strategy == strategy,
            schema.orders.c.code == code,
            schema.orders.c.side == "SELL",
            schema.orders.c.position_cycle_id == cycle,
            schema.orders.c.portfolio_epoch_id == portfolio,
            schema.orders.c.trading_epoch_id == str(source["trading_epoch_id"]),
        )).limit(1)).mappings().first()
        if not order or str(order["status"]).upper() != "FILLED":
            return False, "ORIGINAL_ORDER_NOT_FILLED"
        order = dict(order)
        price_evidence = _as_obj(order.get("response_json"))
        submitted = int(order.get("qty") or 0)
        if (int(price_evidence.get("confirmed_fill_qty") or 0) != submitted
                or float(price_evidence.get("confirmed_fill_notional") or 0) <= 0
                or float(price_evidence.get("confirmed_fill_price") or 0) <= 0):
            return False, "KIS_NATIVE_ORDER_ECONOMICS_NOT_DURABLE"
        fills = conn.execute(sa.select(schema.fills).where(sa.and_(
            schema.fills.c.env == env,
            schema.fills.c.order_id == original_id,
            schema.fills.c.side == "SELL",
            schema.fills.c.position_cycle_id == cycle,
            schema.fills.c.portfolio_epoch_id == portfolio,
        ))).mappings().all()
        cumulative: list[int] = []
        individual = 0
        for fill in fills:
            meta = _as_obj(fill.get("fill_meta_json"))
            kind = str(meta.get("evidence_kind") or "").upper()
            qty = int(fill.get("qty") or 0)
            price = float(fill.get("price") or 0)
            if qty <= 0 or price <= 0:
                continue
            if kind == "KIS_ORDER_CUMULATIVE_EXECUTION":
                cumulative.append(qty)
            elif kind == "KIS_INDIVIDUAL_EXECUTION":
                individual += qty
        native_qty = max(max(cumulative, default=0), individual)
        if native_qty != submitted:
            return False, "NATIVE_PRICED_SELL_FILL_GAP"
        positions = conn.execute(sa.select(schema.positions).where(sa.and_(
            schema.positions.c.env == env,
            schema.positions.c.strategy == strategy,
            schema.positions.c.code == code,
            schema.positions.c.position_cycle_id == cycle,
            schema.positions.c.portfolio_epoch_id == portfolio,
            schema.positions.c.trading_epoch_id == str(source["trading_epoch_id"]),
        ))).mappings().all()
    if len(positions) != 1:
        return False, "ORIGINAL_POSITION_CYCLE_NOT_UNIQUE"
    pos = dict(positions[0])
    if str(pos.get("closed_reason") or "").upper() == "BROKER_ZERO_ECONOMIC_SETTLEMENT_PENDING":
        return False, "MANUAL_ECONOMICS_NOT_SETTLED"
    request = _as_obj(order.get("request_json"))
    full_close = bool(request.get("force_eod_close")) or str(request.get("reason") or "") == "KR_CLOSE_DAY_FORCE_EOD"
    if full_close:
        if int(pos.get("qty") or 0) != 0 or str(pos.get("status") or "").upper() != "CLOSED":
            return False, "ORIGINAL_POSITION_NOT_CLOSED"
    else:
        # Partial SELL must have an idempotent application watermark rather
        # than merely a priced order. Never infer decrement from other trades.
        applied = _as_obj(price_evidence.get("broker_truth_sell_applied"))
        if (int(applied.get("qty") or 0) < submitted
                and not (int(pos.get("qty") or 0) == 0
                         and str(pos.get("status") or "").upper() == "CLOSED")):
            return False, "SELL_POSITION_APPLY_UNPROVEN"
    return True, "ECONOMIC_SETTLED"


def find_strict_original_ack_source(
    *, engine: Any, env: str, strategy: str, code: str,
    odno: str, trade_date: date, requested_qty: int,
) -> dict[str, Any] | None:
    """Recover a missing DB broker-ID field from an immutable successful ACK.

    This lookup is exclusively for *already verified* original-day KIS SELL
    evidence in reconcile_today(strict_sell_targets=...). The durable
    response's output.ODNO is identity evidence, NEVER execution evidence.
    Only a unique same-account/epoch/cycle original SELL can be attributed.
    """
    if env != "practice" or len(code) != 6 or not code.isdigit():
        return None
    if len(odno) != 10 or not odno.isdigit() or requested_qty <= 0:
        return None
    with engine.connect() as conn:
        epoch_id = active_trading_epoch_id(
            conn, env=env, account_id=get_account_key(env=env),
            required=True,
        )
        schema = schema_for_engine(engine)
        rows = conn.execute(sa.select(schema.orders).where(sa.and_(
            schema.orders.c.env == env,
            schema.orders.c.strategy == strategy,
            schema.orders.c.side == "SELL",
            schema.orders.c.code == code,
            schema.orders.c.qty == requested_qty,
            schema.orders.c.trading_epoch_id == str(epoch_id),
        ))).mappings().all()
    verified = []
    for raw in rows:
        row = dict(raw)
        if _kst_date(row.get("submitted_at") or row.get("created_at")) != trade_date:
            continue
        if not row.get("position_cycle_id") or not row.get("portfolio_epoch_id"):
            continue
        durable = _as_obj(row.get("response_json"))
        output = _as_obj(durable.get("output"))
        saved_odno = str(output.get("ODNO") or output.get("odno") or "").strip()
        if str(durable.get("rt_cd") or "") != "0" or saved_odno.zfill(10) != odno:
            continue
        # Do not hijack a different broker order already linked to this row.
        stored = str(row.get("kis_odno") or row.get("broker_order_id") or "").strip()
        if stored and stored.zfill(10) != odno:
            continue
        verified.append(row)
    if len(verified) != 1:
        log.error(
            "[KR_CROSS_DAY_SELL][ACK_LOOKUP_BLOCK] code=%s date=%s matching=%s",
            code, trade_date, len(verified),
        )
        return None
    return verified[0]


def _load_pending_orders(*, engine: Any, env: str, strategy: str, epoch_id: str) -> list[dict[str, Any]]:
    """Read ALL statuses from authoritative DB, not the recent/open-order cache.

    The normal get_open_orders API can scope old orders and terminal-like
    quantity-confirmed SELLs out; that cannot serve as an unbounded recovery queue.
    """
    schema = schema_for_engine(engine)
    with engine.connect() as conn:
        rows = conn.execute(
            sa.select(schema.orders).where(sa.and_(
                schema.orders.c.env == env,
                schema.orders.c.strategy == strategy,
                schema.orders.c.side == "SELL",
                schema.orders.c.status.in_(sorted(_PENDING | {"FILLED"})),
                schema.orders.c.trading_epoch_id == epoch_id,
            )).order_by(schema.orders.c.created_at.asc())
        ).mappings().all()
    candidate_rows = []
    for raw in rows:
        row = dict(raw)
        if str(row.get("status") or "").upper() in {"FILLED", "RECONCILED"}:
            try:
                done, _ = _verify_original_sell_economics(engine=engine, source=row)
            except Exception:
                done = False
            if done:
                continue
            row["_economic_repair_required"] = True
        candidate_rows.append(row)
    return candidate_rows


def _cursor_path(*, account_key: str, epoch_id: str) -> Path:
    """Private, account+epoch-scoped cursor; no account string in filenames."""
    from trader.runtime_paths import repo_root
    identity = hashlib.sha256(
        (str(account_key) + "\\0" + str(epoch_id)).encode("utf-8")
    ).hexdigest()[:20]
    return repo_root() / "runtime" / "state" / ("kr_sell_recovery_" + identity + ".json")


def _order_sort_key(order: dict[str, Any]) -> str:
    day = _kst_date(order.get("submitted_at") or order.get("created_at"))
    return "|".join([
        day.isoformat() if day else "",
        str(order.get("code") or ""),
        str(order.get("kis_odno") or order.get("broker_order_id") or order.get("_ack_only_odno") or ""),
        str(order.get("order_id") or ""),
    ])


def _resume_after_cursor(
    orders: list[dict[str, Any]], *, cursor: Path,
) -> list[dict[str, Any]]:
    """Resume after last attempted order, even if that order was settled."""
    if not orders:
        return []
    ordered = sorted(orders, key=_order_sort_key)
    try:
        state = json.loads(cursor.read_text(encoding="utf-8"))
        last = str(state.get("last_sort_key") or "")
    except (OSError, ValueError, TypeError, AttributeError):
        return ordered
    if not last:
        return ordered
    keys = [_order_sort_key(order) for order in ordered]
    at = bisect.bisect_right(keys, last) % len(ordered)
    return ordered[at:] + ordered[:at]


def _save_cursor_after(*, cursor: Path, order: dict[str, Any]) -> None:
    """Atomic local checkpoint. Failure cannot confirm or release an order."""
    cursor.parent.mkdir(parents=True, exist_ok=True)
    temp = cursor.with_name(cursor.name + "." + uuid4().hex + ".tmp")
    try:
        with temp.open("x", encoding="utf-8") as file:
            os.chmod(temp, 0o600)
            json.dump({"last_sort_key": _order_sort_key(order)}, file)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temp, cursor)
    finally:
        try:
            temp.unlink(missing_ok=True)
        except OSError:
            pass


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
        account_key = get_account_key(env=env, kis=kis)
        with engine.connect() as conn:
            epoch_id = active_trading_epoch_id(
                conn, env=env, account_id=account_key,
                required=True,
            )
        if not epoch_id:
            return {"state": "EPOCH_UNPROVEN"}
        orders = _load_pending_orders(engine=engine, env=env, strategy=strategy, epoch_id=str(epoch_id))
        candidates, blocked = classify_pending_historical_sells(
            orders,
            today=tick_ts.astimezone(_KST).date(), epoch_id=str(epoch_id),
        )
    except Exception as exc:
        log.exception("[KR_CROSS_DAY_SELL][QUEUE_READ_FAILED] err_type=%s", type(exc).__name__)
        return {"state": "RETRY_NEXT_CLOSE", "error_type": type(exc).__name__}

    from trader.reconcile_kis import reconcile_today
        cursor = _cursor_path(account_key=account_key, epoch_id=str(epoch_id))
    candidates = _resume_after_cursor(candidates, cursor=cursor)
    try:
        budget_sec = max(60, min(900, int(os.getenv("KR_CROSS_DAY_SELL_MAX_SECONDS", "420"))))
    except (ValueError, TypeError):
        budget_sec = 420
    deadline = monotonic() + budget_sec
    results: list[dict[str, Any]] = []
    attempted = 0
    settled = 0
    failed = 0
    for order in candidates:
        if monotonic() >= deadline - 25:
            break  # next CLOSE retries remaining original orders; no order state change
        day = _kst_date(order.get("submitted_at") or order.get("created_at"))
        code = str(order["code"])
        odno = str(order.get("kis_odno") or order.get("broker_order_id") or order.get("_ack_only_odno"))
        try:
            # The existing PR244 original-day authoritative all-pages writer
            # validates exactly this broker order before any economic DB write.
            evidence = reconcile_today(
                engine=engine, kis=kis, ctx=ctx, trade_date=day,
                require_authoritative=True,
                strict_sell_targets={code: (odno, int(order["qty"]))},
                strict_sell_order_ids={code: str(order["order_id"])},
                strict_sell_epoch_id=str(epoch_id),
            )
            attempted += 1
            done, state = _verify_original_sell_economics(engine=engine, source=order)
            if done:
                settled += 1
            else:
                failed += 1
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
        finally:
            try:
                _save_cursor_after(cursor=cursor, order=order)
            except OSError as exc:
                log.warning("[KR_CROSS_DAY_SELL][CURSOR_WRITE_FAILED] type=%s", type(exc).__name__)
    deferred = len(candidates) - attempted
    log.info(
        "[KR_CROSS_DAY_SELL][BATCH] eligible=%d attempted=%d apparently_reconciled=%d pending=%d deferred=%d blocked=%d",
        len(candidates), attempted, settled, failed, deferred, len(blocked),
    )
    return {
        "state": "BATCH_COMPLETE" if not failed and not blocked and not deferred else "BATCH_WITH_PENDING",
        "eligible": len(candidates), "attempted": attempted,
        "apparently_reconciled": settled, "pending": failed, "deferred": deferred,
        "blocked": blocked, "orders": results,
    }


def run_one_historical_sell_recovery(**kwargs: Any) -> dict[str, Any]:
    """Backwards-compatible entrypoint now performs the entire batch."""
    return run_all_historical_sell_recovery(**kwargs)
