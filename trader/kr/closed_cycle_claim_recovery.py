"""Strict, no-order retirement of rejected PB1 SELL claims on fully closed cycles."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import sqlalchemy as sa

from trader.db.schema import schema_for_engine

KST = ZoneInfo("Asia/Seoul")


def _mapping(value):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            return {}
    return value if isinstance(value, dict) else {}


def _qty(value):
    try:
        number = int(str(value))
        return number if number >= 0 else None
    except (ValueError, TypeError):
        return None


def prove_closed_rejected_sell(*, claim, position, orders, fills, attempts,
                               account_hash, holdings_rows):
    """Verify exact cycle economics and terminal zero-fill rejection.

    Caller supplies a trusted current account balance and locked scoped rows.
    No synthetic rows, guessed quantities, or cross-cycle offsets.
    """
    code = str(position.get("code") or "")
    cycle = str(position.get("position_cycle_id") or "")
    portfolio = str(position.get("portfolio_epoch_id") or "")
    epoch = str(claim.get("trading_epoch_id") or "")
    env = claim.get("env")
    if (not code or code == "122630" or not cycle or not portfolio or not epoch
            or claim.get("market") != "KR" or claim.get("strategy_owner") != "PB1"
            or claim.get("account_id") != account_hash
            or claim.get("lifecycle_id") != cycle or claim.get("action_state") != "RETRYABLE"
            or not str(claim.get("action") or "").startswith("SELL:")
            or claim.get("active_attempt_id") is not None or claim.get("claim_conflicts") != 0
            or _qty(claim.get("cumulative_filled_qty")) != 0
            or not _qty(claim.get("target_qty"))
            or claim.get("remaining_target_qty") != claim.get("target_qty")
            or position.get("status") != "CLOSED" or position.get("closed_reason") != "FULL_SELL"
            or _qty(position.get("qty")) != 0 or position.get("position_origin") != "SYSTEM"
            or position.get("strategy") != "pb1_pullback_close" or not position.get("closed_ts")
            or position.get("env") != env or str(position.get("trading_epoch_id")) != epoch):
        return None
    if not isinstance(holdings_rows, list):
        return None
    for row in holdings_rows:
        if not isinstance(row, dict) or not row.get("pdno") or _qty(row.get("hldg_qty")) is None:
            return None
        if str(row["pdno"]).zfill(6) == code and _qty(row["hldg_qty"]) != 0:
            return None

    def same_scope(row):
        return (row.get("env") == env and str(row.get("trading_epoch_id")) == epoch
                and str(row.get("position_cycle_id")) == cycle
                and str(row.get("portfolio_epoch_id")) == portfolio
                and str(row.get("code")) == code)

    if not attempts or not orders or not fills:
        return None
    rejected_keys = set()
    last_rejected_at = None
    for attempt in attempts:
        key = attempt.get("client_order_key")
        if (attempt.get("action_key") != claim.get("action_key")
                or attempt.get("attempt_state") != "REJECTED_EXPLICIT"
                or attempt.get("authoritative") is not True
                or _qty(attempt.get("cumulative_filled_qty")) != 0
                or not key or key in rejected_keys):
            return None
        at = attempt.get("created_at")
        if not isinstance(at, datetime):
            return None
        # SQLite offline fixtures can lose UTC tzinfo on round trip.
        at = at if at.tzinfo else at.replace(tzinfo=timezone.utc)
        last_rejected_at = max(last_rejected_at, at) if last_rejected_at else at
        rejected_keys.add(key)
    closed_at = position.get("closed_ts")
    if not isinstance(closed_at, datetime):
        return None
    closed_at = closed_at if closed_at.tzinfo else closed_at.replace(tzinfo=timezone.utc)
    if closed_at < last_rejected_at:
        return None
    order_by_id = {}
    verified_rejections = set()
    for order in orders:
        if not same_scope(order) or not order.get("order_id"):
            return None
        oid = str(order["order_id"])
        if oid in order_by_id:
            return None
        order_by_id[oid] = order
        key = order.get("client_order_key")
        if key in rejected_keys:
            response = _mapping(order.get("response_json"))
            exec_meta = _mapping(response.get("_order_execution"))
            request = _mapping(order.get("request_json"))
            if (order.get("side") != "SELL" or order.get("status") not in {"ERROR", "REJECTED"}
                    or order.get("kis_odno") or order.get("broker_order_id")
                    or str(response.get("rt_cd")) != "1" or exec_meta.get("accepted") is not False
                    or exec_meta.get("broker_order_id") or exec_meta.get("code") != code
                    or exec_meta.get("side") != "SELL"
                    or request.get("strategy_owner") != "PB1"
                    or request.get("semantic_action") != claim.get("action")
                    or str(request.get("position_lifecycle_id")) != cycle):
                return None
            verified_rejections.add(key)
        elif order.get("status") != "FILLED":
            # No unresolved or other non-economic order in this closure proof.
            return None
    if rejected_keys != verified_rejections:
        return None
    bought = sold = later_sold = 0
    seen_fills = set()
    filled_orders = set()
    for fill in fills:
        oid = str(fill.get("order_id") or "")
        order = order_by_id.get(oid)
        side = fill.get("side")
        qty = _qty(fill.get("qty"))
        meta = _mapping(fill.get("fill_meta_json"))
        raw = _mapping(_mapping(fill.get("raw_json")).get("kis_response"))
        broker_id = str(fill.get("broker_fill_id") or "")
        odno = str(fill.get("kis_odno") or "")
        day = str(raw.get("ord_dt") or "")
        fill_at = fill.get("filled_at")
        if not isinstance(fill_at, datetime):
            return None
        fill_at = fill_at if fill_at.tzinfo else fill_at.replace(tzinfo=timezone.utc)
        if (not same_scope(fill) or not order or order.get("status") != "FILLED"
                or side not in {"BUY", "SELL"} or order.get("side") != side
                or not qty or _qty(order.get("qty")) != qty or not odno
                or str(order.get("kis_odno")) != odno or str(raw.get("odno")) != odno
                or str(raw.get("pdno")) != code or len(day) != 8 or not day.isdigit()
                or fill_at.astimezone(KST).strftime("%Y%m%d") != day
                or fill_at > closed_at or _qty(raw.get("ord_qty")) != qty
                or raw.get("sll_buy_dvsn_cd") != ("01" if side == "SELL" else "02")
                or _qty(raw.get("tot_ccld_qty")) != qty or _qty(raw.get("rmn_qty")) != 0
                or meta.get("evidence_kind") != "KIS_ORDER_CUMULATIVE_EXECUTION"
                or broker_id != f"KR_KIS_CUM:{account_hash}:{day}:{odno}:{code}:{side}"
                or broker_id in seen_fills or oid in filled_orders):
            return None
        seen_fills.add(broker_id)
        filled_orders.add(oid)
        if side == "BUY":
            bought += qty
        else:
            sold += qty
            if fill_at >= last_rejected_at:
                later_sold += qty
    if (not bought or bought != sold or later_sold < claim["target_qty"] or
            filled_orders != {oid for oid, row in order_by_id.items() if row.get("status") == "FILLED"}):
        return None
    return {"code": code, "position_cycle_id": cycle, "portfolio_epoch_id": portfolio,
            "buy_qty": bought, "sell_qty": sold, "later_sell_qty": later_sold,
            "broker_fill_ids": sorted(seen_fills),
            "rejected_attempt_ids": [str(a["attempt_id"]) for a in attempts],
            "reason": "CLOSED_CYCLE_BY_OTHER_NATIVE_FILLS"}


def retire_closed_pb1_rejected_claims(*, engine, scope, holdings_rows, now,
                                     limit=8):
    """Run only from verified historical reconciliation, never from BUY submit.

    Scope is the resolved active account+epoch; holdings_rows must be from the
    caller's trusted current broker snapshot. Each transition and audit commit
    together. No broker call, fill, position or order mutation.
    """
    if scope is None or scope.env != "practice":
        return 0
    schema = schema_for_engine(engine)
    a, t = schema.execution_claims, schema.execution_attempts
    p, o, f = schema.positions, schema.orders, schema.fills
    pe, te = schema.portfolio_epochs, schema.trading_epochs
    account_hash = hashlib.sha256(scope.account_key.encode("utf-8")).hexdigest()
    filters = (a.c.env == scope.env, a.c.market == "KR", a.c.account_id == account_hash,
               a.c.trading_epoch_id == scope.trading_epoch_id, a.c.strategy_owner == "PB1",
               a.c.action_state == "RETRYABLE", a.c.active_attempt_id.is_(None),
               a.c.action.like("SELL:%"))
    with engine.connect() as conn:
        candidates = conn.execute(sa.select(a.c.action_key, a.c.lifecycle_id)
                                  .where(*filters).order_by(a.c.updated_at, a.c.action_key)
                                  .limit(max(1, min(8, int(limit))))).all()
    retired = 0
    for key, cycle in candidates:
        with engine.begin() as conn:
            if conn.dialect.name == "postgresql":
                lock_scope = json.dumps((scope.env, account_hash, "KR",
                                         scope.trading_epoch_id, "PB1", str(cycle)),
                                        separators=(",", ":"))
                lock_id = int.from_bytes(hashlib.sha256(lock_scope.encode()).digest()[:8],
                                         "big", signed=True)
                if not conn.execute(sa.select(sa.func.pg_try_advisory_xact_lock(lock_id))).scalar_one():
                    continue
            claim = conn.execute(sa.select(a).where(a.c.action_key == key, *filters)
                                 .with_for_update()).mappings().one_or_none()
            if claim is None:
                continue
            active_epoch = conn.execute(sa.select(te.c.trading_epoch_id).where(
                te.c.trading_epoch_id == scope.trading_epoch_id, te.c.env == scope.env,
                te.c.account_id == scope.account_key, te.c.status == "ACTIVE"
            ).with_for_update()).scalar_one_or_none()
            if active_epoch is None:
                continue
            position = conn.execute(sa.select(p).where(
                p.c.position_cycle_id == cycle, p.c.trading_epoch_id == scope.trading_epoch_id,
                p.c.env == scope.env).with_for_update()).mappings().one_or_none()
            if position is None:
                continue
            portfolio_id = position["portfolio_epoch_id"]
            portfolio = conn.execute(sa.select(pe.c.portfolio_epoch_id).where(
                pe.c.portfolio_epoch_id == portfolio_id, pe.c.env == scope.env,
                pe.c.account_id == scope.account_key, pe.c.trading_epoch_id == scope.trading_epoch_id,
                pe.c.strategy == "pb1_pullback_close", pe.c.status == "ACTIVE"
            ).with_for_update()).scalar_one_or_none()
            if portfolio is None:
                continue
            siblings = conn.execute(sa.select(a).where(
                a.c.env == scope.env, a.c.account_id == account_hash,
                a.c.market == "KR", a.c.trading_epoch_id == scope.trading_epoch_id,
                a.c.strategy_owner == "PB1", a.c.lifecycle_id == cycle,
                a.c.action_key != key
            ).with_for_update()).mappings().all()
            if any(s["active_attempt_id"] or s["action_state"] in {"IN_FLIGHT", "UNCERTAIN"}
                   or s["claim_conflicts"] for s in siblings):
                continue
            attempts = conn.execute(sa.select(t).where(t.c.action_key == key)
                                    .with_for_update()).mappings().all()
            orders = conn.execute(sa.select(o).where(o.c.position_cycle_id == cycle)
                                  .with_for_update()).mappings().all()
            fills = conn.execute(sa.select(f).where(f.c.position_cycle_id == cycle)
                                 .with_for_update()).mappings().all()
            proof = prove_closed_rejected_sell(claim=claim, position=position, orders=orders,
                                               fills=fills, attempts=attempts,
                                               account_hash=account_hash, holdings_rows=holdings_rows)
            if proof is None:
                continue
            result = conn.execute(sa.update(a).where(a.c.action_key == key, *filters)
                                  .values(action_state="SUPERSEDED", updated_at=datetime.now(timezone.utc)))
            if result.rowcount != 1:
                continue
            conn.execute(sa.insert(schema.reconcile_log).values(
                env=scope.env, strategy="pb1_pullback_close", tick_ts=now,
                action="closed_cycle_claim_superseded",
                details_json={**proof, "action_key": key, "trading_epoch_id": scope.trading_epoch_id,
                              "original_cumulative_filled_qty": 0,
                              "original_remaining_target_qty": claim["remaining_target_qty"],
                              "broker_submit": False}))
            retired += 1
    return retired
