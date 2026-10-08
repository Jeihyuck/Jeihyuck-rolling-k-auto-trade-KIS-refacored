"""Evidence-gated repair of missing KR holdings-promoted BUY watermarks.

Only marks quantities demonstrably already present in one exact position cycle.
No quantity is ever added by this repair; uncertain provenance remains unresolved.
"""
from __future__ import annotations

import logging

import sqlalchemy as sa

from trader.account_state import get_account_key
from trader.db.schema import schema_for_engine
from trader.db.trading_epoch import active_trading_epoch_id, trading_epoch_enforced

log = logging.getLogger(__name__)
_MARKER = "broker_truth_buy_applied_orders"


def _as_dict(value):
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        import json
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except (ValueError, TypeError):
            pass
    return {}


def repair_promoted_buy_watermark(*, engine, env: str, order: dict) -> bool:
    """Return True only when an already-applied BUY can be proved without a replay.

    The original immutable pre-order quantity, saved post-order holdings evidence,
    exact linked fill, sole open position, and absence of subsequent broker-risk
    orders must all agree. Nothing is inferred from today's unrelated balance.
    """
    schema = schema_for_engine(engine)
    order_id = str(order.get("order_id") or "")
    cycle = str(order.get("position_cycle_id") or "")
    portfolio_epoch = str(order.get("portfolio_epoch_id") or "")
    code = str(order.get("code") or "").zfill(6)
    response = _as_dict(order.get("response_json"))
    request = _as_dict(order.get("request_json"))
    confirmed = int(response.get("confirmed_fill_qty") or 0)
    pre = request.get("pre_order_holding_qty", response.get("pre_order_holding_qty"))
    post = response.get("holding_qty")
    if not (order_id and cycle and portfolio_epoch and code
            and str(order.get("side") or "").upper() == "BUY"
            and response.get("promotion_source") == "kis_holdings"
            and confirmed > 0 and pre is not None and post is not None
            and int(post) == int(pre) + confirmed
            and order.get("created_at") is not None):
        return False

    with engine.begin() as conn:
        active_epoch = active_trading_epoch_id(
            conn, env=env, account_id=get_account_key(env=env),
            required=trading_epoch_enforced(),
        )
        if active_epoch is not None and str(order.get("trading_epoch_id") or "") != str(active_epoch):
            return False
        predicate = sa.and_(
            schema.positions.c.env == env,
            schema.positions.c.strategy == order.get("strategy"),
            schema.positions.c.code == code,
            schema.positions.c.position_cycle_id == cycle,
            schema.positions.c.portfolio_epoch_id == portfolio_epoch,
            schema.positions.c.status == "OPEN",
        )
        if active_epoch is not None:
            predicate = sa.and_(predicate, schema.positions.c.trading_epoch_id == active_epoch)
        positions = list(conn.execute(
            sa.select(schema.positions).where(predicate).with_for_update()
        ).mappings().all())
        recovery_alias = False
        if not positions:
            # 10/08: KIS balance restoration can create a different RECOVERY
            # cycle for an already-applied BUY. Never replay the qty. Only
            # restore its watermark if the exact frozen contract was recovered
            # using one proven source cycle and the broker account qty is exact.
            from trader.db.repos import _assert_kr_buy_entry_contract, _kr_entry_exit_plan_sha256
            if request.get("enforce_entry_contract") is not True or int(pre) != 0:
                return False
            try:
                _assert_kr_buy_entry_contract(request)
            except (RuntimeError, TypeError, ValueError):
                return False
            possible = list(conn.execute(
                sa.select(schema.positions).where(sa.and_(
                    schema.positions.c.env == env,
                    schema.positions.c.strategy == order.get("strategy"),
                    schema.positions.c.code == code,
                    schema.positions.c.portfolio_epoch_id == portfolio_epoch,
                    schema.positions.c.status == "OPEN",
                )).with_for_update()
            ).mappings().all())
            if len(possible) != 1:
                return False
            row = dict(possible[0])
            origin_meta = _as_dict(row.get("position_meta"))
            frozen_meta = _as_dict(row.get("entry_meta_json"))
            source_plan = _as_dict(request.get("entry_exit_plan"))
            if not (
                str(row.get("position_origin") or "").upper() == "RECOVERY"
                and origin_meta.get("provenance_verified") is True
                and str(origin_meta.get("recovered_from_cycle_id") or "") == cycle
                and str(origin_meta.get("recovered_from_epoch_id") or "") == portfolio_epoch
                and _as_dict(row.get("entry_exit_plan_json")) == source_plan
                and str(frozen_meta.get("entry_exit_plan_sha256") or "") == _kr_entry_exit_plan_sha256(source_plan)
                and str(frozen_meta.get("entry_contract_sha256") or "") == request.get("entry_contract_sha256")
                and int(row.get("qty") or 0) == int(post) == confirmed
            ):
                return False
            # Prevent accepting an apparently matching quantity from a later
            # order in another lifecycle or a manual/unknown submit.
            later_any_cycle = conn.execute(sa.select(schema.orders.c.order_id).where(sa.and_(
                schema.orders.c.env == env,
                schema.orders.c.strategy == order.get("strategy"),
                schema.orders.c.code == code,
                schema.orders.c.created_at > order["created_at"],
                schema.orders.c.order_id != order_id,
                schema.orders.c.status.in_((
                    "SUBMITTED", "ACKED", "ACCEPTED", "UNRESOLVED_ACK",
                    "PARTIAL_FILLED", "FILLED", "FILLED_QTY_CONFIRMED_PRICE_UNRESOLVED",
                )),
            )).limit(1)).first()
            if later_any_cycle:
                return False
            positions = [row]
            recovery_alias = True
        if len(positions) != 1 or int(positions[0].get("qty") or 0) != int(post):
            return False

        # A later order in the same lifecycle makes current position arithmetic
        # ambiguous, even if its observed net quantity coincidentally matches.
        later = conn.execute(
            sa.select(schema.orders.c.order_id).where(sa.and_(
                schema.orders.c.env == env,
                schema.orders.c.strategy == order.get("strategy"),
                schema.orders.c.code == code,
                schema.orders.c.position_cycle_id == cycle,
                schema.orders.c.portfolio_epoch_id == portfolio_epoch,
                schema.orders.c.created_at > order["created_at"],
                schema.orders.c.order_id != order_id,
                schema.orders.c.status.in_((
                    "SUBMITTED", "ACKED", "ACCEPTED", "UNRESOLVED_ACK",
                    "PARTIAL_FILLED", "FILLED",
                    "FILLED_QTY_CONFIRMED_PRICE_UNRESOLVED",
                )),
            )).limit(1)
        ).first()
        if later:
            return False

        rows = list(conn.execute(sa.select(schema.fills).where(sa.and_(
            schema.fills.c.env == env,
            schema.fills.c.order_id == order_id,
            schema.fills.c.side == "BUY",
        ))).mappings().all())
        if not rows or sum(int(r["qty"] or 0) for r in rows) != confirmed:
            return False
        if recovery_alias and any(
            str(r.get("position_cycle_id") or "") != cycle
            or str(r.get("portfolio_epoch_id") or "") != portfolio_epoch
            for r in rows
        ):
            return False

        qty = confirmed
        notional = sum(float(r["price"] or 0) * int(r["qty"] or 0) for r in rows)
        if notional <= 0:
            return False
        marker = {
            "qty": qty,
            "notional": notional,
            "fee": sum(float(r["fee"] or 0) for r in rows),
            "tax": sum(float(r["tax"] or 0) for r in rows),
        }
        meta = _as_dict(positions[0].get("entry_meta_json"))
        applied = _as_dict(meta.get(_MARKER))
        prior = _as_dict(applied.get(order_id))
        if prior:
            return int(prior.get("qty") or 0) == qty and abs(
                float(prior.get("notional") or 0) - notional
            ) < 1e-6
        applied[order_id] = marker
        meta[_MARKER] = applied
        conn.execute(sa.update(schema.positions).where(
            schema.positions.c.position_id == positions[0]["position_id"]
        ).values(entry_meta_json=meta, updated_at=sa.func.now()))
        log.warning(
            "[KR_BROKER_TRUTH][BUY_WATERMARK][REPAIRED] code=%s order_id=%s qty=%s source=%s",
            code, order_id, qty,
            "proven_recovery_alias_no_qty_apply" if recovery_alias else "exact_promoted_holding",
        )
        return True
