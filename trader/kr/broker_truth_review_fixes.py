"""Follow-up guards for PR125 review findings.

These replacements are installed after :mod:`trader.kr.broker_truth_hardening`.
They keep the same public/runtime contract while fixing review edge cases:

* one broker BUY order may arrive as multiple daily-ccld fill rows; position
  accounting must apply the cumulative execution exactly once rather than only
  the first row;
* a fresh KIS balance that omits a DB-open symbol is broker quantity zero for
  health reporting, even though destructive zeroing still uses the existing
  multi-snapshot safety guard;
* a symbol with multiple simultaneous OPEN lifecycles is ambiguous and must be
  fenced instead of receiving the same account-level KIS quantity/policy on
  every row;
* recovered policy/metadata writes are constrained to the exact selected
  position_id + cycle + epoch lifecycle.
"""
from __future__ import annotations

import logging
from typing import Any

import sqlalchemy as sa

from trader.db.repos import FillsRepo, OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.time_utils import now_kst
import trader.kr.broker_truth_hardening as base

logger = logging.getLogger(__name__)
_INSTALLED = False
_APPLIED_META_KEY = "broker_truth_applied_orders"


def _update_exact_open_position_fields(
    *,
    engine,
    schema,
    position: dict[str, Any],
    fields: dict[str, Any],
) -> bool:
    """Update exactly one already-selected OPEN lifecycle or fail closed."""
    position_id = position.get("position_id")
    cycle_id = position.get("position_cycle_id")
    epoch_id = position.get("portfolio_epoch_id")
    env = str(position.get("env") or "")
    strategy = str(position.get("strategy") or "")
    code = base._normalize_code(position.get("code"))
    if not position_id or not cycle_id or not epoch_id or not env or not strategy or not code:
        logger.error(
            "[KR_BROKER_TRUTH][EXACT_POSITION_UPDATE][BLOCK] code=%s position_id=%s cycle=%s epoch=%s reason=IDENTITY_MISSING",
            code,
            position_id,
            cycle_id,
            epoch_id,
        )
        return False

    values = {key: value for key, value in fields.items() if value is not None}
    if not values:
        return True
    values["updated_at"] = sa.func.now()
    with engine.begin() as conn:
        result = conn.execute(
            sa.update(schema.positions)
            .where(
                sa.and_(
                    schema.positions.c.position_id == position_id,
                    schema.positions.c.env == env,
                    schema.positions.c.strategy == strategy,
                    schema.positions.c.code == code,
                    schema.positions.c.position_cycle_id == cycle_id,
                    schema.positions.c.portfolio_epoch_id == epoch_id,
                    schema.positions.c.status == "OPEN",
                )
            )
            .values(**values)
        )
    updated = int(result.rowcount or 0)
    if updated != 1:
        logger.error(
            "[KR_BROKER_TRUTH][EXACT_POSITION_UPDATE][FAIL] code=%s position_id=%s cycle=%s epoch=%s expected_rows=1 updated=%s",
            code,
            position_id,
            cycle_id,
            epoch_id,
            updated,
        )
        return False
    return True


def _link_unowned_daily_fills_fixed(*, engine, env: str, strategy: str) -> dict[str, int]:
    """Link daily fills by broker order and apply cumulative BUY deltas exactly once."""
    schema = schema_for_engine(engine)
    fills_repo = FillsRepo(engine)
    rows = [dict(row) for row in (fills_repo.list_today_fills(env) or [])]
    unowned = [row for row in rows if not row.get("order_id") and row.get("kis_odno")]
    if not unowned:
        return {"linked_fills": 0, "positions_promoted": 0}

    by_odno: dict[str, list[dict[str, Any]]] = {}
    for row in unowned:
        by_odno.setdefault(str(row.get("kis_odno") or "").strip(), []).append(row)

    linked = 0
    promoted = 0
    for kis_odno, new_rows in by_odno.items():
        if not kis_odno:
            continue
        with engine.begin() as conn:
            order_row = conn.execute(
                sa.select(schema.orders).where(
                    sa.and_(
                        schema.orders.c.env == env,
                        schema.orders.c.strategy == strategy,
                        sa.or_(
                            schema.orders.c.kis_odno == kis_odno,
                            schema.orders.c.broker_order_id == kis_odno,
                        ),
                    )
                ).order_by(schema.orders.c.created_at.desc()).limit(1)
            ).mappings().first()
            if not order_row:
                continue
            order = dict(order_row)
            order_id = order.get("order_id")
            for fill in new_rows:
                conn.execute(
                    sa.update(schema.fills)
                    .where(schema.fills.c.fill_id == fill.get("fill_id"))
                    .values(
                        order_id=order_id,
                        position_cycle_id=order.get("position_cycle_id"),
                        portfolio_epoch_id=order.get("portfolio_epoch_id"),
                    )
                )
                linked += 1

        # Re-read every execution already attributed to this durable order. This
        # makes repeated invocations and fills arriving over several ticks safe.
        with engine.connect() as conn:
            attributed = [
                dict(row)
                for row in conn.execute(
                    sa.select(schema.fills).where(schema.fills.c.order_id == order_id)
                    .order_by(schema.fills.c.filled_at.asc())
                ).mappings().all()
            ]
        cumulative_qty = sum(base._qty(row.get("qty")) for row in attributed)
        cumulative_notional = sum(base._qty(row.get("qty")) * base._px(row.get("price")) for row in attributed)
        cumulative_fee = sum(base._px(row.get("fee")) for row in attributed)
        cumulative_tax = sum(base._px(row.get("tax")) for row in attributed)
        order_qty = base._qty(order.get("qty"))
        canonical_status = "FILLED" if order_qty > 0 and cumulative_qty >= order_qty else "PARTIAL_FILLED"
        with engine.begin() as conn:
            conn.execute(
                sa.update(schema.orders)
                .where(schema.orders.c.order_id == order_id)
                .values(status=canonical_status, updated_at=sa.func.now())
            )

        if str(order.get("side") or "").upper() == "BUY" and cumulative_qty > 0 and cumulative_notional > 0:
            cycle = order.get("position_cycle_id")
            epoch = order.get("portfolio_epoch_id")
            code = base._normalize_code(order.get("code"))
            with engine.connect() as conn:
                pos = conn.execute(
                    sa.select(schema.positions).where(
                        sa.and_(
                            schema.positions.c.env == env,
                            schema.positions.c.strategy == strategy,
                            schema.positions.c.code == code,
                            schema.positions.c.position_cycle_id == cycle,
                            schema.positions.c.portfolio_epoch_id == epoch,
                            schema.positions.c.status == "OPEN",
                        )
                    ).limit(1)
                ).mappings().first()
            pos_dict = dict(pos) if pos else {}
            pos_meta = base._json_dict(pos_dict.get("position_meta"))
            applied_map = base._json_dict(pos_meta.get(_APPLIED_META_KEY))
            applied = base._json_dict(applied_map.get(str(order_id)))
            applied_qty = base._qty(applied.get("qty"))
            applied_notional = base._px(applied.get("notional"))

            # If an exact-cycle position predates this guard and has no marker,
            # do not guess whether some/all fills were already applied.
            if pos and not applied and int(pos_dict.get("qty") or 0) > 0:
                logger.error(
                    "[KR_BROKER_TRUTH][SPLIT_FILL][REVIEW_REQUIRED] code=%s order_id=%s position_qty=%s cumulative_fill_qty=%s",
                    code, order_id, pos_dict.get("qty"), cumulative_qty,
                )
            else:
                delta_qty = cumulative_qty - applied_qty
                delta_notional = cumulative_notional - applied_notional
                if delta_qty > 0 and delta_notional > 0:
                    delta_price = delta_notional / delta_qty
                    entry_meta, entry_plan = base._entry_contract(order)
                    filled_at = attributed[-1].get("filled_at") or order.get("acked_at") or order.get("submitted_at") or now_kst()
                    PositionsRepo(engine).apply_fill(
                        env=env,
                        strategy=strategy,
                        sid=int(order.get("sid") or 1),
                        mode=int(order.get("mode") or 1),
                        code=code,
                        market=order.get("market"),
                        side="BUY",
                        qty=delta_qty,
                        price=delta_price,
                        fee=max(0.0, cumulative_fee - base._px(applied.get("fee"))),
                        tax=max(0.0, cumulative_tax - base._px(applied.get("tax"))),
                        filled_at=filled_at,
                        entry_meta=entry_meta,
                        entry_exit_plan=entry_plan,
                        portfolio_epoch_id=str(epoch),
                        position_cycle_id=str(cycle),
                        order_id=str(order_id),
                    )
                    # Persist cumulative application evidence on the exact
                    # lifecycle so another OPEN row for the same code cannot be
                    # polluted by a broad code-only metadata update.
                    with engine.connect() as conn:
                        current = conn.execute(
                            sa.select(schema.positions).where(
                                sa.and_(
                                    schema.positions.c.env == env,
                                    schema.positions.c.strategy == strategy,
                                    schema.positions.c.code == code,
                                    schema.positions.c.position_cycle_id == cycle,
                                    schema.positions.c.portfolio_epoch_id == epoch,
                                    schema.positions.c.status == "OPEN",
                                )
                            ).limit(1)
                        ).mappings().first()
                    current_dict = dict(current) if current else {}
                    current_meta = base._json_dict(current_dict.get("position_meta"))
                    current_map = base._json_dict(current_meta.get(_APPLIED_META_KEY))
                    current_map[str(order_id)] = {
                        "qty": cumulative_qty,
                        "notional": cumulative_notional,
                        "fee": cumulative_fee,
                        "tax": cumulative_tax,
                    }
                    current_meta[_APPLIED_META_KEY] = current_map
                    if current_dict and _update_exact_open_position_fields(
                        engine=engine,
                        schema=schema,
                        position=current_dict,
                        fields={"position_meta": current_meta, "entry_ts": base._iso(filled_at)},
                    ):
                        promoted += 1
                    else:
                        logger.error(
                            "[KR_BROKER_TRUTH][SPLIT_FILL][MARKER_WRITE_FAIL] code=%s order_id=%s cycle=%s epoch=%s",
                            code,
                            order_id,
                            cycle,
                            epoch,
                        )

        logger.warning(
            "[KR_BROKER_TRUTH][FILL_LINK][OK] code=%s kis_odno=%s order_id=%s linked_now=%s cumulative_qty=%s status=%s",
            base._normalize_code(order.get("code")), kis_odno, order_id, len(new_rows), cumulative_qty, canonical_status,
        )

    return {"linked_fills": linked, "positions_promoted": promoted}


def _recover_proven_policy_positions_fixed(
    *,
    engine,
    env: str,
    strategy: str,
    holdings_rows: list[dict] | None,
) -> dict[str, Any]:
    """Recover POLICY_MISSING only into one exact, unambiguous OPEN row."""
    schema = schema_for_engine(engine)
    kis = base._holdings_index(holdings_rows)
    recovered: list[str] = []
    review: list[str] = []
    with engine.connect() as conn:
        positions = [
            dict(row)
            for row in conn.execute(
                sa.select(schema.positions).where(
                    sa.and_(
                        schema.positions.c.env == env,
                        schema.positions.c.strategy == strategy,
                        schema.positions.c.status == "OPEN",
                        schema.positions.c.qty > 0,
                    )
                )
            ).mappings().all()
        ]

    by_code: dict[str, list[dict[str, Any]]] = {}
    for position in positions:
        by_code.setdefault(base._normalize_code(position.get("code")), []).append(position)

    for code, code_positions in sorted(by_code.items()):
        if len(code_positions) != 1:
            review.append(code)
            logger.error(
                "[KR_BROKER_TRUTH][POLICY_REVIEW_REQUIRED] code=%s open_lifecycles=%s position_ids=%s action=KEEP_POLICY_MISSING reason=MULTIPLE_OPEN_LIFECYCLES",
                code,
                len(code_positions),
                [str(row.get("position_id") or "") for row in code_positions],
            )
            continue

        position = code_positions[0]
        broker_qty = int((kis.get(code) or {}).get("qty") or 0)
        if broker_qty <= 0:
            continue
        family = str(position.get("exit_policy_family") or "").strip().upper()
        if family and family != "POLICY_MISSING":
            continue

        with engine.connect() as conn:
            rows = [
                dict(row)
                for row in conn.execute(
                    sa.select(
                        schema.fills,
                        schema.orders.c.entry_meta_json.label("order_entry_meta_json"),
                        schema.orders.c.request_json.label("order_request_json"),
                        schema.orders.c.position_cycle_id.label("order_cycle_id"),
                        schema.orders.c.portfolio_epoch_id.label("order_epoch_id"),
                    )
                    .join(schema.orders, schema.orders.c.order_id == schema.fills.c.order_id)
                    .where(
                        sa.and_(
                            schema.fills.c.env == env,
                            schema.fills.c.code == code,
                            schema.orders.c.env == env,
                            schema.orders.c.strategy == strategy,
                            schema.fills.c.position_cycle_id.is_not(None),
                            schema.fills.c.portfolio_epoch_id.is_not(None),
                        )
                    )
                    .order_by(schema.fills.c.filled_at.asc())
                ).mappings().all()
            ]

        groups: dict[tuple[str, str], dict[str, Any]] = {}
        for row in rows:
            cycle = str(row.get("position_cycle_id") or row.get("order_cycle_id") or "")
            epoch = str(row.get("portfolio_epoch_id") or row.get("order_epoch_id") or "")
            if not cycle or not epoch:
                continue
            group = groups.setdefault(
                (cycle, epoch),
                {"net": 0, "buy_order": None, "first_buy_at": None},
            )
            side = str(row.get("side") or "").upper()
            q = base._qty(row.get("qty"))
            group["net"] += q if side == "BUY" else -q if side == "SELL" else 0
            if side == "BUY":
                request = base._json_dict(row.get("order_request_json"))
                meta = base._json_dict(request.get("entry_meta")) or base._json_dict(row.get("order_entry_meta_json"))
                plan = base._json_dict(request.get("entry_exit_plan"))
                if meta or plan:
                    group["buy_order"] = {
                        "request_json": request,
                        "entry_meta_json": row.get("order_entry_meta_json"),
                    }
                    if group["first_buy_at"] is None:
                        group["first_buy_at"] = row.get("filled_at")

        candidates = []
        for (cycle, epoch), group in groups.items():
            if int(group.get("net") or 0) != broker_qty or not group.get("buy_order"):
                continue
            meta, plan = base._entry_contract(group["buy_order"])
            recovered_family = str(plan.get("exit_policy_family") or meta.get("exit_policy_family") or "").strip()
            recovered_horizon = str(plan.get("trade_horizon") or meta.get("trade_horizon") or "").strip()
            if not recovered_family or recovered_family.upper() == "POLICY_MISSING" or not recovered_horizon:
                continue
            candidates.append((cycle, epoch, group, meta, plan))

        if len(candidates) != 1:
            review.append(code)
            logger.error(
                "[KR_BROKER_TRUTH][POLICY_REVIEW_REQUIRED] code=%s broker_qty=%s candidates=%s action=KEEP_POLICY_MISSING",
                code,
                broker_qty,
                len(candidates),
            )
            continue

        source_cycle, source_epoch, group, meta, plan = candidates[0]
        position_meta = base._json_dict(position.get("position_meta"))
        position_meta.update(
            {
                "provenance_verified": True,
                "recovered_from_cycle_id": source_cycle,
                "recovered_from_epoch_id": source_epoch,
                "holding_age_unknown": False,
                "policy_recovery_source": "unique_net_fill_lifecycle",
            }
        )
        fields = {
            "position_origin": "RECOVERY",
            "position_meta": position_meta,
            "entry_ts": base._iso(group.get("first_buy_at")),
            "entry_reason": plan.get("entry_reason") or meta.get("entry_reason"),
            "entry_style_selected": plan.get("entry_style_selected") or meta.get("entry_style_selected"),
            "entry_thesis": plan.get("entry_thesis") or meta.get("entry_thesis"),
            "trade_horizon": plan.get("trade_horizon") or meta.get("trade_horizon"),
            "exit_policy_family": plan.get("exit_policy_family") or meta.get("exit_policy_family"),
            "eod_action": plan.get("eod_action") or meta.get("eod_action"),
            "force_eod_close": bool(
                plan.get("force_eod_close")
                if plan.get("force_eod_close") is not None
                else meta.get("force_eod_close") or False
            ),
            "entry_exit_plan_json": plan,
            "entry_meta_json": meta,
            "policy_source": "recovered_net_fill_provenance",
            "policy_version": plan.get("policy_version") or meta.get("policy_version"),
        }
        if not _update_exact_open_position_fields(
            engine=engine,
            schema=schema,
            position=position,
            fields=fields,
        ):
            review.append(code)
            continue

        recovered.append(code)
        logger.warning(
            "[KR_BROKER_TRUTH][POLICY_RECOVER][OK] code=%s position_id=%s broker_qty=%s family=%s horizon=%s source_cycle=%s",
            code,
            position.get("position_id"),
            broker_qty,
            fields.get("exit_policy_family"),
            fields.get("trade_horizon"),
            source_cycle,
        )

    return {"recovered": recovered, "review_required": sorted(set(review))}


def _health_after_reconcile_fixed(*, engine, env: str, strategy: str, holdings_rows: list[dict] | None) -> dict[str, Any]:
    """Report quantity gaps and ambiguous duplicate OPEN lifecycles as RED evidence."""
    schema = schema_for_engine(engine)
    kis = base._holdings_index(holdings_rows)
    mismatches: list[dict[str, Any]] = []
    duplicate_open_lifecycles: list[dict[str, Any]] = []
    with engine.connect() as conn:
        positions = [
            dict(row)
            for row in conn.execute(
                sa.select(schema.positions).where(
                    sa.and_(
                        schema.positions.c.env == env,
                        schema.positions.c.strategy == strategy,
                        schema.positions.c.status == "OPEN",
                        schema.positions.c.qty > 0,
                    )
                )
            ).mappings().all()
        ]

    by_code: dict[str, list[dict[str, Any]]] = {}
    for row in positions:
        by_code.setdefault(base._normalize_code(row.get("code")), []).append(row)

    for code, rows in sorted(by_code.items()):
        db_qty = sum(base._qty(row.get("qty")) for row in rows)
        broker_qty = int((kis.get(code) or {}).get("qty") or 0)
        if len(rows) > 1:
            duplicate = {
                "code": code,
                "count": len(rows),
                "position_ids": [str(row.get("position_id") or "") for row in rows],
                "db_qty": db_qty,
                "kis_qty": broker_qty,
            }
            duplicate_open_lifecycles.append(duplicate)
            # Keep this in qty_mismatches too so the existing post-tick RED
            # contract remains fail-closed without changing its caller.
            mismatches.append({**duplicate, "reason": "MULTIPLE_OPEN_LIFECYCLES"})
            continue
        if db_qty != broker_qty:
            mismatches.append({"code": code, "db_qty": db_qty, "kis_qty": broker_qty})

    today = now_kst().date()
    stale_orders: list[dict[str, Any]] = []
    try:
        for order in OrdersRepo(engine).get_open_orders(env, include_stale=True) or []:
            if str(order.get("strategy") or "") != strategy:
                continue
            created_day = base._date_of(order.get("created_at"))
            if created_day is not None and created_day < today:
                stale_orders.append(
                    {
                        "order_id": str(order.get("order_id") or ""),
                        "code": base._normalize_code(order.get("code")),
                        "side": str(order.get("side") or ""),
                        "status": str(order.get("status") or ""),
                        "created_at": base._iso(order.get("created_at")),
                    }
                )
    except Exception as exc:
        logger.exception("[KR_BROKER_TRUTH][HEALTH][STALE_ORDER_SCAN_FAIL] err=%s", exc)

    if duplicate_open_lifecycles:
        logger.error(
            "[KR_BROKER_TRUTH][HEALTH][MULTIPLE_OPEN_LIFECYCLES] count=%s rows=%s",
            len(duplicate_open_lifecycles),
            duplicate_open_lifecycles,
        )
    if mismatches:
        logger.error("[KR_BROKER_TRUTH][HEALTH][QTY_MISMATCH] count=%s rows=%s", len(mismatches), mismatches)
    if stale_orders:
        logger.error("[KR_BROKER_TRUTH][HEALTH][STALE_OPEN_ORDER] count=%s rows=%s", len(stale_orders), stale_orders)
    return {
        "qty_mismatch_count": len(mismatches),
        "qty_mismatches": mismatches,
        "duplicate_open_lifecycle_count": len(duplicate_open_lifecycles),
        "duplicate_open_lifecycles": duplicate_open_lifecycles,
        "stale_open_order_count": len(stale_orders),
        "stale_open_orders": stale_orders,
    }


def install_review_feedback_guards() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    base._link_unowned_daily_fills = _link_unowned_daily_fills_fixed
    base._recover_proven_policy_positions = _recover_proven_policy_positions_fixed
    base._health_after_reconcile = _health_after_reconcile_fixed
    _INSTALLED = True
    logger.info("[KR_BROKER_TRUTH][REVIEW_FIX_GUARD][INSTALLED]")
