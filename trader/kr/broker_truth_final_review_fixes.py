"""Final PR125 review guards for durable broker-truth application.

Two remaining crash-window defects are closed here without changing any trading
policy:

* OrdersRepo reconciliation must preserve broker-truth application watermarks
  already stored in ``response_json``.
* BUY reconciliation must retry fills that are already linked to an order but
  are not yet reflected in the exact position lifecycle.  The BUY application
  watermark is persisted in ``entry_meta_json`` by ``PositionsRepo.apply_fill``
  in the same transaction as quantity/cost accounting.
"""
from __future__ import annotations

import functools
import logging
from typing import Any

import sqlalchemy as sa

from trader.db.repos import FillsRepo, OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.time_utils import now_kst
import trader.kr.broker_truth_hardening as base
import trader.kr.broker_truth_review_fixes as review

logger = logging.getLogger(__name__)
_INSTALLED = False
_BUY_APPLIED_KEY = "broker_truth_buy_applied_orders"


def _deep_merge_one_level(existing: Any, incoming: Any) -> dict[str, Any]:
    merged = base._json_dict(existing)
    for key, value in base._json_dict(incoming).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            nested = dict(merged.get(key) or {})
            nested.update(value)
            merged[key] = nested
        else:
            merged[key] = value
    return merged


def _install_order_response_merge_guard() -> None:
    if getattr(OrdersRepo, "_pr125_response_merge_guard_installed", False):
        return
    original = OrdersRepo.upsert_reconciled_order

    @functools.wraps(original)
    def _merged_upsert(self, *args, **kwargs):
        env = str(kwargs.get("env") or "")
        kis_odno = str(kwargs.get("kis_odno") or "").strip()
        client_order_key = str(kwargs.get("client_order_key") or "").strip()
        existing_response: dict[str, Any] = {}
        try:
            schema = self._schema
            conditions = [schema.orders.c.env == env]
            if kis_odno:
                conditions.append(
                    sa.or_(
                        schema.orders.c.kis_odno == kis_odno,
                        schema.orders.c.broker_order_id == kis_odno,
                    )
                )
            elif client_order_key:
                conditions.append(schema.orders.c.client_order_key == client_order_key)
            else:
                conditions = []
            if conditions:
                with self.engine.connect() as conn:
                    existing = conn.execute(
                        sa.select(schema.orders.c.response_json)
                        .where(sa.and_(*conditions))
                        .order_by(schema.orders.c.created_at.desc())
                        .limit(1)
                    ).scalar()
                existing_response = base._json_dict(existing)
        except Exception as exc:
            logger.warning(
                "[KR_BROKER_TRUTH][ORDER_RESPONSE_MERGE][READ_FAIL] kis_odno=%s err_type=%s err=%s",
                kis_odno,
                type(exc).__name__,
                exc,
            )
        kwargs["response_json"] = _deep_merge_one_level(
            existing_response,
            kwargs.get("response_json") or {},
        )
        return original(self, *args, **kwargs)

    OrdersRepo.upsert_reconciled_order = _merged_upsert
    OrdersRepo._pr125_response_merge_guard_installed = True


def _baseline_qty(order: dict[str, Any]) -> int | None:
    request = base._json_dict(order.get("request_json"))
    meta = base._json_dict(request.get("entry_meta"))
    order_meta = base._json_dict(order.get("entry_meta_json"))
    for source in (request, meta, order_meta):
        if "pre_order_holding_qty" in source:
            try:
                return int(float(source.get("pre_order_holding_qty") or 0))
            except Exception:
                return None
    return None


def _all_buy_fills_by_odno(*, engine, env: str) -> dict[str, list[dict[str, Any]]]:
    rows = [dict(row) for row in (FillsRepo(engine).list_today_fills(env) or [])]
    result: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if str(row.get("side") or "").upper() != "BUY":
            continue
        odno = str(row.get("kis_odno") or "").strip()
        if not odno:
            continue
        result.setdefault(odno, []).append(row)
    return result


def _link_buy_fills_retry_safe(*, engine, env: str, strategy: str) -> dict[str, int]:
    """Reconcile BUY fills with a durable retry path for already-owned fills."""
    schema = schema_for_engine(engine)
    grouped = _all_buy_fills_by_odno(engine=engine, env=env)
    if not grouped:
        return {"linked_fills": 0, "positions_promoted": 0}

    linked = 0
    promoted = 0
    for kis_odno, observed_rows in grouped.items():
        with engine.begin() as conn:
            order_row = conn.execute(
                sa.select(schema.orders)
                .where(
                    sa.and_(
                        schema.orders.c.env == env,
                        schema.orders.c.strategy == strategy,
                        sa.or_(
                            schema.orders.c.kis_odno == kis_odno,
                            schema.orders.c.broker_order_id == kis_odno,
                        ),
                    )
                )
                .order_by(schema.orders.c.created_at.desc())
                .limit(1)
            ).mappings().first()
            if not order_row:
                continue
            order = dict(order_row)
            if str(order.get("side") or "").upper() != "BUY":
                continue
            order_id = order.get("order_id")
            cycle = order.get("position_cycle_id")
            epoch = order.get("portfolio_epoch_id")
            code = base._normalize_code(order.get("code"))
            if not order_id or not cycle or not epoch or not code:
                logger.error(
                    "[KR_BROKER_TRUTH][BUY_RETRY][BLOCK] kis_odno=%s reason=ORDER_PROVENANCE_MISSING",
                    kis_odno,
                )
                continue

            # Claim only still-unowned executions. Already-owned rows are kept in
            # scope so a prior crash after attribution can be repaired here.
            for fill in observed_rows:
                if fill.get("order_id"):
                    continue
                result = conn.execute(
                    sa.update(schema.fills)
                    .where(
                        sa.and_(
                            schema.fills.c.fill_id == fill.get("fill_id"),
                            schema.fills.c.order_id.is_(None),
                        )
                    )
                    .values(
                        order_id=order_id,
                        position_cycle_id=cycle,
                        portfolio_epoch_id=epoch,
                    )
                )
                if int(result.rowcount or 0) == 1:
                    linked += 1

            attributed = [
                dict(row)
                for row in conn.execute(
                    sa.select(schema.fills)
                    .where(schema.fills.c.order_id == order_id)
                    .order_by(schema.fills.c.filled_at.asc())
                ).mappings().all()
            ]
            cumulative_qty = sum(base._qty(row.get("qty")) for row in attributed)
            cumulative_notional = sum(
                base._qty(row.get("qty")) * base._px(row.get("price")) for row in attributed
            )
            cumulative_fee = sum(base._px(row.get("fee")) for row in attributed)
            cumulative_tax = sum(base._px(row.get("tax")) for row in attributed)
            order_qty = base._qty(order.get("qty"))
            canonical_status = (
                "FILLED" if order_qty > 0 and cumulative_qty >= order_qty else "PARTIAL_FILLED"
            )
            conn.execute(
                sa.update(schema.orders)
                .where(schema.orders.c.order_id == order_id)
                .values(status=canonical_status, updated_at=sa.func.now())
            )

        if cumulative_qty <= 0 or cumulative_notional <= 0:
            continue

        with engine.connect() as conn:
            exact_rows = [
                dict(row)
                for row in conn.execute(
                    sa.select(schema.positions).where(
                        sa.and_(
                            schema.positions.c.env == env,
                            schema.positions.c.strategy == strategy,
                            schema.positions.c.code == code,
                            schema.positions.c.position_cycle_id == cycle,
                            schema.positions.c.portfolio_epoch_id == epoch,
                            schema.positions.c.status == "OPEN",
                        )
                    )
                ).mappings().all()
            ]
        if len(exact_rows) > 1:
            logger.error(
                "[KR_BROKER_TRUTH][BUY_RETRY][BLOCK] code=%s order_id=%s reason=MULTIPLE_EXACT_OPEN_LIFECYCLES rows=%s",
                code,
                order_id,
                len(exact_rows),
            )
            continue

        position = exact_rows[0] if exact_rows else None
        entry_json = base._json_dict((position or {}).get("entry_meta_json"))
        legacy_pos_meta = base._json_dict((position or {}).get("position_meta"))
        applied_map = base._json_dict(entry_json.get(_BUY_APPLIED_KEY))
        if not applied_map:
            # Compatibility with the earlier PR125 marker location.
            applied_map = base._json_dict(legacy_pos_meta.get(review._APPLIED_META_KEY))
        applied = base._json_dict(applied_map.get(str(order_id)))
        applied_qty = base._qty(applied.get("qty"))
        applied_notional = base._px(applied.get("notional"))
        applied_fee = base._px(applied.get("fee"))
        applied_tax = base._px(applied.get("tax"))

        if position and not applied:
            baseline = _baseline_qty(order)
            current_qty = int(position.get("qty") or 0)
            if baseline is None or current_qty != baseline:
                logger.error(
                    "[KR_BROKER_TRUTH][BUY_RETRY][REVIEW_REQUIRED] code=%s order_id=%s current_qty=%s baseline_qty=%s cumulative_fill_qty=%s reason=MISSING_APPLICATION_WATERMARK",
                    code,
                    order_id,
                    current_qty,
                    baseline,
                    cumulative_qty,
                )
                continue

        delta_qty = cumulative_qty - applied_qty
        delta_notional = cumulative_notional - applied_notional
        delta_fee = cumulative_fee - applied_fee
        delta_tax = cumulative_tax - applied_tax
        if delta_qty < 0 or delta_notional < -1e-9 or delta_fee < -1e-9 or delta_tax < -1e-9:
            logger.error(
                "[KR_BROKER_TRUTH][BUY_RETRY][BLOCK] code=%s order_id=%s reason=CUMULATIVE_REGRESSION cumulative=%s applied=%s",
                code,
                order_id,
                cumulative_qty,
                applied_qty,
            )
            continue
        if delta_qty == 0:
            continue
        if delta_notional <= 0:
            logger.error(
                "[KR_BROKER_TRUTH][BUY_RETRY][BLOCK] code=%s order_id=%s reason=NOTIONAL_MISSING delta_qty=%s",
                code,
                order_id,
                delta_qty,
            )
            continue

        entry_meta, entry_plan = base._entry_contract(order)
        new_applied_map = dict(applied_map)
        new_applied_map[str(order_id)] = {
            "qty": cumulative_qty,
            "notional": cumulative_notional,
            "fee": cumulative_fee,
            "tax": cumulative_tax,
        }
        # apply_fill merges entry_meta_json in the same transaction as the
        # quantity/cost update (or position insert). The watermark therefore
        # cannot be committed separately from position accounting.
        entry_meta = dict(entry_meta)
        entry_meta[_BUY_APPLIED_KEY] = new_applied_map
        filled_at = (
            attributed[-1].get("filled_at")
            or order.get("acked_at")
            or order.get("submitted_at")
            or now_kst()
        )
        PositionsRepo(engine).apply_fill(
            env=env,
            strategy=strategy,
            sid=int(order.get("sid") or 1),
            mode=int(order.get("mode") or 1),
            code=code,
            market=order.get("market"),
            side="BUY",
            qty=delta_qty,
            price=delta_notional / delta_qty,
            fee=max(delta_fee, 0.0),
            tax=max(delta_tax, 0.0),
            filled_at=filled_at,
            entry_meta=entry_meta,
            entry_exit_plan=entry_plan,
            portfolio_epoch_id=str(epoch),
            position_cycle_id=str(cycle),
            order_id=str(order_id),
        )

        # Verify the atomic marker made it to the exact lifecycle. If it did not,
        # fail loud; a later tick will retry already-owned fills rather than lose
        # the execution silently.
        with engine.connect() as conn:
            verified = conn.execute(
                sa.select(schema.positions.c.entry_meta_json).where(
                    sa.and_(
                        schema.positions.c.env == env,
                        schema.positions.c.strategy == strategy,
                        schema.positions.c.code == code,
                        schema.positions.c.position_cycle_id == cycle,
                        schema.positions.c.portfolio_epoch_id == epoch,
                        schema.positions.c.status == "OPEN",
                    )
                ).limit(1)
            ).scalar()
        verified_map = base._json_dict(base._json_dict(verified).get(_BUY_APPLIED_KEY))
        verified_applied = base._json_dict(verified_map.get(str(order_id)))
        if base._qty(verified_applied.get("qty")) != cumulative_qty:
            logger.error(
                "[KR_BROKER_TRUTH][BUY_RETRY][VERIFY_FAIL] code=%s order_id=%s expected_qty=%s marker=%s",
                code,
                order_id,
                cumulative_qty,
                verified_applied,
            )
            continue
        promoted += 1
        logger.warning(
            "[KR_BROKER_TRUTH][BUY_RETRY][OK] code=%s order_id=%s cumulative_qty=%s delta_qty=%s status=%s",
            code,
            order_id,
            cumulative_qty,
            delta_qty,
            canonical_status,
        )

    return {"linked_fills": linked, "positions_promoted": promoted}


def install_final_review_guards() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _install_order_response_merge_guard()
    # broker_truth_sell_fixes delegates BUY work through this module attribute at
    # runtime, so replacing it here upgrades the active linker without touching
    # strategy decisions or the SELL atomic path.
    review._link_unowned_daily_fills_fixed = _link_buy_fills_retry_safe
    _INSTALLED = True
    logger.info("[KR_BROKER_TRUTH][FINAL_REVIEW_GUARDS][INSTALLED]")
