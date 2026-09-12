"""KR broker-truth reconciliation hardening.

Installed only from the Korean execution entry point.  The code closes execution
ledger gaps proven by the 2026-08-19, 2026-08-24 and 2026-09-11 incidents without
changing strategy thresholds, sizing, ownership, or exit philosophy.

Rules:
- broker ACK is never treated as a fill by itself;
- a balance-delta fill is promoted only when the durable order already carries
  a pre-order baseline (the base reconciler enforces that contract);
- a confirmed fill is linked back to its exact durable order/cycle/epoch before
  it is allowed to create or repair a SYSTEM position;
- POLICY_MISSING is repaired only from a unique lifecycle whose net linked fills
  exactly equal the fresh KIS holding quantity and whose BUY order contains an
  explicit entry/exit policy.  Otherwise it remains blocked for review.
"""
from __future__ import annotations

from datetime import datetime
import functools
import logging
from typing import Any

import sqlalchemy as sa

from trader.db.repos import FillsRepo, OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.time_utils import now_kst

logger = logging.getLogger(__name__)
_INSTALLED = False


def _json_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _normalize_code(value: Any) -> str:
    return str(value or "").lstrip("A").zfill(6)


def _qty(value: Any) -> int:
    try:
        return int(float(value or 0))
    except Exception:
        return 0


def _px(value: Any) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    text = str(value).strip()
    return text or None


def _date_of(value: Any):
    if value is None:
        return None
    if hasattr(value, "date"):
        try:
            return value.date()
        except Exception:
            pass
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
    except Exception:
        return None


def _holdings_index(rows: list[dict] | None) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for raw in rows or []:
        row = dict(raw or {})
        code = _normalize_code(row.get("pdno") or row.get("code") or row.get("stck_shrn_iscd"))
        if not code or code == "000000":
            continue
        result[code] = {
            "qty": _qty(row.get("hldg_qty") or row.get("qty")),
            "avg": _px(row.get("pchs_avg_pric") or row.get("pchs_avg_price") or row.get("avg_price")),
            "raw": row,
        }
    return result


def _entry_contract(order: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    request = _json_dict(order.get("request_json"))
    meta = _json_dict(request.get("entry_meta")) or _json_dict(order.get("entry_meta_json"))
    plan = _json_dict(request.get("entry_exit_plan")) or _json_dict(request.get("entry_plan"))
    if plan:
        risk = _json_dict(plan.get("risk_plan"))
        time_plan = _json_dict(plan.get("time_plan"))
        for key in (
            "entry_thesis",
            "entry_style_selected",
            "entry_reason",
            "trade_horizon",
            "exit_policy_family",
            "eod_action",
            "force_eod_close",
            "policy_source",
            "policy_version",
        ):
            if plan.get(key) is not None and meta.get(key) is None:
                meta[key] = plan.get(key)
        if risk.get("initial_stop") is not None:
            meta.setdefault("initial_stop_price", risk.get("initial_stop"))
        if risk.get("risk_R") is not None:
            meta.setdefault("initial_risk_r", risk.get("risk_R"))
        if time_plan.get("max_trading_days") is not None:
            meta.setdefault("max_trading_days", time_plan.get("max_trading_days"))
    return meta, plan


def _apply_proven_buy_fill_to_position(*, engine, order: dict[str, Any], fill: dict[str, Any]) -> bool:
    """Apply a proven BUY fill to its exact order lifecycle once."""
    if str(fill.get("side") or "").upper() != "BUY":
        return False
    order_id = str(order.get("order_id") or "")
    cycle_id = str(order.get("position_cycle_id") or "")
    epoch_id = str(order.get("portfolio_epoch_id") or "")
    code = _normalize_code(order.get("code"))
    if not order_id or not cycle_id or not epoch_id or not code:
        logger.error(
            "[KR_BROKER_TRUTH][POSITION_PROMOTE][BLOCK] code=%s reason=ORDER_PROVENANCE_MISSING",
            code,
        )
        return False

    env = str(order.get("env") or "")
    strategy = str(order.get("strategy") or "")
    schema = schema_for_engine(engine)
    with engine.connect() as conn:
        same_cycle = conn.execute(
            sa.select(schema.positions.c.position_id).where(
                sa.and_(
                    schema.positions.c.env == env,
                    schema.positions.c.strategy == strategy,
                    schema.positions.c.code == code,
                    schema.positions.c.position_cycle_id == order.get("position_cycle_id"),
                    schema.positions.c.portfolio_epoch_id == order.get("portfolio_epoch_id"),
                    schema.positions.c.status == "OPEN",
                )
            ).limit(1)
        ).scalar()
    if same_cycle:
        return False

    entry_meta, entry_plan = _entry_contract(order)
    filled_at = fill.get("filled_at") or order.get("acked_at") or order.get("submitted_at") or now_kst()
    qty = _qty(fill.get("qty"))
    price = _px(fill.get("price"))
    if qty <= 0 or price <= 0:
        logger.error(
            "[KR_BROKER_TRUTH][POSITION_PROMOTE][BLOCK] code=%s reason=INVALID_FILL qty=%s price=%s",
            code,
            qty,
            price,
        )
        return False

    repo = PositionsRepo(engine)
    repo.apply_fill(
        env=env,
        strategy=strategy,
        sid=int(order.get("sid") or 1),
        mode=int(order.get("mode") or 1),
        code=code,
        market=order.get("market"),
        side="BUY",
        qty=qty,
        price=price,
        fee=_px(fill.get("fee")),
        tax=_px(fill.get("tax")),
        filled_at=filled_at,
        entry_meta=entry_meta,
        entry_exit_plan=entry_plan,
        portfolio_epoch_id=epoch_id,
        position_cycle_id=cycle_id,
        order_id=order_id,
    )
    repo.update_position_fields(
        env=env,
        strategy=strategy,
        sid=int(order.get("sid") or 1),
        mode=int(order.get("mode") or 1),
        code=code,
        fields={"entry_ts": _iso(filled_at)},
    )
    logger.warning(
        "[KR_BROKER_TRUTH][POSITION_PROMOTE][OK] code=%s qty=%s cycle=%s source=proven_buy_fill",
        code,
        qty,
        cycle_id,
    )
    return True


def _promote_positions_for_codes(*, engine, env: str, codes: list[str]) -> int:
    orders_repo = OrdersRepo(engine)
    fills_repo = FillsRepo(engine)
    applied = 0
    for code in sorted({_normalize_code(c) for c in codes if c}):
        fills = fills_repo.list_today_fills(env, code=code, side="BUY")
        if not fills:
            continue
        orders = orders_repo.list_today_orders(env, code=code, side="BUY", status_exclude=())
        orders_by_id = {str(row.get("order_id")): dict(row) for row in orders if row.get("order_id")}
        for fill in fills:
            order = orders_by_id.get(str(fill.get("order_id") or ""))
            if order and _apply_proven_buy_fill_to_position(engine=engine, order=order, fill=dict(fill)):
                applied += 1
    return applied


def _link_unowned_daily_fills(*, engine, env: str, strategy: str) -> dict[str, int]:
    """Attach daily-ccld fills to the durable PB1 order and exact lifecycle."""
    schema = schema_for_engine(engine)
    fills_repo = FillsRepo(engine)
    linked = 0
    positions_promoted = 0
    for fill in fills_repo.list_today_fills(env):
        if fill.get("order_id") or not fill.get("kis_odno"):
            continue
        kis_odno = str(fill.get("kis_odno") or "").strip()
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
            fill_qty = _qty(fill.get("qty"))
            order_qty = _qty(order.get("qty"))
            canonical_status = "FILLED" if order_qty > 0 and fill_qty >= order_qty else "PARTIAL_FILLED"
            conn.execute(
                sa.update(schema.fills)
                .where(schema.fills.c.fill_id == fill.get("fill_id"))
                .values(
                    order_id=order.get("order_id"),
                    position_cycle_id=order.get("position_cycle_id"),
                    portfolio_epoch_id=order.get("portfolio_epoch_id"),
                )
            )
            conn.execute(
                sa.update(schema.orders)
                .where(schema.orders.c.order_id == order.get("order_id"))
                .values(status=canonical_status, updated_at=sa.func.now())
            )
        repaired_fill = dict(fill)
        repaired_fill.update(
            {
                "order_id": order.get("order_id"),
                "position_cycle_id": order.get("position_cycle_id"),
                "portfolio_epoch_id": order.get("portfolio_epoch_id"),
            }
        )
        linked += 1
        if str(repaired_fill.get("side") or "").upper() == "BUY":
            if _apply_proven_buy_fill_to_position(engine=engine, order=order, fill=repaired_fill):
                positions_promoted += 1
        logger.warning(
            "[KR_BROKER_TRUTH][FILL_LINK][OK] code=%s kis_odno=%s order_id=%s status=%s",
            _normalize_code(fill.get("code")),
            kis_odno,
            order.get("order_id"),
            canonical_status,
        )
    return {"linked_fills": linked, "positions_promoted": positions_promoted}


def _recover_proven_policy_positions(
    *,
    engine,
    env: str,
    strategy: str,
    holdings_rows: list[dict] | None,
) -> dict[str, Any]:
    """Recover POLICY_MISSING only from one exact net-fill lifecycle proof."""
    schema = schema_for_engine(engine)
    kis = _holdings_index(holdings_rows)
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

    for position in positions:
        code = _normalize_code(position.get("code"))
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
            q = _qty(row.get("qty"))
            group["net"] += q if side == "BUY" else -q if side == "SELL" else 0
            if side == "BUY":
                request = _json_dict(row.get("order_request_json"))
                meta = _json_dict(request.get("entry_meta")) or _json_dict(row.get("order_entry_meta_json"))
                plan = _json_dict(request.get("entry_exit_plan"))
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
            meta, plan = _entry_contract(group["buy_order"])
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
        position_meta = _json_dict(position.get("position_meta"))
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
            "entry_ts": _iso(group.get("first_buy_at")),
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
        PositionsRepo(engine).update_position_fields(
            env=env,
            strategy=strategy,
            sid=int(position.get("sid") or 1),
            mode=int(position.get("mode") or 1),
            code=code,
            fields={key: value for key, value in fields.items() if value is not None},
        )
        recovered.append(code)
        logger.warning(
            "[KR_BROKER_TRUTH][POLICY_RECOVER][OK] code=%s broker_qty=%s family=%s horizon=%s source_cycle=%s",
            code,
            broker_qty,
            fields.get("exit_policy_family"),
            fields.get("trade_horizon"),
            source_cycle,
        )

    return {"recovered": recovered, "review_required": review}


def _health_after_reconcile(*, engine, env: str, strategy: str, holdings_rows: list[dict] | None) -> dict[str, Any]:
    """Return RED evidence that must not be hidden after reconciliation."""
    schema = schema_for_engine(engine)
    kis = _holdings_index(holdings_rows)
    mismatches: list[dict[str, Any]] = []
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
    for row in positions:
        code = _normalize_code(row.get("code"))
        if code not in kis:
            continue
        db_qty = _qty(row.get("qty"))
        broker_qty = int(kis[code]["qty"])
        if broker_qty > 0 and db_qty != broker_qty:
            mismatches.append({"code": code, "db_qty": db_qty, "kis_qty": broker_qty})

    today = now_kst().date()
    stale_orders: list[dict[str, Any]] = []
    try:
        for order in OrdersRepo(engine).get_open_orders(env, include_stale=True) or []:
            if str(order.get("strategy") or "") != strategy:
                continue
            created_day = _date_of(order.get("created_at"))
            if created_day is not None and created_day < today:
                stale_orders.append(
                    {
                        "order_id": str(order.get("order_id") or ""),
                        "code": _normalize_code(order.get("code")),
                        "side": str(order.get("side") or ""),
                        "status": str(order.get("status") or ""),
                        "created_at": _iso(order.get("created_at")),
                    }
                )
    except Exception as exc:
        logger.exception("[KR_BROKER_TRUTH][HEALTH][STALE_ORDER_SCAN_FAIL] err=%s", exc)

    if mismatches:
        logger.error("[KR_BROKER_TRUTH][HEALTH][QTY_MISMATCH] count=%s rows=%s", len(mismatches), mismatches)
    if stale_orders:
        logger.error("[KR_BROKER_TRUTH][HEALTH][STALE_OPEN_ORDER] count=%s rows=%s", len(stale_orders), stale_orders)
    return {
        "qty_mismatch_count": len(mismatches),
        "qty_mismatches": mismatches,
        "stale_open_order_count": len(stale_orders),
        "stale_open_orders": stale_orders,
    }


def _install_reconcile_guards() -> None:
    import trader.reconcile_kis as rk

    if not getattr(rk, "_kr_broker_truth_promote_guard_installed", False):
        original_promote = rk._promote_open_buy_orders_from_holdings

        @functools.wraps(original_promote)
        def promote_guarded(*args: Any, **kwargs: Any):
            result = original_promote(*args, **kwargs)
            try:
                orders_repo = kwargs.get("orders_repo")
                engine = getattr(orders_repo, "engine", None)
                env = str(kwargs.get("env") or "")
                codes = list((result or {}).get("codes") or [])
                if engine is not None and codes:
                    result["positions_promoted"] = _promote_positions_for_codes(
                        engine=engine,
                        env=env,
                        codes=codes,
                    )
            except Exception as exc:
                logger.exception("[KR_BROKER_TRUTH][POSITION_PROMOTE][FAIL] err=%s", exc)
            return result

        rk._promote_open_buy_orders_from_holdings = promote_guarded
        rk._kr_broker_truth_promote_guard_installed = True

    if not getattr(rk, "_kr_broker_truth_daily_guard_installed", False):
        original_today = rk.reconcile_today

        @functools.wraps(original_today)
        def today_guarded(*args: Any, **kwargs: Any):
            result = original_today(*args, **kwargs)
            try:
                engine = kwargs.get("engine")
                ctx = kwargs.get("ctx")
                env = str(getattr(ctx, "env", "") or "")
                strategy = str(getattr(ctx, "strategy", "") or "")
                if engine is not None and env and strategy:
                    repair = _link_unowned_daily_fills(engine=engine, env=env, strategy=strategy)
                    result["linked_fills"] = int(repair.get("linked_fills") or 0)
                    result["positions_promoted_from_daily"] = int(repair.get("positions_promoted") or 0)
            except Exception as exc:
                logger.exception("[KR_BROKER_TRUTH][DAILY_CCLD_LINK][FAIL] err=%s", exc)
            return result

        rk.reconcile_today = today_guarded
        rk._kr_broker_truth_daily_guard_installed = True


def _post_pb1_tick_reconcile(engine_obj: Any) -> None:
    if bool(getattr(engine_obj, "dry_run", True)):
        return
    if not bool(getattr(engine_obj, "intended_live", False)):
        return
    env = str(getattr(engine_obj, "env", "") or "").strip().lower()
    if env not in {"practice", "real", "live"}:
        return
    db_engine = getattr(engine_obj, "engine", None)
    kis = getattr(engine_obj, "kis", None) or getattr(engine_obj, "kis_api", None)
    if db_engine is None or kis is None:
        return

    if hasattr(kis, "invalidate_balance_cache"):
        try:
            kis.invalidate_balance_cache(reason="post_pb1_tick_broker_truth")
        except Exception:
            pass
    snapshot = kis.get_balance_cached(force=True) if hasattr(kis, "get_balance_cached") else kis.get_balance()
    if not isinstance(snapshot, dict):
        raise RuntimeError("KR_BROKER_TRUTH_BALANCE_INVALID")

    import trader.reconcile_kis as rk

    strategy = str(getattr(engine_obj, "STRATEGY_NAME", "pb1_pullback_close") or "pb1_pullback_close")
    result = rk.reconcile_kis(
        engine=db_engine,
        kis=kis,
        env=env,
        run_id=getattr(engine_obj, "run_id", None),
        strategy=strategy,
        tick_ts=now_kst(),
        balance_snapshot=snapshot,
    )
    holdings_rows = snapshot.get("output1") or []
    policy = _recover_proven_policy_positions(
        engine=db_engine,
        env=env,
        strategy=strategy,
        holdings_rows=holdings_rows,
    )
    health = _health_after_reconcile(
        engine=db_engine,
        env=env,
        strategy=strategy,
        holdings_rows=holdings_rows,
    )
    review_count = len(policy.get("review_required") or [])
    red = bool(health.get("qty_mismatch_count") or health.get("stale_open_order_count") or review_count)
    payload = getattr(engine_obj, "_run_summary_payload", None)
    if isinstance(payload, dict):
        payload.update(
            {
                "broker_truth_reconcile_ran": 1,
                "broker_truth_orders": int(result.get("orders") or 0),
                "broker_truth_fills": int(result.get("fills") or 0),
                "broker_truth_promoted_fills": int(result.get("promoted_fills") or 0),
                "broker_truth_linked_fills": int(result.get("linked_fills") or 0),
                "broker_truth_policy_recovered": list(policy.get("recovered") or []),
                "broker_truth_policy_review_required": list(policy.get("review_required") or []),
                "broker_truth_qty_mismatch_count": int(health.get("qty_mismatch_count") or 0),
                "broker_truth_stale_open_order_count": int(health.get("stale_open_order_count") or 0),
                "broker_truth_health_status": "RED" if red else "OK",
            }
        )
    logger.info(
        "[KR_BROKER_TRUTH][POST_TICK][DONE] orders=%s fills=%s promoted_fills=%s linked_fills=%s policy_recovered=%s policy_review=%s qty_mismatch=%s stale_open=%s health=%s",
        result.get("orders"),
        result.get("fills"),
        result.get("promoted_fills"),
        result.get("linked_fills"),
        len(policy.get("recovered") or []),
        review_count,
        health.get("qty_mismatch_count"),
        health.get("stale_open_order_count"),
        "RED" if red else "OK",
    )


def _install_engine_post_tick_guard() -> None:
    from trader.pb1_engine import PB1Engine

    if getattr(PB1Engine, "_kr_broker_truth_post_tick_installed", False):
        return
    original_run = PB1Engine.run

    @functools.wraps(original_run)
    def run_guarded(self: Any, *args: Any, **kwargs: Any):
        result = original_run(self, *args, **kwargs)
        try:
            _post_pb1_tick_reconcile(self)
        except Exception as exc:
            # Broker may already have accepted an order, so do not crash trading
            # after the fact.  Persist explicit RED summary evidence for retry.
            logger.exception("[KR_BROKER_TRUTH][POST_TICK][FAIL] err=%s", exc)
            payload = getattr(self, "_run_summary_payload", None)
            if isinstance(payload, dict):
                payload["broker_truth_reconcile_failed"] = 1
                payload["broker_truth_reconcile_error"] = str(exc)
                payload["broker_truth_health_status"] = "RED"
        return result

    PB1Engine.run = run_guarded
    PB1Engine._kr_broker_truth_post_tick_installed = True


def install_kr_broker_truth_runtime_guards() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _install_reconcile_guards()
    _install_engine_post_tick_guard()
    _INSTALLED = True
    logger.info("[KR_BROKER_TRUTH][GUARD][INSTALLED] scope=KR")
