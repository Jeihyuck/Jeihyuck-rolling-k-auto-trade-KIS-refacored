"""KR broker-truth reconciliation hardening.

This module is installed only from the Korean execution entry point.  It closes
three production gaps proven by the 2026-08-19, 2026-08-24 and 2026-09-11 logs:

* an accepted KIS order may be filled even when daily-ccld is temporarily
  unavailable;
* a holdings-delta promoted BUY fill must create the SYSTEM position with the
  exact order cycle/epoch and entry policy metadata before generic KIS holding
  restoration can create an IMPORTED/POLICY_MISSING row;
* daily-ccld fills must be linked to their durable order_id so session metrics
  cannot report fills=0 when the broker actually filled the order.

The recovery is proof based.  It never invents a strategy/policy for an
unproven legacy holding and it does not change any TP/SL or sizing policy.
"""
from __future__ import annotations

from datetime import datetime
import functools
import logging
import os
from typing import Any

import sqlalchemy as sa

from trader.db.repos import OrdersRepo, FillsRepo, PositionsRepo
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
    meta = _json_dict(request.get("entry_meta"))
    if not meta:
        meta = _json_dict(order.get("entry_meta_json"))
    plan = _json_dict(request.get("entry_exit_plan"))
    if not plan:
        plan = _json_dict(request.get("entry_plan"))
    # EntryExitPlan is authoritative for horizon/exit family.  Merge only known
    # policy fields into entry_meta; never infer a policy from symbol/name.
    if plan:
        risk = _json_dict(plan.get("risk_plan"))
        time_plan = _json_dict(plan.get("time_plan"))
        for key in (
            "entry_thesis", "entry_style_selected", "entry_reason",
            "trade_horizon", "exit_policy_family", "eod_action",
            "force_eod_close", "policy_source", "policy_version",
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


def _apply_proven_buy_fill_to_position(
    *,
    engine,
    order: dict[str, Any],
    fill: dict[str, Any],
) -> bool:
    """Create the SYSTEM position for a newly proven BUY fill exactly once."""
    if str(fill.get("side") or "").upper() != "BUY":
        return False
    order_id = str(order.get("order_id") or "")
    cycle_id = str(order.get("position_cycle_id") or "")
    epoch_id = str(order.get("portfolio_epoch_id") or "")
    if not order_id or not cycle_id or not epoch_id:
        logger.error(
            "[KR_BROKER_TRUTH][POSITION_PROMOTE][BLOCK] code=%s reason=ORDER_PROVENANCE_MISSING",
            _normalize_code(order.get("code")),
        )
        return False

    schema = schema_for_engine(engine)
    env = str(order.get("env") or "")
    strategy = str(order.get("strategy") or "")
    code = _normalize_code(order.get("code"))
    with engine.connect() as conn:
        same_cycle = conn.execute(
            sa.select(schema.positions.c.position_id).where(sa.and_(
                schema.positions.c.env == env,
                schema.positions.c.strategy == strategy,
                schema.positions.c.code == code,
                schema.positions.c.position_cycle_id == order.get("position_cycle_id"),
                schema.positions.c.portfolio_epoch_id == order.get("portfolio_epoch_id"),
                schema.positions.c.status == "OPEN",
            )).limit(1)
        ).scalar()
    if same_cycle:
        # The fill lifecycle was already applied.  Do not double count.
        return False

    entry_meta, entry_plan = _entry_contract(order)
    filled_at = fill.get("filled_at") or order.get("acked_at") or order.get("submitted_at") or now_kst()
    repo = PositionsRepo(engine)
    repo.apply_fill(
        env=env,
        strategy=strategy,
        sid=int(order.get("sid") or 1),
        mode=int(order.get("mode") or 1),
        code=code,
        market=order.get("market"),
        side="BUY",
        qty=_qty(fill.get("qty")),
        price=_px(fill.get("price")),
        fee=_px(fill.get("fee")),
        tax=_px(fill.get("tax")),
        filled_at=filled_at,
        entry_meta=entry_meta,
        entry_exit_plan=entry_plan,
        portfolio_epoch_id=epoch_id,
        position_cycle_id=cycle_id,
        order_id=order_id,
    )
    # Older apply_fill implementations populate opened_at but can leave entry_ts
    # null.  Entry age must be explicit and must never silently become 0 days.
    repo.update_position_fields(
        env=env,
        strategy=strategy,
        sid=int(order.get("sid") or 1),
        mode=int(order.get("mode") or 1),
        code=code,
        fields={"entry_ts": filled_at},
    )
    logger.warning(
        "[KR_BROKER_TRUTH][POSITION_PROMOTE][OK] code=%s qty=%s cycle=%s source=proven_buy_fill",
        code, _qty(fill.get("qty")), cycle_id,
    )
    return True


def _promote_positions_for_codes(
    *,
    engine,
    env: str,
    codes: list[str],
) -> int:
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
            if not order:
                continue
            if _apply_proven_buy_fill_to_position(engine=engine, order=order, fill=dict(fill)):
                applied += 1
    return applied


def _link_unowned_daily_fills(*, engine, env: str, strategy: str) -> dict[str, int]:
    """Attach daily-ccld fills to the durable order and canonicalize order state.

    `pb1_runner._write_session_result_file` intentionally ignores fills whose
    order_id is not one of the session's durable orders.  Therefore a broker fill
    saved with order_id=NULL would otherwise be reported as fills=0.
    """
    schema = schema_for_engine(engine)
    fills_repo = FillsRepo(engine)
    linked = 0
    positions_promoted = 0
    fills = fills_repo.list_today_fills(env)
    for fill in fills:
        if fill.get("order_id") or not fill.get("kis_odno"):
            continue
        kis_odno = str(fill.get("kis_odno") or "").strip()
        if not kis_odno:
            continue
        with engine.begin() as conn:
            order = conn.execute(
                sa.select(schema.orders).where(sa.and_(
                    schema.orders.c.env == env,
                    schema.orders.c.strategy == strategy,
                    sa.or_(
                        schema.orders.c.kis_odno == kis_odno,
                        schema.orders.c.broker_order_id == kis_odno,
                    ),
                )).order_by(schema.orders.c.created_at.desc()).limit(1)
            ).mappings().first()
            if not order:
                continue
            order = dict(order)
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
                    updated_at=sa.func.now(),
                )
            )
            conn.execute(
                sa.update(schema.orders)
                .where(schema.orders.c.order_id == order.get("order_id"))
                .values(status=canonical_status, updated_at=sa.func.now())
            )
        repaired_fill = dict(fill)
        repaired_fill.update({
            "order_id": order.get("order_id"),
            "position_cycle_id": order.get("position_cycle_id"),
            "portfolio_epoch_id": order.get("portfolio_epoch_id"),
        })
        linked += 1
        if str(repaired_fill.get("side") or "").upper() == "BUY":
            if _apply_proven_buy_fill_to_position(engine=engine, order=order, fill=repaired_fill):
                positions_promoted += 1
        logger.warning(
            "[KR_BROKER_TRUTH][FILL_LINK][OK] code=%s kis_odno=%s order_id=%s status=%s",
            _normalize_code(fill.get("code")), kis_odno, order.get("order_id"), canonical_status,
        )
    return {"linked_fills": linked, "positions_promoted": positions_promoted}


def _recover_proven_policy_positions(
    *,
    engine,
    env: str,
    strategy: str,
    holdings_rows: list[dict] | None,
) -> dict[str, Any]:
    """Recover POLICY_MISSING only from a unique net-fill lifecycle proof.

    A candidate lifecycle is acceptable only when its linked BUY/SELL fills net
    exactly to the fresh KIS holding quantity and its BUY order contains an
    explicit entry/exit contract.  Ambiguous or absent evidence remains
    POLICY_MISSING_HARD_STOP_ONLY and is returned for operator review.
    """
    schema = schema_for_engine(engine)
    kis = _holdings_index(holdings_rows)
    recovered: list[str] = []
    review: list[str] = []
    with engine.connect() as conn:
        positions = [dict(r) for r in conn.execute(
            sa.select(schema.positions).where(sa.and_(
                schema.positions.c.env == env,
                schema.positions.c.strategy == strategy,
                schema.positions.c.status == "OPEN",
                schema.positions.c.qty > 0,
            ))
        ).mappings().all()]

    for position in positions:
        code = _normalize_code(position.get("code"))
        broker_qty = int((kis.get(code) or {}).get("qty") or 0)
        if broker_qty <= 0:
            continue
        family = str(position.get("exit_policy_family") or "").strip().upper()
        if family and family != "POLICY_MISSING":
            continue

        with engine.connect() as conn:
            rows = [dict(r) for r in conn.execute(
                sa.select(
                    schema.fills,
                    schema.orders.c.strategy.label("order_strategy"),
                    schema.orders.c.entry_meta_json.label("order_entry_meta_json"),
                    schema.orders.c.request_json.label("order_request_json"),
                    schema.orders.c.position_cycle_id.label("order_cycle_id"),
                    schema.orders.c.portfolio_epoch_id.label("order_epoch_id"),
                ).join(schema.orders, schema.orders.c.order_id == schema.fills.c.order_id)
                .where(sa.and_(
                    schema.fills.c.env == env,
                    schema.fills.c.code == code,
                    schema.orders.c.env == env,
                    schema.orders.c.strategy == strategy,
                    schema.fills.c.position_cycle_id.is_not(None),
                    schema.fills.c.portfolio_epoch_id.is_not(None),
                ))
                .order_by(schema.fills.c.filled_at.asc())
            ).mappings().all()]

        groups: dict[tuple[str, str], dict[str, Any]] = {}
        for row in rows:
            cycle = str(row.get("position_cycle_id") or row.get("order_cycle_id") or "")
            epoch = str(row.get("portfolio_epoch_id") or row.get("order_epoch_id") or "")
            if not cycle or not epoch:
                continue
            group = groups.setdefault((cycle, epoch), {"net": 0, "rows": [], "buy_order": None, "first_buy_at": None})
            side = str(row.get("side") or "").upper()
            q = _qty(row.get("qty"))
            group["net"] += q if side == "BUY" else -q if side == "SELL" else 0
            group["rows"].append(row)
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
                code, broker_qty, len(candidates),
            )
            continue

        source_cycle, source_epoch, group, meta, plan = candidates[0]
        position_meta = _json_dict(position.get("position_meta"))
        position_meta.update({
            "provenance_verified": True,
            "recovered_from_cycle_id": source_cycle,
            "recovered_from_epoch_id": source_epoch,
            "holding_age_unknown": False,
            "policy_recovery_source": "unique_net_fill_lifecycle",
        })
        entry_ts = group.get("first_buy_at")
        fields = {
            "position_origin": "RECOVERY",
            "position_meta": position_meta,
            "entry_ts": entry_ts,
            "entry_reason": plan.get("entry_reason") or meta.get("entry_reason"),
            "entry_style_selected": plan.get("entry_style_selected") or meta.get("entry_style_selected"),
            "entry_thesis": plan.get("entry_thesis") or meta.get("entry_thesis"),
            "trade_horizon": plan.get("trade_horizon") or meta.get("trade_horizon"),
            "exit_policy_family": plan.get("exit_policy_family") or meta.get("exit_policy_family"),
            "eod_action": plan.get("eod_action") or meta.get("eod_action"),
            "force_eod_close": bool(plan.get("force_eod_close") if plan.get("force_eod_close") is not None else meta.get("force_eod_close") or False),
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
            fields={k: v for k, v in fields.items() if v is not None},
        )
        recovered.append(code)
        logger.warning(
            "[KR_BROKER_TRUTH][POLICY_RECOVER][OK] code=%s broker_qty=%s family=%s horizon=%s source_cycle=%s",
            code, broker_qty, fields.get("exit_policy_family"), fields.get("trade_horizon"), source_cycle,
        )

    return {"recovered": recovered, "review_required": review}


def _health_after_reconcile(*, engine, env: str, strategy: str, holdings_rows: list[dict] | None) -> dict[str, Any]:
    schema = schema_for_engine(engine)
    kis = _holdings_index(holdings_rows)
    mismatches: list[dict[str, Any]] = []
    with engine.connect() as conn:
        rows = [dict(r) for r in conn.execute(
            sa.select(schema.positions).where(sa.and_(
                schema.positions.c.env == env,
                schema.positions.c.strategy == strategy,
                schema.positions.c.status == "OPEN",
                schema.positions.c.qty > 0,
            ))
        ).mappings().all()]
    for row in rows:
        code = _normalize_code(row.get("code"))
        if code not in kis:
            continue
        db_qty = _qty(row.get("qty"))
        broker_qty = int(kis[code]["qty"])
        if broker_qty > 0 and db_qty != broker_qty:
            mismatches.append({"code": code, "db_qty": db_qty, "kis_qty": broker_qty})
    if mismatches:
        logger.error("[KR_BROKER_TRUTH][HEALTH][QTY_MISMATCH] count=%s rows=%s", len(mismatches), mismatches)
    return {"qty_mismatch_count": len(mismatches), "qty_mismatches": mismatches}


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
                    applied = _promote_positions_for_codes(engine=engine, env=env, codes=codes)
                    result["positions_promoted"] = applied
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
    if hasattr(kis, "get_balance_cached"):
        snapshot = kis.get_balance_cached(force=True)
    else:
        snapshot = kis.get_balance()
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
    policy = _recover_proven_policy_positions(
        engine=db_engine,
        env=env,
        strategy=strategy,
        holdings_rows=snapshot.get("output1") or [],
    )
    health = _health_after_reconcile(
        engine=db_engine,
        env=env,
        strategy=strategy,
        holdings_rows=snapshot.get("output1") or [],
    )
    logger.info(
        "[KR_BROKER_TRUTH][POST_TICK][DONE] orders=%s fills=%s promoted_fills=%s linked_fills=%s policy_recovered=%s policy_review=%s qty_mismatch=%s",
        result.get("orders"), result.get("fills"), result.get("promoted_fills"), result.get("linked_fills"),
        len(policy.get("recovered") or []), len(policy.get("review_required") or []), health.get("qty_mismatch_count"),
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
            # Trading must not crash after the broker may already have accepted
            # an order.  Leave explicit RED evidence and retry on the next tick.
            logger.exception("[KR_BROKER_TRUTH][POST_TICK][FAIL] err=%s", exc)
            try:
                payload = getattr(self, "_run_summary_payload", None)
                if isinstance(payload, dict):
                    payload["broker_truth_reconcile_failed"] = 1
                    payload["broker_truth_reconcile_error"] = str(exc)
            except Exception:
                pass
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
