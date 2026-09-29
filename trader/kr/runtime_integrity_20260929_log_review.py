"""16:00 log-review fixes for the Sep-29 KR runtime integrity PR.

The full 2026-09-29 trading log exposed implementation gaps that were not
visible in the earlier DB-only incident review:

* PB1 BUYs can exhaust the shared tick budget while obtaining `/uapi/hashkey`
  before the order-cash endpoint guard is reached.  Guard the public BUY method
  before *any* hashkey/order HTTP boundary.
* `OrdersRepo.get_open_orders(..., trade_date=<date>)` enters a broken legacy
  branch (local ``datetime`` shadowing / undefined ``dtime``).  PR147's
  unresolved probe then fails closed on every tick and unnecessarily forces a
  fresh KIS balance.  Normalize date/datetime inputs to the already-supported
  ISO-string path before entering the legacy method.
* the legacy promoted-position metadata restore issues untyped CASE bind
  parameters that PostgreSQL rejects with `AmbiguousParameter`.  Replace that
  runtime helper with SQLAlchemy typed reads/updates while preserving the same
  fail-closed provenance rule: IMPORTED/RECOVERY cycles are never symbol-matched.

No PB1 strategy policy, sizing, exits, or KR Infinite ownership is changed.
"""
from __future__ import annotations

from datetime import date, datetime
import functools
import json
import logging
import math
import os
from typing import Any, Callable

import sqlalchemy as sa

from trader.db.repos import OrdersRepo
from trader.db.schema import schema_for_engine
from trader.kis_wrapper import KisAPI, KisTemporaryError, kr_tick_remaining_sec

logger = logging.getLogger(__name__)
_INSTALLED = False


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except Exception:
        return float(default)


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            decoded = json.loads(value)
            return dict(decoded) if isinstance(decoded, dict) else {}
        except Exception:
            return {}
    return {}


def _assert_buy_pipeline_budget(kis: Any) -> None:
    """Fail retry-safe before hashkey when too little shared tick budget remains."""
    remaining = kr_tick_remaining_sec(getattr(kis, "_kr_stage_deadline", None))
    minimum = max(1.0, _env_float("KR_ORDER_SUBMIT_MIN_REMAINING_SEC", 20.0))
    if math.isfinite(remaining) and remaining < minimum:
        logger.warning(
            "[KR_P0][BUY_PIPELINE_PRE_HASHKEY_DEFER] remaining_sec=%.3f "
            "min_required_sec=%.3f action=DO_NOT_START_HASHKEY_OR_ORDER_HTTP",
            max(0.0, remaining),
            minimum,
        )
        # The ambiguity classifier explicitly treats BEFORE_KIS_REQUEST as
        # pre-submit/retry-safe, so PB1 records a local ERROR rather than
        # UNRESOLVED_ACK and may re-evaluate on a later tick.
        raise KisTemporaryError("KR_TICK_DEADLINE_EXHAUSTED_BEFORE_KIS_REQUEST")


def _build_buy_pipeline_budget_guard(original: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(original)
    def guarded(self, *args: Any, **kwargs: Any):
        _assert_buy_pipeline_budget(self)
        return original(self, *args, **kwargs)

    return guarded


def _normalize_trade_date_arg(value: Any) -> Any:
    # OrdersRepo's string branch is authoritative and already supported.  Route
    # date/datetime values through it to avoid the legacy date-object branch.
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _build_get_open_orders_trade_date_guard(original: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(original)
    def guarded(self, env: str, *args: Any, **kwargs: Any):
        if "trade_date" in kwargs:
            kwargs = dict(kwargs)
            kwargs["trade_date"] = _normalize_trade_date_arg(kwargs.get("trade_date"))
        return original(self, env, *args, **kwargs)

    return guarded


def _resolved_entry_meta(position: dict[str, Any], order_meta: Any) -> dict[str, Any]:
    existing = _json_dict(position.get("entry_meta_json"))
    if existing.get("book") or existing.get("trade_horizon"):
        return existing
    candidate = _json_dict(order_meta)
    if candidate.get("book") or candidate.get("trade_horizon"):
        return candidate
    return {}


def _typed_restore_entry_meta_for_promoted_positions(
    *,
    env: str,
    strategy: str,
    engine: Any,
    orders_repo: Any,
    positions_repo: Any,
    ledger_repo: Any,
) -> int:
    """Typed equivalent of the legacy metadata restore; no symbol-only recovery."""
    del orders_repo, positions_repo, ledger_repo
    if os.getenv("PB1_RECONCILE_RESTORE_ENTRY_META", "1") != "1":
        return 0

    schema = schema_for_engine(engine)
    restored = 0
    with engine.begin() as conn:
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
            origin = str(position.get("position_origin") or "").upper()
            # Preserve the existing anti-contamination contract.  Cross-day
            # IMPORTED/RECOVERY recovery is owned by the exact-ODNO Sep-29 path.
            if origin in {"IMPORTED", "RECOVERY"}:
                continue

            cycle = position.get("position_cycle_id")
            epoch = position.get("portfolio_epoch_id")
            if not cycle or not epoch:
                continue

            order_meta = conn.execute(
                sa.select(schema.orders.c.entry_meta_json)
                .where(
                    sa.and_(
                        schema.orders.c.env == env,
                        schema.orders.c.strategy == strategy,
                        schema.orders.c.code == position.get("code"),
                        schema.orders.c.side == "BUY",
                        schema.orders.c.position_cycle_id == cycle,
                        schema.orders.c.portfolio_epoch_id == epoch,
                        schema.orders.c.entry_meta_json.is_not(None),
                    )
                )
                .order_by(schema.orders.c.created_at.desc())
                .limit(1)
            ).scalar()
            meta = _resolved_entry_meta(position, order_meta)
            if not meta:
                values: dict[str, Any] = {}
                if not str(position.get("entry_thesis") or "").strip():
                    values["entry_thesis"] = "POLICY_MISSING"
                if not str(position.get("exit_policy_family") or "").strip():
                    values["exit_policy_family"] = "POLICY_MISSING"
                if not str(position.get("policy_source") or "").strip():
                    values["policy_source"] = "missing"
                if position.get("force_eod_close") is None:
                    values["force_eod_close"] = False
                if values:
                    conn.execute(
                        sa.update(schema.positions)
                        .where(schema.positions.c.position_id == position.get("position_id"))
                        .values(**values)
                    )
                continue

            position_meta = _json_dict(position.get("position_meta"))
            position_meta.update(
                {
                    "book": meta.get("book"),
                    "trade_horizon": meta.get("trade_horizon"),
                    "meta_source": meta.get("meta_source") or (
                        "position_entry_meta"
                        if _json_dict(position.get("entry_meta_json")).get("trade_horizon")
                        else "order_meta"
                    ),
                }
            )
            if meta.get("exit_policy_family"):
                position_meta["exit_policy_family"] = meta.get("exit_policy_family")

            raw_horizon = str(meta.get("trade_horizon") or "").strip().upper()
            horizon = {
                "DAY_PROTECT": "DAY_TRADE",
                "SWING_CARRY": "SWING",
                "CORE_CARRY": "CORE",
            }.get(raw_horizon, raw_horizon or None)
            values = {
                "position_meta": position_meta,
                "entry_meta_json": meta,
                "policy_source": str(meta.get("policy_source") or "").strip() or "recovered_order_meta",
            }
            optional_map = {
                "entry_thesis": meta.get("entry_thesis"),
                "trade_horizon": horizon,
                "exit_policy_family": meta.get("exit_policy_family"),
                "eod_action": meta.get("eod_action"),
                "policy_version": meta.get("policy_version"),
                "entry_reason": meta.get("entry_reason"),
                "entry_style_selected": meta.get("entry_style_selected"),
            }
            values.update({key: value for key, value in optional_map.items() if value not in (None, "")})
            if meta.get("force_eod_close") is not None:
                values["force_eod_close"] = bool(meta.get("force_eod_close"))

            result = conn.execute(
                sa.update(schema.positions)
                .where(
                    sa.and_(
                        schema.positions.c.position_id == position.get("position_id"),
                        schema.positions.c.position_cycle_id == cycle,
                        schema.positions.c.portfolio_epoch_id == epoch,
                        schema.positions.c.status == "OPEN",
                    )
                )
                .values(**values)
            )
            if int(result.rowcount or 0) == 1:
                restored += 1
                logger.info(
                    "[RECONCILE][META_RESTORE][OK_TYPED] env=%s code=%s horizon=%s source=%s",
                    env,
                    position.get("code"),
                    meta.get("trade_horizon"),
                    position_meta.get("meta_source"),
                )
    return restored


def install_kr_20260929_log_review_guards() -> None:
    """Install after the Sep-29 broker-truth/review guards."""
    global _INSTALLED
    if _INSTALLED:
        return

    if not getattr(KisAPI, "_kr_p0_20260929_pre_hashkey_budget_installed", False):
        KisAPI.buy_stock_limit = _build_buy_pipeline_budget_guard(KisAPI.buy_stock_limit)
        KisAPI.buy_stock_market = _build_buy_pipeline_budget_guard(KisAPI.buy_stock_market)
        KisAPI._kr_p0_20260929_pre_hashkey_budget_installed = True

    if not getattr(OrdersRepo, "_kr_p0_20260929_trade_date_guard_installed", False):
        OrdersRepo.get_open_orders = _build_get_open_orders_trade_date_guard(OrdersRepo.get_open_orders)
        OrdersRepo._kr_p0_20260929_trade_date_guard_installed = True

    import trader.reconcile_kis as rk
    if not getattr(rk, "_kr_p1_20260929_typed_meta_restore_installed", False):
        rk._restore_entry_meta_for_promoted_positions = _typed_restore_entry_meta_for_promoted_positions
        rk._kr_p1_20260929_typed_meta_restore_installed = True

    _INSTALLED = True
    logger.info(
        "[KR_P0][20260929_LOG_REVIEW][INSTALLED] pre_hashkey_buy_budget=1 "
        "orders_trade_date_normalization=1 typed_meta_restore=1"
    )
