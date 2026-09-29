"""16:00 log-review fixes for the Sep-29 KR runtime integrity PR.

The full 2026-09-29 trading log exposed implementation gaps that were not
visible in the earlier DB-only incident review:

* PB1 BUYs can exhaust the shared tick budget while obtaining `/uapi/hashkey`
  before the order-cash endpoint guard is reached. Guard the public PB1 BUY
  method before *any* hashkey/order HTTP boundary, and classify hashkey
  temporary failures as definitely pre-submit.
* the Sep-29 20-second PB1 tail-budget protection must not override the
  dedicated KR_INFINITE owner's own 8-second runtime policy for 122630.
* `OrdersRepo.get_open_orders(..., trade_date=<date>)` enters a broken legacy
  branch (local ``datetime`` shadowing / undefined ``dtime``). PR147's
  unresolved probe then fails closed on every tick and unnecessarily forces a
  fresh KIS balance. Bypass that branch and filter the already epoch-scoped
  open rows by explicit KST trade date.
* the legacy promoted-position metadata restore issues untyped CASE bind
  parameters that PostgreSQL rejects with `AmbiguousParameter`. Replace that
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
from zoneinfo import ZoneInfo

import sqlalchemy as sa

from trader.db.repos import OrdersRepo
from trader.db.schema import schema_for_engine
from trader.kis_wrapper import KisAPI, KisTemporaryError, kr_tick_remaining_sec

logger = logging.getLogger(__name__)
_INSTALLED = False
_KST = ZoneInfo("Asia/Seoul")


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


def _normalize_code(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.startswith("A") and text[1:].isdigit():
        text = text[1:]
    return text.zfill(6) if text.isdigit() else text


def _kr_infinite_symbol() -> str:
    # InfiniteConfig.validate() currently restricts the dedicated sleeve to
    # 122630, but keep the environment contract explicit for owner isolation.
    return _normalize_code(os.getenv("KR_INFINITE_SYMBOL", "122630"))


def _is_kr_infinite_code(value: Any) -> bool:
    return _normalize_code(value) == _kr_infinite_symbol()


def _public_buy_code(args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
    if args:
        return _normalize_code(args[0])
    return _normalize_code(kwargs.get("pdno") or kwargs.get("code") or kwargs.get("symbol"))


def _request_body_dict(kwargs: dict[str, Any]) -> dict[str, Any]:
    raw = kwargs.get("json")
    if isinstance(raw, dict):
        return dict(raw)
    raw = kwargs.get("data")
    if isinstance(raw, memoryview):
        raw = raw.tobytes()
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = bytes(raw).decode("utf-8")
        except Exception:
            return {}
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            return dict(parsed) if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _request_order_code(kwargs: dict[str, Any]) -> str:
    body = _request_body_dict(kwargs)
    return _normalize_code(body.get("PDNO") or body.get("pdno") or body.get("code") or body.get("symbol"))


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
        code = _public_buy_code(args, kwargs)
        if not _is_kr_infinite_code(code):
            _assert_buy_pipeline_budget(self)
        return original(self, *args, **kwargs)

    return guarded


def _build_hashkey_pre_submit_classification_guard(original: Callable[..., Any]) -> Callable[..., Any]:
    """Tag temporary hashkey failures so they can never become ambiguous orders."""
    @functools.wraps(original)
    def guarded(self, *args: Any, **kwargs: Any):
        try:
            return original(self, *args, **kwargs)
        except KisTemporaryError as exc:
            message = str(exc or "")
            if "HASHKEY" in message.upper():
                raise
            # /uapi/hashkey runs before /order-cash. Even if its retry budget is
            # exhausted, no economic order has crossed the broker order boundary.
            raise KisTemporaryError(f"HASHKEY_PRE_SUBMIT:{message}") from exc

    return guarded


def _build_owner_scoped_buy_order_predicate(original: Callable[..., bool]) -> Callable[..., bool]:
    """Keep the base 20s order-endpoint gate out of the KR_INFINITE sleeve."""
    @functools.wraps(original)
    def guarded(url: str, kwargs: dict[str, Any]) -> bool:
        is_buy = bool(original(url, kwargs))
        if not is_buy:
            return False
        if _is_kr_infinite_code(_request_order_code(kwargs)):
            return False
        return True

    return guarded


def _target_trade_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.astimezone(_KST).date()
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return datetime.fromisoformat(str(value)[:10]).date()
    except Exception:
        return None


def _created_trade_date_kst(value: Any) -> date | None:
    if value is None:
        return None
    parsed = value if isinstance(value, datetime) else None
    if parsed is None:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except Exception:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_KST)
    return parsed.astimezone(_KST).date()


def _build_get_open_orders_trade_date_guard(original: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(original)
    def guarded(self, env: str, *args: Any, **kwargs: Any):
        target = _target_trade_date(kwargs.get("trade_date"))
        if target is None:
            return original(self, env, *args, **kwargs)

        # The underlying no-date/include_stale path still applies environment,
        # status and active-epoch scoping. Only the broken legacy day-boundary
        # branch is bypassed; KST date filtering is then explicit and testable.
        call_kwargs = dict(kwargs)
        call_kwargs.pop("trade_date", None)
        call_kwargs["include_stale"] = True
        rows = original(self, env, *args, **call_kwargs) or []
        return [row for row in rows if _created_trade_date_kst(row.get("created_at")) == target]

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
            # Preserve the existing anti-contamination contract. Cross-day
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

    if not getattr(KisAPI, "_kr_p0_20260929_hashkey_classification_installed", False):
        KisAPI._create_hashkey = _build_hashkey_pre_submit_classification_guard(KisAPI._create_hashkey)
        KisAPI._kr_p0_20260929_hashkey_classification_installed = True

    # The base Sep-29 order-endpoint gate is a PB1 execution guard. Keep the
    # dedicated 122630 KR_INFINITE owner on its own KR_INF_MIN_REMAINING_SEC
    # contract rather than silently imposing the PB1 20-second threshold.
    import trader.kr.runtime_integrity_20260929 as base29
    if not getattr(base29, "_kr_p0_20260929_owner_scope_installed", False):
        base29._is_buy_order_request = _build_owner_scoped_buy_order_predicate(base29._is_buy_order_request)
        base29._kr_p0_20260929_owner_scope_installed = True

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
        "hashkey_pre_submit_classification=1 pb1_owner_scoped_budget=1 "
        "orders_trade_date_kst_filter=1 typed_meta_restore=1"
    )
