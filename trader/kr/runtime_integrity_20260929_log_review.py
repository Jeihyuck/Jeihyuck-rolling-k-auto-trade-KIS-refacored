"""16:00 log-review fixes for the Sep-29 KR runtime integrity PR.

The full 2026-09-29 trading log exposed two implementation gaps that were not
visible in the earlier DB-only incident review:

* PB1 BUYs can exhaust the shared tick budget while obtaining `/uapi/hashkey`
  before the order-cash endpoint guard is reached.  Guard the public BUY method
  before *any* hashkey/order HTTP boundary.
* `OrdersRepo.get_open_orders(..., trade_date=<date>)` enters a broken legacy
  branch (local ``datetime`` shadowing / undefined ``dtime``).  PR147's
  unresolved probe then fails closed on every tick and unnecessarily forces a
  fresh KIS balance.  Normalize date/datetime inputs to the already-supported
  ISO-string path before entering the legacy method.

No PB1 strategy policy, sizing, exits, or KR Infinite ownership is changed.
"""
from __future__ import annotations

from datetime import date, datetime
import functools
import logging
import math
import os
from typing import Any, Callable

from trader.db.repos import OrdersRepo
from trader.kis_wrapper import KisAPI, KisTemporaryError, kr_tick_remaining_sec

logger = logging.getLogger(__name__)
_INSTALLED = False


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except Exception:
        return float(default)


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

    _INSTALLED = True
    logger.info(
        "[KR_P0][20260929_LOG_REVIEW][INSTALLED] pre_hashkey_buy_budget=1 "
        "orders_trade_date_normalization=1"
    )
