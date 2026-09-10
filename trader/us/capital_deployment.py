# -*- coding: utf-8 -*-
"""US capital deployment controller.

Capital/exposure policy is intentionally based on risk capital, not on a
mislabelled broker-equity value. Broker account equity is reported separately
and is optional unless an authoritative source is available.
"""
from __future__ import annotations

import os
from typing import Any

from trader.accounting import configured_us_deployment_capital


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def env_float(name: str, default: float) -> float:
    return _f(os.getenv(name), default)


def compute_deployment_metrics(
    *,
    invested_market_value_usd: float,
    cash_usd: float | None = None,
    account_equity_usd: float | None = None,
    risk_capital_usd: float | None = None,
    risk_capital_source: str | None = None,
) -> dict:
    """Compute deployment metrics with explicit accounting semantics.

    account_equity_usd is accepted for backward compatibility only when the
    caller has a real/explicit equity value. If absent, configured strategy
    capital becomes the risk/exposure denominator, but is never relabelled as
    broker account equity.
    """
    fx = env_float("US_BUDGET_FX_KRW_PER_USD", 1450.0)
    target_exposure_pct = env_float("US_TARGET_EXPOSURE_PCT", 0.70)
    max_exposure_pct = env_float("US_MAX_EXPOSURE_PCT", 0.85)
    min_cash_buffer_pct = env_float("US_MIN_CASH_BUFFER_PCT", 0.15)

    explicit_equity = _f(account_equity_usd, 0.0)
    risk_capital = _f(risk_capital_usd, 0.0)
    source = str(risk_capital_source or "")
    if risk_capital <= 0:
        if explicit_equity > 0:
            risk_capital = explicit_equity
            source = source or "explicit_account_equity"
        else:
            configured = configured_us_deployment_capital()
            risk_capital = _f(configured.get("deployment_capital_usd"), 0.0)
            source = source or str(configured.get("deployment_capital_source") or "configured_strategy_cap")

    broker_cash_known = cash_usd is not None
    broker_cash = _f(cash_usd, 0.0) if broker_cash_known else None
    deployment_cash_headroom = (
        max(0.0, risk_capital - invested_market_value_usd)
        if broker_cash is None
        else max(0.0, broker_cash)
    )

    gross_exposure_pct = invested_market_value_usd / risk_capital if risk_capital > 0 else 0.0
    target_invested_usd = risk_capital * target_exposure_pct
    max_invested_usd = risk_capital * max_exposure_pct
    min_cash_buffer_usd = risk_capital * min_cash_buffer_pct
    deployment_gap_usd = max(0.0, target_invested_usd - invested_market_value_usd)
    deployable_cash_usd = max(
        0.0,
        min(
            deployment_cash_headroom - min_cash_buffer_usd,
            max_invested_usd - invested_market_value_usd,
        ),
    )
    allowed_new_buy_usd = max(0.0, min(deployment_gap_usd, deployable_cash_usd))

    return {
        "account_equity_usd": explicit_equity if explicit_equity > 0 else None,
        "account_equity_krw": explicit_equity * fx if explicit_equity > 0 else None,
        "risk_capital_usd": risk_capital,
        "risk_capital_krw": risk_capital * fx if risk_capital > 0 else 0.0,
        "risk_capital_source": source or "unavailable",
        "deployment_capital_usd": risk_capital,
        "deployment_capital_krw": risk_capital * fx if risk_capital > 0 else 0.0,
        "invested_market_value_usd": invested_market_value_usd,
        "cash_usd": broker_cash,
        "cash_source": "broker_orderable_cash" if broker_cash_known else "unavailable",
        "deployment_cash_headroom_usd": deployment_cash_headroom,
        "gross_exposure_pct": gross_exposure_pct,
        "target_exposure_pct": target_exposure_pct,
        "max_exposure_pct": max_exposure_pct,
        "min_cash_buffer_pct": min_cash_buffer_pct,
        "deployment_gap_usd": deployment_gap_usd,
        "deployable_cash_usd": deployable_cash_usd,
        "allowed_new_buy_usd": allowed_new_buy_usd,
        "underdeployed": gross_exposure_pct < target_exposure_pct,
        "overdeployed": gross_exposure_pct >= max_exposure_pct,
    }


def decide_deployment_action(metrics: dict, *, position_count: int, max_positions: int) -> str:
    if metrics.get("overdeployed"):
        return "TRIM_ONLY"
    if metrics.get("underdeployed") and position_count >= max_positions:
        return "ADD_TO_EXISTING_ONLY"
    if metrics.get("underdeployed") and position_count < max_positions:
        return "NEW_AND_ADD_ALLOWED"
    return "NORMAL"
