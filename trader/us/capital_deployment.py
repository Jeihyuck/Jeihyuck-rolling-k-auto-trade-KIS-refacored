# -*- coding: utf-8 -*-
"""US capital deployment controller.

Keeps US exposure within account-level targets while treating US_MAX_POSITIONS as
new-symbol capacity only. Existing high-quality holdings can be topped up toward
target weight when the account is underdeployed.
"""
from __future__ import annotations

import os
from typing import Any


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def env_float(name: str, default: float) -> float:
    return _f(os.getenv(name), default)


def compute_deployment_metrics(*, account_equity_usd: float, invested_market_value_usd: float, cash_usd: float | None = None) -> dict:
    fx = env_float("US_BUDGET_FX_KRW_PER_USD", 1450.0)
    target_exposure_pct = env_float("US_TARGET_EXPOSURE_PCT", 0.70)
    max_exposure_pct = env_float("US_MAX_EXPOSURE_PCT", 0.85)
    min_cash_buffer_pct = env_float("US_MIN_CASH_BUFFER_PCT", 0.15)
    if account_equity_usd <= 0:
        krw = env_float("US_EXPECTED_PRACTICE_CAPITAL_KRW", env_float("US_PAPER_MAX_CAPITAL_KRW", 300_000_000.0))
        account_equity_usd = krw / fx if fx > 0 else 0.0
    if cash_usd is None:
        cash_usd = max(0.0, account_equity_usd - invested_market_value_usd)
    gross_exposure_pct = invested_market_value_usd / account_equity_usd if account_equity_usd > 0 else 0.0
    target_invested_usd = account_equity_usd * target_exposure_pct
    max_invested_usd = account_equity_usd * max_exposure_pct
    min_cash_buffer_usd = account_equity_usd * min_cash_buffer_pct
    deployment_gap_usd = max(0.0, target_invested_usd - invested_market_value_usd)
    deployable_cash_usd = max(0.0, min(cash_usd - min_cash_buffer_usd, max_invested_usd - invested_market_value_usd))
    allowed_new_buy_usd = max(0.0, min(deployment_gap_usd, deployable_cash_usd))
    return {
        "account_equity_krw": account_equity_usd * fx,
        "account_equity_usd": account_equity_usd,
        "invested_market_value_usd": invested_market_value_usd,
        "cash_usd": cash_usd,
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
