"""Canonical KR/US accounting semantics for trading risk and reporting.

This module intentionally separates:
- broker/account equity (only when an authoritative source exists),
- strategy/risk capital (configured deployment ceiling),
- holdings market value,
- broker orderable cash,
- KIS percent-point fields versus internal fraction fields.

Do not infer account equity from holdings market value or orderable cash.
"""
from __future__ import annotations

import math
import os
from typing import Any


def safe_float(value: Any, default: float | None = None) -> float | None:
    if value in (None, ""):
        return default
    try:
        parsed = float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return default
    if not math.isfinite(parsed):
        return default
    return parsed


def first_number(row: dict | None, keys: tuple[str, ...], *, positive: bool = False) -> tuple[float | None, str]:
    src = row or {}
    for key in keys:
        value = safe_float(src.get(key))
        if value is None:
            continue
        if positive and value <= 0:
            continue
        return value, key
    return None, ""


def kis_percent_points_to_fraction(value: Any) -> float | None:
    """Convert a KIS percentage-point field to an internal fraction.

    Examples: -0.32 means -0.32% -> -0.0032; 3.82 means 3.82% -> 0.0382.
    This conversion is field-contract based. Never use magnitude heuristics.
    """
    parsed = safe_float(value)
    return None if parsed is None else parsed / 100.0


def explicit_fraction(value: Any) -> float | None:
    """Parse an internal/config value that is already expressed as a fraction."""
    return safe_float(value)


def configured_us_deployment_capital() -> dict[str, float | str | None]:
    fx = safe_float(os.getenv("US_BUDGET_FX_KRW_PER_USD"), 1450.0) or 1450.0
    cap_krw = safe_float(
        os.getenv("US_EXPECTED_PRACTICE_CAPITAL_KRW"),
        safe_float(os.getenv("US_PAPER_MAX_CAPITAL_KRW"), 300_000_000.0),
    )
    cap_krw = float(cap_krw or 0.0)
    cap_usd = cap_krw / fx if cap_krw > 0 and fx > 0 else None
    return {
        "deployment_capital_krw": cap_krw if cap_krw > 0 else None,
        "deployment_capital_usd": cap_usd,
        "deployment_capital_source": "configured_strategy_cap",
        "fx_krw_per_usd": fx,
    }


def resolve_us_accounting(
    *,
    reconcile: dict | None,
    invested_market_value_usd: float,
    broker_orderable_cash_usd: float | None,
) -> dict[str, Any]:
    """Resolve US accounting without inventing broker equity.

    The current KIS overseas balance normalization exposes total_pvs as holdings
    market value. It is never eligible to become account equity.
    """
    recon = reconcile or {}
    explicit_equity = safe_float(recon.get("account_equity_usd"))
    explicit_source = str(recon.get("account_equity_source") or "")
    if explicit_equity is not None and explicit_equity > 0 and explicit_source in {
        "kis_account_equity_authoritative",
        "explicit_account_equity",
    }:
        account_equity_usd = explicit_equity
        account_equity_source = explicit_source
    else:
        env_equity = safe_float(os.getenv("US_ACCOUNT_EQUITY_USD"))
        if env_equity is not None and env_equity > 0:
            account_equity_usd = env_equity
            account_equity_source = "explicit_env_override"
        else:
            account_equity_usd = None
            account_equity_source = "unavailable_from_current_kis_balance_contract"

    cap = configured_us_deployment_capital()
    configured_cap = safe_float(cap.get("deployment_capital_usd"))
    if account_equity_usd is not None and configured_cap is not None:
        risk_capital_usd = min(account_equity_usd, configured_cap)
        risk_capital_source = "min_explicit_account_equity_and_configured_cap"
    elif account_equity_usd is not None:
        risk_capital_usd = account_equity_usd
        risk_capital_source = "explicit_account_equity"
    else:
        risk_capital_usd = configured_cap
        risk_capital_source = "configured_strategy_cap"

    total_pvs_source = str(recon.get("total_pvs_source") or "")
    holdings_mv = safe_float(
        recon.get("holdings_market_value_usd"),
        safe_float(recon.get("total_pvs")) if total_pvs_source == "positions_market_value_sum" else None,
    )
    if holdings_mv is None:
        holdings_mv = float(invested_market_value_usd or 0.0)

    return {
        **cap,
        "account_equity_usd": account_equity_usd,
        "account_equity_source": account_equity_source,
        "risk_capital_usd": risk_capital_usd,
        "risk_capital_source": risk_capital_source,
        "holdings_market_value_usd": holdings_mv,
        "broker_orderable_cash_usd": safe_float(broker_orderable_cash_usd),
        "total_pvs_source": total_pvs_source,
        "total_pvs_eligible_as_account_equity": False,
    }


def resolve_kr_accounting(
    *,
    summary: dict | None,
    invested_market_value_krw: float,
    orderable_cash_krw: float | None,
) -> dict[str, Any]:
    """Resolve KR account equity and return units from authoritative KIS fields."""
    src = summary or {}
    total_asset, total_asset_key = first_number(src, ("tot_evlu_amt", "nass_amt"), positive=True)
    cash_total, cash_key = first_number(
        src,
        ("dnca_tot_amt", "nxdy_excc_amt", "prvs_rcdl_excc_amt"),
        positive=False,
    )
    if total_asset is not None and total_asset > 0:
        equity = total_asset
        equity_source = f"kis_balance:{total_asset_key}"
        equity_authoritative = True
    else:
        # An orderable-cash value is not account cash/equity. Do not use it to
        # synthesize NAV. Missing total asset must be visible to the BUY gate.
        equity = None
        equity_source = "missing_kis_total_asset"
        equity_authoritative = False

    env_intraday = os.getenv("KR_ACCOUNT_INTRADAY_PNL_PCT")
    if env_intraday not in (None, ""):
        intraday = explicit_fraction(env_intraday)
        intraday_source = "env_fraction:KR_ACCOUNT_INTRADAY_PNL_PCT"
    else:
        raw_intraday, intraday_key = first_number(
            src,
            ("account_intraday_pnl_pct", "intraday_pnl_pct", "day_pnl_pct", "asst_icdc_erng_rt"),
            positive=False,
        )
        if raw_intraday is None:
            intraday = None
            intraday_source = "missing"
        elif intraday_key == "asst_icdc_erng_rt":
            intraday = kis_percent_points_to_fraction(raw_intraday)
            intraday_source = "kis_percent_points:asst_icdc_erng_rt"
        else:
            intraday = explicit_fraction(raw_intraday)
            intraday_source = f"snapshot_fraction:{intraday_key}"

    env_5d = os.getenv("KR_ACCOUNT_5D_PNL_PCT")
    five_day = explicit_fraction(env_5d) if env_5d not in (None, "") else None
    five_day_source = "env_fraction:KR_ACCOUNT_5D_PNL_PCT" if five_day is not None else "missing"

    gross = (
        float(invested_market_value_krw or 0.0) / equity
        if equity is not None and equity > 0
        else None
    )
    return {
        "portfolio_equity_krw": equity,
        "portfolio_equity_source": equity_source,
        "portfolio_equity_authoritative": equity_authoritative,
        "invested_market_value_krw": float(invested_market_value_krw or 0.0),
        "cash_krw": cash_total,
        "cash_source": f"kis_balance:{cash_key}" if cash_key else "missing",
        "orderable_cash_krw": safe_float(orderable_cash_krw),
        "gross_exposure_pct": gross,
        "account_intraday_pnl_pct": intraday,
        "account_intraday_pnl_source": intraday_source,
        "account_5d_pnl_pct": five_day,
        "account_5d_pnl_source": five_day_source,
    }
