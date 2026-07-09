# -*- coding: utf-8 -*-
"""Account risk inputs for the KR market-state overlay."""
from __future__ import annotations

import os
from typing import Any

_INTRADAY_KEYS = ("account_intraday_pnl_pct", "today_pnl_pct", "dnca_tot_evlu_pfls_rt", "asst_icdc_erng_rt")
_UNREALIZED_KEYS = ("portfolio_unrealized_pnl_pct", "unrealized_pnl_pct", "evlu_pfls_rt")


def _pct(value: Any) -> float | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        raw = float(str(value).replace("%", "").replace(",", ""))
    except Exception:
        return None
    return raw / 100.0 if abs(raw) > 1 else raw


def build_kr_account_risk_snapshot(balance_snapshot: dict[str, Any] | None = None, *, env: dict[str, str] | None = None) -> dict[str, Any]:
    """Build account risk while keeping intraday and unrealized PnL separate."""
    env = env or os.environ
    snap = balance_snapshot or {}
    account_intraday = _pct(env.get("KR_ACCOUNT_INTRADAY_PNL_PCT"))
    source = "env" if account_intraday is not None else "unknown"
    if account_intraday is None:
        for key in _INTRADAY_KEYS:
            account_intraday = _pct(snap.get(key))
            if account_intraday is not None:
                source = f"holdings_summary.{key}"
                break
    unrealized = None
    for key in _UNREALIZED_KEYS:
        unrealized = _pct(snap.get(key))
        if unrealized is not None:
            break
    return {
        "account_intraday_pnl_pct": account_intraday,
        "account_intraday_pnl_pct_source": source,
        "account_intraday_pnl_pct_unknown": account_intraday is None,
        "portfolio_unrealized_pnl_pct": unrealized,
    }


def is_account_loss_kill_switch_triggered(snapshot: dict[str, Any], threshold: float = -0.018) -> bool:
    value = snapshot.get("account_intraday_pnl_pct")
    return value is not None and float(value) <= float(threshold)
