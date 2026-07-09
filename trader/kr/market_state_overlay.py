# -*- coding: utf-8 -*-
"""KR Market State Overlay final safeguards (PR50)."""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)

KR_INDEX_DEFAULTS = {
    "KR_INDEX_KOSPI_SYMBOL": "KOSPI",
    "KR_INDEX_KOSDAQ_SYMBOL": "KOSDAQ",
    "KR_INDEX_KOSPI200_PROXY": "KOSPI200",
    "KR_INDEX_KOSDAQ150_PROXY": "229200",
    "KR_INDEX_KOSPI_FALLBACK_PROXY": "069500",
    "KR_INDEX_KOSDAQ_FALLBACK_PROXY": "229200",
    "KR_INDEX_KOSPI200_FALLBACK_PROXY": "069500",
    "KR_INDEX_KOSDAQ150_FALLBACK_PROXY": "229200",
}

FORBIDDEN_KR_PRODUCTS = {"INVERSE", "KODEX_INVERSE", "252670", "114800"}
ReturnProvider = Callable[[str, int], float | None]


def _env(key: str) -> str:
    return os.getenv(key) or KR_INDEX_DEFAULTS[key]


def _resolve_one(index: str, primary_symbol: str, fallback_symbol: str, lookback: int, provider: ReturnProvider) -> dict[str, Any]:
    primary_return = provider(primary_symbol, lookback)
    proxy_warning = ""
    if primary_return is not None:
        selected_symbol = primary_symbol
        value = float(primary_return)
        status = "primary_ok"
        fallback_reason = ""
    else:
        fallback_return = provider(fallback_symbol, lookback) if fallback_symbol else None
        if fallback_return is not None:
            selected_symbol = fallback_symbol
            value = float(fallback_return)
            status = "fallback_ok"
            fallback_reason = "primary_missing"
            if index == "kosdaq" and fallback_symbol == "229200":
                proxy_warning = "KOSDAQ fallback uses KOSDAQ150 proxy"
            logger.warning(
                "[KR_MARKET_STATE][INDEX_FALLBACK] index=%s primary_symbol=%s fallback_symbol=%s selected_symbol=%s lookback=%s return=%s fallback_reason=primary_missing proxy_warning=%s",
                index, primary_symbol, fallback_symbol, selected_symbol, lookback, value, proxy_warning,
            )
        else:
            selected_symbol = ""
            value = None
            status = "missing"
            fallback_reason = "primary_missing"
            logger.warning(
                "[KR_MARKET_STATE][INDEX_MISSING] index=%s primary_symbol=%s fallback_symbol=%s selected_symbol=%s lookback=%s return=%s fallback_reason=primary_missing proxy_warning=%s",
                index, primary_symbol, fallback_symbol, selected_symbol, lookback, value, proxy_warning,
            )
    return {"primary": primary_symbol, "fallback": fallback_symbol, "selected": selected_symbol, "status": status, "return": value, "fallback_reason": fallback_reason, "proxy_warning": proxy_warning}


def build_index_context(provider: ReturnProvider, lookback: int = 1) -> dict[str, Any]:
    specs = {
        "kospi": (_env("KR_INDEX_KOSPI_SYMBOL"), _env("KR_INDEX_KOSPI_FALLBACK_PROXY")),
        "kosdaq": (_env("KR_INDEX_KOSDAQ_SYMBOL"), _env("KR_INDEX_KOSDAQ_FALLBACK_PROXY")),
        "kospi200": (_env("KR_INDEX_KOSPI200_PROXY"), _env("KR_INDEX_KOSPI200_FALLBACK_PROXY")),
        "kosdaq150": (_env("KR_INDEX_KOSDAQ150_PROXY"), _env("KR_INDEX_KOSDAQ150_FALLBACK_PROXY")),
    }
    resolution = {k: _resolve_one(k, p, f, lookback, provider) for k, (p, f) in specs.items()}
    return {
        "index_resolution": resolution,
        "kospi_1d_return": resolution["kospi"]["return"],
        "kosdaq_1d_return": resolution["kosdaq"]["return"],
        "kospi200_1d_return": resolution["kospi200"]["return"],
        "kosdaq150_1d_return": resolution["kosdaq150"]["return"],
    }


def evaluate_kr_market_state(index_context: dict[str, Any], sector_proxy_summary: dict[str, Any] | None = None, account_snapshot: dict[str, Any] | None = None, final30_stress: bool = False) -> dict[str, Any]:
    account_snapshot = account_snapshot or {}
    warnings: list[str] = []
    state = "KR_NORMAL"
    acct = account_snapshot.get("account_intraday_pnl_pct")
    unreal = account_snapshot.get("portfolio_unrealized_pnl_pct")
    resolution = index_context.get("index_resolution", {}) or {}
    only_kosdaq150 = (
        resolution.get("kospi", {}).get("status") == "missing"
        and resolution.get("kospi200", {}).get("status") == "missing"
        and resolution.get("kosdaq", {}).get("selected") == "229200"
        and resolution.get("kosdaq", {}).get("status") == "fallback_ok"
        and index_context.get("kosdaq150_1d_return") is not None
    )
    if only_kosdaq150:
        warnings.append("only_kosdaq150_proxy_available")
    try:
        acct_f = None if acct is None or str(acct).strip() == "" else float(acct)
    except Exception:
        acct_f = None
    if acct_f is not None and acct_f <= -0.018:
        state = "KR_DEFENSE_CRASH"
    elif final30_stress and only_kosdaq150:
        state = "KR_DEFENSE_CAUTION"
    elif not only_kosdaq150:
        core = [index_context.get("kospi_1d_return"), index_context.get("kosdaq_1d_return"), index_context.get("kospi200_1d_return")]
        valid = [float(v) for v in core if v is not None]
        suspect_only = bool(sector_proxy_summary) and all((v or {}).get("source_quality") == "suspect" for v in sector_proxy_summary.values())
        if valid and sum(valid) / len(valid) >= 0.01 and not suspect_only:
            state = "KR_STRONG_RISK_ON" if len(valid) >= 2 else "KR_RISK_ON"
    data_quality = "degraded" if warnings else "ok"
    return {"overlay_version": "PR50", "overlay_base": "PR49", "market_state": state, "data_quality": data_quality, "data_quality_warnings": warnings, "index_resolution": index_context.get("index_resolution", {}), "sector_proxy_summary": sector_proxy_summary or {}, "account_intraday_pnl_pct": acct_f, "portfolio_unrealized_pnl_pct": unreal, "account_loss_kill_switch_triggered": acct_f is not None and acct_f <= -0.018}


def filter_kr_entry_intent(intent: dict[str, Any], market_state_overlay: dict[str, Any]) -> dict[str, Any]:
    out = dict(intent or {})
    side = str(out.get("side") or out.get("action") or "BUY").upper()
    if side != "BUY":
        return out
    symbol = str(out.get("symbol") or out.get("code") or "").upper()
    if symbol in FORBIDDEN_KR_PRODUCTS:
        out["status"] = "BLOCKED"
        out["blocked_reason"] = "forbidden_product_buy_only"
        return out
    if (market_state_overlay or {}).get("market_state") == "KR_DEFENSE_CRASH":
        out["status"] = "BLOCKED"
        out["blocked_reason"] = "market_state_entry_block"
    return out


def write_overlay_artifact(path: str | Path, overlay: dict[str, Any]) -> None:
    payload = {"overlay_version": "PR50", "overlay_base": "PR49", **(overlay or {})}
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
