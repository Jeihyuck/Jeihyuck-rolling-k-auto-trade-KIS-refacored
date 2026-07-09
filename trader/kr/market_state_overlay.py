# -*- coding: utf-8 -*-
"""KR Market State Overlay: PR49 full integration plus PR50 final safeguards."""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Callable, Iterable

from trader.kr.account_risk import build_kr_account_risk_snapshot, is_account_loss_kill_switch_triggered
from trader.kr.forbidden_products import FORBIDDEN_KR_BUY_SYMBOLS, is_forbidden_kr_buy_product

logger = logging.getLogger(__name__)
ReturnProvider = Callable[[str, int], float | None]

KR_INDEX_DEFAULTS = {
    "KR_MARKET_STATE_OVERLAY_ENABLE": "1",
    "KR_INDEX_KOSPI_SYMBOL": "KOSPI",
    "KR_INDEX_KOSDAQ_SYMBOL": "KOSDAQ",
    "KR_INDEX_KOSPI200_PROXY": "KOSPI200",
    "KR_INDEX_KOSDAQ150_PROXY": "229200",
    "KR_INDEX_KOSPI_FALLBACK_PROXY": "069500",
    "KR_INDEX_KOSDAQ_FALLBACK_PROXY": "229200",
    "KR_INDEX_KOSPI200_FALLBACK_PROXY": "069500",
    "KR_INDEX_KOSDAQ150_FALLBACK_PROXY": "229200",
    "KR_USE_229200_AS_PRIMARY_REGIME": "0",
    "KR_USE_229200_AS_GROWTH_PROXY": "1",
    "KR_LEGACY_REGIME_FALLBACK_ENABLE": "1",
    "KR_SECTOR_PROXY_CONFIG_PATH": "config/kr_sector_proxy_map.json",
}


def _env(key: str) -> str:
    return os.getenv(key) or KR_INDEX_DEFAULTS[key]


def _resolve_one(index: str, primary_symbol: str, fallback_symbol: str, lookback: int, provider: ReturnProvider) -> dict[str, Any]:
    primary_return = provider(primary_symbol, lookback)
    proxy_warning = ""
    if primary_return is not None:
        selected_symbol, value, status, fallback_reason = primary_symbol, float(primary_return), "primary_ok", ""
    else:
        fallback_return = provider(fallback_symbol, lookback) if fallback_symbol else None
        if fallback_return is not None:
            selected_symbol, value, status, fallback_reason = fallback_symbol, float(fallback_return), "fallback_ok", "primary_missing"
            if index == "kosdaq" and fallback_symbol == "229200":
                proxy_warning = "KOSDAQ fallback uses KOSDAQ150 proxy"
            logger.warning(
                "[KR_MARKET_STATE][INDEX_FALLBACK] index=%s primary_symbol=%s fallback_symbol=%s selected_symbol=%s lookback=%s return=%s fallback_reason=primary_missing proxy_warning=%s",
                index, primary_symbol, fallback_symbol, selected_symbol, lookback, value, proxy_warning,
            )
        else:
            selected_symbol, value, status, fallback_reason = "", None, "missing", "primary_missing"
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


def _only_kosdaq150_proxy_available(index_context: dict[str, Any]) -> bool:
    resolution = index_context.get("index_resolution", {}) or {}
    return (
        resolution.get("kospi", {}).get("status") == "missing"
        and resolution.get("kospi200", {}).get("status") == "missing"
        and resolution.get("kosdaq", {}).get("selected") == "229200"
        and resolution.get("kosdaq", {}).get("status") == "fallback_ok"
        and index_context.get("kosdaq150_1d_return") is not None
    )


def evaluate_kr_market_state(
    index_context: dict[str, Any] | None = None,
    sector_proxy_summary: dict[str, Any] | None = None,
    account_snapshot: dict[str, Any] | None = None,
    final30_stress: bool = False,
) -> dict[str, Any]:
    index_context = index_context or {"index_resolution": {}}
    sector_proxy_summary = sector_proxy_summary or {}
    acct = build_kr_account_risk_snapshot(account_snapshot or {})
    warnings: list[str] = []
    state = "KR_NORMAL"
    only_229200 = _only_kosdaq150_proxy_available(index_context)
    if only_229200:
        warnings.append("only_kosdaq150_proxy_available")
    if is_account_loss_kill_switch_triggered(acct):
        state = "KR_DEFENSE_CRASH"
    elif final30_stress and only_229200:
        state = "KR_DEFENSE_CAUTION"
    elif not only_229200:
        core = [index_context.get("kospi_1d_return"), index_context.get("kosdaq_1d_return"), index_context.get("kospi200_1d_return")]
        valid = [float(v) for v in core if v is not None]
        suspect_only = bool(sector_proxy_summary) and all((v or {}).get("source_quality") == "suspect" for v in sector_proxy_summary.values())
        if valid and sum(valid) / len(valid) >= 0.01 and not suspect_only:
            state = "KR_STRONG_RISK_ON" if len(valid) >= 2 else "KR_RISK_ON"
    exposure = {"KR_DEFENSE_CRASH": 0.0, "KR_DEFENSE_CAUTION": 0.35, "KR_NORMAL": 1.0, "KR_RISK_ON": 1.15, "KR_STRONG_RISK_ON": 1.3}.get(state, 1.0)
    return {
        "overlay_version": "PR50",
        "overlay_base": "PR49",
        "market_state": state,
        "exposure_multiplier": exposure,
        "allow_new_buy": state != "KR_DEFENSE_CRASH",
        "force_entry_block": state == "KR_DEFENSE_CRASH",
        "trim_required": state in {"KR_DEFENSE_CRASH", "KR_DEFENSE_CAUTION"},
        "profit_capture_enabled": True,
        "data_quality": "degraded" if warnings else "ok",
        "data_quality_warnings": warnings,
        "index_resolution": index_context.get("index_resolution", {}),
        "sector_proxy_summary": sector_proxy_summary,
        "forbidden_products": sorted(FORBIDDEN_KR_BUY_SYMBOLS),
        **acct,
        "account_loss_kill_switch_triggered": is_account_loss_kill_switch_triggered(acct),
    }


def apply_kr_market_state_to_budget(tick_budget_krw: float, overlay: dict[str, Any] | None) -> tuple[float, dict[str, Any]]:
    multiplier = float((overlay or {}).get("exposure_multiplier") if overlay else 1.0)
    adjusted = max(0.0, float(tick_budget_krw or 0.0) * multiplier)
    return adjusted, {"original_tick_budget_krw": float(tick_budget_krw or 0.0), "adjusted_tick_budget_krw": adjusted, "exposure_multiplier": multiplier, "market_state": (overlay or {}).get("market_state", "KR_NORMAL")}


def _side(intent: dict[str, Any]) -> str:
    return str(intent.get("side") or intent.get("action") or "BUY").upper()


def filter_kr_entry_intent(intent: dict[str, Any], market_state_overlay: dict[str, Any] | None) -> dict[str, Any]:
    out = dict(intent or {})
    if _side(out) != "BUY":
        return out
    symbol = str(out.get("symbol") or out.get("code") or "").upper()
    if is_forbidden_kr_buy_product(symbol):
        out.update({"status": "BLOCKED", "blocked_reason": "forbidden_product_buy_only"})
        return out
    if (market_state_overlay or {}).get("force_entry_block") or (market_state_overlay or {}).get("market_state") == "KR_DEFENSE_CRASH":
        out.update({"status": "BLOCKED", "blocked_reason": "market_state_entry_block"})
    return out


def filter_kr_entry_candidates(candidates: Iterable[Any], market_state_overlay: dict[str, Any] | None) -> list[Any]:
    kept = []
    for cf in candidates or []:
        code = getattr(cf, "code", None) or getattr(cf, "symbol", None)
        result = filter_kr_entry_intent({"side": "BUY", "code": code}, market_state_overlay)
        if result.get("status") == "BLOCKED":
            if hasattr(cf, "setup_ok"):
                cf.setup_ok = False
            if hasattr(cf, "reasons"):
                cf.reasons.append(result.get("blocked_reason"))
            continue
        kept.append(cf)
    return kept


def pre_api_kr_buy_block(intent: dict[str, Any], market_state_overlay: dict[str, Any] | None) -> tuple[bool, str]:
    filtered = filter_kr_entry_intent(intent, market_state_overlay)
    if filtered.get("status") == "BLOCKED":
        return True, str(filtered.get("blocked_reason") or "market_state_entry_block")
    return False, ""


def generate_kr_profit_capture_intents(positions: list[dict[str, Any]] | None, overlay: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not (overlay or {}).get("profit_capture_enabled", True):
        return []
    out = []
    for pos in positions or []:
        pnl = pos.get("pnl_pct") or pos.get("return_pct")
        qty = int(pos.get("qty") or pos.get("quantity") or 0)
        if pnl is not None and float(pnl) >= 0.08 and qty > 0:
            out.append({"side": "SELL", "code": pos.get("code") or pos.get("symbol"), "qty": max(1, qty // 2), "reason": "KR_MARKET_STATE_PROFIT_CAPTURE"})
    return out


def generate_kr_defense_trim_intents(positions: list[dict[str, Any]] | None, overlay: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not (overlay or {}).get("trim_required"):
        return []
    out = []
    for pos in positions or []:
        qty = int(pos.get("qty") or pos.get("quantity") or 0)
        if qty > 0:
            out.append({"side": "SELL", "code": pos.get("code") or pos.get("symbol"), "qty": max(1, qty // 3), "reason": "KR_MARKET_STATE_DEFENSE_TRIM"})
    return out


def write_overlay_artifact(path: str | Path, overlay: dict[str, Any]) -> None:
    payload = {"overlay_version": "PR50", "overlay_base": "PR49", **(overlay or {})}
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
