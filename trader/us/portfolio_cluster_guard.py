# -*- coding: utf-8 -*-
"""Portfolio cluster exposure guard for US rotation regimes."""
from __future__ import annotations

import logging
from typing import Any

from trader.us.rotation import AI_CLUSTERS, cluster_caps_for_regime, compute_cluster_exposure, theme_cluster_for

logger = logging.getLogger(__name__)


def _qty(pos: dict) -> int:
    return int(float(pos.get("qty") or pos.get("quantity") or pos.get("holdings_qty") or 0))


def evaluate_portfolio_cluster_guard(positions: list[dict], regime: str, equity_usd: float, current_exit_intents: list[dict] | None = None, provider: Any | None = None, now: Any | None = None) -> dict:
    exposure = compute_cluster_exposure(positions or [], equity_usd or None)
    caps = cluster_caps_for_regime(regime)
    cap = float(caps.get("AI_TECH_COMBINED", caps.get("AI_COMBINED", 1.0)))
    ai_weight = sum((exposure.get(c) or {}).get("cluster_weight", 0.0) for c in AI_CLUSTERS)
    over_cap = ai_weight > cap or regime in ("UNKNOWN", "RISK_GUARD")
    blocked_clusters = set(AI_CLUSTERS) if over_cap and regime in ("RISK_OFF", "AI_OFF_ROTATION", "UNKNOWN", "RISK_GUARD", "CONSERVATIVE_ROTATION") else set()
    existing_sell_symbols = {str(i.get("symbol") or "").upper().strip() for i in (current_exit_intents or []) if str(i.get("side") or "").upper() == "SELL"}
    trim_intents: list[dict] = []
    if over_cap and regime in ("RISK_OFF", "AI_OFF_ROTATION", "CONSERVATIVE_ROTATION"):
        ai_positions = [p for p in positions or [] if theme_cluster_for(str(p.get("symbol") or p.get("code") or ""), p) in AI_CLUSTERS and str(p.get("symbol") or p.get("code") or "").upper().strip() not in existing_sell_symbols]
        ai_positions.sort(key=lambda p: (float(p.get("pnl_1d") or p.get("day_pnl") or 0.0), float(p.get("unrealized_pnl") or p.get("unrealized_pnl_usd") or 0.0)))
        for pos in ai_positions[:3]:
            q = _qty(pos)
            if q <= 0:
                continue
            sell_qty = max(1, int(q * (0.30 if regime == "RISK_OFF" else 0.20)))
            price = float(pos.get("last_price") or pos.get("current_price") or pos.get("price") or 0.0)
            sym = str(pos.get("symbol") or pos.get("code") or "").upper()
            trim_intents.append({"symbol": sym, "side": "SELL", "qty": min(q, sell_qty), "quantity": min(q, sell_qty), "notional_usd": price * min(q, sell_qty), "reason": "CLUSTER_EXPOSURE_TRIM", "meta": {"reason": "CLUSTER_EXPOSURE_TRIM", "rotation_regime": regime, "portfolio_ai_tech_weight": ai_weight}})
    trim_notional = sum(float(i.get("notional_usd") or 0.0) for i in trim_intents)
    status = "OVER_CAP" if over_cap else "OK"
    log = logger.warning if over_cap else logger.info
    log("[US_CLUSTER_GUARD][PORTFOLIO] rotation_regime=%s ai_tech_weight=%.4f cap=%.4f over_cap=%s blocked_buy_symbols=%s trim_symbols=%s trim_notional=%.2f", regime, ai_weight, cap, over_cap, sorted(blocked_clusters), [i.get("symbol") for i in trim_intents], trim_notional)
    return {"portfolio_cluster_guard_status": status, "portfolio_ai_tech_weight": ai_weight, "portfolio_equity_usd": equity_usd, "portfolio_cluster_cap_violations": ["AI_TECH_COMBINED"] if over_cap else [], "blocked_clusters": sorted(blocked_clusters), "cluster_guard_trim_intents": trim_intents, "cluster_guard_trim_notional": trim_notional, "cluster_guard_existing_sell_symbols": sorted(existing_sell_symbols)}


def filter_entry_intents_for_cluster_guard(entry_intents: list[dict], guard: dict) -> tuple[list[dict], list[str]]:
    blocked = set(guard.get("blocked_clusters") or [])
    kept, blocked_syms = [], []
    for intent in entry_intents or []:
        if str(intent.get("side") or "BUY").upper() == "BUY" and theme_cluster_for(str(intent.get("symbol") or ""), intent) in blocked:
            intent.setdefault("meta", {})["blocked_reason"] = "BLOCKED_CLUSTER_EXPOSURE"
            blocked_syms.append(str(intent.get("symbol") or "").upper())
            continue
        kept.append(intent)
    return kept, blocked_syms
