# -*- coding: utf-8 -*-
"""US rotation-regime and theme-cluster utilities.

Pure helpers used by US prep/watchlist scoring and daily reporting.  The module is
intentionally provider-agnostic so regression tests can exercise the rotation
logic without KIS/network access.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from math import ceil
from typing import Any

AI_CLUSTERS = {"AI_SEMI", "AI_SOFTWARE", "DATA_CENTER_POWER", "MEGA_TECH"}
AI_CAP_CLUSTERS = {"AI_SEMI", "AI_SOFTWARE", "DATA_CENTER_POWER", "MEGA_TECH"}
DEFENSIVE_CLUSTERS = {"DEFENSIVE_UTILITY", "HEALTHCARE", "CONSUMER_STAPLES"}
NON_TECH_ROTATION_CLUSTERS = {"INDUSTRIAL", "FINANCIAL", "HEALTHCARE", "CONSUMER_STAPLES", "DEFENSIVE_UTILITY", "ENERGY_MATERIALS", "ETF_INDEX"}

SYMBOL_THEME_CLUSTER: dict[str, str] = {
    # AI / semiconductor
    "NVDA": "AI_SEMI", "AMD": "AI_SEMI", "AVGO": "AI_SEMI", "ARM": "AI_SEMI", "MRVL": "AI_SEMI",
    "MU": "AI_SEMI", "TSM": "AI_SEMI", "ASML": "AI_SEMI", "AMAT": "AI_SEMI", "LRCX": "AI_SEMI",
    "SMH": "AI_SEMI", "SOXX": "AI_SEMI",
    # AI software
    "PLTR": "AI_SOFTWARE", "MSFT": "MEGA_TECH", "ORCL": "AI_SOFTWARE", "SNOW": "AI_SOFTWARE", "MDB": "AI_SOFTWARE",
    # Data center / power
    "VRT": "DATA_CENTER_POWER", "GEV": "DATA_CENTER_POWER", "ETN": "DATA_CENTER_POWER", "PWR": "DATA_CENTER_POWER",
    # Mega tech
    "AAPL": "MEGA_TECH", "GOOGL": "MEGA_TECH", "GOOG": "MEGA_TECH", "AMZN": "MEGA_TECH", "META": "MEGA_TECH", "TSLA": "MEGA_TECH",
    "QQQ": "ETF_INDEX", "QQQM": "ETF_INDEX", "XLK": "ETF_INDEX",
    # Industrials
    "CAT": "INDUSTRIAL", "DE": "INDUSTRIAL", "HON": "INDUSTRIAL", "GE": "INDUSTRIAL", "MMM": "INDUSTRIAL", "XLI": "ETF_INDEX",
    # Financials
    "JPM": "FINANCIAL", "BAC": "FINANCIAL", "WFC": "FINANCIAL", "GS": "FINANCIAL", "MS": "FINANCIAL", "AXP": "FINANCIAL", "V": "FINANCIAL", "MA": "FINANCIAL", "XLF": "ETF_INDEX",
    # Healthcare
    "UNH": "HEALTHCARE", "LLY": "HEALTHCARE", "JNJ": "HEALTHCARE", "MRK": "HEALTHCARE", "ABBV": "HEALTHCARE", "XLV": "ETF_INDEX",
    # Staples
    "WMT": "CONSUMER_STAPLES", "COST": "CONSUMER_STAPLES", "PG": "CONSUMER_STAPLES", "KO": "CONSUMER_STAPLES", "PEP": "CONSUMER_STAPLES", "XLP": "ETF_INDEX",
    # Energy / materials
    "XOM": "ENERGY_MATERIALS", "CVX": "ENERGY_MATERIALS", "COP": "ENERGY_MATERIALS", "FCX": "ENERGY_MATERIALS", "XLE": "ETF_INDEX",
    # Utilities / defensive
    "NEE": "DEFENSIVE_UTILITY", "SO": "DEFENSIVE_UTILITY", "DUK": "DEFENSIVE_UTILITY", "XLU": "ETF_INDEX",
    # Index ETFs
    "SPY": "ETF_INDEX", "DIA": "ETF_INDEX", "IWM": "ETF_INDEX", "RSP": "ETF_INDEX",
}

SECTOR_ETF_TO_CLUSTER = {"XLK": "MEGA_TECH", "XLI": "INDUSTRIAL", "XLF": "FINANCIAL", "XLV": "HEALTHCARE", "XLP": "CONSUMER_STAPLES", "XLU": "DEFENSIVE_UTILITY", "XLE": "ENERGY_MATERIALS"}


def theme_cluster_for(symbol: str, row: dict | None = None) -> str:
    sym = str(symbol or (row or {}).get("symbol") or "").upper().strip()
    if sym in SYMBOL_THEME_CLUSTER:
        return SYMBOL_THEME_CLUSTER[sym]
    sector = str((row or {}).get("sector") or (row or {}).get("sector_name") or "").upper()
    tags = " ".join(str(t).upper() for t in ((row or {}).get("source_tags") or []))
    text = f"{sector} {tags}"
    if any(x in text for x in ("SEMICONDUCTOR", "CHIP", "AI_SEMI")): return "AI_SEMI"
    if "SOFTWARE" in text or "AI_SOFTWARE" in text: return "AI_SOFTWARE"
    if "DATA_CENTER" in text or "POWER" in text: return "DATA_CENTER_POWER"
    if "FINANC" in text: return "FINANCIAL"
    if "HEALTH" in text: return "HEALTHCARE"
    if "INDUSTR" in text: return "INDUSTRIAL"
    if "STAPLES" in text or "CONSUMER_DEFENSIVE" in text: return "CONSUMER_STAPLES"
    if "ENERGY" in text or "MATERIAL" in text: return "ENERGY_MATERIALS"
    if "UTIL" in text: return "DEFENSIVE_UTILITY"
    if str((row or {}).get("asset_type") or "").lower() == "etf": return "ETF_INDEX"
    return "OTHER"


def period_return(closes: list[float], days: int) -> float:
    vals = [float(x) for x in closes if x is not None and float(x) > 0]
    if len(vals) <= days:
        return 0.0
    return vals[-1] / vals[-1 - days] - 1.0


def classify_rotation_regime(returns: dict[str, dict[int, float]], ai_basket_3d: float | None = None) -> dict[str, Any]:
    def r(sym: str, d: int) -> float: return float((returns.get(sym) or {}).get(d, 0.0) or 0.0)
    spy3, spy5 = r("SPY", 3), r("SPY", 5)
    rel = {s: r(s, 3) - spy3 for s in ["QQQ", "SMH", "DIA", "IWM", "RSP", "XLK", "XLI", "XLF", "XLV", "XLP", "XLU", "XLE"]}
    strong_rotation = sum(1 for s in ["DIA", "RSP", "XLI", "XLF", "XLV"] if rel.get(s, 0.0) > 0) >= 2
    ai_basket_3d = spy3 if ai_basket_3d is None else float(ai_basket_3d)
    risk_off = all(r(s, 3) < 0 and r(s, 5) < 0 for s in ["SPY", "QQQ", "DIA", "IWM"])
    weak_nondef = sum(1 for s in ["XLK", "XLI", "XLF", "XLE", "IWM"] if r(s, 3) < 0) >= 4
    if risk_off and weak_nondef:
        regime = "RISK_OFF"
    elif r("QQQ", 3) < spy3 and r("SMH", 3) < spy3 - 0.01 and ai_basket_3d < spy3 - 0.015 and strong_rotation:
        regime = "AI_OFF_ROTATION"
    elif rel.get("QQQ", 0.0) > 0 and rel.get("SMH", 0.0) > 0.01 and ai_basket_3d > spy3:
        regime = "AI_ON"
    elif spy3 > 0 and sum(1 for s in ["QQQ", "DIA", "IWM", "RSP", "XLI", "XLF", "XLV"] if r(s, 3) > 0) >= 5:
        regime = "BROAD_UP"
    else:
        regime = "NEUTRAL"
    sectors = {SECTOR_ETF_TO_CLUSTER[s]: rel[s] for s in SECTOR_ETF_TO_CLUSTER}
    return {"rotation_regime": regime, "relative_strength": rel, "strongest_sectors": [k for k, _ in sorted(sectors.items(), key=lambda kv: kv[1], reverse=True)[:3]], "weakest_sectors": [k for k, _ in sorted(sectors.items(), key=lambda kv: kv[1])[:3]], "qqq_vs_spy_3d": rel.get("QQQ", 0.0), "smh_vs_spy_3d": rel.get("SMH", 0.0), "dia_vs_spy_3d": rel.get("DIA", 0.0), "rsp_vs_spy_3d": rel.get("RSP", 0.0)}


def cluster_caps_for_regime(regime: str) -> dict[str, Any]:
    if regime == "AI_ON": return {"AI_COMBINED": 0.60, "AI_SEMI": 0.35, "DEFENSIVE_MIN": 0.10, "CASH_MIN": 0.05}
    if regime == "AI_OFF_ROTATION": return {"AI_COMBINED": 0.30, "AI_TECH_COMBINED": 0.30, "AI_SEMI": 0.15, "ROTATION_MIN": 0.45, "CASH_MIN": 0.10, "NON_TECH_MIN": 0.50}
    if regime == "RISK_OFF": return {"AI_COMBINED": 0.15, "AI_TECH_COMBINED": 0.15, "DEFENSIVE_CASH_MIN": 0.60, "CASH_MIN": 0.25}
    if regime == "BROAD_UP": return {"SINGLE_CLUSTER": 0.40, "CASH_MIN": 0.05}
    return {"SINGLE_CLUSTER": 0.30, "AI_TECH_COMBINED": 0.40, "CASH_MIN": 0.05}


def compute_cluster_exposure(positions: list[dict], equity: float | None = None) -> dict[str, dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = defaultdict(lambda: {"cluster_market_value": 0.0, "cluster_unrealized_pnl": 0.0, "cluster_1d_pnl": 0.0})
    for p in positions or []:
        c = theme_cluster_for(str(p.get("symbol") or p.get("code") or ""), p)
        b = buckets[c]
        b["cluster_market_value"] += float(p.get("market_value_usd") or p.get("market_value") or p.get("eval_amount_usd") or 0.0)
        b["cluster_unrealized_pnl"] += float(p.get("unrealized_pnl") or p.get("unrealized_pnl_usd") or 0.0)
        b["cluster_1d_pnl"] += float(p.get("pnl_1d") or p.get("day_pnl") or p.get("pnl_1d_usd") or 0.0)
    total = float(equity or 0.0) or sum(v["cluster_market_value"] for v in buckets.values()) or 1.0
    return {k: {**v, "cluster_weight": v["cluster_market_value"] / total} for k, v in buckets.items()}


def apply_cap_flags(exposure: dict[str, dict[str, Any]], regime: str) -> dict[str, dict[str, Any]]:
    caps = cluster_caps_for_regime(regime)
    ai_weight = sum((exposure.get(c) or {}).get("cluster_weight", 0.0) for c in AI_CAP_CLUSTERS)
    out = {}
    for c, v in exposure.items():
        cap = caps.get(c, caps.get("SINGLE_CLUSTER", 1.0))
        if c in AI_CAP_CLUSTERS:
            cap = min(float(cap), float(caps.get("AI_COMBINED", 1.0)))
        out[c] = {**v, "cluster_cap": cap, "over_cap": bool(v.get("cluster_weight", 0.0) > cap or (c in AI_CAP_CLUSTERS and ai_weight > caps.get("AI_COMBINED", 1.0)))}
    return out


def select_bucket_champions(rows: list[dict], finaln: int, regime: str, blocked_clusters: set[str] | None = None) -> tuple[list[dict], dict[str, Any]]:
    blocked_clusters = blocked_clusters or set()
    quotas = _quotas(finaln, regime)
    groups: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        c = r.get("theme_cluster") or theme_cluster_for(r.get("symbol", ""), r)
        if c not in blocked_clusters:
            groups[c].append(r)
    for g in groups.values():
        g.sort(key=lambda x: float(x.get("score_final") or 0), reverse=True)
    selected: list[dict] = []
    counts: Counter = Counter()
    used = set()
    for c, q in quotas.items():
        for r in groups.get(c, [])[:q]:
            selected.append(r); used.add(r.get("symbol")); counts[c] += 1
    allow_ai_fill = regime != "AI_OFF_ROTATION"
    fill_pool = sorted([r for r in rows if r.get("symbol") not in used and (r.get("theme_cluster") not in blocked_clusters) and (allow_ai_fill or r.get("theme_cluster") not in AI_CAP_CLUSTERS)], key=lambda x: float(x.get("score_final") or 0), reverse=True)
    max_single = ceil(finaln * cluster_caps_for_regime(regime).get("SINGLE_CLUSTER", 1.0))
    max_ai = ceil(finaln * cluster_caps_for_regime(regime).get("AI_COMBINED", 1.0))
    for r in fill_pool:
        if len(selected) >= finaln: break
        c = r.get("theme_cluster")
        if counts[c] >= max_single: continue
        if c in AI_CAP_CLUSTERS and sum(counts[x] for x in AI_CAP_CLUSTERS) >= max_ai: continue
        selected.append(r); counts[c] += 1
    return selected[:finaln], {"cluster_quotas": quotas, "final30_cluster_counts": dict(counts), "selected_by_bucket_champion": True}


def _quotas(finaln: int, regime: str) -> dict[str, int]:
    if regime == "AI_OFF_ROTATION":
        # AI/tech combined quota is capped at 8/30 (26.7%); MEGA_TECH is part of the cap.
        return {"AI_SEMI": 3, "AI_SOFTWARE": 2, "DATA_CENTER_POWER": 1, "MEGA_TECH": 2, "INDUSTRIAL": 5, "FINANCIAL": 4, "HEALTHCARE": 4, "CONSUMER_STAPLES": 3, "DEFENSIVE_UTILITY": 1, "ENERGY_MATERIALS": 3, "ETF_INDEX": 2}
    if regime == "RISK_OFF":
        return {"AI_SEMI": 2, "AI_SOFTWARE": 1, "DATA_CENTER_POWER": 1, "HEALTHCARE": 7, "CONSUMER_STAPLES": 6, "DEFENSIVE_UTILITY": 5, "ETF_INDEX": 3, "ENERGY_MATERIALS": 2, "FINANCIAL": 1, "INDUSTRIAL": 1}
    if regime == "AI_ON":
        return {"AI_SEMI": 10, "AI_SOFTWARE": 4, "DATA_CENTER_POWER": 4, "MEGA_TECH": 3, "HEALTHCARE": 3, "CONSUMER_STAPLES": 2, "INDUSTRIAL": 2, "FINANCIAL": 1, "ETF_INDEX": 1}
    return {"AI_SEMI": 5, "AI_SOFTWARE": 3, "DATA_CENTER_POWER": 3, "MEGA_TECH": 3, "INDUSTRIAL": 4, "FINANCIAL": 3, "HEALTHCARE": 3, "CONSUMER_STAPLES": 2, "ENERGY_MATERIALS": 2, "ETF_INDEX": 2}
