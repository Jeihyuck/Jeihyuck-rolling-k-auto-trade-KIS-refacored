from __future__ import annotations
from trader.kr.sector_classifier import KR_CYCLICAL_VALUE_CLUSTERS, KR_DEFENSIVE_CLUSTERS, KR_HIGH_BETA_CLUSTERS, summarize_final30_clusters
from trader.kr.sector_proxy_map import compute_sector_proxy_return, load_kr_sector_proxy_map

def evaluate_kr_sector_rotation(*, trade_date: str, provider, final30_rows: list[dict] | None, index_context: dict, positions: list[dict] | None=None) -> dict:
    sectors = list(load_kr_sector_proxy_map().keys())
    strength = {s: compute_sector_proxy_return(sector=s, provider=provider, trade_date=trade_date, index_context=index_context) for s in sectors}
    # merge explicit test sector_context-like provider strengths
    supplied = getattr(provider, "sector_strength", None) or {}
    for s, v in supplied.items():
        strength.setdefault(s, {"sector": s}).update(v if isinstance(v, dict) else {"return_3d": v, "vs_kospi_3d": v})
    ranked = sorted(strength, key=lambda s: (strength[s].get("vs_kospi_3d") if strength[s].get("vs_kospi_3d") is not None else -99), reverse=True)
    leaders = [s for s in ranked if (strength[s].get("vs_kospi_3d") or 0) > 0][:3]
    laggards = [s for s in reversed(ranked) if (strength[s].get("vs_kospi_3d") or 0) < 0][:3]
    def avg(group):
        vals=[strength.get(s,{}).get("vs_kospi_3d") for s in group if strength.get(s,{}).get("vs_kospi_3d") is not None]
        return sum(vals)/len(vals) if vals else None
    growth, value, defensive = avg(KR_HIGH_BETA_CLUSTERS), avg(KR_CYCLICAL_VALUE_CLUSTERS), avg(KR_DEFENSIVE_CLUSTERS)
    kosdaq150_3d = index_context.get("kosdaq150_3d_return"); kospi200_3d = index_context.get("kospi200_3d_return")
    kosdaq_growth_stress = (kosdaq150_3d is not None and kospi200_3d is not None and float(kosdaq150_3d)-float(kospi200_3d) <= -0.015)
    kospi_value_leadership = bool(kospi200_3d is not None and float(kospi200_3d) > 0 and any(s in leaders for s in ("FINANCIAL","AUTO","SHIPBUILDING_MACHINERY","SEMICONDUCTOR")))
    if (index_context.get("kospi_1d_return") or 0) < -0.012 and (index_context.get("kosdaq_1d_return") or 0) < -0.018: regime="KR_BROAD_RISK_OFF"
    elif kospi_value_leadership: regime="KR_KOSPI_VALUE_LEAD"
    elif "SEMICONDUCTOR" in leaders: regime="KR_SEMICONDUCTOR_LEAD"
    elif any(s in leaders for s in KR_DEFENSIVE_CLUSTERS): regime="KR_DEFENSIVE_LEAD"
    elif growth is not None and defensive is not None and growth < defensive - 0.01: regime="KR_HIGH_BETA_OFF"
    elif (index_context.get("kosdaq150_3d_return") or 0) > 0 and any(s in leaders for s in ("BIO_HEALTHCARE","SECONDARY_BATTERY")): regime="KR_KOSDAQ_GROWTH_LEAD"
    elif (index_context.get("kospi_1d_return") or 0) > 0 and (index_context.get("kosdaq_1d_return") or 0) > 0: regime="KR_BROAD_UP"
    else: regime="KR_MIXED" if leaders or laggards else "KR_UNKNOWN"
    cluster = summarize_final30_clusters(final30_rows)
    quality = {s: strength[s].get("source_quality", "suspect") for s in strength}
    suspect = cluster["sector_context_suspect"] or any(q in {"low","suspect"} for q in quality.values())
    return {"rotation_regime":regime,"sector_leaders":leaders,"sector_laggards":laggards,"sector_strength":strength,"sector_proxy_quality":quality,"growth_vs_value": None if growth is None or value is None else growth-value,"high_beta_vs_defensive": None if growth is None or defensive is None else growth-defensive,"kosdaq_growth_stress":kosdaq_growth_stress,"kospi_value_leadership":kospi_value_leadership,"rotation_context_suspect":suspect,"rotation_warnings":["sector_context_suspect"] if suspect else []}
