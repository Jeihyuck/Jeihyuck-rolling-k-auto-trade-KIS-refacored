from __future__ import annotations

import json
import logging
from collections import Counter
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Iterable

from trader.runtime_paths import runtime_path
from trader.time_utils import now_kst

logger = logging.getLogger(__name__)

_REASON_MAP = {
    "rs_below_min": "RS_BELOW",
    "trend_template_fail": "REGIME_FAIL",
    "ma200_not_rising": "REGIME_FAIL",
    "illiquid": "LIQ_FAIL",
    "liquidity_fail": "LIQ_FAIL",
    "gap_fail": "GAP_FAIL",
    "spread_fail": "SPREAD_FAIL",
    "range_fail": "RANGE_FAIL",
    "vcp_fail": "VCP_LOW",
    "score_below_min": "SCORE_BELOW_MIN",
    "score_below_cut": "SCORE_BELOW_CUT",
    "price_scale_outlier": "PRICE_SCALE_OUTLIER",
    "insufficient_candles": "INSUFFICIENT_CANDLES",
    "data_empty": "DATA_MISSING",
    "planned_qty_zero_or_min_order": "MIN_ORDER_FAIL",
    "atr_pct_too_high": "ATR_TOO_HIGH",
    "liquidity_too_low": "LIQ_TOO_LOW",
    "data_short": "DATA_SHORT",
    "ma200_slope_unknown": "MA200_SLOPE_UNKNOWN",
    "ma200_nan": "MA200_NAN",
    "atr_pct_missing": "ATR_MISSING",
}


def _map_reason_codes(reasons: Iterable[str] | None) -> list[str]:
    mapped = []
    for reason in reasons or []:
        mapped.append(_REASON_MAP.get(reason, str(reason).upper()))
    if not mapped:
        mapped = ["UNSPECIFIED_FAIL"]
    return mapped


def _cfg_snapshot(cfg: object) -> dict:
    if is_dataclass(cfg):
        return asdict(cfg)
    if hasattr(cfg, "__dict__"):
        return dict(cfg.__dict__)
    return {"value": str(cfg)}


def _candidate_attr(candidate: object, name: str, default=None):
    if hasattr(candidate, name):
        return getattr(candidate, name)
    if isinstance(candidate, dict):
        return candidate.get(name, default)
    return default


def _extract_numeric_details(candidate: object) -> dict:
    """
    rejected 리스트를 위해 후보자의 수치 세부사항 추출
    주요 수치: rs_pctile, vcp_score, regime_pass, pullback_pct, vol_contraction, close, ma20, ma50, ma200 등
    """
    details = {}
    features = _candidate_attr(candidate, "features", {})
    
    if not features:
        return details
    
    # Minervini scores
    for key in ["rs_pctile", "rs_percentile", "vcp_score", "regime_pass"]:
        val = features.get(key)
        if val is not None:
            details[key] = val
    
    # PB1 filters
    for key in ["pullback_pct", "vol_contraction", "volu_contraction", 
                "vol_contraction_ratio", "volu_contraction_ratio"]:
        val = features.get(key)
        if val is not None:
            details[key] = val
    
    # Price & MA
    for key in ["close", "ma20", "ma50", "ma200", "ma20_slope", "pivot", "tight_low"]:
        val = features.get(key)
        if val is not None:
            details[key] = val
    
    # Risk metrics
    for key in ["atr14", "atr_pct", "atr_max_pct"]:
        val = features.get(key)
        if val is not None:
            details[key] = val
    
    # Sizing details
    sizing_reason = _candidate_attr(candidate, "sizing_reason", None)
    sizing_details = _candidate_attr(candidate, "sizing_details", {})
    if sizing_reason:
        details["sizing_reason"] = sizing_reason
    if sizing_details:
        details["sizing_details"] = sizing_details
    
    # Detail reasons (minervini_not_buyable 시 세부 원인)
    detail_reasons = _candidate_attr(candidate, "detail_reasons", None)
    if detail_reasons:
        details["minervini_detail_reasons"] = detail_reasons
    
    return details


def _collect_relax_pass_counts(candidates: list[object]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for candidate in candidates or []:
        features = _candidate_attr(candidate, "features", {}) or {}
        label = str(features.get("relax_pass") or features.get("relax_label") or "unknown")
        counts[label] += 1
    return dict(counts)


def run_minervini_report(
    universe_members: list[dict],
    cfg: object,
    *,
    as_of: str | None = None,
    candidates: list[object] | None = None,
    report_dir: str | Path | None = None,
) -> str | None:
    as_of = as_of or now_kst().date().isoformat()
    report_root = Path(report_dir) if report_dir else runtime_path("runtime", "reports", "minervini", as_of)
    report_root.mkdir(parents=True, exist_ok=True)
    report_path = report_root / "minervini_report.json"

    name_map = {
        str(m.get("code") or "").zfill(6): (m.get("meta_json") or {}).get("name")
        for m in universe_members
    }

    candidates_payload: list[dict] = []
    rejected_payload: list[dict] = []
    reason_counts: Counter[str] = Counter()

    for cf in candidates or []:
        code = str(_candidate_attr(cf, "code", "") or "").zfill(6)
        setup_ok = bool(_candidate_attr(cf, "setup_ok", False))
        reasons = _candidate_attr(cf, "reasons", None)
        score = _candidate_attr(cf, "score", None)
        if setup_ok:
            candidates_payload.append({"code": code, "name": name_map.get(code), "score": score})
            continue
        mapped_reasons = _map_reason_codes(reasons)
        for reason in mapped_reasons:
            reason_counts[reason] += 1
        
        # 수치 세부사항 추가
        numeric_details = _extract_numeric_details(cf)
        rejected_item = {
            "code": code,
            "name": name_map.get(code),
            "reasons": mapped_reasons,
        }
        rejected_item.update(numeric_details)
        rejected_payload.append(rejected_item)

    payload = {
        "input_members": len(universe_members),
        "total_scanned": len(candidates or []),
        "after_pb1_filter": len(candidates_payload),
        "after_relax_pass_counts": _collect_relax_pass_counts(candidates or []),
        "candidates": candidates_payload,
        "rejected": rejected_payload,
        "reason_counts": dict(reason_counts),
        "rejection_reason_histogram": dict(reason_counts),
        "top_rejected_symbols": rejected_payload[:10],
        "cfg_snapshot": _cfg_snapshot(cfg),
    }
    report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info(
        "[MINERVINI][STATS] input=%s candidates=%s rejected=%s",
        len(universe_members),
        len(candidates_payload),
        len(rejected_payload),
    )
    summary_text = " ".join([f"{key}={count}" for key, count in reason_counts.most_common(10)]) or "(none)"
    logger.info("[MINERVINI][REJECT_SUMMARY] %s", summary_text)
    logger.info(
        "[MINERVINI][REPORT] path=%s candidates=%s rejected=%s",
        report_path,
        len(candidates_payload),
        len(rejected_payload),
    )
    return str(report_path)
