from __future__ import annotations

import json
import logging
from collections import Counter
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Iterable

from trader.botstate_paths import botstate_path
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


def run_minervini_report(
    universe_members: list[dict],
    cfg: object,
    *,
    as_of: str | None = None,
    candidates: list[object] | None = None,
    report_dir: str | Path | None = None,
) -> str | None:
    as_of = as_of or now_kst().date().isoformat()
    report_root = Path(report_dir) if report_dir else botstate_path("runtime", "reports", "minervini", as_of)
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
        rejected_payload.append(
            {
                "code": code,
                "name": name_map.get(code),
                "reasons": mapped_reasons,
            }
        )

    payload = {
        "input_members": len(universe_members),
        "candidates": candidates_payload,
        "rejected": rejected_payload,
        "reason_counts": dict(reason_counts),
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
