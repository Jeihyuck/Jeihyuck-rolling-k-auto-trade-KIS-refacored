# -*- coding: utf-8 -*-
"""US Score Alias Canonicalization.

한국장 score_columns.py 패턴을 미국장용으로 이식.

핵심 기능:
1. score alias 복구: score, score_final, final_score, meta.score 등 모든 후보 탐색
2. numeric canonicalization: Decimal, string, None, NaN 안전 변환
3. JSON dict 파싱: meta가 문자열일 때 복구
4. 0 vs None 구별: zero_invalid=True일 때 0은 fallback으로만 사용
5. raw score fields 보존: 진단용
"""
from __future__ import annotations

import json
import logging
import math
from decimal import Decimal
from typing import Any

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# US Score Alias 후보
# ══════════════════════════════════════════════════════════════════════════════

US_SCORE_ALIAS_CANDIDATES: dict[str, tuple[str, ...]] = {
    "final": (
        "score",
        "score_final",
        "final_score",
        "final",
        "composite_score",
        "rank_score",
        "total_score",
    ),
    "momentum": (
        "momentum_score",
        "score_momentum",
        "momentum",
    ),
    "breakout": (
        "breakout_score",
        "score_breakout",
        "breakout",
    ),
    "pullback": (
        "pullback_score",
        "score_pullback",
        "pullback",
    ),
    "rs": (
        "rs_score",
        "relative_strength_score",
        "rs",
    ),
    "trend": (
        "trend_score",
        "trend",
    ),
    "vcp": (
        "vcp_score",
        "vcp",
    ),
}


# ══════════════════════════════════════════════════════════════════════════════
# Low-level 정규화 함수
# ══════════════════════════════════════════════════════════════════════════════

def _is_blankish(value: Any) -> bool:
    """None, 빈 문자열, 'nan', 'none', pd.NA 등을 blank로 판단."""
    if value is None:
        return True
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped or stripped.lower() in {"nan", "none", "null", ""}:
            return True
    # pd.isna 사용 (pandas 없으면 fallback)
    try:
        import pandas as pd
        return bool(pd.isna(value))
    except Exception:
        pass
    # math.isnan (숫자 타입만)
    if isinstance(value, (int, float, Decimal)):
        try:
            return math.isnan(float(value))
        except Exception:
            pass
    return False


def coerce_json_dict(value: Any) -> dict:
    """value가 JSON 문자열이면 dict로 파싱, 이미 dict이면 그대로, 아니면 빈 dict."""
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass
    return {}


def safe_float_or_none(value: Any) -> float | None:
    """숫자형 값을 float로 변환. None/NaN/Inf이면 None 반환."""
    if _is_blankish(value):
        return None
    
    # Decimal 처리
    if isinstance(value, Decimal):
        try:
            f = float(value)
            if math.isfinite(f):
                return f
        except Exception:
            pass
        return None
    
    # 문자열 → float
    if isinstance(value, str):
        try:
            f = float(value.strip())
            if math.isfinite(f):
                return f
        except Exception:
            pass
        return None
    
    # 이미 숫자형
    if isinstance(value, (int, float)):
        try:
            f = float(value)
            if math.isfinite(f):
                return f
        except Exception:
            pass
    
    return None


def pick_numeric(*values: Any, zero_invalid: bool = False) -> float | None:
    """여러 후보 중 첫 번째로 유효한 숫자를 반환.
    
    Args:
        *values: 후보 값들
        zero_invalid: True이면 0을 무효로 취급하고 fallback으로만 사용
        
    Returns:
        첫 번째 유효 숫자. 모두 None이면 None.
        zero_invalid=True일 때 0 외의 값이 없으면 0을 fallback으로 반환.
    """
    fallback: float | None = None
    for v in values:
        num = safe_float_or_none(v)
        if num is None:
            continue
        # zero_invalid이면 0은 일단 skip
        if zero_invalid and num == 0.0:
            if fallback is None:
                fallback = num
            continue
        # 유효한 비제로 값 발견
        return num
    # 모든 후보가 None이거나 0이었으면 fallback 반환
    return fallback


# ══════════════════════════════════════════════════════════════════════════════
# Score Alias 복구
# ══════════════════════════════════════════════════════════════════════════════

def extract_us_score(
    row: dict,
    logical_name: str = "final",
    return_source: bool = False,
) -> float | None | tuple[float | None, str]:
    """US watchlist row에서 score를 복구한다.
    
    Args:
        row: watchlist row dict
        logical_name: "final", "momentum", "breakout", "pullback", "rs", "trend", "vcp"
        return_source: True이면 (score, source) 반환
        
    Returns:
        score: float | None
        source (return_source=True): "row.score_final" | "meta.score" | ...
    """
    candidates = US_SCORE_ALIAS_CANDIDATES.get(logical_name, ())
    if not candidates:
        if return_source:
            return None, "no_alias_candidates"
        return None
    
    # meta, scores, features, reason_json, risk_snapshot_json를 dict로 coerce
    meta_raw = row.get("meta") or {}
    meta = coerce_json_dict(meta_raw)

    reason_json_raw = row.get("reason_json") or meta.get("reason_json") or {}
    reason_json = coerce_json_dict(reason_json_raw)
    
    scores_raw = row.get("scores") or {}
    scores = coerce_json_dict(scores_raw)
    
    features_raw = row.get("features") or {}
    features = coerce_json_dict(features_raw)

    risk_snapshot_raw = row.get("risk_snapshot_json") or meta.get("risk_snapshot_json") or {}
    risk_snapshot_json = coerce_json_dict(risk_snapshot_raw)

    containers: list[tuple[str, dict]] = [
        ("row", row),
        ("meta", meta),
        ("reason_json", reason_json),
        ("scores", scores),
        ("features", features),
        ("risk_snapshot_json", risk_snapshot_json),
    ]

    zero_fallback: tuple[float, str] | None = None

    for container_name, container in containers:
        for field in candidates:
            num = safe_float_or_none(container.get(field))
            if num is None:
                continue
            source = f"{container_name}.{field}"
            if num != 0.0:
                if return_source:
                    return num, source
                return num
            if zero_fallback is None:
                zero_fallback = (num, source)
    
    if zero_fallback is not None:
        if return_source:
            return zero_fallback
        return zero_fallback[0]
    
    # 완전 실패
    if return_source:
        return None, "alias_resolution_failed"
    return None


# ══════════════════════════════════════════════════════════════════════════════
# Canonical Watchlist Row
# ══════════════════════════════════════════════════════════════════════════════

def canonicalize_us_watchlist_row(row: dict) -> dict:
    """US watchlist row를 canonical 형태로 정규화한다.
    
    처리 항목:
    1. symbol, exchange 정규화
    2. score alias 복구 (final, momentum, breakout, pullback, rs, trend, vcp)
    3. meta 필드 복구
    4. raw score fields 보존
    5. source 정보 기록
    
    Returns:
        정규화된 dict (원본은 변경하지 않음)
    """
    src = dict(row)
    meta_raw = src.get("meta") or {}
    meta = coerce_json_dict(meta_raw)

    reason_json_raw = src.get("reason_json") or meta.get("reason_json") or {}
    reason_json = coerce_json_dict(reason_json_raw)

    risk_snapshot_raw = src.get("risk_snapshot_json") or meta.get("risk_snapshot_json") or {}
    risk_snapshot_json = coerce_json_dict(risk_snapshot_raw)
    
    scores_raw = src.get("scores") or {}
    scores = coerce_json_dict(scores_raw)
    
    out = dict(src)
    
    # ── Symbol / Exchange ──────────────────────────────────────────────────────
    symbol = str(src.get("symbol") or "").strip().upper()
    exchange = str(src.get("exchange") or meta.get("exchange") or "").strip().upper()
    
    out["symbol"] = symbol if symbol else None
    out["exchange"] = exchange if exchange else None
    
    # ── Score Alias Recovery ────────────────────────────────────────────────────
    # raw score fields 보존 (진단용)
    raw_score_fields: dict[str, Any] = {}
    for key in ("score", "score_final", "final_score", "composite_score", "rank_score"):
        if key in src:
            raw_score_fields[f"row_{key}"] = src[key]
        if key in meta:
            raw_score_fields[f"meta_{key}"] = meta[key]
        if key in reason_json:
            raw_score_fields[f"reason_json_{key}"] = reason_json[key]
        if key in scores:
            raw_score_fields[f"scores_{key}"] = scores[key]
        if key in risk_snapshot_json:
            raw_score_fields[f"risk_snapshot_json_{key}"] = risk_snapshot_json[key]
    
    # Final score 복구
    final_score, final_source = extract_us_score(src, "final", return_source=True)
    out["score"] = final_score
    out["score_final"] = final_score
    out["final_score"] = final_score
    out["score_source"] = final_source
    out["reason_json"] = reason_json
    out["risk_snapshot_json"] = risk_snapshot_json
    out["scores"] = scores
    
    # Component scores 복구
    momentum_score, momentum_source = extract_us_score(src, "momentum", return_source=True)
    out["momentum_score"] = momentum_score
    
    breakout_score, breakout_source = extract_us_score(src, "breakout", return_source=True)
    out["breakout_score"] = breakout_score
    
    pullback_score, pullback_source = extract_us_score(src, "pullback", return_source=True)
    out["pullback_score"] = pullback_score
    
    rs_score, rs_source = extract_us_score(src, "rs", return_source=True)
    out["rs_score"] = rs_score
    
    trend_score, trend_source = extract_us_score(src, "trend", return_source=True)
    out["trend_score"] = trend_score
    
    vcp_score, vcp_source = extract_us_score(src, "vcp", return_source=True)
    out["vcp_score"] = vcp_score
    
    # ── Meta 필드 정규화 ───────────────────────────────────────────────────────
    # meta는 dict로 유지
    canonical_meta = dict(meta)
    canonical_meta["score"] = final_score
    canonical_meta["score_final"] = final_score
    canonical_meta["final_score"] = final_score
    canonical_meta["score_source"] = final_source
    canonical_meta["raw_score_fields"] = raw_score_fields
    canonical_meta["reason_json"] = reason_json
    canonical_meta["risk_snapshot_json"] = risk_snapshot_json
    canonical_meta["momentum_source"] = momentum_source
    canonical_meta["breakout_source"] = breakout_source
    canonical_meta["pullback_source"] = pullback_source
    canonical_meta["rs_source"] = rs_source
    canonical_meta["trend_source"] = trend_source
    canonical_meta["vcp_source"] = vcp_source
    
    out["meta"] = canonical_meta
    
    return out


# ══════════════════════════════════════════════════════════════════════════════
# Score Quality Stats
# ══════════════════════════════════════════════════════════════════════════════

def collect_us_score_nonzero_stats(rows: list[dict]) -> dict:
    """US watchlist rows의 score 통계를 수집한다.
    
    Args:
        rows: canonical watchlist rows (canonicalize_us_watchlist_row 통과한 상태)
        
    Returns:
        {
            "total": int,
            "unique_symbols": int,
            "score_nonzero": int,
            "score_zero": int,
            "score_missing": int,
            "score_nonzero_ratio": float,
            "momentum_nonzero": int,
            "breakout_nonzero": int,
            "pullback_nonzero": int,
            "rs_nonzero": int,
            "trend_nonzero": int,
            "vcp_nonzero": int,
        }
    """
    if not rows:
        return {
            "total": 0,
            "unique_symbols": 0,
            "score_nonzero": 0,
            "score_zero": 0,
            "score_missing": 0,
            "score_nonzero_ratio": 0.0,
            "momentum_nonzero": 0,
            "breakout_nonzero": 0,
            "pullback_nonzero": 0,
            "rs_nonzero": 0,
            "trend_nonzero": 0,
            "vcp_nonzero": 0,
        }
    
    total = len(rows)
    symbols = set()
    score_nonzero = 0
    score_zero = 0
    score_missing = 0
    
    momentum_nonzero = 0
    breakout_nonzero = 0
    pullback_nonzero = 0
    rs_nonzero = 0
    trend_nonzero = 0
    vcp_nonzero = 0
    
    for row in rows:
        sym = row.get("symbol")
        if sym:
            symbols.add(sym)
        
        s = safe_float_or_none(row.get("score"))
        if s is None:
            score_missing += 1
        elif s == 0.0:
            score_zero += 1
        else:
            score_nonzero += 1
        
        # Component scores
        if safe_float_or_none(row.get("momentum_score")):
            momentum_nonzero += 1
        if safe_float_or_none(row.get("breakout_score")):
            breakout_nonzero += 1
        if safe_float_or_none(row.get("pullback_score")):
            pullback_nonzero += 1
        if safe_float_or_none(row.get("rs_score")):
            rs_nonzero += 1
        if safe_float_or_none(row.get("trend_score")):
            trend_nonzero += 1
        if safe_float_or_none(row.get("vcp_score")):
            vcp_nonzero += 1
    
    unique_count = len(symbols)
    ratio = score_nonzero / total if total > 0 else 0.0
    
    return {
        "total": total,
        "unique_symbols": unique_count,
        "score_nonzero": score_nonzero,
        "score_zero": score_zero,
        "score_missing": score_missing,
        "score_nonzero_ratio": round(ratio, 4),
        "momentum_nonzero": momentum_nonzero,
        "breakout_nonzero": breakout_nonzero,
        "pullback_nonzero": pullback_nonzero,
        "rs_nonzero": rs_nonzero,
        "trend_nonzero": trend_nonzero,
        "vcp_nonzero": vcp_nonzero,
    }


# ══════════════════════════════════════════════════════════════════════════════
# Score Contract
# ══════════════════════════════════════════════════════════════════════════════

def validate_us_watchlist_score_contract(
    rows: list[dict],
    stage: str,
    min_nonzero_ratio: float = 0.80,
) -> dict:
    """US watchlist score contract 검증.
    
    Args:
        rows: canonical watchlist rows
        stage: "prep_save" | "trade_load" | ...
        min_nonzero_ratio: 최소 nonzero ratio (기본 0.80)
        
    Returns:
        {
            "stage": str,
            "ok": bool,
            "stats": {...},
            "errors": list[str],
            "warnings": list[str],
        }
    """
    stats = collect_us_score_nonzero_stats(rows)
    errors: list[str] = []
    warnings: list[str] = []
    
    # Empty watchlist
    if stats["total"] == 0:
        errors.append(f"[{stage}] empty_watchlist")
    
    # Score missing
    if stats["score_missing"] > 0:
        errors.append(
            f"[{stage}] score_missing count={stats['score_missing']} total={stats['total']}"
        )
    
    # Score nonzero ratio
    if stats["score_nonzero_ratio"] < min_nonzero_ratio:
        errors.append(
            f"[{stage}] score_nonzero_ratio below threshold: {stats['score_nonzero_ratio']:.4f} < {min_nonzero_ratio:.4f}"
        )
    
    # Score zero mass warning
    if stats["score_zero"] > 0:
        warnings.append(
            f"[{stage}] score_zero count={stats['score_zero']} (may be fallback from alias recovery)"
        )
    
    ok = len(errors) == 0
    
    return {
        "stage": stage,
        "ok": ok,
        "stats": stats,
        "errors": errors,
        "warnings": warnings,
    }
