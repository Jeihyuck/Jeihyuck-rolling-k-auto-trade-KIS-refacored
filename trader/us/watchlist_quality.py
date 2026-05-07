# -*- coding: utf-8 -*-
"""US Watchlist Quality Contract.

한국장 final30_quality.py 패턴을 미국장용으로 이식.

핵심 검증:
1. rows > 0
2. unique_symbols >= US_MIN_LOCKED_WATCHLIST_COUNT
3. score_nonzero_ratio >= US_MIN_WATCHLIST_SCORE_NONZERO_RATIO
4. score_missing_count == 0
5. score_zero_mass 방지
6. symbol/exchange/schema valid
7. duplicate count 기록
"""
from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# 환경변수
# ══════════════════════════════════════════════════════════════════════════════

def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default


def _env_bool(key: str, default: bool) -> bool:
    value = str(os.getenv(key, str(int(default)))).strip().lower()
    return value in {"1", "true", "yes", "on"}


US_MIN_LOCKED_WATCHLIST_COUNT = _env_int("US_MIN_LOCKED_WATCHLIST_COUNT", 10)
US_MIN_WATCHLIST_SCORE_NONZERO_RATIO = _env_float("US_MIN_WATCHLIST_SCORE_NONZERO_RATIO", 0.80)
US_REQUIRE_WATCHLIST_SCORE_CONTRACT = _env_bool("US_REQUIRE_WATCHLIST_SCORE_CONTRACT", True)
US_FAIL_ON_SCORE_ZERO_MASS = _env_bool("US_FAIL_ON_SCORE_ZERO_MASS", True)


# ══════════════════════════════════════════════════════════════════════════════
# US Watchlist Quality Contract
# ══════════════════════════════════════════════════════════════════════════════

def validate_us_locked_watchlist_quality(
    rows: list[dict],
    stage: str = "trade_load",
) -> dict:
    """US locked watchlist quality contract 검증.
    
    한국장의 verify_final30_scored_rows() 패턴과 동일.
    
    Args:
        rows: canonical watchlist rows (canonicalize_us_watchlist_row 통과 상태)
        stage: "prep_save" | "trade_load" | "trade_tick"
        
    Returns:
        {
            "stage": str,
            "ok": bool,
            "rows": int,
            "unique_symbols": int,
            "duplicate_count": int,
            "score_nonzero": int,
            "score_zero": int,
            "score_missing": int,
            "score_nonzero_ratio": float,
            "errors": list[str],
            "warnings": list[str],
            "sample_bad_symbols": list[str],
            "trade_can_proceed": bool,
        }
    """
    from trader.us.score_columns import collect_us_score_nonzero_stats
    
    errors: list[str] = []
    warnings: list[str] = []
    sample_bad: list[str] = []
    
    # ── Empty watchlist ────────────────────────────────────────────────────────
    if not rows:
        errors.append(f"[{stage}] locked_watchlist_empty")
        return {
            "stage": stage,
            "ok": False,
            "rows": 0,
            "unique_symbols": 0,
            "duplicate_count": 0,
            "score_nonzero": 0,
            "score_zero": 0,
            "score_missing": 0,
            "score_nonzero_ratio": 0.0,
            "errors": errors,
            "warnings": warnings,
            "sample_bad_symbols": sample_bad,
            "trade_can_proceed": False,
        }
    
    # ── Score stats ────────────────────────────────────────────────────────────
    stats = collect_us_score_nonzero_stats(rows)
    
    total = stats["total"]
    unique = stats["unique_symbols"]
    duplicate = total - unique
    score_nonzero = stats["score_nonzero"]
    score_zero = stats["score_zero"]
    score_missing = stats["score_missing"]
    ratio = stats["score_nonzero_ratio"]
    
    # ── Unique symbol count contract ───────────────────────────────────────────
    min_count = US_MIN_LOCKED_WATCHLIST_COUNT
    if unique < min_count:
        errors.append(
            f"[{stage}] locked_watchlist_unique_below_min: unique={unique} min={min_count}"
        )
    
    # ── Score missing contract ─────────────────────────────────────────────────
    if score_missing > 0:
        errors.append(
            f"[{stage}] score_missing: count={score_missing} total={total}"
        )
        # sample bad symbols
        for row in rows:
            if row.get("score") is None:
                sym = row.get("symbol") or "UNKNOWN"
                if len(sample_bad) < 5:
                    sample_bad.append(sym)
    
    # ── Score nonzero ratio contract ───────────────────────────────────────────
    min_ratio = US_MIN_WATCHLIST_SCORE_NONZERO_RATIO
    if US_REQUIRE_WATCHLIST_SCORE_CONTRACT:
        if ratio < min_ratio:
            errors.append(
                f"[{stage}] locked_watchlist_score_contract_fail: ratio={ratio:.4f} < {min_ratio:.4f}"
            )
    
    # ── Score zero mass contract ───────────────────────────────────────────────
    if US_FAIL_ON_SCORE_ZERO_MASS:
        # score zero mass: nonzero < 50% of unique
        if unique > 0 and score_nonzero < unique * 0.5:
            errors.append(
                f"[{stage}] watchlist_score_zero_mass: nonzero={score_nonzero} unique={unique} ratio={ratio:.4f}"
            )
            # sample bad symbols
            for row in rows:
                s = row.get("score")
                if s is None or s == 0.0:
                    sym = row.get("symbol") or "UNKNOWN"
                    if len(sample_bad) < 10:
                        sample_bad.append(sym)
    
    # ── Warnings: duplicate rows ───────────────────────────────────────────────
    if duplicate > 0:
        warnings.append(
            f"[{stage}] duplicate_rows: raw={total} unique={unique} duplicate={duplicate}"
        )
    
    # ── Warnings: score zero (fallback 가능) ───────────────────────────────────
    if score_zero > 0 and score_zero < unique * 0.2:
        warnings.append(
            f"[{stage}] score_zero: count={score_zero} (may be alias fallback)"
        )
    
    # ── Final verdict ──────────────────────────────────────────────────────────
    ok = len(errors) == 0
    trade_can_proceed = ok
    
    result = {
        "stage": stage,
        "ok": ok,
        "rows": total,
        "unique_symbols": unique,
        "duplicate_count": duplicate,
        "score_nonzero": score_nonzero,
        "score_zero": score_zero,
        "score_missing": score_missing,
        "score_nonzero_ratio": round(ratio, 4),
        "errors": errors,
        "warnings": warnings,
        "sample_bad_symbols": sample_bad,
        "trade_can_proceed": trade_can_proceed,
    }
    
    # Log
    logger.info(
        "[US_WATCHLIST][QUALITY] stage=%s rows=%d unique=%d duplicate=%d "
        "score_nonzero=%d score_zero=%d missing=%d ratio=%.4f ok=%d",
        stage, total, unique, duplicate, score_nonzero, score_zero, score_missing, ratio, int(ok)
    )
    
    if errors:
        for err in errors:
            logger.error("[US_WATCHLIST][CONTRACT_FAIL] %s", err)
    
    if warnings:
        for warn in warnings:
            logger.warning("[US_WATCHLIST][CONTRACT_WARN] %s", warn)
    
    return result


# ══════════════════════════════════════════════════════════════════════════════
# Format Error Message
# ══════════════════════════════════════════════════════════════════════════════

def format_us_watchlist_error_message(contract: dict) -> str:
    """Contract 실패 메시지 포맷팅 (한국장 format_final30_abort_message 패턴).
    
    Args:
        contract: validate_us_locked_watchlist_quality() 결과
        
    Returns:
        포맷팅된 에러 메시지
    """
    lines = [
        "[US_WATCHLIST][CONTRACT_FAIL]",
        f"stage={contract.get('stage')}",
        f"rows={contract.get('rows', 0)}",
        f"unique={contract.get('unique_symbols', 0)}",
        f"duplicate={contract.get('duplicate_count', 0)}",
        f"score_nonzero={contract.get('score_nonzero', 0)}",
        f"score_zero={contract.get('score_zero', 0)}",
        f"score_missing={contract.get('score_missing', 0)}",
        f"ratio={contract.get('score_nonzero_ratio', 0.0):.4f}",
        f"ok={int(contract.get('ok', False))}",
    ]
    
    errors = contract.get("errors", [])
    if errors:
        lines.append("errors:")
        for err in errors:
            lines.append(f"  - {err}")
    
    warnings = contract.get("warnings", [])
    if warnings:
        lines.append("warnings:")
        for warn in warnings:
            lines.append(f"  - {warn}")
    
    sample = contract.get("sample_bad_symbols", [])
    if sample:
        lines.append(f"sample_bad_symbols: {', '.join(sample[:10])}")
    
    return "\n".join(lines)
