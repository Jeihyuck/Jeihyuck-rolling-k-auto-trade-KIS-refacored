# -*- coding: utf-8 -*-
"""US Final30 Quality Contract.

한국장 final30_quality.py 패턴을 미국장 dual-agent 구조로 이식.
한국장 파일 import 금지.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# 필수 컬럼 목록
_REQUIRED_COLS = [
    "symbol",
    "exchange",
    "asset_type",
    "rank_final30",
    "score_final",
    "agent_a_score",
    "agent_b_score",
    "candidate_score",
    "tech_score",
    "rs_20d",
    "rs_60d",
    "rs_120d",
    "rs_percentile",
    "trend_score",
    "breakout_score",
    "pullback_score",
    "momentum_score",
    "vcp_score",
    "volume_accel_score",
    "liquidity_score",
    "risk_score",
    "atr_pct",
    "close",
    "ma20",
    "ma50",
    "ma150",
    "pullback_pct",
    "entry_style_selected",
    "source_tags",
    "reason_json",
    "trade_date",
]

# soft-required (경고만, 실패 아님)
_SOFT_REQUIRED_COLS = {"ma150", "rs_120d", "vcp_score"}

# ETF 최대 허용 수
_MAX_ETF = 5
_CORE_ETFS: set[str] = {"SPY", "QQQ", "QQQM", "SMH", "SOXX"}


def _safe_float(val: Any) -> float | None:
    if val is None or val == "":
        return None
    try:
        return float(str(val).replace(",", ""))
    except Exception:
        return None


def _row_has_close(row: dict) -> bool:
    """row에서 종가 (close 또는 price) 조회."""
    for key in ("close", "price", "clos"):
        val = row.get(key)
        if val is not None:
            f = _safe_float(val)
            if f is not None and f > 0:
                return True
    return False


def _get_close(row: dict) -> float | None:
    for key in ("close", "price", "clos"):
        val = row.get(key)
        if val is not None:
            f = _safe_float(val)
            if f is not None and f > 0:
                return f
    return None


def verify_us_final30_scored_rows(
    rows: list[dict],
    *,
    required_rows: int = 30,
    source: str = "unknown",
) -> dict:
    """US final30_scored strict contract 검증.

    Args:
        rows: final30_scored 리스트
        required_rows: 필수 행 수 (기본 30)
        source: 로그용 source 식별자

    Returns:
        {
            "ok": bool,
            "rows": int,
            "unique_symbols": int,
            "required_cols_ok": bool,
            "rank_ok": bool,
            "score_nonzero_count": int,
            "agent_a_nonzero_count": int,
            "agent_b_nonzero_count": int,
            "invalid_row_count": int,
            "errors": list[str],
            "warnings": list[str],
        }
    """
    errors: list[str] = []
    warnings: list[str] = []

    row_count = len(rows)
    invalid_row_count = 0

    # ── 행 수 검증 ──────────────────────────────────────────────────────────
    if row_count != required_rows:
        errors.append(f"rows={row_count} != required={required_rows}")

    # ── 심볼 유일성 ───────────────────────────────────────────────────────────
    symbols = [str(r.get("symbol", "")).upper() for r in rows]
    unique_symbols = len(set(s for s in symbols if s))
    if unique_symbols != row_count:
        errors.append(f"duplicate_symbols: unique={unique_symbols} rows={row_count}")

    # ── 필수 컬럼 존재 여부 ────────────────────────────────────────────────────
    required_cols_ok = True
    if rows:
        first_row_keys = set(rows[0].keys())
        # close 컬럼은 price/clos로도 허용
        check_cols = [c for c in _REQUIRED_COLS if c != "close"]
        missing_hard = []
        for col in check_cols:
            if col in _SOFT_REQUIRED_COLS:
                continue
            if col not in first_row_keys:
                missing_hard.append(col)
        if missing_hard:
            required_cols_ok = False
            errors.append(f"missing_required_cols={missing_hard}")

        # soft required → 경고
        missing_soft = [c for c in _SOFT_REQUIRED_COLS if c not in first_row_keys]
        if missing_soft:
            warnings.append(f"missing_soft_cols={missing_soft}")

    # ── rank_final30 검증 ─────────────────────────────────────────────────────
    rank_ok = True
    ranks = [r.get("rank_final30") for r in rows]
    valid_ranks = [rk for rk in ranks if rk is not None]
    if valid_ranks:
        int_ranks = []
        for rk in valid_ranks:
            try:
                int_ranks.append(int(rk))
            except Exception:
                pass
        if int_ranks:
            if min(int_ranks) != 1:
                errors.append(f"rank_min={min(int_ranks)} != 1")
                rank_ok = False
            if max(int_ranks) != required_rows:
                errors.append(f"rank_max={max(int_ranks)} != {required_rows}")
                rank_ok = False
            if len(set(int_ranks)) != len(int_ranks):
                errors.append("rank_final30 not unique")
                rank_ok = False
        else:
            errors.append("rank_final30 all non-integer")
            rank_ok = False
    else:
        errors.append("rank_final30 all missing")
        rank_ok = False

    # ── score_final nonzero ───────────────────────────────────────────────────
    score_nonzero_count = 0
    for row in rows:
        sf = _safe_float(row.get("score_final"))
        if sf is not None and sf > 0:
            score_nonzero_count += 1
    if score_nonzero_count != required_rows:
        errors.append(f"score_nonzero_count={score_nonzero_count} != {required_rows}")

    # ── agent_a_score nonzero ─────────────────────────────────────────────────
    agent_a_nonzero_count = 0
    for row in rows:
        af = _safe_float(row.get("agent_a_score"))
        if af is not None and af > 0:
            agent_a_nonzero_count += 1
    if agent_a_nonzero_count < required_rows:
        errors.append(f"agent_a_nonzero_count={agent_a_nonzero_count} < {required_rows}")

    # ── agent_b_score nonzero ─────────────────────────────────────────────────
    agent_b_nonzero_count = 0
    for row in rows:
        bf = _safe_float(row.get("agent_b_score"))
        if bf is not None and bf > 0:
            agent_b_nonzero_count += 1
    if agent_b_nonzero_count < required_rows:
        errors.append(f"agent_b_nonzero_count={agent_b_nonzero_count} < {required_rows}")

    # ── close (price) 존재 ────────────────────────────────────────────────────
    close_count = sum(1 for r in rows if _row_has_close(r))
    if close_count != required_rows:
        errors.append(f"close_not_null_count={close_count} != {required_rows}")

    # ── ma20, ma50 존재 (required_rows 기준) ────────────────────────────────
    ma20_count = sum(1 for r in rows if _safe_float(r.get("ma20")) is not None and _safe_float(r.get("ma20", 0)) > 0)
    if ma20_count < required_rows:
        errors.append(f"ma20_not_null_count={ma20_count} < {required_rows}")

    ma50_count = sum(1 for r in rows if _safe_float(r.get("ma50")) is not None and _safe_float(r.get("ma50", 0)) > 0)
    if ma50_count < required_rows:
        errors.append(f"ma50_not_null_count={ma50_count} < {required_rows}")

    # ── atr_pct 존재 (required_rows 기준) ────────────────────────────────────
    atr_count = sum(1 for r in rows if _safe_float(r.get("atr_pct")) is not None)
    if atr_count < required_rows:
        errors.append(f"atr_pct_not_null_count={atr_count} < {required_rows}")

    # ── entry_style_selected 존재 (== required_rows) ──────────────────────────
    entry_style_count = sum(1 for r in rows if r.get("entry_style_selected") not in (None, ""))
    if entry_style_count != required_rows:
        errors.append(f"entry_style_selected_count={entry_style_count} != {required_rows}")

    # ── ETF 최대 5개 ────────────────────────────────────────────────────────────
    etf_count = sum(1 for r in rows if r.get("asset_type") == "etf" or r.get("symbol") in _CORE_ETFS)
    if etf_count > _MAX_ETF:
        errors.append(f"etf_count={etf_count} > max={_MAX_ETF}")

    # invalid rows 카운트 (score_final == 0 또는 symbol 없음)
    for row in rows:
        if not row.get("symbol") or _safe_float(row.get("score_final", 0)) == 0:
            invalid_row_count += 1

    ok = len(errors) == 0

    logger.info(
        "[US_PREP][FINAL30][STRICT_VALIDATE][%s] ok=%d rows=%d invalid_rows=%d errors=%s warnings=%s",
        source,
        int(ok),
        row_count,
        invalid_row_count,
        errors,
        warnings,
    )
    logger.info(
        "[US_PREP][FINAL30_SCORED][FIELDS] has_score_final=%d has_agent_a_score=%d"
        " has_agent_b_score=%d has_rs_60d=%d has_ma20=%d",
        int(score_nonzero_count > 0),
        int(agent_a_nonzero_count > 0),
        int(agent_b_nonzero_count > 0),
        int(any(_safe_float(r.get("rs_60d")) is not None for r in rows)),
        int(ma20_count > 0),
    )

    return {
        "ok": ok,
        "rows": row_count,
        "unique_symbols": unique_symbols,
        "required_cols_ok": required_cols_ok,
        "rank_ok": rank_ok,
        "score_nonzero_count": score_nonzero_count,
        "agent_a_nonzero_count": agent_a_nonzero_count,
        "agent_b_nonzero_count": agent_b_nonzero_count,
        "invalid_row_count": invalid_row_count,
        "errors": errors,
        "warnings": warnings,
    }
