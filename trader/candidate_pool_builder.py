"""Candidate Pool Builder - 주말에 후보군을 생성하고 DB에 저장."""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import Engine

from trader.config import (
    CANDIDATE_POOL_ENABLED,
    CANDIDATE_POOL_TTL_DAYS,
    CANDIDATE_POOL_SIZE,
    CANDIDATE_POOL_MIN_SIZE,
    CANDIDATE_POOL_STRATEGY_KEY,
    CANDIDATE_POOL_FORCE_REBUILD,
    CANDIDATE_POOL_MIN_PRICE,
    CANDIDATE_POOL_LIQ_DAYS,
    CANDIDATE_POOL_MIN_ROWS,
    MARKET_MAP,
    MINERVINI_ONLY,
    RS_MIN_PCTILE,
    VCP_MIN_SCORE,
    PB1_BOOTSTRAP_ENABLE,
    BOOTSTRAP_MINERVINI_RS_MIN_PCTILE,
    BOOTSTRAP_MINERVINI_VCP_MIN_SCORE,
    BOOTSTRAP_RELAX_PASSES,
)
from trader.db.repos import WatchlistRepo
from trader.flow_score import calculate_flow_score, rank_by_dollar_volume, calculate_final_score
from trader.ohlcv_prefetch import prefetch_ohlcv_to_db
from trader.report.pdf_report import generate_watchlist_pdf
from trader.runtime_paths import runtime_path
from trader.time_utils import now_kst, prev_business_day
from trader.time_coerce import to_date
from trader.snapshot_policy import validate_snapshot_date

logger = logging.getLogger(__name__)


_LAST_CANDIDATE_POOL_BUILD_REPORT: Dict[str, Any] = {}


def get_last_candidate_pool_build_report() -> Dict[str, Any]:
    return dict(_LAST_CANDIDATE_POOL_BUILD_REPORT)


def resolve_env(cli_env: str | None) -> str:
    if cli_env:
        v = str(cli_env).strip().lower()
        if v not in ("paper", "live", "practice"):
            raise RuntimeError(f"INVALID_ENV={cli_env}")
        return v
    strategy_env = os.getenv("STRATEGY_ENV")
    if strategy_env:
        v = str(strategy_env).strip().lower()
        if v not in ("paper", "live", "practice"):
            raise RuntimeError(f"INVALID_STRATEGY_ENV={strategy_env}")
        return v
    raise RuntimeError("ENV_NOT_DEFINED")


class CandidatePoolBuilder:
    """
    후보군 생성기 - 가벼운 스캔으로 195 유니버스를 80~150개로 압축.
    
    주말에 실행하여 주중 Minervini/PB1 입력으로 사용할 후보군을 만든다.
    """
    
    def __init__(
        self,
        *,
        ohlcv_provider: Any,
        target_size: int = CANDIDATE_POOL_SIZE,
        min_price: float = CANDIDATE_POOL_MIN_PRICE,
        liq_days: int = CANDIDATE_POOL_LIQ_DAYS,
        min_rows: int = CANDIDATE_POOL_MIN_ROWS,
        minervini_rs_min: Optional[float] = None,
        minervini_vcp_min: Optional[float] = None,
        relax_passes: Optional[int] = None,
    ):
        """
        Args:
            ohlcv_provider: OHLCV 데이터 제공자
            target_size: 목표 후보군 크기
            min_price: 최소 주가 (낮은 주가 제외)
            liq_days: 유동성 계산 기간(일)
            min_rows: OHLCV 최소 행수
        """
        self.ohlcv_provider = ohlcv_provider
        self.target_size = target_size
        self.min_price = min_price
        self.liq_days = liq_days
        self.min_rows = min_rows
        self.minervini_rs_min = minervini_rs_min
        self.minervini_vcp_min = minervini_vcp_min
        self.relax_passes = relax_passes
        self.last_build_report: Dict[str, Any] = {}

    def resolve_minervini_thresholds(self) -> Dict[str, Any]:
        """Resolve base/floor thresholds and identify source for diagnostics."""
        source = "config_default"

        env_base_rs = os.getenv("CANDIDATE_POOL_RS_MIN_PCTILE")
        env_base_vcp = os.getenv("CANDIDATE_POOL_VCP_MIN_SCORE")
        base_rs = float(os.getenv("RS_MIN_PCTILE", str(RS_MIN_PCTILE)) or RS_MIN_PCTILE)
        base_vcp = float(os.getenv("VCP_MIN_SCORE", str(VCP_MIN_SCORE)) or VCP_MIN_SCORE)

        if env_base_rs is not None:
            base_rs = float(env_base_rs)
            source = "env_override"
        if env_base_vcp is not None:
            base_vcp = float(env_base_vcp)
            source = "env_override"

        if self.minervini_rs_min is not None:
            base_rs = float(self.minervini_rs_min)
            source = "explicit_builder_param"
        if self.minervini_vcp_min is not None:
            base_vcp = float(self.minervini_vcp_min)
            source = "explicit_builder_param"

        bootstrap_enabled = os.getenv("PB1_BOOTSTRAP_ENABLE", "1" if PB1_BOOTSTRAP_ENABLE else "0") == "1"
        bootstrap_rs = float(
            os.getenv("BOOTSTRAP_MINERVINI_RS_MIN_PCTILE", str(BOOTSTRAP_MINERVINI_RS_MIN_PCTILE))
            or BOOTSTRAP_MINERVINI_RS_MIN_PCTILE
        )
        bootstrap_vcp = float(
            os.getenv("BOOTSTRAP_MINERVINI_VCP_MIN_SCORE", str(BOOTSTRAP_MINERVINI_VCP_MIN_SCORE))
            or BOOTSTRAP_MINERVINI_VCP_MIN_SCORE
        )
        relax_passes = int(
            self.relax_passes
            if self.relax_passes is not None
            else os.getenv("BOOTSTRAP_RELAX_PASSES", str(BOOTSTRAP_RELAX_PASSES))
        )
        relax_passes = max(0, relax_passes)

        if bootstrap_enabled and source == "config_default":
            source = "bootstrap_override"

        return {
            "base_rs_min": float(base_rs),
            "base_vcp_min": float(base_vcp),
            "bootstrap_rs_min": float(bootstrap_rs),
            "bootstrap_vcp_min": float(bootstrap_vcp),
            "relax_passes": relax_passes,
            "source": source,
            "bootstrap_enabled": bootstrap_enabled,
        }

    def _build_relax_thresholds(
        self,
        *,
        base_rs: float,
        base_vcp: float,
        floor_rs: float,
        floor_vcp: float,
        relax_passes: int,
    ) -> List[Tuple[float, float]]:
        if relax_passes <= 0:
            return [(float(base_rs), float(base_vcp))]

        thresholds: List[Tuple[float, float]] = []
        rs_drop = max(0.0, float(base_rs) - float(floor_rs))
        vcp_drop = max(0.0, float(base_vcp) - float(floor_vcp))
        for p in range(relax_passes + 1):
            vcp_progress = p / float(relax_passes)
            rs_progress = min(1.0, (2.0 * p) / float(relax_passes))
            rs_min = max(float(floor_rs), float(base_rs) - rs_drop * rs_progress)
            vcp_min = max(float(floor_vcp), float(base_vcp) - vcp_drop * vcp_progress)
            thresholds.append((round(rs_min, 1), round(vcp_min, 1)))
        return thresholds

    def _apply_minervini_filter(self, rows: List[Dict[str, Any]], rs_min: float, vcp_min: float) -> List[Dict[str, Any]]:
        filtered: List[Dict[str, Any]] = []
        for row in rows:
            rs_val = row.get("rs_percentile")
            vcp_val = row.get("vcp_score")
            if rs_val is None or float(rs_val) < float(rs_min):
                continue
            if vcp_val is None or float(vcp_val) < float(vcp_min):
                continue
            if not bool(row.get("trend_ok", False)):
                continue
            filtered.append(row)
        return filtered
    
    def build_light_scan(
        self,
        *,
        members: List[Dict[str, Any]],
        as_of: date,
    ) -> List[str]:
        """
        가벼운 스캔으로 후보군 생성.
        
        Args:
            members: 유니버스 멤버 리스트 [{"code": "005930", ...}, ...]
            as_of: 기준일
        
        Returns:
            후보군 종목코드 리스트 (예: ["005930", "035720", ...])
        """
        logger.info(
            "[CANDIDATE_POOL][BUILD][START] as_of=%s universe_size=%s target_size=%s",
            as_of, len(members), self.target_size
        )
        
        codes = [m["code"] for m in members]
        scored = []
        excluded_data_insufficient = 0
        excluded_low_price = 0
        excluded_high_volatility = 0
        excluded_provider_error = 0
        total = len(codes)
        progress_every = max(1, total // 10)
        ts0 = time.monotonic()
        
        for idx, code in enumerate(codes, start=1):
            try:
                df = self.ohlcv_provider(code, days=max(self.liq_days + 10, 80))
                if df is None or len(df) < self.min_rows:
                    excluded_data_insufficient += 1
                    continue
                
                # 최소 주가 필터
                last_close = df["close"].iloc[-1]
                if last_close < self.min_price:
                    excluded_low_price += 1
                    continue
                
                # 유동성 점수 계산 (평균 거래대금)
                recent = df.tail(self.liq_days)
                avg_value = (recent["close"] * recent["volume"]).mean()
                
                # 추세 점수 (간단히 MA20 > MA50 > MA200 체크)
                trend_score = 0
                trend_ok = False
                if len(df) >= 200:
                    ma20 = df["close"].rolling(20).mean().iloc[-1]
                    ma50 = df["close"].rolling(50).mean().iloc[-1]
                    ma200 = df["close"].rolling(200).mean().iloc[-1]
                    if ma20 > ma50:
                        trend_score += 1
                    if ma50 > ma200:
                        trend_score += 1
                    trend_ok = bool(ma20 > ma50 > ma200)
                elif len(df) >= 50:
                    ma20 = df["close"].rolling(20).mean().iloc[-1]
                    ma50 = df["close"].rolling(50).mean().iloc[-1]
                    if ma20 > ma50:
                        trend_score += 1
                    trend_ok = bool(ma20 > ma50)

                ret_90d = ((df["close"].iloc[-1] / df["close"].iloc[-90]) - 1.0) if len(df) >= 90 else 0.0
                ret_180d = ((df["close"].iloc[-1] / df["close"].iloc[-180]) - 1.0) if len(df) >= 180 else 0.0
                rs_raw_score = ret_90d * 0.6 + ret_180d * 0.4

                vol_recent = float(df["volume"].tail(10).mean() or 0.0)
                vol_prior = float(df["volume"].tail(30).head(20).mean() or 0.0)
                if vol_prior > 0:
                    contraction_ratio = max(0.0, min(1.0, 1.0 - (vol_recent / vol_prior)))
                    vcp_score = contraction_ratio * 100.0
                else:
                    vcp_score = 0.0
                
                # 변동성 필터 (과도한 변동성 제외)
                volatility = recent["close"].pct_change().std()
                if volatility > 0.08:  # 일일 8% 이상 변동은 제외
                    excluded_high_volatility += 1
                    continue
                
                # 복합 점수 (유동성 70% + 추세 30%)
                composite_score = avg_value * 0.7 + trend_score * 1e9 * 0.3
                
                scored.append({
                    "code": code,
                    "score": composite_score,
                    "avg_value": avg_value,
                    "trend_score": trend_score,
                    "trend_ok": trend_ok,
                    "rs_raw_score": float(rs_raw_score),
                    "vcp_score": float(vcp_score),
                })
                
            except Exception as exc:
                excluded_provider_error += 1
                logger.debug("[CANDIDATE_POOL][OHLCV_FAIL] code=%s err=%s", code, exc)
                continue
            finally:
                if idx == 1 or idx % progress_every == 0 or idx == total:
                    logger.info(
                        "[CANDIDATE_POOL][BUILD][PROGRESS] processed=%s/%s scored=%s excluded(data=%s,price=%s,vol=%s,err=%s) elapsed=%.1fs",
                        idx,
                        total,
                        len(scored),
                        excluded_data_insufficient,
                        excluded_low_price,
                        excluded_high_volatility,
                        excluded_provider_error,
                        time.monotonic() - ts0,
                    )

        logger.info(
            "[CANDIDATE_POOL][BUILD][EXCLUDE] total=%s data_insufficient=%s low_price=%s high_volatility=%s provider_error=%s",
            len(codes), excluded_data_insufficient, excluded_low_price, excluded_high_volatility, excluded_provider_error,
        )
        
        # ✅ scored=0 즉시 실패 처리 (진단 로그 강화)
        if len(scored) == 0:
            # 샘플링하여 왜 실패했는지 진단
            sample_codes = codes[:5]
            logger.error(
                "[CANDIDATE_POOL][BUILD][FAIL][ZERO_SCORED] scored=0 from universe_size=%s. "
                "Diagnosing first %s codes: %s",
                len(codes), len(sample_codes), sample_codes
            )
            
            # Provider 진단 정보 추가
            provider_type = type(self.ohlcv_provider).__name__ if hasattr(self.ohlcv_provider, '__name__') else str(type(self.ohlcv_provider))
            available_methods = [a for a in ["fetch", "get_ohlcv", "load", "read_daily", "read", "__call__"] 
                                if hasattr(self.ohlcv_provider, a)]
            logger.error(
                "[CANDIDATE_POOL][DIAG][PROVIDER] type=%s available_methods=%s",
                provider_type, available_methods
            )
            
            for code in sample_codes:
                try:
                    df = self.ohlcv_provider(code, days=max(self.liq_days + 10, 80))
                    if df is None:
                        logger.error("[CANDIDATE_POOL][DIAG] code=%s: OHLCV provider returned None", code)
                    elif len(df) < self.min_rows:
                        logger.error("[CANDIDATE_POOL][DIAG] code=%s: rows=%s < min_rows=%s", code, len(df), self.min_rows)
                    else:
                        last_close = df["close"].iloc[-1]
                        logger.error("[CANDIDATE_POOL][DIAG] code=%s: rows=%s last_close=%s (min_price=%s)", 
                                     code, len(df), last_close, self.min_price)
                except Exception as exc:
                    logger.error("[CANDIDATE_POOL][DIAG] code=%s: exception=%s", code, str(exc)[:200])
            
            logger.error(
                "[CANDIDATE_POOL][BUILD][FAIL] Possible causes: "
                "(1) OHLCV data missing for all symbols (check DB price_daily table), "
                "(2) env/strategy mismatch between universe and candidate pool, "
                "(3) network/API failure during prefetch, "
                "(4) filter criteria too strict (min_price=%s, min_rows=%s).",
                self.min_price, self.min_rows
            )
            if excluded_data_insufficient >= len(codes):
                prefetch_days = os.getenv("CANDIDATE_POOL_PREFETCH_DAYS", "unknown")
                raise RuntimeError(
                    f"candidate pool excluded: data_insufficient for all {len(codes)} symbols "
                    f"(min_rows={self.min_rows}, CANDIDATE_POOL_PREFETCH_DAYS={prefetch_days}). "
                    "Increase prefetch window (recommend >=260, preferred 520) and rebuild."
                )
            raise RuntimeError(f"candidate pool scored=0 from {len(codes)} universe members")
        
        # Cross-sectional RS percentile 계산
        rs_raw_scores = [float(item.get("rs_raw_score", 0.0) or 0.0) for item in scored]
        if rs_raw_scores:
            rs_series = pd.Series(rs_raw_scores)
            rs_pctiles = (rs_series.rank(pct=True) * 100.0).tolist()
            for item, pct in zip(scored, rs_pctiles):
                item["rs_percentile"] = float(pct)
        else:
            for item in scored:
                item["rs_percentile"] = 0.0

        thresholds = self.resolve_minervini_thresholds()
        source = thresholds.get("source", "config_default")
        source_label = "config/bootstrap" if source == "bootstrap_override" else str(source)

        base_rs_min = float(thresholds["base_rs_min"])
        base_vcp_min = float(thresholds["base_vcp_min"])
        floor_rs_min = float(thresholds["bootstrap_rs_min"])
        floor_vcp_min = float(thresholds["bootstrap_vcp_min"])
        relax_passes = int(thresholds["relax_passes"])
        min_size = int(os.getenv("CANDIDATE_POOL_MIN_SIZE", str(CANDIDATE_POOL_MIN_SIZE)))

        logger.info(
            "[CANDIDATE_POOL][BUILD][THRESHOLDS] base_rs_min=%.1f base_vcp_min=%.1f bootstrap_rs_min=%.1f bootstrap_vcp_min=%.1f relax_passes=%s source=%s",
            base_rs_min,
            base_vcp_min,
            floor_rs_min,
            floor_vcp_min,
            relax_passes,
            source_label,
        )

        rs_vals = pd.Series([row.get("rs_percentile") for row in scored], dtype="float64")
        vcp_vals = pd.Series([row.get("vcp_score") for row in scored], dtype="float64")
        rs_null = int(rs_vals.isna().sum())
        vcp_null = int(vcp_vals.isna().sum())
        rs_pass = int((rs_vals >= base_rs_min).sum())
        vcp_pass = int((vcp_vals >= base_vcp_min).sum())
        both_pass = int(((rs_vals >= base_rs_min) & (vcp_vals >= base_vcp_min)).sum())

        rs_clean = rs_vals.dropna()
        vcp_clean = vcp_vals.dropna()
        rs_summary = (
            float(rs_clean.min()) if not rs_clean.empty else 0.0,
            float(rs_clean.quantile(0.25)) if not rs_clean.empty else 0.0,
            float(rs_clean.median()) if not rs_clean.empty else 0.0,
            float(rs_clean.quantile(0.75)) if not rs_clean.empty else 0.0,
            float(rs_clean.max()) if not rs_clean.empty else 0.0,
        )
        vcp_summary = (
            float(vcp_clean.min()) if not vcp_clean.empty else 0.0,
            float(vcp_clean.quantile(0.25)) if not vcp_clean.empty else 0.0,
            float(vcp_clean.median()) if not vcp_clean.empty else 0.0,
            float(vcp_clean.quantile(0.75)) if not vcp_clean.empty else 0.0,
            float(vcp_clean.max()) if not vcp_clean.empty else 0.0,
        )

        logger.info(
            "[CANDIDATE_POOL][BUILD][MINERVINI][DISTRIBUTION] input=%s rs_min=%.1f vcp_min=%.1f rs_pass=%s vcp_pass=%s both_pass=%s rs_null=%s vcp_null=%s rs_stats=min:%.1f,p25:%.1f,median:%.1f,p75:%.1f,max:%.1f vcp_stats=min:%.1f,p25:%.1f,median:%.1f,p75:%.1f,max:%.1f",
            len(scored),
            base_rs_min,
            base_vcp_min,
            rs_pass,
            vcp_pass,
            both_pass,
            rs_null,
            vcp_null,
            rs_summary[0],
            rs_summary[1],
            rs_summary[2],
            rs_summary[3],
            rs_summary[4],
            vcp_summary[0],
            vcp_summary[1],
            vcp_summary[2],
            vcp_summary[3],
            vcp_summary[4],
        )

        sample_rows: List[Dict[str, Any]] = []
        for row in sorted(scored, key=lambda x: float(x.get("score", 0.0) or 0.0), reverse=True)[:10]:
            rs_val = row.get("rs_percentile")
            vcp_val = row.get("vcp_score")
            pass_rs = rs_val is not None and float(rs_val) >= base_rs_min
            pass_vcp = vcp_val is not None and float(vcp_val) >= base_vcp_min
            sample_rows.append(
                {
                    "code": str(row.get("code", "")),
                    "rs_percentile": round(float(rs_val), 2) if rs_val is not None else None,
                    "vcp_score": round(float(vcp_val), 2) if vcp_val is not None else None,
                    "pass_rs": int(pass_rs),
                    "pass_vcp": int(pass_vcp),
                    "pass_both": int(pass_rs and pass_vcp),
                }
            )
        logger.info("[CANDIDATE_POOL][BUILD][MINERVINI][SAMPLE] %s", sample_rows)

        logger.info(
            "[CANDIDATE_POOL][BUILD][RELAX_POLICY] base=(%.1f,%.1f) floor=(%.1f,%.1f) relax_passes=%s",
            base_rs_min,
            base_vcp_min,
            floor_rs_min,
            floor_vcp_min,
            relax_passes,
        )

        pass_thresholds = self._build_relax_thresholds(
            base_rs=base_rs_min,
            base_vcp=base_vcp_min,
            floor_rs=floor_rs_min,
            floor_vcp=floor_vcp_min,
            relax_passes=relax_passes,
        )

        strict_rows: List[Dict[str, Any]] = []
        chosen_rows: List[Dict[str, Any]] = []
        chosen_pass = 0
        chosen_rs = base_rs_min
        chosen_vcp = base_vcp_min
        for pass_idx, (pass_rs_min, pass_vcp_min) in enumerate(pass_thresholds):
            pass_rows = self._apply_minervini_filter(scored, pass_rs_min, pass_vcp_min)
            if pass_idx == 0:
                strict_rows = pass_rows[:]
            logger.info(
                "[CANDIDATE_POOL][BUILD][RELAX_PASS] pass=%s rs_min=%.1f vcp_min=%.1f kept=%s min_size=%s",
                pass_idx,
                pass_rs_min,
                pass_vcp_min,
                len(pass_rows),
                min_size,
            )
            chosen_rows = pass_rows
            chosen_pass = pass_idx
            chosen_rs = pass_rs_min
            chosen_vcp = pass_vcp_min
            if len(pass_rows) >= min_size:
                break

        for row in strict_rows:
            row["selection_mode"] = "strict_minervini"
        strict_codes = {str(row.get("code", "")) for row in strict_rows}
        for row in chosen_rows:
            code = str(row.get("code", ""))
            if code not in strict_codes:
                row["selection_mode"] = "relaxed_minervini"

        chosen_sorted = sorted(chosen_rows, key=lambda x: float(x.get("score", 0.0) or 0.0), reverse=True)
        selected_rows = chosen_sorted[: self.target_size]

        fallback_added_rows: List[Dict[str, Any]] = []
        if len(selected_rows) < min_size:
            selected_codes = {str(row.get("code", "")) for row in selected_rows}
            fallback_eligible = sorted(scored, key=lambda x: float(x.get("score", 0.0) or 0.0), reverse=True)
            needed = max(0, min_size - len(selected_rows))
            for row in fallback_eligible:
                code = str(row.get("code", ""))
                if code in selected_codes:
                    continue
                row["selection_mode"] = "fallback_topup"
                fallback_added_rows.append(row)
                selected_rows.append(row)
                selected_codes.add(code)
                if len(fallback_added_rows) >= needed:
                    break
            logger.warning(
                "[CANDIDATE_POOL][BUILD][FALLBACK_TOPUP] strict_kept=%s needed=%s added=%s final_selected=%s",
                len(chosen_rows),
                needed,
                len(fallback_added_rows),
                len(selected_rows),
            )

        selected_rows = selected_rows[: max(min_size, min(self.target_size, len(selected_rows)))]
        result_codes = [str(item.get("code", "")) for item in selected_rows]

        strict_final = sum(1 for row in selected_rows if row.get("selection_mode") == "strict_minervini")
        relaxed_final = sum(1 for row in selected_rows if row.get("selection_mode") == "relaxed_minervini")
        fallback_final = sum(1 for row in selected_rows if row.get("selection_mode") == "fallback_topup")

        final_sample: List[Dict[str, Any]] = []
        for row in selected_rows[:10]:
            final_sample.append(
                {
                    "code": str(row.get("code", "")),
                    "selection_mode": row.get("selection_mode", "unknown"),
                    "rs_percentile": round(float(row.get("rs_percentile", 0.0) or 0.0), 2),
                    "vcp_score": round(float(row.get("vcp_score", 0.0) or 0.0), 2),
                }
            )

        logger.info(
            "[CANDIDATE_POOL][BUILD][FINAL] selected=%s strict=%s relaxed=%s fallback=%s min_size=%s target_size=%s",
            len(result_codes),
            strict_final,
            relaxed_final,
            fallback_final,
            min_size,
            self.target_size,
        )
        logger.info("[CANDIDATE_POOL][BUILD][FINAL_SAMPLE] %s", final_sample)

        self.last_build_report = {
            "universe": len(codes),
            "prefilter_survivors": len(scored),
            "strict_kept": len(strict_rows),
            "relaxed_kept": len(chosen_rows),
            "fallback_eligible": len(scored),
            "fallback_added": len(fallback_added_rows),
            "final_selected": len(result_codes),
            "min_size": min_size,
            "target_size": self.target_size,
            "chosen_pass": chosen_pass,
            "thresholds_used": {
                "base_rs_min": base_rs_min,
                "base_vcp_min": base_vcp_min,
                "floor_rs_min": floor_rs_min,
                "floor_vcp_min": floor_vcp_min,
                "used_rs_min": chosen_rs,
                "used_vcp_min": chosen_vcp,
                "relax_passes": relax_passes,
                "source": source_label,
            },
            "selection_mode_by_code": {
                str(row.get("code", "")): str(row.get("selection_mode", "unknown")) for row in selected_rows
            },
            "selection_mode_counts": {
                "strict_minervini": strict_final,
                "relaxed_minervini": relaxed_final,
                "fallback_topup": fallback_final,
            },
        }

        if len(result_codes) < min_size:
            reason = "insufficient_prefilter_survivors" if len(scored) < min_size else "thresholds_too_strict_or_score_distribution"
            logger.error(
                "[CANDIDATE_POOL][BUILD][FAIL] universe=%s prefilter_survivors=%s strict_kept=%s relaxed_kept=%s fallback_eligible=%s final_selected=%s min_size=%s reason=%s thresholds_used=%s",
                len(codes),
                len(scored),
                len(strict_rows),
                len(chosen_rows),
                len(scored),
                len(result_codes),
                min_size,
                reason,
                self.last_build_report.get("thresholds_used"),
            )
            raise RuntimeError(
                "candidate_pool_build_failed: "
                f"universe={len(codes)} prefilter_survivors={len(scored)} strict_kept={len(strict_rows)} "
                f"relaxed_kept={len(chosen_rows)} fallback_eligible={len(scored)} final_selected={len(result_codes)} "
                f"min_size={min_size} reason={reason}"
            )

        logger.info(
            "[CANDIDATE_POOL][BUILD][DONE] as_of=%s universe_size=%s scored=%s selected=%s",
            as_of,
            len(codes),
            len(scored),
            len(result_codes),
        )
        return result_codes
    
    def build_final30_pipeline(
        self,
        *,
        members: List[Dict[str, Any]],
        as_of: date,
        engine: Engine,
        env: str,
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Universe → 120 → 50 → Final 30 파이프라인.
        
        Args:
            members: 유니버스 멤버 (195개)
            as_of: 기준일
            engine: DB 엔진 (스냅샷 저장용)
            env: 환경
        
        Returns:
            (pool120, top50, final30)
        """
        logger.info("[FINAL30_PIPELINE][START] as_of=%s universe=%s", as_of, len(members))
        
        # Step 1: Universe → 120 (기존 로직)
        codes = [m["code"] for m in members]
        scored = []
        total_step1 = len(codes)
        progress_step1 = max(1, total_step1 // 10)
        ts_step1 = time.monotonic()
        
        for idx, code in enumerate(codes, start=1):
            try:
                df = self.ohlcv_provider(code, days=max(self.liq_days + 10, 80))
                if df is None or len(df) < self.min_rows:
                    continue
                
                last_close = df["close"].iloc[-1]
                if last_close < self.min_price:
                    continue
                
                # 유동성 점수
                recent = df.tail(self.liq_days)
                avg_value = (recent["close"] * recent["volume"]).mean()
                
                # 추세 점수
                trend_score = 0
                if len(df) >= 200:
                    ma20 = df["close"].rolling(20).mean().iloc[-1]
                    ma50 = df["close"].rolling(50).mean().iloc[-1]
                    ma200 = df["close"].rolling(200).mean().iloc[-1]
                    if ma20 > ma50:
                        trend_score += 1
                    if ma50 > ma200:
                        trend_score += 1
                elif len(df) >= 50:
                    ma20 = df["close"].rolling(20).mean().iloc[-1]
                    ma50 = df["close"].rolling(50).mean().iloc[-1]
                    if ma20 > ma50:
                        trend_score += 1
                
                # 변동성 필터
                volatility = recent["close"].pct_change().std()
                if volatility > 0.08:
                    continue
                
                # 복합 점수
                composite_score = avg_value * 0.7 + trend_score * 1e9 * 0.3
                
                scored.append({
                    "code": code,
                    "name": next((m["name"] for m in members if m["code"] == code), ""),
                    "score": composite_score,
                    "avg_value": avg_value,
                    "trend_score": trend_score,
                })
                
            except Exception as exc:
                logger.debug("[FINAL30][120] code=%s err=%s", code, exc)
                continue
            finally:
                if idx == 1 or idx % progress_step1 == 0 or idx == total_step1:
                    logger.info(
                        "[FINAL30_PIPELINE][120][PROGRESS] processed=%s/%s passed=%s elapsed=%.1fs",
                        idx,
                        total_step1,
                        len(scored),
                        time.monotonic() - ts_step1,
                    )
        
        if len(scored) == 0:
            raise RuntimeError("scored=0 in 120 step")
        
        # 상위 120개 선택
        scored.sort(key=lambda x: x["score"], reverse=True)
        pool120 = scored[:min(120, len(scored))]
        
        logger.info("[FINAL30_PIPELINE][120] selected=%s", len(pool120))
        
        # Step 2: 120 → 50 (기술적 점수 강화)
        top50_scored = []
        total_step2 = len(pool120)
        progress_step2 = max(1, total_step2 // 10)
        ts_step2 = time.monotonic()
        for idx, item in enumerate(pool120, start=1):
            code = item["code"]
            try:
                df = self.ohlcv_provider(code, days=200)
                if df is None or len(df) < 50:
                    continue
                
                # RS 계산 (간단 버전)
                ret_90d = ((df["close"].iloc[-1] / df["close"].iloc[-90]) - 1) if len(df) >= 90 else 0
                ret_180d = ((df["close"].iloc[-1] / df["close"].iloc[-180]) - 1) if len(df) >= 180 else 0
                rs_score = ret_90d * 0.6 + ret_180d * 0.4
                
                # Pullback 계산
                high_52w = df["high"].tail(252).max() if len(df) >= 252 else df["high"].max()
                pullback_pct = (high_52w - df["close"].iloc[-1]) / high_52w if high_52w > 0 else 0
                
                # VCP 점수 (간단 버전: 최근 변동성 감소)
                vol_recent = df["volume"].tail(10).mean()
                vol_prior = df["volume"].tail(30).head(20).mean()
                vcp_score = 1.0 if vol_prior > 0 and vol_recent < vol_prior * 0.7 else 0.0
                
                # 기술적 점수
                tech_score = rs_score * 50 + (1 - pullback_pct) * 30 + vcp_score * 20
                
                item["tech_score"] = tech_score
                item["rs_score"] = rs_score
                item["pullback_pct"] = pullback_pct
                item["vcp_score"] = vcp_score
                
                top50_scored.append(item)
                
            except Exception as exc:
                logger.debug("[FINAL30][50] code=%s err=%s", code, exc)
                continue
            finally:
                if idx == 1 or idx % progress_step2 == 0 or idx == total_step2:
                    logger.info(
                        "[FINAL30_PIPELINE][50][PROGRESS] processed=%s/%s passed=%s elapsed=%.1fs",
                        idx,
                        total_step2,
                        len(top50_scored),
                        time.monotonic() - ts_step2,
                    )
        
        top50_scored.sort(key=lambda x: x["tech_score"], reverse=True)
        top50 = top50_scored[:min(50, len(top50_scored))]
        
        logger.info("[FINAL30_PIPELINE][50] selected=%s", len(top50))
        
        # Step 3: 50 → 30 (Flow 점수 추가)
        final30_scored = []
        for item in top50:
            code = item["code"]
            try:
                df = self.ohlcv_provider(code, days=30)
                if df is None:
                    continue
                
                # Flow 점수 계산 (외국인/기관 데이터가 없으면 0)
                # 실제 구현에서는 외국인/기관 데이터를 별도로 가져와야 함
                # 여기서는 간단히 0으로 설정
                flow_result = calculate_flow_score(
                    code=code,
                    ohlcv_df=df,
                    foreign_df=None,  # TODO: 외국인 데이터 연결
                    inst_df=None,  # TODO: 기관 데이터 연결
                    window=20,
                )
                
                item["flow_score"] = flow_result["flow_score"]
                item["foreign_20_ratio"] = flow_result["foreign_20_ratio"]
                item["inst_20_ratio"] = flow_result["inst_20_ratio"]
                
                # 최종 점수 (Tech 70% + Flow 30%)
                final_score = calculate_final_score(
                    tech_score=item["tech_score"],
                    flow_score=item["flow_score"],
                    tech_weight=0.7,
                    flow_weight=0.3,
                )
                
                item["final_score"] = final_score
                
                # 선정 사유
                item["reasons"] = {
                    "trend_template": item.get("trend_score", 0) >= 1,
                    "rs_percentile": item.get("rs_score", 0) * 100,
                    "vcp": item.get("vcp_score", 0) > 0,
                    "pullback_pct": item.get("pullback_pct", 0),
                    "foreign_20_ratio": item.get("foreign_20_ratio", 0),
                    "inst_20_ratio": item.get("inst_20_ratio", 0),
                    "dollar_vol_rank": 0,  # 아래에서 계산
                }
                
                final30_scored.append(item)
                
            except Exception as exc:
                logger.debug("[FINAL30][30] code=%s err=%s", code, exc)
                continue
        
        # 거래대금 순위 추가
        final30_scored = rank_by_dollar_volume(final30_scored, self.ohlcv_provider, window=20)
        
        # 최종 점수로 정렬하여 상위 30개 선택
        final30_scored.sort(key=lambda x: x["final_score"], reverse=True)
        final30 = final30_scored[:min(30, len(final30_scored))]
        
        # 랭킹 추가
        for i, item in enumerate(final30, 1):
            item["rank"] = i
            item["reasons"]["dollar_vol_rank"] = item.get("dollar_vol_rank", 0)
        
        logger.info("[FINAL30_PIPELINE][30] selected=%s", len(final30))
        
        # JSON 저장
        output_dir = runtime_path("runtime", "watchlist", as_of.strftime("%Y-%m-%d"))
        output_dir.mkdir(parents=True, exist_ok=True)
        
        with open(output_dir / "final30.json", "w") as f:
            json.dump(final30, f, indent=2, default=str)
        
        logger.info("[FINAL30_PIPELINE][JSON] saved to %s", output_dir / "final30.json")
        
        # PDF 생성
        try:
            pdf_path = generate_watchlist_pdf(final30=final30, as_of=as_of)
            logger.info("[FINAL30_PIPELINE][PDF] generated %s", pdf_path)
        except Exception as exc:
            logger.warning("[FINAL30_PIPELINE][PDF] failed: %s", exc)
        
        return pool120, top50, final30


def build_and_save_candidate_pool(
    *,
    engine: Engine,
    env: str,
    as_of: date,
    members: List[Dict[str, Any]],
    ohlcv_provider: Any,
    force_rebuild: bool = False,
    skip_prefetch: bool = False,
) -> List[str]:
    """
    후보군을 생성하고 DB에 저장.
    
    Args:
        engine: DB 엔진
        env: 환경 (LIVE/PAPER)
        as_of: 기준일
        members: 유니버스 멤버
        ohlcv_provider: OHLCV 데이터 제공자
        force_rebuild: 강제 재생성 여부
        skip_prefetch: prefetch 스킵 여부 (build-only 모드)
    
    Returns:
        후보군 종목코드 리스트
    """
    repo = WatchlistRepo(engine)
    strategy = CANDIDATE_POOL_STRATEGY_KEY

    if os.getenv("MODE") == "trade" and force_rebuild:
        raise RuntimeError("TRADE_MODE_FORBIDS_CANDIDATE_POOL_REBUILD")
    
    # 이미 당일 후보군이 있으면 재사용
    if not force_rebuild:
        existing, _ = repo.load_watchlist(
            env=env,
            strategy=strategy,
            as_of=as_of,
            allow_latest_fallback=False,  # 빌드 시에는 정확한 날짜만 허용
        )
        if existing:
            codes = [item["code"] for item in existing]
            logger.info(
                "[CANDIDATE_POOL][CACHE] hit=True as_of=%s count=%s",
                as_of, len(codes)
            )
            return codes
    
    # ✅ OHLCV 프리패치 (build-only 모드가 아닐 때만)
    if not skip_prefetch:
        logger.info("[CANDIDATE_POOL][PREFETCH] starting OHLCV prefetch for %s members", len(members))
        try:
            prefetch_ohlcv_to_db(
                engine=engine,
                members=members,
                force_rebuild=force_rebuild,
                env=env,
                strategy=strategy,
                as_of=as_of,
            )
        except Exception as exc:
            logger.warning("[CANDIDATE_POOL][PREFETCH][FAIL] err=%s (continuing with existing DB data)", exc)
    else:
        logger.info("[CANDIDATE_POOL][PREFETCH] skipped (build-only mode)")
    
    # 후보군 생성
    builder = CandidatePoolBuilder(
        ohlcv_provider=ohlcv_provider,
        target_size=CANDIDATE_POOL_SIZE,
        min_price=CANDIDATE_POOL_MIN_PRICE,
        liq_days=CANDIDATE_POOL_LIQ_DAYS,
        min_rows=CANDIDATE_POOL_MIN_ROWS,
    )
    
    try:
        pool_codes = builder.build_light_scan(members=members, as_of=as_of)
    except Exception as exc:
        logger.error("[CANDIDATE_POOL][BUILD][FAIL] err=%s", exc, exc_info=True)
        raise

    build_report = dict(getattr(builder, "last_build_report", {}) or {})
    global _LAST_CANDIDATE_POOL_BUILD_REPORT
    _LAST_CANDIDATE_POOL_BUILD_REPORT = build_report
    mode_map = build_report.get("selection_mode_by_code", {}) if isinstance(build_report, dict) else {}
    
    # DB에 저장 (pb1_watchlist)
    pool_members = [
        {
            "code": code,
            "rank": idx + 1,
            "score": None,
            "meta": {
                "kind": "pool",
                "selection_mode": mode_map.get(str(code), "unknown"),
            },
        }
        for idx, code in enumerate(pool_codes)
    ]
    
    # ✅ 빈 리스트 체크 (이중 안전장치) - 실패로 처리
    if not pool_members:
        logger.error("[WATCHLIST][SAVE] empty members - this should have failed in build_light_scan")
        raise RuntimeError("candidate pool is empty after build - cannot save")
    
    min_size = int(os.getenv("CANDIDATE_POOL_MIN_SIZE", "40"))
    if len(pool_members) < min_size:
        logger.error(
            "[WATCHLIST][SAVE][FAIL] members=%s < min_size=%s strict_kept=%s relaxed_kept=%s fallback_eligible=%s",
            len(pool_members),
            min_size,
            build_report.get("strict_kept", -1),
            build_report.get("relaxed_kept", -1),
            build_report.get("fallback_eligible", -1),
        )
        raise RuntimeError(
            "candidate_pool_save_failed: "
            f"selected={len(pool_members)} min_size={min_size} "
            f"strict_kept={build_report.get('strict_kept', 'na')} "
            f"relaxed_kept={build_report.get('relaxed_kept', 'na')} "
            f"fallback_eligible={build_report.get('fallback_eligible', 'na')}"
        )
    
    repo.save_watchlist(
        env=env,
        strategy=strategy,
        as_of=as_of,
        members=pool_members,
    )
    
    logger.info(
        "[CANDIDATE_POOL][SAVE] env=%s strategy=%s size=%s as_of=%s",
        env, strategy, len(pool_codes), as_of
    )

    if build_report:
        logger.info(
            "[CANDIDATE_POOL][SAVE][SELECTION_MODE] strict=%s relaxed=%s fallback=%s",
            build_report.get("selection_mode_counts", {}).get("strict_minervini", 0),
            build_report.get("selection_mode_counts", {}).get("relaxed_minervini", 0),
            build_report.get("selection_mode_counts", {}).get("fallback_topup", 0),
        )
    
    return pool_codes


def load_candidate_pool(
    *,
    engine: Engine,
    env: str,
    today: date,
    base_strategy: str = "best_k_meta",
    force_rebuild: bool = False,
) -> tuple[Optional[List[str]], Optional[date], str]:
    """
    후보군 로드 (TTL 검사 포함).
    
    Args:
        engine: DB 엔진
        env: 환경
        today: 오늘 날짜
        base_strategy: 기본 전략 (미사용, 하위 호환용)
        force_rebuild: 강제 재생성 플래그 (MINERVINI_ONLY=1일 때는 무시됨)
    
    Returns:
        (pool_codes, pool_as_of, reason)
        - pool_codes: 종목코드 리스트 또는 None
        - pool_as_of: 후보군 생성 기준일 또는 None
        - reason: "hit" | "not_found" | "future_snapshot" | "stale_snapshot" | "too_small" | "forced_rebuild"
    """
    # [CRITICAL] MINERVINI_ONLY=1이면 강제 재생성 금지
    if MINERVINI_ONLY:
        force_rebuild = False
        logger.info("[CANDIDATE_POOL][MINERVINI_ONLY] force_rebuild disabled")

    if force_rebuild:
        logger.info("[CANDIDATE_POOL][LOAD] miss reason=forced_rebuild")
        return None, None, "forced_rebuild"
    
    repo = WatchlistRepo(engine)
    strategy = os.getenv("CANDIDATE_POOL_STRATEGY_KEY", "pb1_candidate_pool")
    
    requested_as_of = today

    # 최신 후보군 날짜 조회
    latest_date = repo.get_latest_watchlist_date(env=env, strategy=strategy)
    
    if not latest_date:
        if MINERVINI_ONLY:
            raise RuntimeError(
                "[CANDIDATE_POOL][MINERVINI_ONLY] No existing candidate pool in DB. "
                "MINERVINI_ONLY requires existing pool. Run pool build first."
            )
        logger.warning("[CANDIDATE_POOL][LOAD] miss reason=not_found")
        return None, None, "not_found"
    
    # Unified snapshot policy guard for future/stale behavior.
    date_guard = validate_snapshot_date(
        requested_as_of=requested_as_of,
        actual_as_of=latest_date,
        ttl_days=CANDIDATE_POOL_TTL_DAYS,
        allow_stale=True,
    )
    if date_guard.is_future:
        logger.error(
            "[CANDIDATE_POOL][DATE_GUARD] requested_as_of=%s actual_as_of=%s action=reject_future_snapshot",
            requested_as_of,
            latest_date,
        )
        logger.warning(
            "[CANDIDATE_POOL][LOAD] miss reason=future_snapshot requested_as_of=%s actual_as_of=%s",
            requested_as_of,
            latest_date,
        )
        logger.info(
            "[CANDIDATE_POOL][CACHE] requested_as_of=%s actual_as_of=%s hit=%s exact=%s stale_days=%s ttl_days=%s",
            requested_as_of,
            latest_date,
            False,
            False,
            date_guard.stale_days,
            CANDIDATE_POOL_TTL_DAYS,
        )
        return None, None, "future_snapshot"

    age_days = date_guard.stale_days
    exact_hit = bool(date_guard.is_exact)
    if age_days > CANDIDATE_POOL_TTL_DAYS:
        if MINERVINI_ONLY:
            logger.warning(
                "[CANDIDATE_POOL][MINERVINI_ONLY] Pool expired (age=%s > ttl=%s) but continuing anyway",
                age_days, CANDIDATE_POOL_TTL_DAYS
            )
        else:
            logger.warning(
                "[CANDIDATE_POOL][LOAD] miss reason=stale_snapshot as_of=%s age=%s ttl=%s",
                latest_date, age_days, CANDIDATE_POOL_TTL_DAYS
            )
            logger.info(
                "[CANDIDATE_POOL][CACHE] requested_as_of=%s actual_as_of=%s hit=%s exact=%s stale_days=%s ttl_days=%s",
                requested_as_of,
                latest_date,
                False,
                exact_hit,
                age_days,
                CANDIDATE_POOL_TTL_DAYS,
            )
            return None, None, "stale_snapshot"
    
    # 후보군 로드
    pool_members, _ = repo.load_watchlist(
        env=env,
        strategy=strategy,
        as_of=latest_date,
        allow_latest_fallback=False,  # 이미 latest_date를 사용하므로 fallback 불필요
    )
    if not pool_members:
        if MINERVINI_ONLY:
            raise RuntimeError(
                f"[CANDIDATE_POOL][MINERVINI_ONLY] Pool loaded but empty for date={latest_date}"
            )
        logger.warning("[CANDIDATE_POOL][LOAD] miss reason=missing as_of=%s", latest_date)
        return None, None, "missing"
    
    pool_codes = [item["code"] for item in pool_members]
    
    # 최소 크기 검사
    if len(pool_codes) < CANDIDATE_POOL_MIN_SIZE:
        if MINERVINI_ONLY:
            logger.warning(
                "[CANDIDATE_POOL][MINERVINI_ONLY] Pool too small (size=%s < min=%s) but continuing anyway",
                len(pool_codes), CANDIDATE_POOL_MIN_SIZE
            )
        else:
            logger.warning(
                "[CANDIDATE_POOL][LOAD] miss reason=too_small as_of=%s size=%s min=%s",
                latest_date, len(pool_codes), CANDIDATE_POOL_MIN_SIZE
            )
            logger.info(
                "[CANDIDATE_POOL][CACHE] requested_as_of=%s actual_as_of=%s hit=%s exact=%s stale_days=%s ttl_days=%s",
                requested_as_of,
                latest_date,
                False,
                exact_hit,
                age_days,
                CANDIDATE_POOL_TTL_DAYS,
            )
            return None, None, "too_small"
    
    logger.info(
        "[CANDIDATE_POOL][LOAD] hit=True as_of=%s actual_as_of=%s size=%s age=%s",
        requested_as_of,
        latest_date,
        len(pool_codes),
        age_days,
    )
    logger.info(
        "[CANDIDATE_POOL][CACHE] requested_as_of=%s actual_as_of=%s hit=%s exact=%s stale_days=%s ttl_days=%s",
        requested_as_of,
        latest_date,
        True,
        exact_hit,
        age_days,
        CANDIDATE_POOL_TTL_DAYS,
    )
    
    return pool_codes, latest_date, "hit"


def main():
    """CLI 엔트리포인트."""
    parser = argparse.ArgumentParser(description="Candidate Pool Builder")
    parser.add_argument("--build", choices=["pool"], help="Build candidate pool")
    parser.add_argument("--prefetch-only", action="store_true", help="Run OHLCV prefetch only")
    parser.add_argument("--build-only", action="store_true", help="Run pool build only (skip prefetch)")
    parser.add_argument("--as_of", type=str, help="As-of date (YYYY-MM-DD), default: prev business day")
    parser.add_argument("--env", type=str, default=None, help="Environment (practice/live)")
    
    args = parser.parse_args()
    
    if not args.build and not args.prefetch_only and not args.build_only:
        parser.print_help()
        return
    
    # 기준일 계산 (기본값: 전일 영업일)
    if args.as_of:
        as_of = datetime.strptime(args.as_of, "%Y-%m-%d").date()
    else:
        # 주말에 실행 시 금요일로 자동 설정
        now = now_kst()
        if now.weekday() >= 5:  # 토요일(5) 또는 일요일(6)
            as_of = prev_business_day(now.date())
        else:
            as_of = now.date()
    
    resolved_env = resolve_env(args.env)
    logger.info("[CANDIDATE_POOL][CLI] build=%s as_of=%s env=%s", args.build, as_of, resolved_env)
    
    # 환경 변수 출력 (디버깅용)
    universe_env = os.getenv("CANDIDATE_POOL_UNIVERSE_ENV")
    if not universe_env:
        universe_env = resolve_env(None)
    universe_env = universe_env.strip().lower()
    universe_strategy = os.getenv("CANDIDATE_POOL_UNIVERSE_STRATEGY", "best_k_meta")
    if resolved_env != universe_env:
        raise RuntimeError(f"ENV_NAMESPACE_MISMATCH env={resolved_env} universe_env={universe_env}")
    logger.info(
        "[CANDIDATE_POOL][CONFIG] CANDIDATE_POOL_UNIVERSE_ENV=%s CANDIDATE_POOL_UNIVERSE_STRATEGY=%s",
        universe_env, universe_strategy
    )
    
    # DB 연결
    from trader.db.engine import get_engine
    from trader.db.repos import UniverseRepo
    from trader.data.ohlcv_provider import KISOHLCVProvider
    
    engine = get_engine()
    
    # 유니버스 로드
    universe_repo = UniverseRepo(engine)
    members = universe_repo.get_current_universe_members(env=universe_env, strategy=universe_strategy)
    
    if not members:
        logger.error(
            "[CANDIDATE_POOL][UNIVERSE][FAIL] no members found. "
            "env=%s strategy=%s. "
            "Check: (1) universe_build job ran successfully, "
            "(2) env/strategy match between universe_build and candidate_pool, "
            "(3) PBCORE_DB_URL points to correct database",
            universe_env, universe_strategy
        )
        sys.exit(1)
    
    logger.info(
        "[CANDIDATE_POOL][UNIVERSE] env=%s strategy=%s members=%s",
        universe_env, universe_strategy, len(members)
    )
    print(f"[CANDIDATE_POOL][UNIVERSE] env={universe_env} strategy={universe_strategy} members={len(members)}")
    
    # ✅ PREFETCH-ONLY 모드
    if args.prefetch_only:
        logger.info("[CANDIDATE_POOL][MODE] prefetch-only mode")
        from trader.ohlcv_prefetch import prefetch_ohlcv_to_db
        
        force_rebuild = CANDIDATE_POOL_FORCE_REBUILD or os.getenv("CANDIDATE_POOL_FORCE_REBUILD", "0") == "1"
        strategy_key = os.getenv("CANDIDATE_POOL_STRATEGY_KEY", "pb1_candidate_pool")
        
        prefetch_ohlcv_to_db(
            engine=engine,
            members=members,
            force_rebuild=force_rebuild,
            env=resolved_env,
            strategy=strategy_key,
            as_of=as_of,
        )
        logger.info("[CANDIDATE_POOL][CLI] prefetch-only completed")
        print("OHLCV prefetch completed")
        return
    
    # OHLCV 프로바이더 설정 (빌드 단계는 DB-only 강제)
    if args.build or args.build_only:
        os.environ["OHLCV_PREFETCH_ONLY"] = "1"
        logger.info(
            "[CANDIDATE_POOL][OHLCV] build phase enforces DB-only provider (OHLCV_PREFETCH_ONLY=1)"
        )

    db_first_provider = KISOHLCVProvider(kis=None)

    def ohlcv_provider_func(code: str, days: int):
        result = db_first_provider.get_ohlcv(code, days)
        if result.df is None or len(result.df) < CANDIDATE_POOL_MIN_ROWS:
            logger.debug(
                "[CANDIDATE_POOL][OHLCV][DATA_INSUFFICIENT] code=%s days=%s rows=%s source=%s error=%s",
                code,
                days,
                0 if result.df is None else len(result.df),
                result.meta.get("source"),
                result.meta.get("error"),
            )
        return result.df
    
    # 후보군 생성 및 저장
    force_rebuild = CANDIDATE_POOL_FORCE_REBUILD or os.getenv("CANDIDATE_POOL_FORCE_REBUILD", "0") == "1"
    skip_prefetch = args.build_only  # build-only 모드일 때 prefetch 스킵
    
    pool_codes = build_and_save_candidate_pool(
        engine=engine,
        env=resolved_env,
        as_of=as_of,
        members=members,
        ohlcv_provider=ohlcv_provider_func,
        force_rebuild=force_rebuild,
        skip_prefetch=skip_prefetch,
    )
    
    logger.info("[CANDIDATE_POOL][CLI] success: %s codes saved", len(pool_codes))
    print(f"Candidate pool saved: {len(pool_codes)} codes")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    main()
