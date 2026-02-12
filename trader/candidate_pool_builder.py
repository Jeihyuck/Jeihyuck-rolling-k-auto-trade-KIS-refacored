"""Candidate Pool Builder - 주말에 후보군을 생성하고 DB에 저장."""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

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
)
from trader.db.repos import WatchlistRepo, WatchlistSnapshotRepo
from trader.flow_score import calculate_flow_score, rank_by_dollar_volume, calculate_final_score
from trader.ohlcv_prefetch import prefetch_ohlcv_to_db
from trader.report.pdf_report import generate_watchlist_pdf
from trader.time_utils import now_kst, prev_business_day
from trader.time_coerce import to_date

logger = logging.getLogger(__name__)


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
        
        for code in codes:
            try:
                df = self.ohlcv_provider(code, days=max(self.liq_days + 10, 80))
                if df is None or len(df) < self.min_rows:
                    continue
                
                # 최소 주가 필터
                last_close = df["close"].iloc[-1]
                if last_close < self.min_price:
                    continue
                
                # 유동성 점수 계산 (평균 거래대금)
                recent = df.tail(self.liq_days)
                avg_value = (recent["close"] * recent["volume"]).mean()
                
                # 추세 점수 (간단히 MA20 > MA50 > MA200 체크)
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
                
                # 변동성 필터 (과도한 변동성 제외)
                volatility = recent["close"].pct_change().std()
                if volatility > 0.08:  # 일일 8% 이상 변동은 제외
                    continue
                
                # 복합 점수 (유동성 70% + 추세 30%)
                composite_score = avg_value * 0.7 + trend_score * 1e9 * 0.3
                
                scored.append({
                    "code": code,
                    "score": composite_score,
                    "avg_value": avg_value,
                    "trend_score": trend_score,
                })
                
            except Exception as exc:
                logger.debug("[CANDIDATE_POOL][OHLCV_FAIL] code=%s err=%s", code, exc)
                continue
        
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
            raise RuntimeError(f"candidate pool scored=0 from {len(codes)} universe members")
        
        # 점수 내림차순 정렬 후 상위 target_size개 선택
        scored.sort(key=lambda x: x["score"], reverse=True)
        selected = scored[:self.target_size]
        
        result_codes = [item["code"] for item in selected]
        
        logger.info(
            "[CANDIDATE_POOL][BUILD][DONE] as_of=%s universe_size=%s scored=%s selected=%s",
            as_of, len(codes), len(scored), len(result_codes)
        )
        
        # ✅ selected < min_size 즉시 실패 처리
        min_size = int(os.getenv("CANDIDATE_POOL_MIN_SIZE", "40"))
        if len(result_codes) < min_size:
            logger.error(
                "[CANDIDATE_POOL][BUILD][FAIL] selected=%s < min_size=%s (from %s scored). "
                "Filter criteria too strict or data quality issue. "
                "Consider: (1) lowering min_price=%s, (2) lowering min_rows=%s, (3) increasing target_size=%s",
                len(result_codes), min_size, len(scored), self.min_price, self.min_rows, self.target_size
            )
            raise RuntimeError(f"candidate pool size {len(result_codes)} < min_size {min_size}")
        
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
        
        for code in codes:
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
        
        if len(scored) == 0:
            raise RuntimeError("scored=0 in 120 step")
        
        # 상위 120개 선택
        scored.sort(key=lambda x: x["score"], reverse=True)
        pool120 = scored[:min(120, len(scored))]
        
        logger.info("[FINAL30_PIPELINE][120] selected=%s", len(pool120))
        
        # Step 2: 120 → 50 (기술적 점수 강화)
        top50_scored = []
        for item in pool120:
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
        
        # DB에 스냅샷 저장
        snapshot_repo = WatchlistSnapshotRepo(engine)
        snapshot_repo.save_snapshot(as_of=as_of, final30=final30)
        
        # JSON 저장
        output_dir = Path("runtime/watchlist") / as_of.strftime("%Y-%m-%d")
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
    strategy = os.getenv("CANDIDATE_POOL_STRATEGY_KEY", "pb1_candidate_pool")

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
    
    # DB에 저장 (WATCHLIST 테이블에 저장)
    pool_members = [{"code": code} for code in pool_codes]
    
    # ✅ 빈 리스트 체크 (이중 안전장치) - 실패로 처리
    if not pool_members:
        logger.error("[WATCHLIST][SAVE] empty members - this should have failed in build_light_scan")
        raise RuntimeError("candidate pool is empty after build - cannot save")
    
    min_size = int(os.getenv("CANDIDATE_POOL_MIN_SIZE", "40"))
    if len(pool_members) < min_size:
        logger.error("[WATCHLIST][SAVE] members=%s < min_size=%s - failing", len(pool_members), min_size)
        raise RuntimeError(f"candidate pool size {len(pool_members)} < min_size {min_size}")
    
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
        - reason: "hit" | "expired" | "missing" | "too_small"
    """
    # [CRITICAL] MINERVINI_ONLY=1이면 강제 재생성 금지
    if MINERVINI_ONLY:
        force_rebuild = False
        logger.info("[CANDIDATE_POOL][MINERVINI_ONLY] force_rebuild disabled")
    
    repo = WatchlistRepo(engine)
    strategy = os.getenv("CANDIDATE_POOL_STRATEGY_KEY", "pb1_candidate_pool")
    
    # 최신 후보군 날짜 조회
    latest_date = repo.get_latest_watchlist_date(env=env, strategy=strategy)
    
    if not latest_date:
        if MINERVINI_ONLY:
            raise RuntimeError(
                "[CANDIDATE_POOL][MINERVINI_ONLY] No existing candidate pool in DB. "
                "MINERVINI_ONLY requires existing pool. Run pool build first."
            )
        logger.warning("[CANDIDATE_POOL][LOAD] miss reason=missing")
        return None, None, "missing"
    
    # TTL 검사
    age_days = (today - latest_date).days
    if age_days > CANDIDATE_POOL_TTL_DAYS:
        if MINERVINI_ONLY:
            logger.warning(
                "[CANDIDATE_POOL][MINERVINI_ONLY] Pool expired (age=%s > ttl=%s) but continuing anyway",
                age_days, CANDIDATE_POOL_TTL_DAYS
            )
        else:
            logger.warning(
                "[CANDIDATE_POOL][LOAD] miss reason=expired as_of=%s age=%s ttl=%s",
                latest_date, age_days, CANDIDATE_POOL_TTL_DAYS
            )
            return None, None, "expired"
    
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
            return None, None, "too_small"
    
    logger.info(
        "[CANDIDATE_POOL][LOAD] hit=True as_of=%s size=%s age=%s",
        latest_date, len(pool_codes), age_days
    )
    
    return pool_codes, latest_date, "hit"


def main():
    """CLI 엔트리포인트."""
    parser = argparse.ArgumentParser(description="Candidate Pool Builder")
    parser.add_argument("--build", choices=["pool"], help="Build candidate pool")
    parser.add_argument("--prefetch-only", action="store_true", help="Run OHLCV prefetch only")
    parser.add_argument("--build-only", action="store_true", help="Run pool build only (skip prefetch)")
    parser.add_argument("--as_of", type=str, help="As-of date (YYYY-MM-DD), default: prev business day")
    parser.add_argument("--env", type=str, default=os.getenv("STRATEGY_ENV", "PAPER"), help="Environment (LIVE/PAPER)")
    
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
    
    logger.info("[CANDIDATE_POOL][CLI] build=%s as_of=%s env=%s", args.build, as_of, args.env)
    
    # 환경 변수 출력 (디버깅용)
    universe_env = os.getenv("CANDIDATE_POOL_UNIVERSE_ENV", os.getenv("KIS_ENV", args.env))
    universe_strategy = os.getenv("CANDIDATE_POOL_UNIVERSE_STRATEGY", "best_k_meta")
    logger.info(
        "[CANDIDATE_POOL][CONFIG] CANDIDATE_POOL_UNIVERSE_ENV=%s CANDIDATE_POOL_UNIVERSE_STRATEGY=%s",
        universe_env, universe_strategy
    )
    
    # DB 연결
    from trader.db.engine import get_engine
    from trader.db.repos import UniverseRepo
    from trader.data.ohlcv_provider import ChainOHLCVProvider, KISOHLCVProvider, KRXOHLCVProvider
    from trader.kis_wrapper import KisAPI
    
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
            env=args.env,
            strategy=strategy_key,
            as_of=as_of,
        )
        logger.info("[CANDIDATE_POOL][CLI] prefetch-only completed")
        print("OHLCV prefetch completed")
        return
    
    # OHLCV 프로바이더 설정
    kis_http_enabled = os.getenv("KIS_HTTP_ENABLED", "1") == "1"
    
    if kis_http_enabled:
        kis = KisAPI()
        ohlcv_provider_func = lambda code, days: kis.fetch_daily_ohlcv(code, days=days)
    else:
        # DIAG 모드: KRX fallback만 사용
        krx_provider = KRXOHLCVProvider()
        ohlcv_provider_func = lambda code, days: krx_provider.fetch(code, days=days)
    
    # 후보군 생성 및 저장
    force_rebuild = CANDIDATE_POOL_FORCE_REBUILD or os.getenv("CANDIDATE_POOL_FORCE_REBUILD", "0") == "1"
    skip_prefetch = args.build_only  # build-only 모드일 때 prefetch 스킵
    
    pool_codes = build_and_save_candidate_pool(
        engine=engine,
        env=args.env,
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
