"""Candidate Pool Builder - 주말에 후보군을 생성하고 DB에 저장."""
from __future__ import annotations

import argparse
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
)
from trader.db.repos import WatchlistRepo, load_price_daily, upsert_price_daily
from trader.time_utils import now_kst, prev_business_day
from trader.time_coerce import to_date

logger = logging.getLogger(__name__)


def prefetch_ohlcv_to_db(
    *,
    engine: Engine,
    members: List[Dict[str, Any]],
    days: Optional[int] = None,
) -> None:
    """
    유니버스 멤버들의 OHLCV를 FDR로 가져와서 DB에 저장.
    
    Actions runner는 깨끗한 환경이므로 DB에 가격 데이터가 없을 수 있음.
    candidate pool scoring 전에 이 함수를 실행하여 DB를 채운다.
    
    Args:
        engine: DB 엔진
        members: 유니버스 멤버 리스트
        days: 가져올 영업일 수 (기본 180일, 환경변수로 조정 가능)
    """
    import time
    try:
        import FinanceDataReader as fdr
    except ImportError:
        logger.warning("[OHLCV][PREFETCH] FinanceDataReader not available, skipping prefetch")
        return
    
    # 환경변수로 days 설정 (250 -> 180으로 기본값 변경)
    if days is None:
        days = int(os.getenv("CANDIDATE_POOL_PREFETCH_DAYS", "180"))
    
    # chunk size 설정 (기본 25)
    chunk_size = int(os.getenv("CANDIDATE_POOL_PREFETCH_CHUNK", "25"))
    
    codes = [m["code"] for m in members]
    total_codes = len(codes)
    logger.info("[OHLCV][PREFETCH][START] codes=%s days=%s chunk_size=%s", total_codes, days, chunk_size)
    
    end_date = now_kst().date()
    start_date = end_date - timedelta(days=days * 2)  # 영업일 감안하여 2배
    
    total_success = 0
    total_skip = 0
    total_fail = 0
    
    overall_start = time.time()
    
    # chunk별로 분할 처리
    for chunk_idx in range(0, total_codes, chunk_size):
        chunk_codes = codes[chunk_idx:chunk_idx + chunk_size]
        chunk_num = (chunk_idx // chunk_size) + 1
        total_chunks = (total_codes + chunk_size - 1) // chunk_size
        
        chunk_start = time.time()
        logger.info(
            "[OHLCV][PREFETCH][CHUNK] chunk=%s/%s codes=%s (indices %s-%s)",
            chunk_num, total_chunks, len(chunk_codes), chunk_idx, chunk_idx + len(chunk_codes) - 1
        )
        
        success_count = 0
        skip_count = 0
        fail_count = 0
        
        for code in chunk_codes:
            try:
                # 이미 DB에 최근 데이터가 있는지 확인
                existing = load_price_daily(
                    engine, 
                    code, 
                    start_date=end_date - timedelta(days=30), 
                    end_date=end_date
                )
                
                if existing and len(existing) >= 20:
                    # 최근 20일치 데이터가 있으면 스킵
                    skip_count += 1
                    continue
                
                # FDR로 가격 데이터 가져오기 (timeout 적용)
                df = None
                max_retries = 3
                for attempt in range(1, max_retries + 1):
                    try:
                        df = fdr.DataReader(code, start=start_date, end=end_date)
                        break
                    except Exception as fetch_err:
                        if attempt < max_retries:
                            logger.warning(
                                "[OHLCV][PREFETCH][RETRY] code=%s attempt=%s/%s err=%s",
                                code, attempt, max_retries, str(fetch_err)[:100]
                            )
                            time.sleep(1)  # 재시도 전 대기
                        else:
                            raise
                
                if df is None or df.empty:
                    logger.debug("[OHLCV][PREFETCH][EMPTY] code=%s", code)
                    fail_count += 1
                    continue
                
                # DataFrame을 candles 형식으로 변환
                df = df.reset_index()
                candles = []
                for _, row in df.iterrows():
                    candles.append({
                        "date": row.get("Date", row.name).strftime("%Y-%m-%d") if hasattr(row.get("Date", row.name), "strftime") else str(row.get("Date", row.name)),
                        "open": float(row.get("Open", 0)),
                        "high": float(row.get("High", 0)),
                        "low": float(row.get("Low", 0)),
                        "close": float(row.get("Close", 0)),
                        "volume": int(row.get("Volume", 0)),
                    })
                
                if not candles:
                    fail_count += 1
                    continue
                
                # DB에 저장
                market = MARKET_MAP.get(code, "KOSPI")
                upsert_price_daily(engine, candles, market, code)
                success_count += 1
                
                logger.debug("[OHLCV][PREFETCH][OK] code=%s rows=%s", code, len(candles))
                
            except Exception as exc:
                logger.warning("[OHLCV][PREFETCH][FAIL] code=%s err=%s", code, str(exc)[:100])
                fail_count += 1
                continue
        
        chunk_elapsed = time.time() - chunk_start
        total_success += success_count
        total_skip += skip_count
        total_fail += fail_count
        
        logger.info(
            "[OHLCV][PREFETCH][CHUNK_DONE] chunk=%s/%s elapsed=%.1fs success=%s skip=%s fail=%s",
            chunk_num, total_chunks, chunk_elapsed, success_count, skip_count, fail_count
        )
    
    overall_elapsed = time.time() - overall_start
    logger.info(
        "[OHLCV][PREFETCH][DONE] total_codes=%s success=%s skip=%s fail=%s elapsed=%.1fs",
        total_codes, total_success, total_skip, total_fail, overall_elapsed
    )


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


def build_and_save_candidate_pool(
    *,
    engine: Engine,
    env: str,
    as_of: date,
    members: List[Dict[str, Any]],
    ohlcv_provider: Any,
    force_rebuild: bool = False,
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
    
    Returns:
        후보군 종목코드 리스트
    """
    repo = WatchlistRepo(engine)
    strategy = os.getenv("CANDIDATE_POOL_STRATEGY_KEY", "pb1_candidate_pool")
    
    # 이미 당일 후보군이 있으면 재사용
    if not force_rebuild:
        existing = repo.load_watchlist(env=env, strategy=strategy, as_of=as_of)
        if existing:
            codes = [item["code"] for item in existing]
            logger.info(
                "[CANDIDATE_POOL][CACHE] hit=True as_of=%s count=%s",
                as_of, len(codes)
            )
            return codes
    
    # ✅ OHLCV 프리패치 (scoring 전에 DB에 가격 데이터 채우기)
    logger.info("[CANDIDATE_POOL][PREFETCH] starting OHLCV prefetch for %s members", len(members))
    try:
        prefetch_ohlcv_to_db(engine=engine, members=members)  # days는 환경변수로 처리
    except Exception as exc:
        logger.warning("[CANDIDATE_POOL][PREFETCH][FAIL] err=%s (continuing with existing DB data)", exc)
    
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
) -> tuple[Optional[List[str]], Optional[date], str]:
    """
    후보군 로드 (TTL 검사 포함).
    
    Args:
        engine: DB 엔진
        env: 환경
        today: 오늘 날짜
        base_strategy: 기본 전략 (미사용, 하위 호환용)
    
    Returns:
        (pool_codes, pool_as_of, reason)
        - pool_codes: 종목코드 리스트 또는 None
        - pool_as_of: 후보군 생성 기준일 또는 None
        - reason: "hit" | "expired" | "missing" | "too_small"
    """
    repo = WatchlistRepo(engine)
    strategy = os.getenv("CANDIDATE_POOL_STRATEGY_KEY", "pb1_candidate_pool")
    
    # 최신 후보군 날짜 조회
    latest_date = repo.get_latest_watchlist_date(env=env, strategy=strategy)
    
    if not latest_date:
        logger.warning("[CANDIDATE_POOL][LOAD] miss reason=missing")
        return None, None, "missing"
    
    # TTL 검사
    age_days = (today - latest_date).days
    if age_days > CANDIDATE_POOL_TTL_DAYS:
        logger.warning(
            "[CANDIDATE_POOL][LOAD] miss reason=expired as_of=%s age=%s ttl=%s",
            latest_date, age_days, CANDIDATE_POOL_TTL_DAYS
        )
        return None, None, "expired"
    
    # 후보군 로드
    pool_members = repo.load_watchlist(env=env, strategy=strategy, as_of=latest_date)
    if not pool_members:
        logger.warning("[CANDIDATE_POOL][LOAD] miss reason=missing as_of=%s", latest_date)
        return None, None, "missing"
    
    pool_codes = [item["code"] for item in pool_members]
    
    # 최소 크기 검사
    if len(pool_codes) < CANDIDATE_POOL_MIN_SIZE:
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
    parser.add_argument("--as_of", type=str, help="As-of date (YYYY-MM-DD), default: prev business day")
    parser.add_argument("--env", type=str, default=os.getenv("STRATEGY_ENV", "PAPER"), help="Environment (LIVE/PAPER)")
    
    args = parser.parse_args()
    
    if not args.build:
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
    
    pool_codes = build_and_save_candidate_pool(
        engine=engine,
        env=args.env,
        as_of=as_of,
        members=members,
        ohlcv_provider=ohlcv_provider_func,
        force_rebuild=force_rebuild,
    )
    
    logger.info("[CANDIDATE_POOL][CLI] success: %s codes saved", len(pool_codes))
    print(f"Candidate pool saved: {len(pool_codes)} codes")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    main()
