"""PB1 Watchlist Builder - 하루 1회 스캔으로 유니버스 필터링."""
from __future__ import annotations

import logging
import os
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from sqlalchemy import Engine

from trader.config import (
    RS_BENCHMARK,
    RS_LOOKBACK_DAYS,
    RS_LOOKBACK2_DAYS,
    RS_MIN_PCTILE,
)
from trader.db.repos import WatchlistRepo
from trader.time_utils import now_kst
from trader.time_coerce import to_date

logger = logging.getLogger(__name__)


def _env_int(key: str, default: int) -> int:
    """환경변수를 int로 파싱, 실패 시 default 반환."""
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default


def _env_float(key: str, default: float) -> float:
    """환경변수를 float로 파싱, 실패 시 default 반환."""
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default


def _env_bool(key: str, default: bool) -> bool:
    """환경변수를 bool로 파싱, 실패 시 default 반환."""
    val = os.getenv(key, str(default)).lower()
    return val in ("1", "true", "yes", "on")


class WatchlistBuilder:
    """
    PB1 Watchlist 빌더.
    
    Stage A: 유니버스 -> topK (유동성 필터)
    Stage B: topK -> finalN (전략 스코어링)
    """
    
    def __init__(
        self,
        *,
        ohlcv_provider: Any,  # OHLCV 데이터 제공자 (pb1_engine._fetch_daily 같은 함수)
        minervini_config: Dict[str, Any],
        topk: int = 50,
        finaln: int = 30,
        min_price: float = 2000.0,
        liq_days: int = 20,
        min_rows: int = 30,
    ):
        self.ohlcv_provider = ohlcv_provider
        self.minervini_config = minervini_config
        self.topk = topk
        self.finaln = finaln
        self.min_price = min_price
        self.liq_days = liq_days
        self.min_rows = min_rows
    
    def build(
        self,
        *,
        members: List[Dict[str, Any]],
        as_of: date,
    ) -> List[Dict[str, Any]]:
        """
        Watchlist 생성.
        
        반환: [{"code": "005930", "rank": 1, "score": 75.5, "meta": {...}}, ...]
        """
        logger.info(
            "[WATCHLIST][BUILD][START] as_of=%s members=%s topk=%s finaln=%s",
            as_of, len(members), self.topk, self.finaln
        )
        
        # Stage A: 유동성 필터
        stage_a_result = self._stage_a_liquidity_filter(members, as_of)
        if not stage_a_result:
            logger.warning("[WATCHLIST][STAGE_A] no members passed -> empty watchlist")
            return []
        
        # Stage B: 전략 스코어링
        try:
            stage_b_result = self._stage_b_strategy_scoring(stage_a_result, as_of)
        except Exception as exc:
            logger.warning(
                "[WATCHLIST][STAGE_B][FAIL] reason=%s -> fallback to stage_a",
                exc
            )
            stage_b_result = stage_a_result[:self.finaln]
        
        # Rank 부여
        for i, item in enumerate(stage_b_result, start=1):
            item["rank"] = i
        
        logger.info(
            "[WATCHLIST][BUILD][DONE] as_of=%s final_members=%s",
            as_of, len(stage_b_result)
        )
        return stage_b_result
    
    def _stage_a_liquidity_filter(
        self,
        members: List[Dict[str, Any]],
        as_of: date,
    ) -> List[Dict[str, Any]]:
        """
        Stage A: 유동성 필터.
        
        1. OHLCV rows 부족 제외 (최근 min_rows일 미만)
        2. 가격 필터 (최근 종가 min_price원 미만 제외)
        3. 거래대금 상위 topK 선정 (최근 liq_days일 평균)
        """
        candidates = []
        excluded_rows = 0
        excluded_price = 0
        excluded_nan = 0
        
        for m in members:
            code = str(m.get("code") or "").zfill(6)
            if not code:
                continue
            
            # OHLCV 조회
            try:
                df, meta = self.ohlcv_provider(code, count=max(self.min_rows, self.liq_days + 10))
            except Exception as exc:
                logger.debug("[WATCHLIST][STAGE_A][OHLCV_FAIL] code=%s err=%s", code, exc)
                excluded_rows += 1
                continue
            
            if df is None or df.empty or len(df) < self.min_rows:
                excluded_rows += 1
                continue
            
            # 가격 필터
            last_close = df["close"].iloc[-1] if "close" in df.columns and len(df) > 0 else None
            if last_close is None or pd.isna(last_close):
                excluded_nan += 1
                continue
            if last_close < self.min_price:
                excluded_price += 1
                continue
            
            # 거래대금 계산 (최근 liq_days일 평균)
            if "close" not in df.columns or "volume" not in df.columns:
                excluded_nan += 1
                continue
            
            recent_df = df.tail(self.liq_days)
            liq_avg = (recent_df["close"] * recent_df["volume"]).mean()
            if pd.isna(liq_avg):
                excluded_nan += 1
                continue
            
            candidates.append({
                "code": code,
                "liq_avg": float(liq_avg),
                "last_close": float(last_close),
                "meta": {
                    "liq_avg": float(liq_avg),
                    "last_close": float(last_close),
                    "rows": len(df),
                },
            })
        
        # 거래대금 내림차순 정렬
        candidates.sort(key=lambda x: x["liq_avg"], reverse=True)
        
        # topK 선정
        topk_candidates = candidates[:self.topk]
        
        logger.info(
            "[WATCHLIST][STAGE_A] kept=%s excluded_rows=%s excluded_price=%s excluded_nan=%s",
            len(topk_candidates), excluded_rows, excluded_price, excluded_nan
        )
        
        return topk_candidates
    
    def _stage_b_strategy_scoring(
        self,
        candidates: List[Dict[str, Any]],
        as_of: date,
    ) -> List[Dict[str, Any]]:
        """
        Stage B: 전략 스코어링.
        
        Minervini RS percentile, VCP score 통과한 것 중 상위 finalN 선정.
        """
        scored = []
        
        # 벤치마크 데이터 로드
        try:
            bench_df, _ = self.ohlcv_provider(RS_BENCHMARK, count=max(RS_LOOKBACK_DAYS, RS_LOOKBACK2_DAYS) + 10)
            if bench_df is None or bench_df.empty:
                raise ValueError(f"benchmark {RS_BENCHMARK} data empty")
            bench_close = bench_df["close"]
        except Exception as exc:
            logger.error("[WATCHLIST][STAGE_B][BENCHMARK_FAIL] err=%s", exc)
            raise
        
        rs_min_pctile = self.minervini_config.get("rs_min_pctile", RS_MIN_PCTILE)
        vcp_min_score = self.minervini_config.get("vcp_min_score", 70.0)
        
        for cand in candidates:
            code = cand["code"]
            
            # OHLCV 조회 (충분한 데이터 필요)
            try:
                df, meta = self.ohlcv_provider(code, count=max(RS_LOOKBACK_DAYS, RS_LOOKBACK2_DAYS, 120) + 10)
            except Exception:
                continue
            
            if df is None or df.empty or len(df) < 120:
                continue
            
            # RS percentile 계산
            try:
                rs_pctile = self._compute_rs_percentile(df["close"], bench_close)
            except Exception:
                rs_pctile = 0.0
            
            # VCP score 계산
            try:
                vcp_score = self._compute_vcp_score(df)
            except Exception:
                vcp_score = 0.0
            
            # 필터링
            if rs_pctile < rs_min_pctile:
                continue
            if vcp_score < vcp_min_score:
                continue
            
            # 종합 점수 (RS + VCP 가중 평균)
            score = rs_pctile * 0.5 + vcp_score * 0.5
            
            scored.append({
                "code": code,
                "score": float(score),
                "meta": {
                    **cand.get("meta", {}),
                    "rs_pctile": float(rs_pctile),
                    "vcp_score": float(vcp_score),
                },
            })
        
        # 점수 내림차순 정렬
        scored.sort(key=lambda x: x["score"], reverse=True)
        
        # finalN 선정
        final = scored[:self.finaln]
        
        logger.info(
            "[WATCHLIST][STAGE_B] kept=%s (from %s candidates)",
            len(final), len(candidates)
        )
        
        return final
    
    def _compute_rs_percentile(self, stock_close: pd.Series, bench_close: pd.Series) -> float:
        """RS percentile 계산 (간단한 구현)."""
        if len(stock_close) < RS_LOOKBACK_DAYS or len(bench_close) < RS_LOOKBACK_DAYS:
            return 0.0
        
        # 최근 RS_LOOKBACK_DAYS일 수익률
        stock_ret = (stock_close.iloc[-1] / stock_close.iloc[-RS_LOOKBACK_DAYS] - 1) * 100
        bench_ret = (bench_close.iloc[-1] / bench_close.iloc[-RS_LOOKBACK_DAYS] - 1) * 100
        
        # RS = stock_ret - bench_ret
        rs = stock_ret - bench_ret
        
        # Percentile 근사 (간단히 0~100 스케일로 변환)
        # 실제로는 전체 유니버스 대비 percentile을 계산해야 하지만,
        # 여기서는 단순히 RS > 0이면 높은 점수를 주는 방식으로 근사
        if rs > 20:
            return 90.0
        elif rs > 10:
            return 80.0
        elif rs > 0:
            return 70.0
        else:
            return max(0.0, 50.0 + rs)  # RS < 0이면 50 미만
    
    def _compute_vcp_score(self, df: pd.Series) -> float:
        """VCP score 계산 (간단한 구현)."""
        if len(df) < 120:
            return 0.0
        
        # 변동성 수축 패턴 감지 (간단한 휴리스틱)
        # 최근 30일 변동성 vs 이전 90일 변동성
        recent_30 = df["high"].tail(30) / df["low"].tail(30) - 1
        prev_90 = df["high"].iloc[-120:-30] / df["low"].iloc[-120:-30] - 1
        
        recent_vol = recent_30.mean()
        prev_vol = prev_90.mean()
        
        if prev_vol == 0:
            return 0.0
        
        # 변동성 수축 비율
        contraction_ratio = recent_vol / prev_vol
        
        # 수축 비율이 낮을수록 높은 점수
        if contraction_ratio < 0.5:
            return 90.0
        elif contraction_ratio < 0.7:
            return 80.0
        elif contraction_ratio < 0.9:
            return 70.0
        else:
            return max(0.0, 100.0 - contraction_ratio * 100)


def build_and_save_watchlist(
    *,
    engine: Engine,
    env: str,
    strategy: str,
    as_of: date,
    members: List[Dict[str, Any]],
    ohlcv_provider: Any,
    minervini_config: Dict[str, Any],
    force_rebuild: bool = False,
) -> List[Dict[str, Any]]:
    """
    Watchlist를 생성하고 DB에 저장.
    
    force_rebuild=False이면 이미 당일 watchlist가 있으면 재사용.
    """
    repo = WatchlistRepo(engine)
    
    # 이미 당일 watchlist가 있으면 재사용
    if not force_rebuild:
        existing = repo.load_watchlist(env=env, strategy=strategy, as_of=as_of)
        if existing:
            logger.info(
                "[WATCHLIST][CACHE] hit=True as_of=%s members=%s",
                as_of, len(existing)
            )
            return existing
    
    # Watchlist 생성
    builder = WatchlistBuilder(
        ohlcv_provider=ohlcv_provider,
        minervini_config=minervini_config,
        topk=_env_int("PB1_WATCHLIST_TOPK", 50),
        finaln=_env_int("PB1_WATCHLIST_FINALN", 30),
        min_price=_env_float("PB1_WATCHLIST_MIN_PRICE", 2000.0),
        liq_days=_env_int("PB1_WATCHLIST_LIQ_DAYS", 20),
        min_rows=_env_int("PB1_WATCHLIST_MIN_ROWS", 30),
    )
    
    try:
        watchlist = builder.build(members=members, as_of=as_of)
    except Exception as exc:
        logger.error("[WATCHLIST][BUILD][FAIL] err=%s", exc, exc_info=True)
        raise
    
    # DB에 저장
    repo.save_watchlist(
        env=env,
        strategy=strategy,
        as_of=as_of,
        members=watchlist,
    )
    
    return watchlist


def load_today_watchlist_with_fallback(
    *,
    engine: Engine,
    env: str,
    strategy: str,
    today: date,
    members: List[Dict[str, Any]],
    ohlcv_provider: Any,
    minervini_config: Dict[str, Any],
    auto_build_if_empty: bool = True,
    strict_fail_on_empty: bool = False,
) -> tuple[List[Dict[str, Any]], str]:
    """
    오늘 watchlist를 로드, 없으면 자동 생성 또는 fallback 전략 적용.
    
    반환: (watchlist, source)
    - source: "watchlist_db" | "watchlist_autobuilt" | "prevday" | "fallback" | "watchlist_empty"
    """
    today = to_date(today)  # Ensure DATE type
    repo = WatchlistRepo(engine)
    
    def _load():
        rows = repo.load_watchlist(env=env, strategy=strategy, as_of=today)
        return rows
    
    # 1) Try load today's watchlist
    rows = _load()
    if rows:
        logger.info("[WATCHLIST][CACHE] hit=True source=watchlist_db as_of=%s count=%s", today, len(rows))
        return rows, "watchlist_db"
    
    # 2) If empty and auto_build enabled -> build & save
    if auto_build_if_empty:
        logger.warning("[WATCHLIST][AUTO_BUILD] today=%s missing -> building now", today)
        try:
            topk = _env_int("PB1_WATCHLIST_TOPK", 50)
            finaln = _env_int("PB1_WATCHLIST_FINALN", 30)
            builder = WatchlistBuilder(
                ohlcv_provider=ohlcv_provider,
                minervini_config=minervini_config,
                topk=topk,
                finaln=finaln,
                min_price=_env_float("PB1_WATCHLIST_MIN_PRICE", 2000.0),
                liq_days=_env_int("PB1_WATCHLIST_LIQ_DAYS", 20),
                min_rows=_env_int("PB1_WATCHLIST_MIN_ROWS", 30),
            )
            built = builder.build(members=members, as_of=today)
            if built:
                # Save to DB
                repo.save_watchlist(env=env, strategy=strategy, as_of=today, members=built)
                logger.info("[WATCHLIST][AUTO_BUILD] saved count=%s -> reloading", len(built))
                # Reload from DB
                rows2 = _load()
                if rows2:
                    logger.info("[WATCHLIST][AUTO_BUILD] success count=%s", len(rows2))
                    return rows2, "watchlist_autobuilt"
        except Exception as exc:
            logger.error("[WATCHLIST][AUTO_BUILD][FAIL] err=%s", exc, exc_info=True)
    
    # 3) Try previous day watchlist
    latest_date = repo.get_latest_watchlist_date(env=env, strategy=strategy)
    if latest_date:
        watchlist = repo.load_watchlist(env=env, strategy=strategy, as_of=latest_date)
        if watchlist:
            logger.warning(
                "[WATCHLIST][CACHE] hit=True source=prevday as_of=%s (today=%s)",
                latest_date, today
            )
            return watchlist, "prevday"
    
    # 4) Fallback: liquidity topK
    logger.warning("[WATCHLIST][CACHE] miss -> fallback to liquidity topK")
    try:
        topk = _env_int("PB1_WATCHLIST_TOPK", 50)
        builder = WatchlistBuilder(
            ohlcv_provider=ohlcv_provider,
            minervini_config=minervini_config,
            topk=topk,
            finaln=topk,  # Stage B 스킵, Stage A만 사용
            min_price=_env_float("PB1_WATCHLIST_MIN_PRICE", 2000.0),
            liq_days=_env_int("PB1_WATCHLIST_LIQ_DAYS", 20),
            min_rows=_env_int("PB1_WATCHLIST_MIN_ROWS", 30),
        )
        fallback = builder._stage_a_liquidity_filter(members, today)
        for i, item in enumerate(fallback, start=1):
            item["rank"] = i
        logger.warning(
            "[WATCHLIST][FALLBACK] source=fallback members=%s",
            len(fallback)
        )
        return fallback, "fallback"
    except Exception as exc:
        logger.error("[WATCHLIST][FALLBACK][FAIL] err=%s -> return empty", exc)
        if strict_fail_on_empty:
            raise RuntimeError(f"Watchlist empty after auto-build: env={env} strategy={strategy} as_of={today}") from exc
        return [], "watchlist_empty"
