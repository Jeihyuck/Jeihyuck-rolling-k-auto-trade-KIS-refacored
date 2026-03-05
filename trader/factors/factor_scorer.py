"""
Factor Scoring Engine
종목 선별 고도화 (Minervini 전 단계)

Factor 구성:
1. Momentum (120일 수익률)
2. Earnings Growth (EPS 성장률)
3. Relative Strength (RS percentile)
4. Volume Trend (거래량 추세)
"""
from __future__ import annotations

import logging
import math
from typing import Dict, Optional, List

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class FactorScorer:
    """
    팩터 기반 종목 점수 계산
    """
    
    def __init__(
        self,
        momentum_weight: float = 0.4,
        rs_weight: float = 0.3,
        earnings_weight: float = 0.2,
        volume_weight: float = 0.1,
    ):
        """
        Args:
            momentum_weight: 모멘텀 가중치 (0~1)
            rs_weight: RS percentile 가중치 (0~1)
            earnings_weight: EPS 성장률 가중치 (0~1)
            volume_weight: 거래량 추세 가중치 (0~1)
        """
        self.momentum_weight = momentum_weight
        self.rs_weight = rs_weight
        self.earnings_weight = earnings_weight
        self.volume_weight = volume_weight
        
        total = momentum_weight + rs_weight + earnings_weight + volume_weight
        if total != 1.0:
            logger.warning(f"Factor weights sum to {total}, not 1.0")
    
    def calculate_momentum_score(self, df: pd.DataFrame) -> float:
        """
        120일 모멘텀 점수 계산 (0~100)
        
        Args:
            df: OHLCV 데이터프레임
        
        Returns:
            Momentum score (0~100)
        """
        if df is None or len(df) < 120:
            return 0.0
        
        try:
            current_price = float(df["close"].iloc[-1])
            price_120d_ago = float(df["close"].iloc[-120])
            
            if price_120d_ago <= 0:
                return 0.0
            
            ret_120d = ((current_price - price_120d_ago) / price_120d_ago) * 100
            
            # 정규화: -30% ~ +100% 범위를 0~100으로
            # -30% = 0점, 0% = 33점, 100% = 100점
            score = (ret_120d + 30) / 1.3
            score = max(0.0, min(100.0, score))
            
            return score
        except Exception as e:
            logger.warning(f"Momentum calculation error: {e}")
            return 0.0
    
    def calculate_rs_score(self, rs_percentile: float) -> float:
        """
        RS percentile을 0~100 점수로 변환
        
        Args:
            rs_percentile: RS percentile (0~100)
        
        Returns:
            RS score (0~100)
        """
        # 그냥 percentile 자체를 점수로 사용
        return max(0.0, min(100.0, rs_percentile))
    
    def calculate_earnings_score(
        self,
        eps_growth_rate: Optional[float] = None,
    ) -> float:
        """
        EPS 성장률 점수 계산
        
        Args:
            eps_growth_rate: EPS 성장률 (-100~200%)
        
        Returns:
            Earnings score (0~100)
        """
        if eps_growth_rate is None:
            return 50.0  # 기본값
        
        try:
            # 정규화: -20% ~ +100% 범위를 0~100으로
            score = (eps_growth_rate + 20) / 1.2
            score = max(0.0, min(100.0, score))
            return score
        except Exception:
            return 50.0
    
    def calculate_volume_trend_score(self, df: pd.DataFrame) -> float:
        """
        거래량 추세 점수 계산 (0~100)
        
        Args:
            df: OHLCV 데이터프레임
        
        Returns:
            Volume trend score (0~100)
        """
        if df is None or len(df) < 20:
            return 50.0
        
        try:
            vol_ma20 = df["volume"].rolling(20).mean()
            vol_ma60 = df["volume"].rolling(60).mean()
            
            current_vol = float(df["volume"].iloc[-1])
            ma20_val = float(vol_ma20.iloc[-1])
            ma60_val = float(vol_ma60.iloc[-1])
            
            if ma60_val <= 0 or ma20_val <= 0:
                return 50.0
            
            # 20일 평균이 60일 평균보다 큰지 확인 (추세)
            trend_score = (ma20_val / ma60_val) * 100 if ma60_val > 0 else 50.0
            trend_score = max(0.0, min(100.0, trend_score))
            
            # 현재 거래량이 20일 평균 대비 얼마나 큰지
            vol_spike_score = min((current_vol / ma20_val) * 50, 100) if ma20_val > 0 else 50.0
            
            # 혼합
            return trend_score * 0.6 + vol_spike_score * 0.4
        except Exception as e:
            logger.warning(f"Volume trend calculation error: {e}")
            return 50.0
    
    def calculate_composite_score(
        self,
        df: pd.DataFrame,
        rs_percentile: float = 50.0,
        eps_growth: Optional[float] = None,
    ) -> Dict[str, float]:
        """
        종합 팩터 점수 계산
        
        Args:
            df: OHLCV 데이터프레임
            rs_percentile: RS percentile
            eps_growth: EPS 성장률
        
        Returns:
            Dict with individual and composite scores
        """
        momentum_score = self.calculate_momentum_score(df)
        rs_score = self.calculate_rs_score(rs_percentile)
        earnings_score = self.calculate_earnings_score(eps_growth)
        volume_score = self.calculate_volume_trend_score(df)
        
        # 종합 점수
        composite = (
            momentum_score * self.momentum_weight +
            rs_score * self.rs_weight +
            earnings_score * self.earnings_weight +
            volume_score * self.volume_weight
        )
        
        return {
            "momentum": momentum_score,
            "rs": rs_score,
            "earnings": earnings_score,
            "volume": volume_score,
            "composite": composite,
        }


def rank_candidates_by_factors(
    candidates: List[Dict],
    factor_scorer: Optional[FactorScorer] = None,
) -> List[Dict]:
    """
    팩터 점수로 후보 종목 랭킹
    
    Args:
        candidates: 후보 종목 리스트 (각 dict는 df, rs_percentile 포함)
        factor_scorer: FactorScorer 인스턴스
    
    Returns:
        Ranked candidates
    """
    if factor_scorer is None:
        factor_scorer = FactorScorer()
    
    for candidate in candidates:
        df = candidate.get("df")
        rs = candidate.get("rs_percentile", 50.0)
        eps = candidate.get("eps_growth", None)
        
        scores = factor_scorer.calculate_composite_score(df, rs, eps)
        candidate["factor_scores"] = scores
        candidate["composite_score"] = scores["composite"]
    
    # 종합 점수로 정렬
    ranked = sorted(
        candidates,
        key=lambda x: x.get("composite_score", 0),
        reverse=True,
    )
    
    return ranked
