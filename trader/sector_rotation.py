"""
Sector Rotation Engine
섹터 강도 계산 및 선별

헤지펀드의 핵심 기능: 강한 섹터 내 종목만 선택
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class SectorRotationEngine:
    """
    섹터 강도 분석 및 로테이션
    """
    
    def __init__(
        self,
        lookback_30d: int = 30,
        lookback_90d: int = 90,
        weight_30d: float = 0.6,
        weight_90d: float = 0.4,
    ):
        """
        Args:
            lookback_30d: 30일 수익률 계산 기간
            lookback_90d: 90일 수익률 계산 기간
            weight_30d: 30일 가중치
            weight_90d: 90일 가중치
        """
        self.lookback_30d = lookback_30d
        self.lookback_90d = lookback_90d
        self.weight_30d = weight_30d
        self.weight_90d = weight_90d
    
    def calculate_sector_return(
        self,
        sector_df: pd.DataFrame,
        days: int,
    ) -> float:
        """
        섹터 평균 수익률 계산
        
        Args:
            sector_df: 섹터 내 종목들의 데이터
                       columns: 'symbol', 'returns_{days}d' 또는 'close'
            days: 기간 (일)
        
        Returns:
            Sector average return (%)
        """
        if sector_df is None or len(sector_df) == 0:
            return 0.0
        
        try:
            if f"returns_{days}d" in sector_df.columns:
                return sector_df[f"returns_{days}d"].mean()
            else:
                # close 기반으로 계산
                # 가정: sorted by date
                if len(sector_df) < days:
                    return 0.0
                
                current = sector_df["close"].iloc[-1]
                past = sector_df["close"].iloc[-days]
                
                if past <= 0:
                    return 0.0
                
                return ((current - past) / past) * 100
        except Exception as e:
            logger.warning(f"Sector return calculation error: {e}")
            return 0.0
    
    def calculate_sector_strength(
        self,
        sector_name: str,
        sector_candidates: List[Dict],
    ) -> Dict[str, float]:
        """
        섹터 강도 계산 (30일, 90일 혼합)
        
        Args:
            sector_name: 섹터명
            sector_candidates: 해당 섹터 후보 종목들
                              각 dict는 'symbol', 'df', 'price_30d_ago', 'price_90d_ago' 등 포함
        
        Returns:
            Dict with sector metrics
        """
        if not sector_candidates:
            return {"sector": sector_name, "strength": 0.0, "count": 0}
        
        returns_30d = []
        returns_90d = []
        
        for candidate in sector_candidates:
            try:
                df = candidate.get("df")
                if df is None or len(df) < self.lookback_90d:
                    continue
                
                current = float(df["close"].iloc[-1])
                
                # 30일
                if len(df) >= self.lookback_30d:
                    price_30d = float(df["close"].iloc[-self.lookback_30d])
                    ret_30d = ((current - price_30d) / price_30d) * 100 if price_30d > 0 else 0.0
                    returns_30d.append(ret_30d)
                
                # 90일
                if len(df) >= self.lookback_90d:
                    price_90d = float(df["close"].iloc[-self.lookback_90d])
                    ret_90d = ((current - price_90d) / price_90d) * 100 if price_90d > 0 else 0.0
                    returns_90d.append(ret_90d)
            except Exception as e:
                logger.warning(f"Error calculating returns for {candidate.get('symbol')}: {e}")
                continue
        
        if not returns_30d and not returns_90d:
            return {"sector": sector_name, "strength": 0.0, "count": len(sector_candidates)}
        
        avg_ret_30d = np.mean(returns_30d) if returns_30d else 0.0
        avg_ret_90d = np.mean(returns_90d) if returns_90d else 0.0
        
        # 가중 평균
        strength = (
            avg_ret_30d * self.weight_30d +
            avg_ret_90d * self.weight_90d
        )
        
        return {
            "sector": sector_name,
            "return_30d": avg_ret_30d,
            "return_90d": avg_ret_90d,
            "strength": strength,
            "count": len(sector_candidates),
        }
    
    def rank_sectors(
        self,
        sector_groups: Dict[str, List[Dict]],
    ) -> List[Tuple[str, float]]:
        """
        모든 섹터를 강도로 랭킹
        
        Args:
            sector_groups: {sector_name: [candidates]}
        
        Returns:
            List of (sector_name, strength) tuples, sorted by strength descending
        """
        ranked = []
        
        for sector_name, candidates in sector_groups.items():
            metrics = self.calculate_sector_strength(sector_name, candidates)
            ranked.append((sector_name, metrics["strength"]))
        
        # 강도로 정렬 (높을수록 앞)
        ranked = sorted(ranked, key=lambda x: x[1], reverse=True)
        
        return ranked
    
    def filter_candidates_by_sector_strength(
        self,
        candidates: List[Dict],
        sector_groups: Dict[str, List[Dict]],
        max_sectors: int = 5,
        min_strength_threshold: float = 0.0,
    ) -> List[Dict]:
        """
        상위 섹터 내 종목만 필터링
        
        Args:
            candidates: 모든 후보 종목
            sector_groups: {sector_name: [candidates]}
            max_sectors: 상위 N개 섹터만 선택
            min_strength_threshold: 최소 섹터 강도 임계값
        
        Returns:
            Filtered candidates (상위 섹터만)
        """
        # 섹터 랭킹
        ranked_sectors = self.rank_sectors(sector_groups)
        
        # 상위 섹터 선택
        top_sectors = []
        for sector_name, strength in ranked_sectors[:max_sectors]:
            if strength >= min_strength_threshold:
                top_sectors.append(sector_name)
        
        # 해당 섹터의 종목만 필터링
        filtered = [
            c for c in candidates
            if c.get("sector") in top_sectors
        ]
        
        logger.info(
            f"[SECTOR_ROTATION] Selected top {len(top_sectors)} sectors: "
            f"{top_sectors}. Filtered candidates: {len(filtered)}"
        )
        
        return filtered
    
    def get_sector_exposure(
        self,
        selected_candidates: List[Dict],
    ) -> Dict[str, float]:
        """
        선택된 종목들의 섹터별 노출도 계산
        
        Args:
            selected_candidates: 최종 선택 종목들
        
        Returns:
            {sector_name: exposure_pct} (0~1)
        """
        if not selected_candidates:
            return {}
        
        sector_count = {}
        for candidate in selected_candidates:
            sector = candidate.get("sector", "unknown")
            sector_count[sector] = sector_count.get(sector, 0) + 1
        
        total = len(selected_candidates)
        exposure = {
            sector: count / total
            for sector, count in sector_count.items()
        }
        
        return exposure
