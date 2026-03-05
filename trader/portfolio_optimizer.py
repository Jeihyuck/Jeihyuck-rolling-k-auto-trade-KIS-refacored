"""
Portfolio Optimizer
포트폴리오 최적화 및 위험 관리

목표:
1. 리스크 최소화
2. 수익 최대화
3. 포지션 집중도 제어
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class PortfolioOptimizer:
    """
    포트폴리오 최적화 엔진
    """
    
    def __init__(
        self,
        max_positions: int = 8,
        max_single_position: float = 0.15,
        max_sector_exposure: float = 0.30,
        correlation_threshold: float = 0.70,
    ):
        """
        Args:
            max_positions: 최대 포지션 수
            max_single_position: 단일 종목 최대 노출 (0~1)
            max_sector_exposure: 섹터 최대 노출 (0~1)
            correlation_threshold: 상관계수 임계값 (0~1)
        """
        self.max_positions = max_positions
        self.max_single_position = max_single_position
        self.max_sector_exposure = max_sector_exposure
        self.correlation_threshold = correlation_threshold
    
    def select_top_signals(
        self,
        signals: List[Dict],
        max_count: int = None,
    ) -> List[Dict]:
        """
        신호 점수 기반 상위 종목 선택
        
        Args:
            signals: 진입 신호 리스트 (각각 confidence, signal_strength 포함)
            max_count: 최대 선택 수 (없으면 max_positions 사용)
        
        Returns:
            Selected top signals
        """
        if max_count is None:
            max_count = self.max_positions
        
        if not signals:
            return []
        
        # 신호 점수 계산
        for sig in signals:
            sig_score = (
                sig.get("confidence", 0.5) * 0.5 +
                sig.get("signal_strength", 0.5) * 0.5
            )
            sig["signal_score"] = sig_score
        
        # 점수로 정렬
        sorted_signals = sorted(
            signals,
            key=lambda x: x.get("signal_score", 0),
            reverse=True,
        )
        
        # 상위 N개 선택
        selected = sorted_signals[:max_count]
        
        logger.info(f"[PORTFOLIO_OPT] Selected {len(selected)} signals from {len(signals)} total")
        
        return selected
    
    def check_sector_concentration(
        self,
        selected_signals: List[Dict],
    ) -> bool:
        """
        섹터 집중도 확인
        
        Args:
            selected_signals: 선택된 신호들 (각각 sector 포함)
        
        Returns:
            True if sector concentration is acceptable
        """
        if not selected_signals:
            return True
        
        sector_count = {}
        for sig in selected_signals:
            sector = sig.get("sector", "unknown")
            sector_count[sector] = sector_count.get(sector, 0) + 1
        
        max_sector_exposure = max(
            count / len(selected_signals)
            for count in sector_count.values()
        )
        
        if max_sector_exposure > self.max_sector_exposure:
            logger.warning(
                f"[PORTFOLIO_OPT WARNING] Sector concentration too high: "
                f"{max_sector_exposure:.1%} > {self.max_sector_exposure:.1%}"
            )
            return False
        
        return True
    
    def filter_high_correlation(
        self,
        selected_signals: List[Dict],
        correlation_matrix: Optional[pd.DataFrame] = None,
    ) -> List[Dict]:
        """
        높은 상관계수 종목 필터링
        
        한 그룹의 상관계수가 높으면, 신호 강도가 가장 높은 것만 유지
        
        Args:
            selected_signals: 선택된 신호들 (각각 symbol 포함)
            correlation_matrix: 종목 간 상관계수 행렬
        
        Returns:
            Filtered signals (중복 제거)
        """
        if correlation_matrix is None or len(selected_signals) <= 1:
            return selected_signals
        
        filtered = []
        processed_symbols = set()
        
        for sig in selected_signals:
            symbol = sig.get("symbol")
            
            if symbol in processed_symbols:
                continue
            
            # 이 신호와 높은 상관계수 종목 찾기
            if symbol in correlation_matrix.columns:
                correlations = correlation_matrix[symbol]
                high_corr_symbols = correlations[
                    correlations >= self.correlation_threshold
                ].index.tolist()
                
                # 이미 선택된 종목들 중 높은 상관계수 종목 확인
                for corr_symbol in high_corr_symbols:
                    if corr_symbol != symbol and corr_symbol in processed_symbols:
                        # 이미 더 좋은 신호가 있으므로 스킵
                        logger.debug(
                            f"[PORTFOLIO_OPT] Skipping {symbol} "
                            f"(correlation {correlations[corr_symbol]:.2f} with {corr_symbol})"
                        )
                        continue
            
            filtered.append(sig)
            processed_symbols.add(symbol)
        
        if len(filtered) < len(selected_signals):
            logger.info(
                f"[PORTFOLIO_OPT] Filtered to {len(filtered)} from {len(selected_signals)} "
                f"(removed high correlations)"
            )
        
        return filtered
    
    def optimize_portfolio(
        self,
        all_signals: List[Dict],
        sector_groups: Optional[Dict] = None,
        correlation_matrix: Optional[pd.DataFrame] = None,
    ) -> List[Dict]:
        """
        전체 포트폴리오 최적화 프로세스
        
        1. 신호 점수로 상위 종목 선택
        2. 섹터 집중도 확인
        3. 높은 상관계수 필터링
        4. 최종 검증
        
        Args:
            all_signals: 모든 진입 신호
            sector_groups: 섹터별 그룹 (선택)
            correlation_matrix: 상관계수 행렬 (선택)
        
        Returns:
            Optimized signals (최종 진입 종목)
        """
        
        # Step 1: 상위 신호 선택
        selected = self.select_top_signals(all_signals)
        
        if not selected:
            logger.warning("[PORTFOLIO_OPT] No signals after selection")
            return []
        
        # Step 2: 섹터 집중도 확인
        sector_ok = self.check_sector_concentration(selected)
        if not sector_ok:
            logger.warning("[PORTFOLIO_OPT] Sector concentration warning (but continuing)")
        
        # Step 3: 상관계수 필터링
        final = self.filter_high_correlation(selected, correlation_matrix)
        
        # Step 4: 최종 검증
        if len(final) > self.max_positions:
            final = final[:self.max_positions]
            logger.info(f"[PORTFOLIO_OPT] Trimmed to {self.max_positions} positions")
        
        logger.info(
            f"[PORTFOLIO_OPT] Final portfolio: {len(final)} positions from {len(all_signals)} signals"
        )
        
        return final
    
    def calculate_position_weights(
        self,
        optimized_signals: List[Dict],
        method: str = "equal",
    ) -> Dict[str, float]:
        """
        각 포지션의 가중치 계산
        
        Args:
            optimized_signals: 최적화된 신호들
            method: 가중치 방식 ("equal", "signal_strength", "risk_parity")
        
        Returns:
            {symbol: weight} (합계 = 1.0)
        """
        if not optimized_signals:
            return {}
        
        weights = {}
        
        if method == "equal":
            # 동일 가중치
            w = 1.0 / len(optimized_signals)
            for sig in optimized_signals:
                weights[sig["symbol"]] = w
        
        elif method == "signal_strength":
            # 신호 강도 기반
            total_strength = sum(
                sig.get("signal_strength", 0.5)
                for sig in optimized_signals
            )
            for sig in optimized_signals:
                strength = sig.get("signal_strength", 0.5)
                weights[sig["symbol"]] = strength / total_strength
        
        elif method == "risk_parity":
            # 위험성 기반 (inverse volatility weighting)
            # 변동성 낮을수록 더 큼
            volatilities = []
            for sig in optimized_signals:
                vol = sig.get("volatility", 0.02)
                volatilities.append(1.0 / vol if vol > 0 else 1.0)
            
            total_inv_vol = sum(volatilities)
            for sig, inv_vol in zip(optimized_signals, volatilities):
                weights[sig["symbol"]] = inv_vol / total_inv_vol
        
        # 최대 포지션 크기 제한
        for symbol in weights:
            weights[symbol] = min(weights[symbol], self.max_single_position)
        
        # 정규화 (합계 = 1.0)
        total = sum(weights.values())
        if total > 0:
            weights = {k: v / total for k, v in weights.items()}
        
        return weights
