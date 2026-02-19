"""Flow Score Calculator - 외국인/기관 수급 점수 계산.

20일 누적 순매수 비율을 기반으로 수급 강도 점수를 생성합니다.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def calculate_flow_score(
    *,
    code: str,
    ohlcv_df: pd.DataFrame,
    foreign_df: Optional[pd.DataFrame] = None,
    inst_df: Optional[pd.DataFrame] = None,
    window: int = 20,
) -> Dict[str, Any]:
    """
    Flow (수급) 점수 계산.
    
    Args:
        code: 종목코드
        ohlcv_df: OHLCV 데이터프레임 (close, volume 컬럼 필요)
        foreign_df: 외국인 순매수 데이터프레임 (date, net_buy 컬럼)
        inst_df: 기관 순매수 데이터프레임 (date, net_buy 컬럼)
        window: 수급 계산 기간 (기본 20일)
    
    Returns:
        {
            "flow_score": float,  # 복합 flow 점수 (0~1)
            "foreign_20_ratio": float,  # 외국인 20일 순매수/거래량 비율
            "inst_20_ratio": float,  # 기관 20일 순매수/거래량 비율
            "dollar_vol_rank": int,  # 거래대금 순위 (높을수록 유동성 좋음)
        }
    """
    result = {
        "flow_score": 0.0,
        "foreign_20_ratio": 0.0,
        "inst_20_ratio": 0.0,
        "dollar_vol_rank": 0,
    }
    
    if ohlcv_df is None or len(ohlcv_df) < 1:
        logger.debug("[FLOW_SCORE][SKIP] code=%s: insufficient OHLCV data", code)
        return result
    
    # 거래대금 계산
    recent = ohlcv_df.tail(window)
    avg_dollar_vol = (recent["close"] * recent["volume"]).mean()
    
    # 외국인 수급
    foreign_ratio = 0.0
    if foreign_df is not None and len(foreign_df) >= 1:
        foreign_recent = foreign_df.tail(max(1, min(window, len(foreign_df))))
        foreign_net = foreign_recent["net_buy"].sum()
        total_vol = recent["volume"].sum()
        if total_vol > 0:
            foreign_ratio = foreign_net / total_vol
    
    # 기관 수급
    inst_ratio = 0.0
    if inst_df is not None and len(inst_df) >= 1:
        inst_recent = inst_df.tail(max(1, min(window, len(inst_df))))
        inst_net = inst_recent["net_buy"].sum()
        total_vol = recent["volume"].sum()
        if total_vol > 0:
            inst_ratio = inst_net / total_vol
    
    # Flow 점수 계산 (외국인 60% + 기관 40%)
    # 정규화: -0.1 ~ +0.1 범위를 0 ~ 1로 변환
    foreign_normalized = max(0, min(1, (foreign_ratio + 0.1) / 0.2))
    inst_normalized = max(0, min(1, (inst_ratio + 0.1) / 0.2))
    
    flow_score = foreign_normalized * 0.6 + inst_normalized * 0.4
    
    result["flow_score"] = flow_score
    result["foreign_20_ratio"] = foreign_ratio
    result["inst_20_ratio"] = inst_ratio
    result["dollar_vol_rank"] = 0  # 랭킹은 전체 종목 스캔 후 계산
    
    logger.debug(
        "[FLOW_SCORE] code=%s flow=%.3f foreign=%.4f inst=%.4f",
        code, flow_score, foreign_ratio, inst_ratio
    )
    
    return result


def rank_by_dollar_volume(
    scored_list: List[Dict[str, Any]],
    ohlcv_provider: Any,
    window: int = 20,
) -> List[Dict[str, Any]]:
    """
    거래대금 순위 계산 및 추가.
    
    Args:
        scored_list: 종목 리스트 (각 항목에 code 포함)
        ohlcv_provider: OHLCV 데이터 제공자
        window: 거래대금 계산 기간
    
    Returns:
        dollar_vol_rank가 추가된 종목 리스트
    """
    # 거래대금 계산
    dollar_vols = []
    for item in scored_list:
        code = item["code"]
        try:
            df = ohlcv_provider(code, days=window + 10)
            if df is not None and len(df) >= window:
                recent = df.tail(window)
                avg_dollar_vol = (recent["close"] * recent["volume"]).mean()
                dollar_vols.append((code, avg_dollar_vol))
            else:
                dollar_vols.append((code, 0))
        except Exception as exc:
            logger.debug("[DOLLAR_VOL][FAIL] code=%s err=%s", code, exc)
            dollar_vols.append((code, 0))
    
    # 순위 계산 (내림차순)
    dollar_vols_sorted = sorted(dollar_vols, key=lambda x: x[1], reverse=True)
    rank_map = {code: rank + 1 for rank, (code, _) in enumerate(dollar_vols_sorted)}
    
    # 결과에 랭킹 추가
    for item in scored_list:
        item["dollar_vol_rank"] = rank_map.get(item["code"], 0)
    
    return scored_list


def calculate_final_score(
    *,
    tech_score: float,
    flow_score: float,
    tech_weight: float = 0.7,
    flow_weight: float = 0.3,
) -> float:
    """
    최종 점수 계산 (기술적 점수 + 수급 점수).
    
    Args:
        tech_score: 기술적 점수
        flow_score: 수급 점수
        tech_weight: 기술적 점수 가중치
        flow_weight: 수급 점수 가중치
    
    Returns:
        최종 점수
    """
    return tech_score * tech_weight + flow_score * flow_weight
