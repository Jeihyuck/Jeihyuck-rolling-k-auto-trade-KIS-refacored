"""Entry/Exit Decision Snapshot Helpers."""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, Optional

from sqlalchemy import Engine

from trader.db.repos import EntryDecisionRepo, ExitAnalysisRepo
from trader.report.pdf_report import generate_exit_analysis_pdf

logger = logging.getLogger(__name__)


def save_entry_snapshot(
    *,
    engine: Engine,
    run_id: str,
    as_of: date,
    code: str,
    entry_price: float,
    stop_price: float,
    qty: int,
    features: Dict[str, Any],
    reasons: Dict[str, Any],
) -> int:
    """
    매수 결정 스냅샷 저장.
    
    Args:
        engine: DB 엔진
        run_id: 실행 ID
        as_of: 매수일
        code: 종목코드
        entry_price: 매수가
        stop_price: 손절가
        qty: 수량
        features: 기술적 특징 스냅샷 (RS, VCP, Trend, Flow 등)
        reasons: 매수 이유
    
    Returns:
        생성된 스냅샷 ID
    """
    repo = EntryDecisionRepo(engine)
    snapshot_id = repo.save_snapshot(
        run_id=run_id,
        as_of=as_of,
        code=code,
        entry_price=entry_price,
        stop_price=stop_price,
        qty=qty,
        features=features,
        reasons=reasons,
    )
    
    logger.info("[ENTRY_SNAPSHOT] saved code=%s snapshot_id=%s", code, snapshot_id)
    return snapshot_id


def save_exit_analysis_with_pdf(
    *,
    engine: Engine,
    code: str,
    exit_date: date,
    exit_price: float,
    current_features: Dict[str, Any],
    generate_pdf: bool = True,
) -> Optional[int]:
    """
    매도 분석 저장 및 PDF 생성.
    
    Args:
        engine: DB 엔진
        code: 종목코드
        exit_date: 매도일
        exit_price: 매도가
        current_features: 현재 시점 기술적 특징
        generate_pdf: PDF 생성 여부
    
    Returns:
        생성된 분석 ID 또는 None (entry snapshot 없으면 None)
    """
    # Entry snapshot 로드
    entry_repo = EntryDecisionRepo(engine)
    entry_snapshot = entry_repo.load_latest_snapshot(code)
    
    if not entry_snapshot:
        logger.warning("[EXIT_ANALYSIS] no entry snapshot found for code=%s", code)
        return None
    
    # 손익 계산
    entry_price = entry_snapshot["entry_price"]
    qty = entry_snapshot["qty"]
    pnl = (exit_price - entry_price) * qty
    pnl_pct = (exit_price / entry_price - 1) if entry_price > 0 else 0
    hold_days = (exit_date - entry_snapshot["as_of"]).days
    
    # 비교 분석
    entry_features = entry_snapshot.get("features", {})
    comparison = _compute_comparison(entry_features, current_features)
    
    # DB 저장
    exit_repo = ExitAnalysisRepo(engine)
    analysis_id = exit_repo.save_analysis(
        code=code,
        entry_snapshot_id=entry_snapshot["id"],
        exit_date=exit_date,
        exit_price=exit_price,
        pnl=pnl,
        pnl_pct=pnl_pct,
        hold_days=hold_days,
        comparison=comparison,
    )
    
    logger.info("[EXIT_ANALYSIS] saved code=%s analysis_id=%s pnl=%.0f", code, analysis_id, pnl)
    
    # PDF 생성
    if generate_pdf:
        try:
            exit_snapshot = {
                "exit_date": exit_date,
                "exit_price": exit_price,
                "pnl": pnl,
                "pnl_pct": pnl_pct,
                "hold_days": hold_days,
            }
            
            pdf_path = generate_exit_analysis_pdf(
                code=code,
                entry_snapshot=entry_snapshot,
                exit_snapshot=exit_snapshot,
                comparison=comparison,
            )
            logger.info("[EXIT_ANALYSIS][PDF] generated %s", pdf_path)
        except Exception as exc:
            logger.warning("[EXIT_ANALYSIS][PDF] failed: %s", exc, exc_info=True)
    
    return analysis_id


def _compute_comparison(
    entry_features: Dict[str, Any],
    current_features: Dict[str, Any],
) -> Dict[str, Any]:
    """
    매수 당시 vs 현재 특징 비교.
    
    Args:
        entry_features: 매수 당시 특징
        current_features: 현재 특징
    
    Returns:
        비교 결과 딕셔너리
    """
    comparison = {}
    
    # RS 변화
    entry_rs = entry_features.get("rs_percentile", 0)
    current_rs = current_features.get("rs_percentile", 0)
    comparison["rs_change"] = current_rs - entry_rs
    
    # VCP 변화
    entry_vcp = entry_features.get("vcp_score", 0)
    current_vcp = current_features.get("vcp_score", 0)
    comparison["vcp_change"] = current_vcp - entry_vcp
    comparison["vcp_broken"] = entry_vcp > 0 and current_vcp == 0
    
    # Trend 변화
    entry_trend = entry_features.get("trend_ok", False)
    current_trend = current_features.get("trend_ok", False)
    comparison["trend_lost"] = entry_trend and not current_trend
    
    # Flow 변화
    entry_flow = entry_features.get("flow_score", 0)
    current_flow = current_features.get("flow_score", 0)
    comparison["flow_deterioration"] = current_flow - entry_flow
    
    # MA50 위치 변화
    entry_ma50_pos = entry_features.get("ma50_position", "above")
    current_ma50_pos = current_features.get("ma50_position", "above")
    comparison["ma50_cross_down"] = entry_ma50_pos == "above" and current_ma50_pos == "below"
    
    # 거래량 변화
    entry_vol = entry_features.get("avg_volume_20", 0)
    current_vol = current_features.get("avg_volume_20", 0)
    comparison["volume_change_pct"] = ((current_vol / entry_vol - 1) if entry_vol > 0 else 0)
    
    # 수익률
    entry_price = entry_features.get("price", 0)
    current_price = current_features.get("price", 0)
    comparison["profit_pct"] = ((current_price / entry_price - 1) if entry_price > 0 else 0)
    
    return comparison
