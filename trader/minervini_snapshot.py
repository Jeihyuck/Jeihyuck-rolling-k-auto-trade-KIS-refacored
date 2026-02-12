"""Minervini Snapshot Manager - 미너비니 통과 종목 스냅샷 관리."""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

from sqlalchemy import Engine

from trader.db.repos import MinerviniSnapshotRepo
from trader.report.pdf_report import generate_minervini_pdf

logger = logging.getLogger(__name__)


def save_minervini_snapshot(
    *,
    engine: Engine,
    as_of: date,
    passed_list: List[Dict[str, Any]],
    generate_pdf: bool = True,
) -> None:
    """
    미너비니 통과 종목 스냅샷 저장 및 PDF 생성.
    
    Args:
        engine: DB 엔진
        as_of: 기준일
        passed_list: 통과 종목 리스트
        generate_pdf: PDF 생성 여부
    """
    if not passed_list:
        logger.warning("[MINERVINI_SNAPSHOT] passed_list is empty, skipping")
        return
    
    # DB 저장
    repo = MinerviniSnapshotRepo(engine)
    repo.save_snapshot(as_of=as_of, passed_list=passed_list)
    
    logger.info("[MINERVINI_SNAPSHOT][SAVED] as_of=%s count=%s", as_of, len(passed_list))
    
    # PDF 생성
    if generate_pdf:
        try:
            pdf_path = generate_minervini_pdf(passed_list=passed_list, as_of=as_of)
            logger.info("[MINERVINI_SNAPSHOT][PDF] generated %s", pdf_path)
        except Exception as exc:
            logger.warning("[MINERVINI_SNAPSHOT][PDF] failed: %s", exc, exc_info=True)


def extract_minervini_info(
    *,
    code: str,
    name: str,
    minervini_result: Dict[str, Any],
) -> Dict[str, Any]:
    """
    미너비니 필터 결과에서 스냅샷 정보 추출.
    
    Args:
        code: 종목코드
        name: 종목명
        minervini_result: 미너비니 필터 결과 딕셔너리
    
    Returns:
        스냅샷 저장용 딕셔너리
    """
    return {
        "code": code,
        "name": name,
        "rs_percentile": minervini_result.get("rs_percentile", 0),
        "vcp_score": minervini_result.get("vcp_score", 0),
        "trend_ok": minervini_result.get("trend_ok", False),
        "score": minervini_result.get("score", 0),
        "reasons": {
            "rs_high": minervini_result.get("rs_percentile", 0) >= 80,
            "vcp_detected": minervini_result.get("vcp_score", 0) > 0,
            "trend_ok": minervini_result.get("trend_ok", False),
            "volume_contraction": minervini_result.get("volume_contraction", False),
            "base_type": minervini_result.get("base_type", "unknown"),
            "stage": minervini_result.get("stage", "unknown"),
        }
    }
