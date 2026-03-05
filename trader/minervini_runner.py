# trader/minervini_runner.py
"""DIAG Minervini-only runner - Minervini 필터만 실행"""
from __future__ import annotations

import json
import logging
import os
import runpy
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, List, Optional, Tuple

import pandas as pd
from sqlalchemy import Engine

from trader.candidate_pool_builder import load_candidate_pool
from trader.config import RS_BENCHMARK, RS_LOOKBACK_DAYS, RS_LOOKBACK2_DAYS
from trader.data.ohlcv_provider import ChainOHLCVProvider, KISOHLCVProvider, KRXOHLCVProvider
from trader.db.repos import UniverseRepo, WatchlistRepo
from trader.minervini.report import run_minervini_report
from trader.runtime_paths import runtime_path
from trader.time_utils import now_kst
from trader.universe.build import build_universe
from trader.watchlist_builder import WatchlistBuilder

logger = logging.getLogger(__name__)


def run_diag_minervini_only(
    *,
    engine: Engine,
    env: str,
    strategy: str = "best_k_meta",
    as_of: Optional[date] = None,
) -> int:
    """
    DIAG 모드: 후보군 기반 Minervini 필터 전용 실행.
    
    Args:
        engine: DB 엔진
        env: 거래 환경 (live, paper, practice)
        strategy: 전략 ("best_k_meta" 등)
        as_of: 기준일 (기본값: 어제 거래일)
    
    Returns:
        0 성공, 1 실패
    """
    if as_of is None:
        as_of = now_kst().date()
    
    logger.info("[DIAG_MINERVINI_ONLY] START env=%s strategy=%s as_of=%s", env, strategy, as_of)
    
    try:
        # ========== Step 1: 후보군 로드 ==========
        pool_codes, pool_as_of, pool_reason = load_candidate_pool(
            engine=engine,
            env=env,
            today=as_of,
            base_strategy=strategy,
            force_rebuild=False,  # MINERVINI_ONLY는 재생성 금지
        )
        
        if not pool_codes:
            logger.error(
                "[DIAG_MINERVINI_ONLY][FAIL] no candidate pool reason=%s",
                pool_reason
            )
            return 1
        
        logger.info(
            "[DIAG_MINERVINI_ONLY][POOL] loaded=%s as_of=%s reason=%s",
            len(pool_codes),
            pool_as_of,
            pool_reason
        )
        
        # ========== Step 2: Universe 로드 ==========
        members = build_universe(
            engine=engine,
            env=env,
            strategy_name=strategy,
            as_of=as_of,
        )
        if not members:
            logger.error("[DIAG_MINERVINI_ONLY][FAIL] universe empty")
            return 1
        
        logger.info("[DIAG_MINERVINI_ONLY][UNIVERSE] loaded=%s", len(members))
        
        # ========== Step 3: OHLCV Provider 준비 ==========
        # ChainOHLCVProvider 구성: KIS → KRX 폴백
        ohlcv_providers = [
            KISOHLCVProvider(),
            KRXOHLCVProvider(),
        ]
        chain_provider = ChainOHLCVProvider(*ohlcv_providers)
        
        def _ohlcv_provider(code: str, count: int = 120) -> Tuple[pd.DataFrame | None, dict]:
            """OHLCV 로드 래퍼"""
            try:
                df = chain_provider.get(code, as_of=as_of, count=count)
                return df, {"source": "chain"}
            except Exception as exc:
                logger.warning(
                    "[DIAG_MINERVINI_ONLY][OHLCV_ERROR] code=%s err=%s",
                    code, exc
                )
                return None, {"source": "error"}
        
        # ========== Step 4: Minervini 필터 실행 (Watchlist Builder) ==========
        logger.info(
            "[DIAG_MINERVINI_ONLY][MINERVINI_START] input_pool=%s",
            len(pool_codes)
        )
        
        # minervini_config 구성
        rs_min_pctile = float(os.getenv("RS_MIN_PCTILE", "80")) / 100.0
        vcp_min_score = float(os.getenv("VCP_MIN_SCORE", "70"))
        minervini_config = {
            "rs_min_pctile": rs_min_pctile,
            "vcp_min_score": vcp_min_score,
        }
        
        # flow_provider는 선택사항
        flow_provider = None
        
        # WatchlistBuilder 초기화
        builder = WatchlistBuilder(
            ohlcv_provider=_ohlcv_provider,
            minervini_config=minervini_config,
            pooln=120,
            topk=50,
            finaln=30,
            flow_provider=flow_provider,
        )
        
        # 파이프라인 실행: 120 → 50 → 30
        final30 = builder.build(members=members, as_of=as_of)
        
        logger.info(
            "[DIAG_MINERVINI_ONLY][MINERVINI_DONE] final30=%s",
            len(final30)
        )
        
        # ========== Step 5: 리포트 생성 ==========
        report_date = as_of.isoformat()
        run_minervini_report(
            universe_members=members,
            cfg=None,  # cfg 미사용
            as_of=report_date,
            candidates=final30,  # type: ignore
            report_dir=runtime_path("runtime", "reports", "minervini", report_date),
        )
        logger.info(
            "[DIAG_MINERVINI_ONLY][REPORT_DONE] as_of=%s",
            report_date
        )
        
        # ========== Step 6: top_candidates.json 저장 ==========
        top_candidates_payload = []
        for item in final30:
            code = str(item.get("code", "")).zfill(6)
            score = item.get("final_score") or item.get("score") or 0.0
            top_candidates_payload.append({
                "code": code,
                "score": float(score),
                "name": item.get("name") or item.get("meta", {}).get("name", ""),
            })
        
        # 스코어 기준 내림차순 정렬 및 top 8 선택
        top_candidates_payload.sort(key=lambda x: x["score"], reverse=True)
        top8_candidates = top_candidates_payload[:8]
        
        top_candidates_path = runtime_path("runtime", "top_candidates.json")
        top_candidates_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(top_candidates_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "as_of": report_date,
                    "total_count": len(final30),
                    "buyable_count": len(top8_candidates),
                    "candidates": top8_candidates,
                },
                f,
                indent=2,
                ensure_ascii=False,
            )
        
        logger.info(
            "[DIAG_MINERVINI_ONLY][TOP_CANDIDATES] saved=%s path=%s",
            len(top8_candidates),
            top_candidates_path
        )
        
        # ========== 성공 ==========
        logger.info(
            "[DIAG_MINERVINI_ONLY][SUCCESS] input=%s passed=%s rejected=%s",
            len(pool_codes),
            len(top8_candidates),
            len(pool_codes) - len(top8_candidates)
        )
        return 0
        
    except Exception as exc:
        logger.exception(
            "[DIAG_MINERVINI_ONLY][EXCEPTION] err=%s",
            exc
        )
        return 1


def main():
    """
    Thin wrapper so `python -m trader.minervini_runner` works.

    We reuse the existing prep pipeline by forcing MINERVINI_ONLY=1.
    This keeps all safety/guards/DB logging consistent with production code paths.
    """
    os.environ.setdefault("MINERVINI_ONLY", "1")

    # delegate to prep_runner entrypoint (module execution)
    runpy.run_module("trader.prep_runner", run_name="__main__")


if __name__ == "__main__":
    main()
