"""Minervini 필터 독립 실행 모듈 - DIAG 모드 전용."""
from __future__ import annotations

import json
import logging
import os
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from sqlalchemy import Engine

from trader.candidate_pool_builder import load_candidate_pool
from trader.config import (
    RS_BENCHMARK,
    RS_COMPOSITE_W1,
    RS_COMPOSITE_W2,
    RS_LOOKBACK_DAYS,
    RS_LOOKBACK2_DAYS,
    VCP_LOOKBACK,
    VCP_MIN_SCORE,
)
from trader.data.ohlcv_provider import ChainOHLCVProvider, KISOHLCVProvider, KRXOHLCVProvider
from trader.db.repos import UniverseRepo, WatchlistRepo
from trader.factors.rs_rank import rank_rs
from trader.kis_wrapper import KisAPI
from trader.minervini.report import run_minervini_report
from trader.runtime_paths import runtime_path
from trader.setups.vcp_pro import VolContractRules, PriceTightRules, score_vcp, is_vcp_ready, detect_vcp
from trader.strategies.pb1_minervini_v2 import MinerviniConfig, compute_features, score_setup
from trader.time_utils import now_kst

logger = logging.getLogger(__name__)

MINERVINI_OHLCV_DAYS_MIN = int(os.getenv("MINERVINI_OHLCV_DAYS", "520"))
MA200_SLOPE_LOOKBACK = int(os.getenv("MA200_SLOPE_LOOKBACK", "20"))


def _minervini_ohlcv_days() -> int:
    base = 200 + MA200_SLOPE_LOOKBACK + 60
    return max(MINERVINI_OHLCV_DAYS_MIN, base)


def run_minervini_for_codes(
    *,
    codes: List[str],
    as_of: date,
    env: str,
    strategy: str,
    engine: Engine,
    ohlcv_provider: Any,
    minervini_config: MinerviniConfig,
    report_dir: str | Path | None = None,
) -> Dict[str, Any]:
    """
    종목 리스트에 대해 Minervini 필터 적용 및 리포트 생성.
    
    Args:
        codes: 종목코드 리스트
        as_of: 기준일
        env: 환경 (live/paper)
        strategy: 전략 키
        engine: DB 엔진
        ohlcv_provider: OHLCV 데이터 제공 함수
        minervini_config: Minervini 설정
        report_dir: 리포트 저장 디렉토리 (None이면 기본 경로 사용)
    
    Returns:
        리포트 딕셔너리 {
            "input_count": 종목 수,
            "passed": 통과한 종목 리스트,
            "rejected": 탈락한 종목 리스트,
            "report_path": 저장된 리포트 파일 경로,
        }
    """
    logger.info(
        "[MINERVINI_RUNNER][START] as_of=%s env=%s codes=%s",
        as_of, env, len(codes)
    )
    
    # 유니버스 로드 (종목명 매핑용)
    universe_repo = UniverseRepo(engine)
    universe_snapshot = universe_repo.get_current_universe_snapshot(env, strategy)
    
    name_map = {}
    if universe_snapshot and universe_snapshot.get("members"):
        for m in universe_snapshot["members"]:
            code = str(m.get("code") or "").zfill(6)
            name = (m.get("meta_json") or {}).get("name") or m.get("name")
            if name:
                name_map[code] = name
    
    # OHLCV 데이터 로드 및 RS/VCP 계산
    candidates = []
    rs_prices: Dict[str, pd.Series] = {}
    
    # 벤치마크 데이터 로드
    bench_df, _ = ohlcv_provider(RS_BENCHMARK, count=max(RS_LOOKBACK_DAYS, RS_LOOKBACK2_DAYS))
    bench_close = bench_df["close"] if not bench_df.empty else pd.Series(dtype=float)
    
    if bench_df.empty or len(bench_df) < max(RS_LOOKBACK_DAYS, RS_LOOKBACK2_DAYS):
        logger.error(
            "[MINERVINI_RUNNER][BENCHMARK_FAIL] rows=%s min_required=%s",
            len(bench_df), max(RS_LOOKBACK_DAYS, RS_LOOKBACK2_DAYS)
        )
        # 벤치마크 데이터 부족 시 빈 결과 반환
        report_result = {
            "input_count": len(codes),
            "passed": [],
            "rejected": [],
            "report_path": None,
            "error": "benchmark_data_insufficient",
        }
        return report_result
    
    need_days = _minervini_ohlcv_days()
    logger.info(
        "[MINERVINI][OHLCV_DAYS] need_days=%s (env MINERVINI_OHLCV_DAYS=%s, slope_lb=%s)",
        need_days,
        os.getenv("MINERVINI_OHLCV_DAYS"),
        MA200_SLOPE_LOOKBACK,
    )

    for code in codes:
        try:
            df, meta = ohlcv_provider(code, count=need_days)
            
            if df is None or df.empty or len(df) < 120:
                candidates.append({
                    "code": code,
                    "setup_ok": False,
                    "reasons": ["insufficient_data"],
                    "score": 0.0,
                })
                continue
            
            # Features 계산
            features = compute_features(df)
            
            # VCP 점수 계산
            vcp_score = score_vcp(
                df,
                VCP_LOOKBACK,
                VolContractRules(),
                PriceTightRules(),
            )
            
            vcp_info = detect_vcp(df, minervini_config)
            
            features["vcp_score"] = float(vcp_score)
            features["vcp_ok"] = bool(vcp_info.get("vcp_ok") or is_vcp_ready(vcp_score, VCP_MIN_SCORE))
            features["vcp_contractions"] = vcp_info.get("contractions")
            
            # RS 계산용 가격 시계열 저장
            rs_prices[code] = df["close"].reset_index(drop=True)
            
            candidates.append({
                "code": code,
                "features": features,
                "setup_ok": False,  # RS 계산 후 업데이트
                "reasons": [],
                "score": 0.0,
            })
            
        except Exception as exc:
            logger.warning("[MINERVINI_RUNNER][OHLCV_FAIL] code=%s err=%s", code, exc)
            candidates.append({
                "code": code,
                "setup_ok": False,
                "reasons": ["data_error"],
                "score": 0.0,
            })
            continue
    
    # RS 계산
    rs_rank = rank_rs(
        rs_prices,
        bench_close,
        lookback_days=RS_LOOKBACK_DAYS,
        lookback2_days=RS_LOOKBACK2_DAYS,
        w1=RS_COMPOSITE_W1,
        w2=RS_COMPOSITE_W2,
    )
    rs_map = {row["ticker"]: row for row in rs_rank.to_dict(orient="records")}
    
    # RS 적용 및 스코어 계산
    for candidate in candidates:
        if not isinstance(candidate.get("features"), dict):
            continue
        
        code = candidate["code"]
        rs_row = rs_map.get(code, {})
        rs_p = float(rs_row.get("pctile") or 0.0)
        
        candidate["features"]["rs_percentile"] = rs_p
        candidate["features"]["rs_pctile"] = rs_p * 100.0
        candidate["features"]["rs_comp"] = rs_row.get("composite")
        
        # VCP 정보
        vcp_info = {
            "score": candidate["features"].get("vcp_score"),
            "vcp_ok": candidate["features"].get("vcp_ok"),
            "contractions": candidate["features"].get("vcp_contractions"),
        }
        
        # 스코어 계산
        score = score_setup(
            candidate["features"],
            rs_percentile=rs_p,
            vcp_info=vcp_info,
            cfg=minervini_config,
        )
        
        candidate["score"] = float(score)
        candidate["features"]["score"] = float(score)
        
        # Minervini 필터 통과 여부 결정
        reasons = []
        
        # RS 필터
        if rs_p < minervini_config.rs_min_percentile:
            reasons.append("rs_below_min")
        
        # VCP 필터
        if not candidate["features"].get("vcp_ok"):
            reasons.append("vcp_fail")
        
        # 트렌드 템플릿 체크 (간단히)
        ma50 = candidate["features"].get("ma50")
        ma150 = candidate["features"].get("ma150")
        ma200 = candidate["features"].get("ma200")
        close = candidate["features"].get("close")
        
        if not (close and ma50 and ma150 and ma200):
            reasons.append("missing_ma")
        elif not (close > ma50 > ma150 > ma200):
            reasons.append("trend_template_fail")
        
        # 유동성 체크
        dollar_vol_50 = candidate["features"].get("dollar_vol_50")
        if not dollar_vol_50 or dollar_vol_50 < minervini_config.min_dollar_vol_50d:
            reasons.append("illiquid")
        
        if reasons:
            candidate["setup_ok"] = False
            candidate["reasons"] = reasons
        else:
            candidate["setup_ok"] = True
            candidate["reasons"] = []
    
    # 통과/탈락 분류
    passed = [c for c in candidates if c.get("setup_ok")]
    rejected = [c for c in candidates if not c.get("setup_ok")]
    
    logger.info(
        "[MINERVINI_RUNNER][RESULT] input=%s passed=%s rejected=%s",
        len(codes), len(passed), len(rejected)
    )
    
    # 리포트 생성 (기존 run_minervini_report 재사용)
    universe_members = [
        {"code": code, "meta_json": {"name": name_map.get(code)}}
        for code in codes
    ]
    
    report_path = run_minervini_report(
        universe_members=universe_members,
        cfg=minervini_config,
        as_of=as_of.isoformat(),
        candidates=candidates,
        report_dir=report_dir,
    )
    
    logger.info(
        "[MINERVINI_RUNNER][REPORT] path=%s passed=%s rejected=%s",
        report_path, len(passed), len(rejected)
    )
    
    return {
        "input_count": len(codes),
        "passed": passed,
        "rejected": rejected,
        "report_path": report_path,
    }


def run_diag_minervini_only(
    *,
    engine: Engine,
    env: str,
    strategy: str,
    as_of: date,
) -> int:
    """
    DIAG 모드 전용: 후보군 기반 Minervini 필터만 실행하고 종료.
    
    Args:
        engine: DB 엔진
        env: 환경 (live/paper)
        strategy: 전략 키 (예: "best_k_meta")
        as_of: 기준일
    
    Returns:
        종료 코드 (0: 성공, 1: 실패)
    """
    logger.info(
        "[DIAG_MINERVINI_ONLY][START] env=%s strategy=%s as_of=%s",
        env, strategy, as_of
    )
    
    # 후보군 로드
    pool_codes, pool_as_of, pool_reason = load_candidate_pool(
        engine=engine,
        env=env,
        today=as_of,
        base_strategy=strategy,
    )
    
    if not pool_codes:
        logger.error(
            "[DIAG_MINERVINI_ONLY][FAIL] no candidate pool reason=%s",
            pool_reason
        )
        return 1
    
    logger.info(
        "[DIAG_MINERVINI_ONLY][POOL] loaded=%s as_of=%s reason=%s",
        len(pool_codes), pool_as_of, pool_reason
    )
    
    # OHLCV Provider 생성
    kis = KisAPI()
    krx_provider = KRXOHLCVProvider()
    kis_provider = KISOHLCVProvider(kis)
    ohlcv_provider = ChainOHLCVProvider([krx_provider, kis_provider])
    
    def _fetch_daily(code: str, count: int = 260):
        """pb1_engine._fetch_daily 호환 래퍼"""
        result = ohlcv_provider.get_ohlcv(code, count)
        if not result or result.df is None:
            return pd.DataFrame(), {"volume_missing": True}
        return result.df, result.meta or {}
    
    # Minervini Config
    minervini_config = MinerviniConfig()
    
    # Minervini 실행
    try:
        result = run_minervini_for_codes(
            codes=pool_codes,
            as_of=as_of,
            env=env,
            strategy=strategy,
            engine=engine,
            ohlcv_provider=_fetch_daily,
            minervini_config=minervini_config,
            report_dir=None,  # 기본 경로 사용
        )
    except Exception as exc:
        logger.exception("[DIAG_MINERVINI_ONLY][ERROR] %s", exc)
        return 1
    
    # top_candidates.json 저장
    top_candidates_path = runtime_path("top_candidates.json")
    top_candidates = [
        {
            "code": c["code"],
            "score": c.get("score", 0.0),
            "rs_pctile": c.get("features", {}).get("rs_pctile", 0.0) if isinstance(c.get("features"), dict) else 0.0,
        }
        for c in result.get("passed", [])
    ]
    
    # 점수 내림차순 정렬
    top_candidates.sort(key=lambda x: x.get("score", 0.0), reverse=True)
    
    try:
        top_candidates_path.parent.mkdir(parents=True, exist_ok=True)
        top_candidates_path.write_text(
            json.dumps(top_candidates, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
        logger.info(
            "[DIAG_MINERVINI_ONLY][TOP_CANDIDATES] path=%s count=%s",
            top_candidates_path, len(top_candidates)
        )
    except Exception as exc:
        logger.warning("[DIAG_MINERVINI_ONLY][TOP_SAVE_FAIL] %s", exc)
    
    logger.info(
        "[DIAG_MINERVINI_ONLY][SUCCESS] input=%s passed=%s rejected=%s report=%s",
        result["input_count"],
        len(result["passed"]),
        len(result["rejected"]),
        result["report_path"],
    )
    
    return 0
