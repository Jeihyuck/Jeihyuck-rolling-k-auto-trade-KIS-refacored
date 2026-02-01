"""OHLCV Prefetch - 증분 업데이트 + 병렬화 + 체크포인트."""
from __future__ import annotations

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import Engine

from trader.config import MARKET_MAP
from trader.db.repos import (
    upsert_price_daily,
    get_ohlcv_last_date,
    load_job_checkpoint,
    save_job_checkpoint,
)
from trader.time_utils import now_kst

logger = logging.getLogger(__name__)


def _next_business_day(dt: date) -> date:
    """간단한 영업일 계산 (주말만 건너뜀)."""
    next_day = dt + timedelta(days=1)
    while next_day.weekday() in (5, 6):  # 토요일, 일요일
        next_day += timedelta(days=1)
    return next_day


def _fetch_and_upsert_single(
    *,
    engine: Engine,
    code: str,
    from_date: date,
    to_date: date,
    timeout_sec: int,
    retries: int,
    force_rebuild: bool,
) -> Dict[str, Any]:
    """
    단일 종목에 대해 OHLCV를 fetch하고 DB에 upsert (증분 업데이트 지원).
    
    Returns:
        {"status": "success"|"skip"|"fail", "elapsed": float, "rows": int, "reason": str}
    """
    start_time = time.time()
    
    try:
        import FinanceDataReader as fdr
    except ImportError:
        return {"status": "fail", "elapsed": 0, "rows": 0, "reason": "FDR not available"}
    
    # 증분 업데이트: DB에서 마지막 날짜 조회
    if not force_rebuild:
        last_date = get_ohlcv_last_date(engine, code)
        if last_date:
            # 마지막 날짜 다음 영업일부터 fetch
            from_date = _next_business_day(last_date)
            if from_date > to_date:
                # 이미 최신
                return {"status": "skip", "elapsed": time.time() - start_time, "rows": 0, "reason": "already_latest"}
    
    df = None
    last_error = None
    
    for attempt in range(1, retries + 2):  # +1 for initial attempt
        try:
            # FDR 호출 (timeout은 FDR 자체에서 미지원, 여기서는 재시도만 적용)
            df = fdr.DataReader(code, start=from_date, end=to_date)
            break
        except Exception as exc:
            last_error = exc
            if attempt <= retries:
                time.sleep(0.5)
            else:
                elapsed = time.time() - start_time
                if elapsed > timeout_sec:
                    return {"status": "fail", "elapsed": elapsed, "rows": 0, "reason": "timeout"}
                return {"status": "fail", "elapsed": elapsed, "rows": 0, "reason": str(last_error)[:100]}
    
    if df is None or df.empty:
        return {"status": "skip", "elapsed": time.time() - start_time, "rows": 0, "reason": "empty_df"}
    
    # DataFrame → candles 형식 변환
    df = df.reset_index()
    candles = []
    for _, row in df.iterrows():
        date_val = row.get("Date", row.name)
        if hasattr(date_val, "strftime"):
            date_str = date_val.strftime("%Y-%m-%d")
        else:
            date_str = str(date_val)
        candles.append({
            "date": date_str,
            "open": float(row.get("Open", 0)),
            "high": float(row.get("High", 0)),
            "low": float(row.get("Low", 0)),
            "close": float(row.get("Close", 0)),
            "volume": int(row.get("Volume", 0)),
        })
    
    if not candles:
        return {"status": "skip", "elapsed": time.time() - start_time, "rows": 0, "reason": "no_candles"}
    
    # DB 저장 (UPSERT)
    market = MARKET_MAP.get(code, "KOSPI")
    upsert_price_daily(engine, candles, market, code)
    
    elapsed = time.time() - start_time
    return {"status": "success", "elapsed": elapsed, "rows": len(candles), "reason": "ok"}


def prefetch_ohlcv_to_db(
    *,
    engine: Engine,
    members: List[Dict[str, Any]],
    days: Optional[int] = None,
    force_rebuild: bool = False,
    env: str = "PAPER",
    strategy: str = "pb1_candidate_pool",
    as_of: Optional[date] = None,
) -> None:
    """
    유니버스 멤버들의 OHLCV를 증분 업데이트로 DB에 저장 (병렬화 + 체크포인트).
    
    Args:
        engine: DB 엔진
        members: 유니버스 멤버 리스트
        days: 초기 워밍업 일수 (기본 180일)
        force_rebuild: 강제 전체 재빌드 여부
        env: 환경 (PAPER/LIVE)
        strategy: 전략명
        as_of: 기준일
    """
    try:
        import FinanceDataReader as fdr
    except ImportError:
        logger.warning("[OHLCV][PREFETCH] FinanceDataReader not available, skipping prefetch")
        return
    
    # 환경변수 설정
    if days is None:
        days = int(os.getenv("CANDIDATE_POOL_PREFETCH_DAYS", "180"))
    
    chunk_size = int(os.getenv("CANDIDATE_POOL_PREFETCH_CHUNK", "25"))
    workers = int(os.getenv("OHLCV_PREFETCH_WORKERS", "6"))
    timeout_sec = int(os.getenv("OHLCV_FETCH_TIMEOUT_SEC", "25"))
    retries = int(os.getenv("OHLCV_FETCH_RETRIES", "1"))
    verbose = os.getenv("OHLCV_PREFETCH_VERBOSE", "0") == "1"
    
    codes = [m["code"] for m in members]
    total_codes = len(codes)
    
    if as_of is None:
        as_of = now_kst().date()
    
    end_date = as_of
    start_date = end_date - timedelta(days=days * 2)  # 영업일 감안 2배
    
    # 체크포인트 키 생성
    job_key = f"ohlcv_prefetch:{env}:{strategy}:{as_of}:{days}:{chunk_size}"
    
    # 체크포인트 로드
    checkpoint = load_job_checkpoint(engine, job_key)
    if checkpoint and checkpoint.get("status") == "done" and not force_rebuild:
        logger.info("[OHLCV][PREFETCH][SKIP] job already completed (checkpoint exists)")
        return
    
    resume_from_chunk = 1
    cumulative_success = 0
    cumulative_skip = 0
    cumulative_fail = 0
    fail_codes = []
    
    if checkpoint and not force_rebuild:
        resume_from_chunk = checkpoint.get("next_chunk_index", 1)
        cumulative_success = checkpoint.get("success", 0)
        cumulative_skip = checkpoint.get("skip", 0)
        cumulative_fail = checkpoint.get("fail", 0)
        fail_codes = checkpoint.get("fail_codes", [])
        logger.info(
            "[OHLCV][PREFETCH][RESUME] resuming from chunk=%s success=%s skip=%s fail=%s",
            resume_from_chunk, cumulative_success, cumulative_skip, cumulative_fail
        )
    
    total_chunks = (total_codes + chunk_size - 1) // chunk_size
    
    logger.info(
        "[OHLCV][PREFETCH][START] codes=%s days=%s chunk_size=%s workers=%s timeout=%s retries=%s force=%s",
        total_codes, days, chunk_size, workers, timeout_sec, retries, force_rebuild
    )
    
    overall_start = time.time()
    
    # chunk별로 분할 처리
    for chunk_idx in range((resume_from_chunk - 1) * chunk_size, total_codes, chunk_size):
        chunk_codes = codes[chunk_idx:chunk_idx + chunk_size]
        chunk_num = (chunk_idx // chunk_size) + 1
        
        chunk_start = time.time()
        logger.info(
            "[OHLCV][PREFETCH][CHUNK] chunk=%s/%s codes=%s (indices %s-%s) resume_from=%s",
            chunk_num, total_chunks, len(chunk_codes), chunk_idx, chunk_idx + len(chunk_codes) - 1, resume_from_chunk
        )
        
        success_count = 0
        skip_count = 0
        fail_count = 0
        total_rows = 0
        
        # 병렬 처리
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {}
            for code in chunk_codes:
                future = executor.submit(
                    _fetch_and_upsert_single,
                    engine=engine,
                    code=code,
                    from_date=start_date,
                    to_date=end_date,
                    timeout_sec=timeout_sec,
                    retries=retries,
                    force_rebuild=force_rebuild,
                )
                futures[future] = code
            
            for future in as_completed(futures):
                code = futures[future]
                try:
                    result = future.result()
                    status = result["status"]
                    rows = result["rows"]
                    elapsed = result["elapsed"]
                    reason = result["reason"]
                    
                    if status == "success":
                        success_count += 1
                        total_rows += rows
                        if verbose:
                            logger.debug("[OHLCV][PREFETCH][OK] code=%s rows=%s elapsed=%.1fs", code, rows, elapsed)
                    elif status == "skip":
                        skip_count += 1
                        if verbose:
                            logger.debug("[OHLCV][PREFETCH][SKIP] code=%s reason=%s", code, reason)
                    else:  # fail
                        fail_count += 1
                        if len(fail_codes) < 20:  # 최대 20개만 저장
                            fail_codes.append(code)
                        logger.warning("[OHLCV][PREFETCH][FAIL] code=%s reason=%s elapsed=%.1fs", code, reason, elapsed)
                except Exception as exc:
                    fail_count += 1
                    if len(fail_codes) < 20:
                        fail_codes.append(code)
                    logger.warning("[OHLCV][PREFETCH][FAIL] code=%s exc=%s", code, str(exc)[:100])
        
        chunk_elapsed = time.time() - chunk_start
        cumulative_success += success_count
        cumulative_skip += skip_count
        cumulative_fail += fail_count
        
        logger.info(
            "[OHLCV][PREFETCH][CHUNK_DONE] chunk=%s/%s elapsed=%.1fs success=%s skip=%s fail=%s rows=%s",
            chunk_num, total_chunks, chunk_elapsed, success_count, skip_count, fail_count, total_rows
        )
        
        # 체크포인트 저장
        next_chunk = chunk_num + 1
        checkpoint_payload = {
            "next_chunk_index": next_chunk,
            "total_chunks": total_chunks,
            "success": cumulative_success,
            "skip": cumulative_skip,
            "fail": cumulative_fail,
            "fail_codes": fail_codes[:20],  # 최대 20개
            "elapsed_total_sec": time.time() - overall_start,
            "status": "in_progress" if next_chunk <= total_chunks else "done",
        }
        if next_chunk > total_chunks:
            checkpoint_payload["completed_ts"] = time.time()
        
        save_job_checkpoint(engine, job_key, checkpoint_payload)
    
    overall_elapsed = time.time() - overall_start
    logger.info(
        "[OHLCV][PREFETCH][DONE] elapsed_total=%.1fs success=%s skip=%s fail=%s fail_codes_sample=%s",
        overall_elapsed, cumulative_success, cumulative_skip, cumulative_fail, fail_codes[:5]
    )
