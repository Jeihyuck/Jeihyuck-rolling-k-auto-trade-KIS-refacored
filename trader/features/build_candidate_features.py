#!/usr/bin/env python3
"""
Candidate Features Cache Builder
주말/전일에 후보 종목들의 Minervini/PB1 피처를 미리 계산해서 DB에 캐시로 저장

Usage:
    python -m trader.features.build_candidate_features
"""
import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

# 프로젝트 루트 확인
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from trader.db.repos import WatchlistRepo
from trader.db.engine import get_engine


def build_candidate_features():
    """
    후보 종목들의 피처 캐시 생성
    
    현재는 placeholder - 향후 구현:
    1. candidate_pool 로드
    2. 각 종목별 OHLCV 로드 (260~300일)
    3. Minervini/PB1 피처 계산 (RS, MA200 slope, VCP score, spread, etc.)
    4. DB에 features 저장 (env, strategy_key, as_of, symbol, features_json)
    """
    
    env = os.getenv("STRATEGY_ENV", "practice")
    strategy = os.getenv("CANDIDATE_POOL_STRATEGY_KEY", "pb1_candidate_pool")
    enabled = os.getenv("FEATURE_CACHE_ENABLED", "1") == "1"
    lookback_days = int(os.getenv("FEATURE_CACHE_LOOKBACK_DAYS", "300"))
    
    if not enabled:
        print("[FEATURE_CACHE] Disabled (FEATURE_CACHE_ENABLED=0)")
        return
    
    # KST 기준 오늘
    kst = ZoneInfo("Asia/Seoul")
    as_of = datetime.now(kst).date()
    
    print("=" * 60)
    print("Candidate Features Cache Builder")
    print("=" * 60)
    print(f"Time: {datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"Env: {env}")
    print(f"Strategy: {strategy}")
    print(f"As of: {as_of}")
    print(f"Lookback: {lookback_days} days")
    print("=" * 60)
    print()
    
    # DB 연결
    engine = get_engine()
    repo = WatchlistRepo(engine)
    
    try:
        # 1) 후보 종목 로드
        print("[FEATURE_CACHE][STEP 1] Loading candidate pool...")
        rows, used_as_of = repo.load_watchlist(
            env=env,
            strategy=strategy,
            as_of=as_of,
            allow_latest_fallback=False,
        )
        
        if not rows:
            print(f"[FEATURE_CACHE][WARN] No candidate pool found for as_of={as_of}")
            print("[FEATURE_CACHE][WARN] Run candidate_pool_builder first!")
            print("[FEATURE_CACHE][WARN] Features cache build SKIPPED (non-critical)")
            return
        
        codes = [row["code"] for row in rows]
        print(f"[FEATURE_CACHE][OK] Loaded {len(codes)} candidates from pool (as_of={used_as_of or as_of})")
        print()
        
        # 2) 피처 계산 (TODO: 실제 구현)
        print("[FEATURE_CACHE][STEP 2] Computing features...")
        print("[FEATURE_CACHE][TODO] Feature computation not yet implemented")
        print("[FEATURE_CACHE][TODO] This is a placeholder for future implementation")
        print()
        print("Planned features:")
        print("  - ma50, ma150, ma200")
        print("  - ma200_slope (20-day)")
        print("  - hi_52w, lo_52w")
        print("  - rs_63, rs_126, rs_percentile")
        print("  - vcp_score, spread_score")
        print("  - trend_template_pass")
        print("  - pivot, dollar_vol_50")
        print()
        
        # 3) DB 저장 (TODO: 실제 구현)
        print("[FEATURE_CACHE][STEP 3] Saving to DB...")
        print("[FEATURE_CACHE][TODO] DB save not yet implemented")
        print()
        
        # 임시 성공 로그 (실제 구현 전까지)
        print("[FEATURE_CACHE][STATUS] ⚠️  Placeholder mode")
        print("[FEATURE_CACHE][STATUS] Trade-tick will still use OHLCV-based computation")
        print("[FEATURE_CACHE][STATUS] To enable cache, implement:")
        print("  1. trader/features/compute.py (feature calculation)")
        print("  2. trader/db/repos.py (upsert_candidate_features)")
        print("  3. trader/pb1_engine.py (load_candidate_features)")
        print()
        
    except Exception as e:
        print(f"[FEATURE_CACHE][ERROR] {e}")
        import traceback
        traceback.print_exc()
        print()
        print("[FEATURE_CACHE][WARN] Features cache build FAILED (non-critical)")
        print("[FEATURE_CACHE][WARN] Trade-tick will still work with OHLCV calculation")
        print()
        # 비치명적 오류로 처리 (주말 빌드가 실패하지 않도록)
        return


if __name__ == "__main__":
    build_candidate_features()
