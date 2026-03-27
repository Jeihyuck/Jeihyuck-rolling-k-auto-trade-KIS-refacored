#!/usr/bin/env python3
"""
후보군 DB 검증 모듈
trade-tick 실행 전에 후보군이 제대로 DB에 저장되어 있는지 확인합니다.

Usage:
    python -m trader.verify_candidate_pool
"""
import os
import sys
from datetime import datetime
from pathlib import Path

# 프로젝트 루트 확인
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from trader.db.repos import WatchlistRepo
from trader.db.engine import get_engine


def verify_candidate_pool():
    """후보군 DB 검증 (주중 trade-tick 전용)"""
    
    # ✅ R1: 환경 키 강제 고정 확인
    env = os.getenv("STRATEGY_ENV", "practice")
    kis_env = os.getenv("KIS_ENV", "practice")
    strategy = os.getenv("CANDIDATE_POOL_STRATEGY_KEY", "pb1_candidate_pool")
    
    # ✅ 환경 키 일치 확인 (중요!)
    if kis_env != env:
        print(f"[VERIFY][WARN] ⚠️  KIS_ENV={kis_env} != STRATEGY_ENV={env}")
        print(f"[VERIFY][WARN] Candidate pool uses STRATEGY_ENV (not KIS_ENV)")
        print(f"[VERIFY][WARN] This may cause fallback to old pool!")
        print()
    
    # as_of 날짜 결정 (KST 기준 오늘)
    from zoneinfo import ZoneInfo
    kst = ZoneInfo("Asia/Seoul")
    as_of = datetime.now(kst).date()
    
    print("[POOL][PRECHECK][START]")
    print(f"[VERIFY] Checking candidate pool...")
    print(f"[VERIFY] env={env} (STRATEGY_ENV)")
    print(f"[VERIFY] kis_env={kis_env} (KIS_ENV - not used for candidate pool)")
    print(f"[VERIFY] strategy={strategy}")
    print(f"[VERIFY] as_of={as_of}")
    print()
    
    # DB에서 후보군 로드 (최신 fallback 허용)
    engine = get_engine()
    repo = WatchlistRepo(engine)
    
    try:
        ttl_days = int(os.getenv("CANDIDATE_POOL_TTL_DAYS", "7"))
        fail_if_missing = os.getenv("FAIL_IF_POOL_MISSING", "1") == "1"
        
        rows, used_as_of = repo.load_watchlist(
            env=env,
            strategy=strategy,
            as_of=as_of,
            allow_latest_fallback=True,
            ttl_days=ttl_days,
        )
        
        if not rows:
            print("[POOL][PRECHECK][FAIL] reason=empty")
            print("[VERIFY][FAIL] ❌ Candidate pool is EMPTY!")
            print()
            print("Possible causes:")
            print("  1. Weekend build did not run or failed")
            print("  2. env/strategy key mismatch between build and verify")
            print(f"  3. No pool found within TTL (last {ttl_days} days)")
            print()
            print("Expected values:")
            print(f"  STRATEGY_ENV={env}")
            print(f"  CANDIDATE_POOL_STRATEGY_KEY={strategy}")
            print(f"  AS_OF={as_of}")
            print(f"  TTL_DAYS={ttl_days}")
            print()
            print("✅ Solution: Run weekend_build.yml manually to create today's pool")
            print()
            
            if fail_if_missing:
                print("[VERIFY][FAIL] FAIL_IF_POOL_MISSING=1 -> exiting")
                sys.exit(1)
            else:
                print("[VERIFY][WARN] FAIL_IF_POOL_MISSING=0 -> continuing (risky!)")
                return
        
        codes = [row["code"] for row in rows]
        sample = codes[:5]
        
        # 최소 크기 확인
        min_size = int(os.getenv("CANDIDATE_POOL_MIN_SIZE", "40"))
        if len(codes) < min_size:
            print(f"[VERIFY][WARN] ⚠️  Pool size {len(codes)} < minimum {min_size}")
            if fail_if_missing:
                print("[POOL][PRECHECK][FAIL] reason=too_small")
                print("[VERIFY][FAIL] Pool too small -> exiting")
                sys.exit(1)
        
        # 날짜 확인 (fallback 여부)
        if used_as_of and used_as_of != as_of:
            age_days = (as_of - used_as_of).days
            
            if age_days > ttl_days:
                print("[POOL][PRECHECK][FAIL] reason=too_old")
                print(f"[VERIFY][FAIL] ❌ Pool too old!")
                print(f"[VERIFY][FAIL] requested_as_of={as_of}")
                print(f"[VERIFY][FAIL] used_as_of={used_as_of} (age={age_days} days > TTL={ttl_days})")
                print()
                print("✅ Solution: Run weekend_build.yml to create fresh pool")
                print()
                
                if fail_if_missing:
                    sys.exit(1)
                else:
                    print("[VERIFY][WARN] Continuing with old pool (risky!)")
            else:
                print(f"[POOL][PRECHECK][OK] mode=fallback used_as_of={used_as_of} age_days={age_days} size={len(codes)}")
                print(f"[VERIFY][OK] ✅ Candidate pool found (fallback)!")
                print(f"[VERIFY][OK] requested_as_of={as_of}")
                print(f"[VERIFY][OK] used_as_of={used_as_of} (age={age_days} days)")
                print(f"[VERIFY][OK] size={len(codes)}")
                print(f"[VERIFY][OK] sample={sample}")
                print()
        else:
            print(f"[POOL][PRECHECK][OK] mode=exact size={len(codes)} as_of={used_as_of or as_of}")
            print(f"[VERIFY][OK] ✅ Candidate pool found (exact)!")
            print(f"[VERIFY][OK] as_of={used_as_of or as_of}")
            print(f"[VERIFY][OK] size={len(codes)}")
            print(f"[VERIFY][OK] sample={sample}")
            print()
        
        # 최종 확인
        print(f"[VERIFY][OK] Pool size {len(codes)} >= minimum {min_size}")
        print(f"[VERIFY][OK] ✅ All checks passed!")
        print()
        print("[POOL][PRECHECK] ✅ Candidate pool verification passed")
        print("[POOL][PRECHECK] trade-tick will load from DB (no rebuild)")
        print()
        
    except Exception as e:
        print(f"[VERIFY][ERROR] Failed to load candidate pool: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    verify_candidate_pool()
