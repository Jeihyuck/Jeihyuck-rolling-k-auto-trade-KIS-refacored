#!/usr/bin/env python3
"""
후보군 DB 검증 스크립트
trade-tick 실행 전에 후보군이 제대로 DB에 저장되어 있는지 확인합니다.
"""
import os
import sys
from datetime import datetime, date
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from trader.db.repos import WatchlistRepo
from trader.db.engine import get_engine


def verify_candidate_pool():
    """후보군 DB 검증"""
    # 환경 변수 읽기
    env = os.getenv("STRATEGY_ENV", "live")
    strategy = os.getenv("CANDIDATE_POOL_STRATEGY_KEY", "pb1_candidate_pool")
    
    # as_of 날짜 결정 (우선순위: AS_OF > PB1_AS_OF > UNIVERSE_AS_OF > today)
    as_of_str = (
        os.getenv("AS_OF") or 
        os.getenv("PB1_AS_OF") or 
        os.getenv("UNIVERSE_AS_OF") or 
        datetime.now().date().isoformat()
    )
    
    if isinstance(as_of_str, str):
        as_of = datetime.strptime(as_of_str, "%Y-%m-%d").date()
    else:
        as_of = as_of_str
    
    print(f"[VERIFY] Checking candidate pool...")
    print(f"[VERIFY] env={env}")
    print(f"[VERIFY] strategy={strategy}")
    print(f"[VERIFY] as_of={as_of}")
    print()
    
    # DB에서 후보군 로드 (Engine 기반)
    engine = get_engine()
    repo = WatchlistRepo(engine)
    
    try:
        # 후보군 로드
        rows = repo.load_watchlist(env=env, strategy=strategy, as_of=as_of)
        
        if not rows:
            print("[VERIFY][FAIL] ❌ Candidate pool is EMPTY!")
            print()
            print("Possible causes:")
            print("  1. Candidate pool was not built yet")
            print("  2. env/strategy/as_of mismatch between build and verify")
            print("  3. Wrong database connection")
            print()
            print("Expected values:")
            print(f"  STRATEGY_ENV={env}")
            print(f"  CANDIDATE_POOL_STRATEGY_KEY={strategy}")
            print(f"  AS_OF={as_of}")
            print()
            sys.exit(1)
        
        codes = [row["code"] for row in rows]
        sample = codes[:5]
        
        print(f"[VERIFY][OK] ✅ Candidate pool found!")
        print(f"[VERIFY][OK] size={len(codes)}")
        print(f"[VERIFY][OK] sample={sample}")
        print()
        
        # 최소 크기 검증
        min_size = int(os.getenv("CANDIDATE_POOL_MIN_SIZE", "40"))
        if len(codes) < min_size:
            print(f"[VERIFY][WARN] ⚠️  Pool size {len(codes)} is below minimum {min_size}")
            print("[VERIFY][WARN] This may cause issues during trading")
            sys.exit(1)
        
        print(f"[VERIFY][OK] Pool size {len(codes)} >= minimum {min_size}")
        print("[VERIFY][OK] ✅ All checks passed!")
        print()
        
        return 0
        
    except Exception as exc:
        print(f"[VERIFY][ERROR] ❌ Exception: {exc}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(verify_candidate_pool())
