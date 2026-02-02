# Trade-Tick 실거래 환경 변수 설정 가이드

이 문서는 **trade-tick** 워크플로우에서 후보군(Candidate Pool)을 DB에서 로드하여 실거래를 수행하기 위한 환경 변수 설정 가이드입니다.

## 📋 필수 환경 변수 체크리스트

### A. 라이브 매매 필수 (Precheck 통과)

```bash
STRATEGY_ENV=live
STRATEGY_MODE=LIVE
LIVE_TRADING_ENABLED=1
DRY_RUN=0
DISABLE_LIVE_TRADING=0
DB_ONLY=0
NONTRADING_SMOKE=0
```

**중요**: `LIVE` 모드에서는 반드시 다음 조건을 만족해야 합니다:
- `DRY_RUN=0` (DRY_RUN이 1이면 LIVE 위반 에러 발생)
- `LIVE_TRADING_ENABLED=1` (이 값이 1이 아니면 LIVE 위반 에러 발생)
- `DISABLE_LIVE_TRADING=0` (안전 스위치)

### B. 후보군 DB 로드 설정 (재빌드 금지)

```bash
# 후보군 기능 활성화
CANDIDATE_POOL_ENABLED=1
CANDIDATE_POOL_STRATEGY_KEY=pb1_candidate_pool
PB1_WATCHLIST_ENABLED=1

# 🚨 중요: 자동 재빌드 및 강제 재생성 금지
PB1_WATCHLIST_AUTOBUILD=0
PB1_WATCHLIST_FORCE_REBUILD=0
CANDIDATE_POOL_FORCE_REBUILD=0
```

**왜 재빌드를 막아야 하나요?**
- 현재 `_fetch_daily(days=...)` 호출 시 버그가 있어 재빌드 시 실패할 수 있습니다
- 이미 DB에 저장된 120개의 후보군을 그대로 사용하면 안전합니다
- 재빌드는 주말에만 수행합니다

### C. as_of 키 고정 (날짜 불일치 방지)

```bash
# 오늘 날짜로 고정 (후보군 생성 시와 동일해야 함)
AS_OF=2026-02-02
# 또는
PB1_AS_OF=2026-02-02
# 또는
UNIVERSE_AS_OF=2026-02-02
```

**중요**: `as_of` 날짜가 후보군 생성 시와 다르면 `members=0`이 발생합니다!

## 🔑 핵심 3종 일치 원칙

trade-tick이 DB에서 후보군을 제대로 로드하려면 다음 3개 값이 **후보군 생성 시와 완전히 일치**해야 합니다:

1. `STRATEGY_ENV` (예: `live`)
2. `CANDIDATE_POOL_STRATEGY_KEY` (예: `pb1_candidate_pool`)
3. `AS_OF` (예: `2026-02-02`)

이 중 하나라도 다르면 DB에서 후보군을 찾지 못하고 `members=0`이 됩니다.

## 🎯 trade-tick 실행 전 검증 스크립트

```bash
# Python 검증 (권장)
python scripts/verify_candidate_pool.py

# 또는 간단한 인라인 검증
python - << 'PY'
from trader.db.repos import WatchlistRepo
from trader.db.engine import get_session
import os, datetime

env = os.getenv("STRATEGY_ENV","live")
strategy = os.getenv("CANDIDATE_POOL_STRATEGY_KEY","pb1_candidate_pool")
as_of = os.getenv("AS_OF") or os.getenv("PB1_AS_OF") or datetime.date.today().isoformat()

with get_session() as s:
    repo = WatchlistRepo(s)
    rows = repo.load_watchlist(env=env, strategy=strategy, as_of=as_of)
    codes = [r["code"] for r in rows]
    print(f"[VERIFY] env={env} strategy={strategy} as_of={as_of} size={len(codes)} sample={codes[:5]}")
    assert len(codes) >= 40, "Candidate pool missing or too small"
print("[VERIFY][OK] candidate pool ready")
PY
```

만약 `size=0`이 나오면:
1. 후보군이 아직 생성되지 않았거나
2. `STRATEGY_ENV`, `CANDIDATE_POOL_STRATEGY_KEY`, `AS_OF` 중 하나가 후보군 생성 시와 다릅니다

## 📊 정상 로그 vs 실패 로그

### ✅ 정상 로그 (OK)
```
[WATCHLIST][LOAD] env=live strategy=pb1_candidate_pool as_of=2026-02-02 members=120
```

### ❌ 실패 로그 (NG)
```
[CANDIDATE_POOL][LOAD] miss
[CANDIDATE_POOL][BUILD][FAIL] rebuild_light_scan
_fetch_daily() got an unexpected keyword argument 'days'
```

## 🔧 문제 해결 (Troubleshooting)

### 문제: `members=0`
**원인**: env/strategy/as_of 불일치
**해결**:
1. 후보군 생성 시 사용한 값 확인
2. trade-tick 환경 변수를 동일하게 설정
3. 검증 스크립트로 확인

### 문제: `_fetch_daily() got unexpected keyword 'days'`
**원인**: 재빌드 시도 시 발생하는 버그
**해결**:
- `PB1_WATCHLIST_AUTOBUILD=0` 설정
- `PB1_WATCHLIST_FORCE_REBUILD=0` 설정
- 이미 수정 완료 (2026-02-02)

### 문제: `LIVE env violations`
**원인**: `DRY_RUN=1` 또는 `LIVE_TRADING_ENABLED != 1`
**해결**:
- `DRY_RUN=0`
- `LIVE_TRADING_ENABLED=1`
- `DISABLE_LIVE_TRADING=0`

## 📝 완벽 설정 예제 (복붙용)

```bash
# === A. 라이브 매매 필수 ===
STRATEGY_ENV=live
STRATEGY_MODE=LIVE
LIVE_TRADING_ENABLED=1
DRY_RUN=0
DISABLE_LIVE_TRADING=0
DB_ONLY=0
NONTRADING_SMOKE=0

# === B. 후보군 DB 로드 (재빌드 금지) ===
CANDIDATE_POOL_ENABLED=1
CANDIDATE_POOL_STRATEGY_KEY=pb1_candidate_pool
PB1_WATCHLIST_ENABLED=1
PB1_WATCHLIST_AUTOBUILD=0
PB1_WATCHLIST_FORCE_REBUILD=0
CANDIDATE_POOL_FORCE_REBUILD=0

# === C. as_of 키 고정 ===
AS_OF=2026-02-02
```

## 🚀 워크플로우 실행 순서

1. **후보군 생성** (주말 또는 사전 준비)
   ```bash
   # MODE=candidate로 실행
   STRATEGY_ENV=live AS_OF=2026-02-02 python -m trader.candidate_pool_builder --build pool
   ```

2. **검증**
   ```bash
   # trade-tick 실행 전에 반드시 검증
   STRATEGY_ENV=live AS_OF=2026-02-02 python scripts/verify_candidate_pool.py
   ```

3. **매매 실행**
   ```bash
   # MODE=trade로 실행 (위 환경 변수 모두 설정)
   # GitHub Actions 또는 로컬에서 trade-tick job 실행
   ```

## ⚠️ 주의사항

1. **재빌드는 주말에만**: 평일 trade-tick에서는 절대 재빌드하지 마세요
2. **3종 일치 필수**: env, strategy, as_of가 후보군 생성 시와 정확히 일치해야 합니다
3. **검증 먼저**: trade-tick 실행 전에 항상 검증 스크립트를 돌리세요
4. **로그 확인**: `members=120` 로그가 나오는지 확인하세요
