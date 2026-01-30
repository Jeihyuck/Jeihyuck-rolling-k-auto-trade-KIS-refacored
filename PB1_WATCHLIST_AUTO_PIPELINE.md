# PB1 Watchlist 완전 자동 파이프라인 구현 완료

## 목표 달성

✅ **pb1_watchlist.as_of 타입 불일치 에러 100% 제거**
✅ **오늘 watchlist 없으면 자동 생성→저장→재조회**
✅ **GitHub Actions 완전 자동화 (Universe → Watchlist → Trade)**

---

## 1. DB 스키마/타입 정합성 강제 (as_of는 DATE)

### 1-1. 마이그레이션 추가

**파일**: `migrations/0021_fix_pb1_watchlist_as_of_date.sql`

```sql
-- Ensure pb1_watchlist.as_of is DATE type
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name='pb1_watchlist'
      AND column_name='as_of'
      AND data_type <> 'date'
  ) THEN
    ALTER TABLE pb1_watchlist
      ALTER COLUMN as_of TYPE date
      USING (as_of::date);
    
    RAISE NOTICE 'pb1_watchlist.as_of converted to DATE type';
  ELSE
    RAISE NOTICE 'pb1_watchlist.as_of already DATE type or column does not exist';
  END IF;
END $$;

-- Optional: Add index for faster lookups
CREATE INDEX IF NOT EXISTS idx_pb1_watchlist_env_strategy_asof_rank
ON pb1_watchlist (env, strategy, as_of, rank);
```

**파일**: `migrations/0022_pb1_watchlist_unique.sql`

```sql
-- Create unique index for UPSERT operations
CREATE UNIQUE INDEX IF NOT EXISTS uq_pb1_watchlist_key
ON pb1_watchlist (env, strategy, as_of, code);
```

### 실행 방법

```bash
python -m trader.db.migrate
```

---

## 2. Python에서 as_of를 "무조건 date"로 처리

### 2-1. 공통 유틸 추가

**파일**: `trader/time_coerce.py`

```python
from datetime import date, datetime
from typing import Any

def to_date(x: Any) -> date:
    """
    Coerce x into datetime.date.
    Accepts date, datetime, 'YYYY-MM-DD' string.
    """
    if x is None:
        raise ValueError("to_date(None) is not allowed")
    if isinstance(x, date) and not isinstance(x, datetime):
        return x
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, str):
        return datetime.strptime(x[:10], "%Y-%m-%d").date()
    raise TypeError(f"Unsupported date type: {type(x)} value={x!r}")
```

### 2-2. repos.py 수정

**변경 사항**:
- `from trader.time_coerce import to_date` 추가
- `save_watchlist()`: `as_of = to_date(as_of)` 추가 (메서드 및 standalone 함수 모두)
- `load_watchlist()`: `as_of = to_date(as_of)` 추가 (메서드 및 standalone 함수 모두)

**효과**: SQL 로그에서 더 이상 `as_of = $3::VARCHAR` 같은 타입 캐스팅이 나타나지 않음.

### 2-3. watchlist_builder.py 수정

**변경 사항**:
- `from trader.time_coerce import to_date` 추가
- `load_today_watchlist_with_fallback()`: `today = to_date(today)` 추가

---

## 3. "오늘 watchlist 없으면 자동 생성" 파이프라인

### 3-1. watchlist_builder.py: 자동 생성 로직 추가

**함수**: `load_today_watchlist_with_fallback()`

**새로운 파라미터**:
- `auto_build_if_empty: bool = True`: 자동 생성 활성화
- `strict_fail_on_empty: bool = False`: 빈 watchlist 시 실패 여부

**동작 흐름**:

```
1. DB에서 오늘 watchlist 조회
   ↓ 있으면 → 반환 (source="watchlist_db")
   ↓ 없으면
2. auto_build_if_empty=True 이면
   - WatchlistBuilder로 생성
   - DB에 저장
   - 재조회 → 반환 (source="watchlist_autobuilt")
   ↓ 실패하면
3. 전날 watchlist 조회 → 반환 (source="prevday")
   ↓ 없으면
4. Fallback: 유동성 topK → 반환 (source="fallback")
   ↓ 실패하면
5. strict_fail_on_empty=True 이면 예외 발생
   아니면 빈 리스트 반환 (source="watchlist_empty")
```

**환경변수 설정**:

```bash
export PB1_WATCHLIST_ENABLED=1
export PB1_WATCHLIST_AUTOBUILD=1
export PB1_WATCHLIST_TOPK=20
export PB1_WATCHLIST_FINALN=10
export PB1_WATCHLIST_STRICT=0  # 실패 시 fallback 허용
```

---

## 4. GitHub Actions 완전 자동 파이프라인

### 4-1. 스케줄 간소화

**변경 전** (5분마다):
```yaml
schedule:
  - cron: "0,30 0-5 * * 1-5"  # KST 09:00~14:30 30분 간격
```

**변경 후** (하루 1회):
```yaml
schedule:
  - cron: "45 23 * * 0-4"  # KST 08:45 universe build
  - cron: "5 0 * * 1-5"    # KST 09:05 daily start
  - cron: "29 6 * * 1-5"   # KST 15:29 close-cancel
```

**설명**: 
- 스케줄은 단순화 (하루 1회 시작)
- 실제 거래 루프는 PB1_RUN_LOOP_MINUTES=15로 내부에서 반복
- 시장 시간(09:00~15:15 KST)만 실거래 실행 (안전장치 유지)

### 4-2. trade_tick 작업에 단계 추가

**새로운 단계**:

```yaml
- name: DB migrate
  run: |
    set -euo pipefail
    python -m trader.db.migrate

- name: DB universe precheck
  run: |
    set -euo pipefail
    python -m trader.universe.db_check --env "${KIS_ENV}" --strategy best_k_meta --mode "${EFFECTIVE_STRATEGY_MODE:-LIVE}"

- name: Ensure watchlist (auto build if empty)
  env:
    PB1_WATCHLIST_AUTOBUILD: "1"
  run: |
    set -euo pipefail
    echo "[WATCHLIST] Auto-build enabled: PB1_WATCHLIST_AUTOBUILD=${PB1_WATCHLIST_AUTOBUILD}"
    echo "[WATCHLIST] Will auto-create today's watchlist if missing"
```

**환경변수 업데이트**:

```yaml
env:
  PB1_WATCHLIST_ENABLED: "1"
  PB1_WATCHLIST_AUTOBUILD: "1"  # 자동 생성 활성화
  PB1_WATCHLIST_TOPK: "20"      # 축소 (실제 매매용)
  PB1_WATCHLIST_FINALN: "10"    # 축소 (실제 매매용)
  PB1_WATCHLIST_STRICT: "0"     # fallback 허용
  PB1_MIN_CANDIDATES: "1"       # 최소 후보 1개
  PB1_TARGET_NEW_POSITIONS: "2" # 신규 포지션 제한
```

---

## 5. 실전용 설정 권장값

### 매매가 일어나도록 강제 축소

```bash
# Watchlist
export PB1_WATCHLIST_ENABLED=1
export PB1_WATCHLIST_AUTOBUILD=1
export PB1_WATCHLIST_TOPK=20      # 충분히 작게
export PB1_WATCHLIST_FINALN=10    # 충분히 작게
export PB1_WATCHLIST_STRICT=0     # fallback 허용

# Candidates
export PB1_MIN_CANDIDATES=1       # 최소 1개만 있어도 진행

# Positions
export PB1_TARGET_NEW_POSITIONS=2 # 과매수 방지
```

---

## 6. 성공 로그 체크리스트

### 정상 실행 시 나타나는 로그 순서

```
1. [DB][MIGRATE][MIGRATE OK] ...
   → 마이그레이션 성공

2. [WATCHLIST][CACHE] hit=True source=watchlist_db as_of=2026-01-30 count=10
   또는
   [WATCHLIST][AUTO_BUILD] today=2026-01-30 missing -> building now
   [WATCHLIST][AUTO_BUILD] saved count=10 -> reloading
   [WATCHLIST][AUTO_BUILD] success count=10
   → Watchlist 로드/생성 성공

3. [ENTRY] ... candidates=...
   → 후보 선정 성공

4. [ORDER][SUBMIT] / [KIS][ORDER] ...
   → 주문 제출

5. [FILL] ...
   → 체결 확인
```

### 절대 나오면 안 되는 로그

```
❌ operator does not exist: date = character varying
❌ type mismatch: as_of
```

---

## 7. 트러블슈팅

### 문제: watchlist가 자동 생성되지 않음

**확인 사항**:
1. `PB1_WATCHLIST_AUTOBUILD=1` 설정 확인
2. OHLCV 데이터 충분한지 확인 (30일 이상)
3. 로그에서 `[WATCHLIST][AUTO_BUILD][FAIL]` 검색

**해결 방법**:
```bash
# 수동으로 watchlist 생성 테스트
python -c "
from trader.watchlist_builder import WatchlistBuilder
from trader.db.engine import make_engine
from trader.time_utils import now_kst
# ... (생성 코드)
"
```

### 문제: 매매가 일어나지 않음

**확인 사항**:
1. 시장 시간인지 확인 (KST 09:00~15:15, 평일)
2. Watchlist 크기 확인 (너무 크면 후보 선정 실패)
3. `PB1_MIN_CANDIDATES=1` 설정 확인
4. 로그에서 `[ENTRY]` 검색

**해결 방법**:
```bash
# Watchlist 크기 축소
export PB1_WATCHLIST_TOPK=10
export PB1_WATCHLIST_FINALN=5
export PB1_MIN_CANDIDATES=1
```

---

## 8. 요약

### 구현 완료 항목

| 항목 | 상태 | 파일 |
|------|------|------|
| ✅ time_coerce 유틸 | 완료 | `trader/time_coerce.py` |
| ✅ DB 마이그레이션 0021 | 완료 | `migrations/0021_fix_pb1_watchlist_as_of_date.sql` |
| ✅ DB 마이그레이션 0022 | 완료 | `migrations/0022_pb1_watchlist_unique.sql` |
| ✅ repos.py date 처리 | 완료 | `trader/db/repos.py` |
| ✅ watchlist_builder 자동생성 | 완료 | `trader/watchlist_builder.py` |
| ✅ GitHub Actions 파이프라인 | 완료 | `.github/workflows/trade-runner.yml` |

### 핵심 성과

1. **에러 제거**: `operator does not exist: date = character varying` 완전 제거
2. **자동화**: watchlist 없으면 자동 생성 → DB 저장 → 재조회
3. **안정성**: 실패 시 fallback (전날 watchlist → 유동성 topK → 빈 리스트)
4. **실전 준비**: 축소된 watchlist (topk=20, finaln=10)로 실제 매매 가능

### 다음 실행 시

```bash
# 1. DB 마이그레이션 실행
python -m trader.db.migrate

# 2. GitHub Actions에서 자동 실행
# - 08:45 KST: universe build
# - 09:05 KST: watchlist auto-build & trading start
# - 내부 루프로 15분마다 반복 (15:15까지)
```

---

**문서 작성일**: 2026-01-30  
**버전**: 1.0  
**상태**: ✅ 구현 완료
