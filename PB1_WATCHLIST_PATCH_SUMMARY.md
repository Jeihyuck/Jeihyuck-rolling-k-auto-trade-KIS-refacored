# PB1 Watchlist Cache 패치 요약

## 목적
매 tick마다 유니버스 전체(195개)를 OHLCV/Minervini/VCP 계산하는 대신, **하루 1회 Watchlist 캐시**를 사용하여 성능을 대폭 개선.

## 주요 변경사항

### 1. DB 스키마 추가 (trader/db/schema.py)
- `pb1_watchlist` 테이블 추가
  - 컬럼: env, strategy, as_of (DATE), code, rank, score, meta (JSONB), created_at
  - PK: (env, strategy, as_of, code)
  - Index: (env, strategy, as_of)

### 2. DB Repository 확장 (trader/db/repos.py)
- `WatchlistRepo` 클래스 추가
  - `save_watchlist()`: Watchlist를 DB에 upsert
  - `load_watchlist()`: 특정 날짜의 watchlist 조회
  - `get_latest_watchlist_date()`: 최근 watchlist 날짜 반환
- Standalone 함수: `save_watchlist()`, `load_watchlist()`

### 3. Watchlist Builder 로직 (trader/watchlist_builder.py) - 새 파일
- **Stage A (초경량)**: 유니버스 195 → topK (기본 50)
  - 최근 20거래일 평균 거래대금 계산 (close × volume)
  - OHLCV rows 부족(30일 미만) 제외
  - 가격 필터: 최근 종가 2000원 미만 제외
- **Stage B (전략 스코어링)**: topK → finalN (기본 30)
  - Minervini RS percentile, VCP score 통과
  - 상위 N 선정
- Fallback 전략:
  1. 오늘 watchlist 없음 → 전날 watchlist 사용
  2. 전날도 없음 → Stage A만으로 단순 watchlist 생성 (무거운 계산 없이)

### 4. PB1 Engine 통합 (trader/pb1_engine.py)
- `_load_today_watchlist_members()` 메서드 추가
  - `PB1_WATCHLIST_ENABLED=1`이면 watchlist 로드
  - disabled면 기존 로직(전체 유니버스) 사용
- `run()` 메서드 수정
  - entry/prep phase에서 members를 watchlist로 대체
  - 로그: `[PB1][WATCHLIST] size=N source=today/prevday/fallback as_of=YYYY-MM-DD`

### 5. CLI 도구 (trader/tools/build_watchlist.py) - 새 파일
- 사용법:
  ```bash
  python -m trader.tools.build_watchlist --env live --strategy best_k_meta --as-of 2026-01-30 --force
  ```
- 기능:
  - Watchlist 생성/저장
  - 상위 10개 출력

### 6. Config 환경변수 추가 (trader/config.py)
```python
PB1_WATCHLIST_ENABLED = 1           # Watchlist 사용 여부
PB1_WATCHLIST_TOPK = 50             # Stage A 통과 수
PB1_WATCHLIST_FINALN = 30           # Stage B 최종 선정 수
PB1_WATCHLIST_MIN_PRICE = 2000      # 최소 가격 (원)
PB1_WATCHLIST_LIQ_DAYS = 20         # 거래대금 계산 기간 (일)
PB1_WATCHLIST_MIN_ROWS = 30         # 최소 OHLCV rows
PB1_WATCHLIST_FORCE_REBUILD = 0     # 강제 재생성 (당일에도)
```

### 7. 마이그레이션 (migrations/0020_pb1_watchlist.sql) - 새 파일
- `pb1_watchlist` 테이블 생성 DDL
- Index 생성
- Comment 추가

### 8. GitHub Actions 환경변수 (.github/workflows/trade-runner.yml)
- `trade_tick` job에 PB1_WATCHLIST_* 환경변수 추가

## 성능 개선 효과

### Before (매 tick)
- 유니버스 195개 × 200일 OHLCV 로딩
- 195개 × Minervini/VCP 계산
- **예상 시간**: 3~5분 (네트워크 지연 포함)

### After (watchlist 사용)
- 하루 1회만 Stage A + B 계산
- tick마다 watchlist 30개만 대상으로 후보 계산
- **예상 시간**: 10~20초 (85~90% 단축)

## 예상 로그 예시 (10줄)

```
[WATCHLIST][BUILD][START] as_of=2026-01-30 members=195 topk=50 finaln=30
[WATCHLIST][STAGE_A] kept=50 excluded_rows=82 excluded_price=38 excluded_nan=25
[WATCHLIST][STAGE_B] kept=30 (from 50 candidates)
[WATCHLIST][BUILD][DONE] as_of=2026-01-30 final_members=30
[WATCHLIST][SAVE] env=live strategy=best_k_meta as_of=2026-01-30 members=30
[PB1][WATCHLIST] enabled -> load today watchlist
[WATCHLIST][CACHE] hit=True source=today as_of=2026-01-30
[PB1][WATCHLIST] size=30 source=today as_of=2026-01-30
[ENTRY][CANDIDATES] trace=abc123 start universe=30 data_ok=28 dt_minervini=2.15
[PB1][WATCHLIST] loaded=30 source=today
```

## Rollback Plan
Watchlist를 비활성화하려면:
```bash
export PB1_WATCHLIST_ENABLED=0
```
기존 로직(전체 유니버스)으로 즉시 복귀.

## 검증 방법
1. 마이그레이션 실행:
   ```bash
   python -m trader.db.migrate
   ```
2. Watchlist 생성 테스트:
   ```bash
   python -m trader.tools.build_watchlist --env live --strategy best_k_meta --as-of $(date +%F) --force
   ```
3. PB1 실행 로그 확인:
   - `[PB1][WATCHLIST]` 로그가 출력되는지 확인
   - `size=30` 정도로 candidates가 줄었는지 확인

## 주의사항
1. **첫 실행 시**: Watchlist가 없으면 fallback으로 전체 유니버스 또는 전날 watchlist 사용
2. **Watchlist 생성 시간**: 장 시작 전(08:45~09:00 KST) 또는 수동 빌드 권장
3. **DB 의존성**: PBCORE_DB_URL이 postgres여야 함 (SQLite 불가)

## 파일 변경 목록
- **신규**: trader/watchlist_builder.py
- **신규**: trader/tools/build_watchlist.py
- **신규**: migrations/0020_pb1_watchlist.sql
- **수정**: trader/db/schema.py
- **수정**: trader/db/repos.py
- **수정**: trader/pb1_engine.py
- **수정**: trader/config.py
- **수정**: .github/workflows/trade-runner.yml
