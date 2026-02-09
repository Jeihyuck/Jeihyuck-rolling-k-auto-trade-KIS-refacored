# 주말 빌드 + 주중 트레이드 완벽 분리 운영 가이드

## ✅ 구현 완료 상태

### 1. 워크플로우 완성 (3개)

#### A) `.github/workflows/weekend_build.yml` ✅
- **목적**: 주말(토요일 00:30 KST)에 다음 주 데이터 준비
- **실행**: 
  - 자동: 매주 토요일 00:30 KST (Fri 15:30 UTC)
  - 수동: Actions → Weekend Build → Run workflow
- **작업**:
  1. Universe build (DB 저장)
  2. OHLCV prefetch 300일 (DB 저장)
  3. Candidate pool build/save (as_of=토요일)
  4. Candidate features cache (placeholder - 향후 구현)

#### B) `.github/workflows/weekday_trade_tick.yml` ✅
- **목적**: 주중(월~금) 장중 trade-tick만 실행
- **실행**:
  - 자동: 월~금 09:00-15:15 KST, 5분마다
  - 수동: Actions → Weekday Trade Tick → Run workflow
- **작업**:
  1. Candidate pool verify (주말 풀 존재 확인)
  2. Trade tick 실행 (LIVE, 후보 재빌드 금지)

#### C) `.github/workflows/trade-or-prefetch.yml` ✅
- **목적**: 수동 테스트/디버깅 전용
- **변경**: schedule 제거 (자동 실행 안 함)
- **용도**: 긴급 패치/수동 테스트만

### 2. 필수 모듈 생성 ✅

#### A) `trader/verify_candidate_pool.py` ✅
- 주중 trade 시작 전 후보 풀 검증
- TTL 확인 (기본 7일)
- 풀 없으면 실패 종료 (FAIL_IF_POOL_MISSING=1)

#### B) `trader/features/` 디렉토리 ✅
- `__init__.py`
- `build_candidate_features.py` (placeholder)
  - 향후 Minervini/PB1 피처 캐시 구현 예정
  - 현재는 비치명적 경고만

#### C) `trader/data/prefetch.py` ✅
- 기존 `trader/ohlcv_prefetch.py` 래퍼
- 주말 빌드에서 OHLCV 데이터 미리 가져오기

## 🚀 오늘 당장 실행하기

### 초기 1회 (최초 세팅)

```bash
# 1) GitHub Actions에서 수동 실행
# Actions → Weekend Build → Run workflow
# 
# 또는 로컬/Codespaces에서:

export STRATEGY_ENV=practice
export KIS_ENV=practice
export FORCE_CANDIDATE=1
export PREFETCH_DAYS=300
export KIS_HTTP_ENABLED=1
export NO_TRADE=1

# DB URL 및 KIS API 키도 설정 필요
export PBCORE_DB_URL="your_db_url"
export KIS_APP_KEY="your_key"
export KIS_APP_SECRET="your_secret"
# ... 기타 secrets

# Universe 생성
python -m trader.universe.build

# OHLCV prefetch
python -m trader.data.prefetch --days 300

# Candidate pool 생성
python -m trader.candidate_pool_builder

# Features cache (optional, 현재 placeholder)
python -m trader.features.build_candidate_features
```

### 검증: 주말 풀이 제대로 저장되었는지 확인

```bash
python -m trader.verify_candidate_pool
```

**기대 출력**:
```
[VERIFY][OK] ✅ Candidate pool found (exact)!
[VERIFY][OK] as_of=2026-02-09
[VERIFY][OK] size=120
[VERIFY][OK] sample=['005930', '000660', ...]
[VERIFY][OK] ✅ All checks passed!
[POOL][PRECHECK] ✅ Candidate pool verification passed
```

### 주중 trade-tick 실행

```bash
export STRATEGY_ENV=practice
export KIS_ENV=practice
export NO_TRADE=0
export FAIL_IF_POOL_MISSING=1
export KIS_HTTP_ENABLED=1

# Trade tick 실행
python -m trader.pb1_runner
```

**기대 출력**:
```
[POOL][PRECHECK] ✅ trade-tick will load from DB (no rebuild)
[WATCHLIST][LOAD] ... as_of=2026-02-09 members=120 (exact)
```

## 🔑 핵심 환경변수 (반드시 통일)

### 공통 (주말/주중 동일)
```bash
STRATEGY_ENV=practice           # 또는 live (절대 혼용 금지!)
KIS_ENV=practice                # STRATEGY_ENV와 동일하게!
CANDIDATE_POOL_STRATEGY_KEY=pb1_candidate_pool
CANDIDATE_POOL_UNIVERSE_STRATEGY=best_k_meta
CANDIDATE_POOL_UNIVERSE_ENV=practice
CANDIDATE_POOL_TTL_DAYS=7
KIS_HTTP_ENABLED=1
```

### 주말 전용
```bash
FORCE_CANDIDATE=1               # 강제 저장
NO_TRADE=1                      # 빌드는 주문 금지
PREFETCH_DAYS=300               # 300일 권장 (520은 과함)
FEATURE_CACHE_ENABLED=1
FEATURE_CACHE_LOOKBACK_DAYS=300
```

### 주중 전용
```bash
FORCE_CANDIDATE=0               # 또는 미설정 (재빌드 금지)
NO_TRADE=0                      # 실거래 허용
FAIL_IF_POOL_MISSING=1          # 풀 없으면 실패
MINERVINI_USE_FEATURE_CACHE=1   # 향후 활성화
FEATURE_CACHE_WARMUP_ON_TICK=0  # 안전형 (미스나면 실패)
```

## ⚠️ 절대 금지 사항

### R1. 환경 키 혼용 금지
- ❌ 주말은 `env=live`, 주중은 `env=practice` → **fallback 지옥**
- ✅ 모든 워크플로우에서 `STRATEGY_ENV=practice` 통일

### R2. 주중에 candidate 재빌드 금지
- ❌ 주중 trade에서 `candidate_pool_builder` 실행 → **의도치 않은 풀 변경**
- ✅ 주중은 `verify → trade`만 실행

### R3. 주중에 풀 없으면 절대 매매 금지
- ❌ 풀 없는데 trade 강행 → **랜덤 종목 매매**
- ✅ `FAIL_IF_POOL_MISSING=1`로 즉시 실패

## 🎯 성공 판정 기준 (3줄 로그)

### 주말 빌드 성공
```
[WATCHLIST][SAVE] env=practice strategy=pb1_candidate_pool as_of=2026-02-09 members=120
```

### 주중 trade 성공
```
[POOL][PRECHECK] trade-tick will load from DB (no rebuild)
[WATCHLIST][LOAD] ... as_of=2026-02-09 members=120 (exact)
또는 fallback from 2026-02-07 age=2 days
```

### 절대 나오면 안 되는 로그 (주중)
```
❌ [OHLCV][DB][NO_FALLBACK] symbol=... days=520  (장중 520일 재계산)
❌ context env=live (환경 키 혼선)
❌ candidate_pool_builder ... (주중 재빌드)
```

## 🛠️ 장애 대응

### 주중 trade에서 "pool missing" 실패
**원인**: 주말 빌드가 안 돌았거나 실패
**해결**:
1. 주중 trade는 그대로 실패 유지 (매매 금지)
2. `weekend_build.yml`을 수동 실행
3. 검증 후 주중 trade 재실행

### 주중 trade에서 "age > 7 days" 실패
**원인**: 1주일 넘게 주말 빌드가 안 됨
**해결**:
1. `weekend_build.yml` 수동 실행
2. TTL 확인 (`CANDIDATE_POOL_TTL_DAYS=7`)

### 환경 키 불일치 (context env=live 등장)
**원인**: 워크플로우/코드에서 env 설정이 섞임
**해결**:
1. 모든 yml에서 `STRATEGY_ENV=practice` 통일
2. 코드에서 `env=live` 하드코딩 제거
3. 로그에서 `env=practice` 확인

## 📋 체크리스트

### 초기 세팅 (오늘 1회)
- [ ] `weekend_build.yml` 수동 실행
- [ ] 로그에서 `[WATCHLIST][SAVE] as_of=오늘` 확인
- [ ] `verify_candidate_pool` 실행해서 OK 확인

### 주간 루틴
- [ ] 토요일 00:30 KST 자동 빌드 확인 (로그 체크)
- [ ] 월~금 장중 trade-tick 정상 실행 확인
- [ ] `fallback age` 확인 (7일 이내여야 함)

### 긴급 복구
- [ ] 주말 빌드 실패 시 즉시 수동 실행
- [ ] 주중 trade는 절대 재빌드하지 않음
- [ ] 풀 없으면 매매 중단 (FAIL_IF_POOL_MISSING=1)

## 🔮 향후 개선 (optional)

### 1. Feature Cache 실제 구현
현재는 placeholder. 구현하면:
- 주말에 Minervini/PB1 피처 계산 (260~300일)
- 주중 tick에서 520일 재계산 제거
- 11분 → 1~2분으로 단축

### 2. 증분 업데이트
- 매일 장 마감 후 전일 종가 1개 bar만 반영
- 피처를 rolling 업데이트 (O(1))

### 3. Prefetch 일수 최적화
- 현재 300일 권장
- Minervini 필수는 260일 (52w + MA200 + slope 20)
- 520일은 과함 (2년치)

## 📞 문제 발생 시

**로그 체크포인트**:
1. 환경 키 통일: `env=practice` 모든 로그에서 동일
2. 주말 저장: `[WATCHLIST][SAVE] as_of=토요일`
3. 주중 로드: `[WATCHLIST][LOAD] ... (exact or fallback ≤7일)`
4. 재빌드 금지: 주중 `candidate_pool_builder` 로그 없음

**3초 진단**:
```bash
# 현재 풀 상태 확인
python -m trader.verify_candidate_pool

# 기대: as_of=최근 토요일, age ≤7 days
```

---

**✅ 이제 "토요일 빌드 → 주중 trade-tick만"이 구조적으로 완성되었습니다!**
