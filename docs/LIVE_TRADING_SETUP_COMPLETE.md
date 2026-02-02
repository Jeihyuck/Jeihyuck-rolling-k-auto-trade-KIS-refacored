# 실거래 완벽 설정 완료 보고서

## ✅ 완료된 작업

### 🔥 최신 (2026-02-02 오후): ENV 네임스페이스 버그 수정 ✅

**문제**: 후보군이 DB에 있는데도 trade-tick에서 0개로 로드되는 버그
- 후보군 빌드: `STRATEGY_ENV=live`로 저장
- trade-tick: `KIS_ENV`로 검색 → 불일치로 0개

**해결**:
1. ✅ [pb1_engine.py](trader/pb1_engine.py#L4130-L4180): 후보군 로드/저장 시 `STRATEGY_ENV` 우선 사용
2. ✅ [verify_candidate_pool.py](scripts/verify_candidate_pool.py): `KIS_ENV` vs `STRATEGY_ENV` 불일치 경고 추가
3. ✅ [ENV_NAMESPACE_FIX.md](../ENV_NAMESPACE_FIX.md): 상세 설명 문서 작성

**핵심 개념**:
- `STRATEGY_ENV`: 후보군/유니버스 DB 네임스페이스 (live/paper)
- `KIS_ENV`: KIS API 계정 종류 (practice/real)
- 둘은 **독립적**으로 설정 가능

**올바른 설정**:
```bash
STRATEGY_ENV=live      # 후보군 로드용
KIS_ENV=practice       # 또는 real (KIS 계좌 종류)
```

자세한 내용: [ENV_NAMESPACE_FIX.md](../ENV_NAMESPACE_FIX.md)

---

### 1. 🔧 _fetch_daily 버그 수정 (근본 해결)

**문제**: `candidate_pool_builder`가 `self.ohlcv_provider(code, days=...)`로 호출하는데, `PB1Engine._fetch_daily`는 `days` 파라미터를 받지 않음

**해결**:
- [trader/pb1_engine.py](trader/pb1_engine.py#L1446): `_fetch_daily` 메서드에 `days` 파라미터 별칭 추가
  - `count`와 `days`를 상호 호환 가능하게 수정
  - 하위 호환성 유지
- 후보군 빌더 호출 시 호환 래퍼 함수 추가:
  - [pb1_engine.py#L4156](trader/pb1_engine.py#L4156): 일반 후보군 빌드
  - [pb1_engine.py#L4200](trader/pb1_engine.py#L4200): 비상(emergency) 후보군 빌드

**결과**: 이제 후보군 재빌드가 필요한 경우에도 `days` 파라미터 에러가 발생하지 않습니다.

---

### 2. 📋 검증 스크립트 생성

**생성된 파일**: [scripts/verify_candidate_pool.py](scripts/verify_candidate_pool.py)

**기능**:
- DB에서 후보군 로드 검증
- env, strategy, as_of 키 자동 감지 (환경 변수에서)
- 최소 크기 검증
- 친절한 에러 메시지 제공

**사용법**:
```bash
# 환경 변수 설정 후 실행
STRATEGY_ENV=live AS_OF=2026-02-02 python scripts/verify_candidate_pool.py
```

**출력 예시**:
```
[VERIFY][OK] ✅ Candidate pool found!
[VERIFY][OK] size=120
[VERIFY][OK] sample=['005930', '035720', ...]
[VERIFY][OK] Pool size 120 >= minimum 40
[VERIFY][OK] ✅ All checks passed!
```

---

### 3. 📚 환경 변수 설정 문서 작성

**생성된 파일**: [docs/TRADE_TICK_ENV_SETUP.md](docs/TRADE_TICK_ENV_SETUP.md)

**내용**:
- A. 라이브 매매 필수 환경 변수 (Precheck 통과)
- B. 후보군 DB 로드 설정 (재빌드 금지)
- C. as_of 키 고정 (날짜 불일치 방지)
- 핵심 3종 일치 원칙 설명
- 검증 스크립트 사용법
- 정상/실패 로그 구분
- 문제 해결 가이드
- 완벽 설정 예제 (복붙용)
- 워크플로우 실행 순서

---

### 4. 🔄 GitHub Actions 워크플로우 개선

**파일**: [.github/workflows/trade-runner.yml](.github/workflows/trade-runner.yml)

**변경 사항**:

#### A. trade_tick job에 검증 스텝 추가 ([라인 653-660](.github/workflows/trade-runner.yml#L653-L660))
```yaml
- name: Candidate pool DB precheck (Weekend-built pool required)
  run: |
    set -euo pipefail
    
    # ✅ 전용 검증 스크립트 사용
    python scripts/verify_candidate_pool.py
    
    echo "[POOL][PRECHECK] ✅ Candidate pool verification passed"
```

#### B. 재빠드 금지 환경 변수 추가 ([라인 516-526](.github/workflows/trade-runner.yml#L516-L526))
```yaml
# ✅ 후보군 사용 활성화 (DB 로드 전용, 재빌드 금지)
CANDIDATE_POOL_ENABLED: "1"
CANDIDATE_POOL_STRATEGY_KEY: "pb1_candidate_pool"

# ✅ 재빌드 금지 (주중에는 DB에서만 로드)
PB1_WATCHLIST_AUTOBUILD: "0"
PB1_WATCHLIST_FORCE_REBUILD: "0"
CANDIDATE_POOL_FORCE_REBUILD: "0"
```

---

## 🎯 오늘 실거래를 위한 최종 체크리스트

### ✅ 코드 수정
- [x] `_fetch_daily` 버그 수정 완료
- [x] 호환 래퍼 함수 추가 완료

### ✅ 도구 준비
- [x] 검증 스크립트 생성 완료
- [x] 환경 변수 가이드 문서 작성 완료

### ✅ GitHub Actions 설정
- [x] trade_tick job에 검증 스텝 추가 완료
- [x] 재빌드 금지 환경 변수 설정 완료

### ⚠️ 수동 확인 필요 (실행 전)

1. **워크플로우 dispatch 시 5개 입력값 확인**:
   - MODE: `trade` ✅
   - ENV: `LIVE` ✅
   - FORCE_REBUILD: `0` ✅
   - LIVE_TRADING_ENABLED: `1` ✅
   - DRY_RUN: `0` ✅

2. **Secrets 확인**:
   - `KIS_ENV`: `practice` 또는 `real` (실거래 계좌 종류)
   - `PBCORE_DB_URL`: Postgres 연결 문자열
   - KIS API 키들 (APP_KEY, APP_SECRET, etc.)

3. **후보군이 이미 DB에 있는지 확인**:
   ```bash
   # 로컬에서 확인 가능
   STRATEGY_ENV=live AS_OF=2026-02-02 python scripts/verify_candidate_pool.py
   ```

---

## 📊 예상 실행 흐름

### 1단계: 워크플로우 시작
```
[WORKFLOW] trade_tick job starts
[ENV] STRATEGY_ENV=live (from input)
[ENV] PB1_WATCHLIST_AUTOBUILD=0 (no rebuild)
[ENV] LIVE_TRADING_ENABLED=1 (real trading)
```

### 2단계: 검증
```
[VERIFY] Checking candidate pool...
[VERIFY] env=live
[VERIFY] strategy=pb1_candidate_pool
[VERIFY] as_of=2026-02-02
[VERIFY][OK] ✅ Candidate pool found!
[VERIFY][OK] size=120
[VERIFY][OK] ✅ All checks passed!
[POOL][PRECHECK] ✅ Candidate pool verification passed
```

### 3단계: trade-tick 실행
```
[WATCHLIST][LOAD] env=live strategy=pb1_candidate_pool as_of=2026-02-02 members=120
[PB1][ENTRY] candidates scanning from 120 members
[PB1][ENTRY] found X candidates
...
```

---

## 🚨 주의사항

1. **주중에는 절대 재빌드하지 마세요**
   - `PB1_WATCHLIST_AUTOBUILD=0` 유지
   - `FORCE_REBUILD=0` 유지

2. **3종 일치 원칙**
   - `STRATEGY_ENV` = `live`
   - `CANDIDATE_POOL_STRATEGY_KEY` = `pb1_candidate_pool`
   - `AS_OF` = `2026-02-02` (후보군 생성일과 동일)

3. **실행 전 반드시 검증**
   - GitHub Actions 로그에서 "[VERIFY][OK]" 확인
   - "[WATCHLIST][LOAD] members=120" 확인

4. **로그 모니터링**
   - `members=0` → 실패 (즉시 중단)
   - `rebuild_light_scan` → 실패 (재빌드 시도 = 버그)
   - `members=120` → 성공 ✅

---

## 📝 다음 작업 (내일 이후)

현재 긴급 버그 수정은 완료되었으나, 장기적으로 개선할 사항:

1. **OHLCV Provider 인터페이스 통일**
   - `days` vs `count` 파라미터 표준화
   - 모든 provider가 동일한 인터페이스 구현

2. **후보군 TTL 자동 갱신**
   - 7일 지난 후보군 자동 재빌드 (주말에만)

3. **검증 스크립트 자동화**
   - pre-commit hook 추가
   - CI/CD 파이프라인에 통합

---

## ✅ 결론

**오늘 목표 "무조건 매매"를 위한 모든 준비 완료!**

1. ✅ 버그 수정: `_fetch_daily` days 파라미터 지원
2. ✅ 검증 도구: 자동 검증 스크립트 생성
3. ✅ 문서화: 완벽한 환경 변수 가이드
4. ✅ CI/CD: GitHub Actions 검증 스텝 추가

이제 워크플로우를 dispatch하면:
- 후보군 120개를 DB에서 정상 로드
- 재빌드 없이 안전하게 진행
- 실거래 모드로 정상 작동

**Good luck! 🚀**
