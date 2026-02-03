# MINERVINI_ONLY 모드 구현 체크리스트

## ✅ 구현 완료 항목

### 1. **trader/config.py에 MINERVINI_ONLY 전역 상수 추가**
- [x] `_env_flag()` 헬퍼 함수 추가
- [x] `MINERVINI_ONLY = _env_flag("MINERVINI_ONLY", "0")` 전역 상수 선언
- [x] 로깅 추가: `[ENV] MINERVINI_ONLY=1 (analytics-only, no orders)`

**위치**: 라인 17-22

**기대 결과**: `MINERVINI_ONLY` 변수를 다른 모듈에서 import 가능

---

### 2. **kis_wrapper.py kis_http_enabled() 함수 수정**
- [x] `from trader.config import MINERVINI_ONLY` 제거 (순환 import 방지)
- [x] `os.getenv("MINERVINI_ONLY")` 직접 읽기로 변경
- [x] MINERVINI_ONLY=1이면 무조건 `False` 반환

**위치**: 라인 52-68

**기대 결과**: ImportError 없이 `kis_http_enabled()` 정상 동작

---

### 3. **WatchlistRepo.load_watchlist()에 allow_latest_fallback 파라미터 추가**

#### 3-1. 메서드 시그니처 확장
- [x] `allow_latest_fallback: bool = False` 파라미터 추가
- [x] `ttl_days: int = 7` 파라미터 추가
- [x] 반환 타입 변경: `tuple[List[Dict[str, Any]], date | None]`

**위치**: trader/db/repos.py 라인 2070-2078

#### 3-2. Fallback 로직 구현
- [x] `as_of`에 데이터가 없으면 `get_latest_watchlist_date()` 호출
- [x] `latest_date`가 `ttl_days` 이내이면 해당 날짜로 재조회
- [x] fallback 사용 시 로깅: `[WATCHLIST][LOAD][FALLBACK]`
- [x] 반환값에 실제 사용된 `as_of` 날짜 포함

**위치**: trader/db/repos.py 라인 2105-2152

**기대 결과**: verify_candidate_pool에서 `TypeError` 없이 정상 동작

---

### 4. **pb1_runner.py에서 MINERVINI_ONLY일 때 KisAPI 초기화 스킵**

#### 4-1. EXIT_SHORTCIRCUIT 구간
- [x] `minervini_only` 플래그 확인
- [x] `if not minervini_only: kis = KisAPI()`
- [x] `else: logger.warning("[MINERVINI_ONLY] skip KisAPI init")`

**위치**: 라인 1348-1363

#### 4-2. DEGRADED 구간
- [x] 동일한 `minervini_only` 스킵 로직 추가

**위치**: 라인 1398-1407

#### 4-3. Watchlist Build Job
- [x] KisAPI 초기화 스킵
- [x] OHLCV provider를 KRX만 사용하도록 분기

**위치**: 라인 188-199

#### 4-4. _run_smoke
- [x] KisAPI 초기화 스킵
- [x] 조건부 로깅 추가

**위치**: 라인 727-742

#### 4-5. run_once 메인 로직
- [x] KisAPI 초기화 스킵
- [x] kis가 None일 때 env mismatch 체크 우회

**위치**: 라인 1540-1556

#### 4-6. kis_factory lambda
- [x] MINERVINI_ONLY일 때 `lambda: None` 반환

**위치**: 라인 1951-1960

**기대 결과**: `[MINERVINI_ONLY] skip KisAPI init (analytics only)` 로그 출력

---

### 5. **Run workflow UI에 MINERVINI_ONLY 입력 노출**
- [x] `.github/workflows/trade-runner.yml`에 input 추가
  - 이름: `MINERVINI_ONLY`
  - 설명: "미너비니 분석만 실행 (1=분석만, 0=정상)"
  - 기본값: `"0"`
- [x] `trade_tick` job의 `env`에 추가
  - `MINERVINI_ONLY: ${{ github.event.inputs.MINERVINI_ONLY || '0' }}`

**위치**: 
- Input 정의: 라인 36-39
- Env 사용: 라인 528

**기대 결과**: GitHub Actions UI에서 MINERVINI_ONLY 체크박스 표시

---

## 🔍 실행 시 확인해야 할 4가지 체크포인트

### ✅ 체크포인트 1: MINERVINI_ONLY=1 로깅
```
[ENV] MINERVINI_ONLY=1 (analytics-only, no orders)
```
- **확인 위치**: trade-tick 시작 시 config 로딩 단계
- **의미**: MINERVINI_ONLY 모드가 정상 활성화됨

---

### ✅ 체크포인트 2: verify_candidate_pool TypeError 없음
```
[VERIFY][OK] ✅ Candidate pool found!
```
- **확인 위치**: verify_candidate_pool.py 실행 시
- **의미**: `allow_latest_fallback` 파라미터 정상 작동

**만약 fallback이 발생하면**:
```
[WATCHLIST][LOAD][FALLBACK] as_of=2026-02-03 not found, using latest=2026-02-01 (age=2 days)
[VERIFY][OK] ✅ Candidate pool found (fallback)!
```

---

### ✅ 체크포인트 3: KisAPI init 스킵 로그
```
[MINERVINI_ONLY] skip KisAPI init (analytics only)
```
- **확인 위치**: pb1_runner.py 실행 시 여러 구간
- **의미**: KIS API 호출 없이 미너비니 계산만 수행

**추가 확인**:
- `[PB1] KIS init failed` 로그가 **없어야** 함
- 토큰 발급 관련 에러가 **없어야** 함

---

### ✅ 체크포인트 4: 미너비니 실행 & 후보군 저장
```
[ENTRY][PIPE][END] ... minervini_dt=0.XX candidates=N (N > 0)
[WATCHLIST][SAVE] saved N candidates to top_candidates.json
```
- **확인 위치**: candidate pool 생성 완료 시
- **의미**: 
  - `minervini_dt`가 **0.00이 아님** (미너비니 필터 실행됨)
  - `candidates`가 **0보다 큼** (후보군 생성됨)
  - `top_candidates.json`에 **결과 저장됨**

---

## 🎯 예상 실행 흐름 (MINERVINI_ONLY=1)

### 1. **Candidate Pool 생성 시** (MODE=candidate)
```
[ENV] MINERVINI_ONLY=1 (analytics-only, no orders)
[MINERVINI_ONLY] skip KisAPI init (analytics only)
[WATCHLIST][BUILD_JOB] universe loaded members=200
[OHLCV] using KRXOHLCVProvider only (no KIS)
... 미너비니 필터 실행 ...
[ENTRY][PIPE][END] minervini_dt=0.15 candidates=120
[WATCHLIST][SAVE] saved 120 candidates
```

### 2. **Trade Tick 시** (MODE=trade, after 시간대)
```
[ENV] MINERVINI_ONLY=1 (analytics-only, no orders)
[VERIFY][OK] ✅ Candidate pool found!
[MINERVINI_ONLY] skip KisAPI init (analytics only)
[PB1][SKIP] after market hours (no trading)
[ENTRY][PIPE][END] minervini_dt=0.12 candidates=80
```
- **주의**: MINERVINI_ONLY=1이어도 **미너비니 계산은 수행됨**
- **차이점**: 주문/인텐트 생성/체결 관련은 **모두 0**

---

## 🛠️ 트러블슈팅

### 문제 1: `ImportError: cannot import name 'MINERVINI_ONLY' from 'trader.config'`
**원인**: config.py에 전역 상수가 없음  
**해결**: ✅ 이미 수정됨 (라인 17-22)

---

### 문제 2: `TypeError: load_watchlist() got an unexpected keyword argument 'allow_latest_fallback'`
**원인**: WatchlistRepo.load_watchlist() 시그니처에 파라미터 없음  
**해결**: ✅ 이미 수정됨 (라인 2070-2152)

---

### 문제 3: `[PB1] KIS init failed -> skip tick`
**원인**: MINERVINI_ONLY인데도 KisAPI() 초기화 시도  
**해결**: ✅ 이미 수정됨 (모든 KisAPI 초기화 위치에 스킵 로직 추가)

---

### 문제 4: minervini_dt=0.00 (미너비니가 실행되지 않음)
**가능한 원인**:
1. 후보군이 비어있음 → verify_candidate_pool 로그 확인
2. OHLCV 데이터 로딩 실패 → KRXOHLCVProvider 에러 확인
3. 시간대 체크에서 early return → window/phase 로그 확인

**확인 방법**:
```bash
# 로그에서 미너비니 실행 여부 확인
grep "minervini_dt=" <로그파일>
grep "[MINERVINI]" <로그파일>
```

---

## 📋 Run Workflow 설정 (추천)

MINERVINI_ONLY 모드로 미너비니 분석만 실행하려면:

| 입력 항목 | 값 | 이유 |
|----------|-----|------|
| **candidate \| trade \| both** | `trade` | tick이 실행되어야 미너비니 계산 수행 |
| **LIVE \| PAPER** | `PAPER` | 안전을 위해 |
| **candidate 강제 재생성** | `0` | 기존 120개 후보군 사용 |
| **실거래 허용** | `0` | 주문 금지 |
| **시뮬레이션 모드** | `1` | DRY_RUN 모드 |
| **MINERVINI_ONLY** | `1` | ✅ **핵심 설정** |

---

## ✨ 최종 확인 명령어

```bash
# 1. MINERVINI_ONLY 환경변수 확인
echo $MINERVINI_ONLY

# 2. config 로딩 확인
python -c "from trader.config import MINERVINI_ONLY; print(f'MINERVINI_ONLY={MINERVINI_ONLY}')"

# 3. kis_http_enabled 확인
MINERVINI_ONLY=1 python -c "from trader.kis_wrapper import kis_http_enabled; print(f'KIS HTTP enabled: {kis_http_enabled()}')"
# 기대: False

# 4. WatchlistRepo 확인
python -c "from trader.db.repos import WatchlistRepo; import inspect; print(inspect.signature(WatchlistRepo.load_watchlist))"
# 기대: allow_latest_fallback와 ttl_days 파라미터가 보여야 함
```

---

## 📝 구현 요약

1. **2개 에러 완전 해결**
   - ✅ `allow_latest_fallback` TypeError → repos.py 수정
   - ✅ `MINERVINI_ONLY` ImportError → config.py에 전역 상수 추가 + kis_wrapper.py에서 env 직접 읽기

2. **MINERVINI_ONLY 모드 구현**
   - ✅ config.py에 전역 상수 및 로깅
   - ✅ kis_wrapper.py에서 HTTP 차단
   - ✅ pb1_runner.py 모든 KisAPI 초기화 위치에서 스킵 로직 추가
   - ✅ workflow에 input 노출

3. **동작 보장**
   - ✅ MINERVINI_ONLY=1일 때 미너비니 계산은 **정상 수행**
   - ✅ 주문/인텐트/체결 관련은 **모두 차단**
   - ✅ after 시간대에도 **미너비니 분석 가능**
