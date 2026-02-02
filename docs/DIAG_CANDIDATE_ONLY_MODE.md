# DIAG Candidate-Only Mode 가이드

## 📋 개요

DIAG trade-tick에서 **후보군(candidate pool)만으로 미너비니를 실행**하는 모드입니다.

### 핵심 철학

✅ **DB에 이미 존재하는 유니버스/후보군은 절대 재생성하지 않음**

✅ **DIAG에서는 후보군(120개)만 스캔, 유니버스(195개)는 스캔에서 배제**

✅ **DIAG에서는 KIS_ENV != STRATEGY_ENV 허용 (실거래에서만 strict)**

---

## 🎯 해결된 문제

### 1. KIS_ENV != STRATEGY_ENV 에러
- **증상**: DIAG에서도 `ValueError: KIS_ENV != STRATEGY_ENV` 발생
- **해결**: DIAG/DRY_RUN에서는 경고만 출력, LIVE 실거래에서만 에러 발생
- **코드**: [pb1_runner.py](../trader/pb1_runner.py#L2160-L2178)

### 2. universe_count=195 exceeds 150 에러
- **증상**: 후보군 120개를 로드했는데도 유니버스 195개로 스캔 시도
- **해결**: `scan_codes`를 후보군으로 강제 설정, CRITICAL GUARD도 `scan_codes` 기준으로 체크
- **코드**: [pb1_engine.py](../trader/pb1_engine.py#L4555-L4610), [pb1_engine.py](../trader/pb1_engine.py#L1485-L1510)

---

## ⚙️ 환경변수 설정

### 필수 환경변수

```bash
# 후보군만 스캔 (유니버스 rebuild/scan 금지)
export PB1_CANDIDATE_ONLY=1

# 후보군 활성화
export PB1_WATCHLIST_ENABLED=1

# 실거래 비활성화 (DIAG 모드)
export LIVE_TRADING_ENABLED=0
export DRY_RUN=1
export SIM_MODE=1

# 전략 환경 (후보군이 있는 환경)
export STRATEGY_ENV=live
export KIS_ENV=paper  # DIAG에서는 mismatch 허용됨
```

### UI 설정 (해당하는 경우)

| 항목 | 값 | 설명 |
|------|-----|------|
| **candidate \| trade \| both** | `candidate` | 후보군 모드 |
| **LIVE \| PAPER** | `PAPER` 또는 `DIAG` | 테스트 모드 |
| **candidate 강제 재생성** | `0` (OFF) | DB에 있는 후보군 재사용 |
| **실거래 허용** | `0` (OFF) | DRY_RUN |
| **시뮬레이션 모드** | `1` (ON) | 시뮬레이션 |

---

## 🔍 정상 작동 시 로그

아래 로그가 출력되어야 정상입니다:

```log
[UNIVERSE][SKIP] PB1_CANDIDATE_ONLY=1 -> skip ensure/build, will use candidate pool only
[PB1][WATCHLIST] enabled -> load today watchlist
[PB1][WATCHLIST] loaded=120 source=db
[ENTRY][SCAN_UNIVERSE] source=candidate_pool scan_count=120 (universe_count=195 watchlist_count=120)
[PB1][PREFILTER] total=120 limit=50 selected=50 lookback_days=30
[MINERVINI][APPLY] universe=50 ...
```

### ❌ 절대 나오면 안 되는 로그

```log
❌ CRITICAL: universe_count=195 exceeds 150
❌ TIMEOUT checked=10/195 ...
❌ [PB1][ENV][CRITICAL] KIS_ENV=paper != STRATEGY_ENV=live -> FAIL (DIAG에서)
```

---

## 📊 코드 플로우

### 1. 환경 변수 체크
- [pb1_runner.py#L2145-L2158](../trader/pb1_runner.py#L2145-L2158): 모드 플래그 설정
- `PB1_CANDIDATE_ONLY`, `DRY_RUN`, `SIM_MODE` 등 확인

### 2. KIS_ENV vs STRATEGY_ENV 검증
- [pb1_runner.py#L2160-L2178](../trader/pb1_runner.py#L2160-L2178): LIVE에서만 strict, DIAG에서는 경고만

### 3. Universe Ensure Skip
- [pb1_runner.py#L1867-L1873](../trader/pb1_runner.py#L1867-L1873): `PB1_CANDIDATE_ONLY=1`이면 유니버스 rebuild 건너뜀

### 4. Scan Universe 결정
- [pb1_engine.py#L4555-L4610](../trader/pb1_engine.py#L4555-L4610): 
  - 후보군이 있으면 `scan_codes = watchlist_members`
  - 없으면 `scan_codes = universe_members`

### 5. CRITICAL GUARD
- [pb1_engine.py#L1485-L1510](../trader/pb1_engine.py#L1485-L1510): 
  - `scan_count`가 150 초과 시 경고
  - `PB1_CANDIDATE_ONLY=1`에서 150 초과면 에러 (후보군 빌드 문제)

---

## 🧪 테스트 시나리오

### 시나리오 1: 후보군 120개로 DIAG 실행
```bash
export PB1_CANDIDATE_ONLY=1
export PB1_WATCHLIST_ENABLED=1
export STRATEGY_ENV=live
export KIS_ENV=paper
export DRY_RUN=1

# 실행
python -m trader.pb1_runner --mode trade-tick
```

**예상 결과**:
- ✅ Universe rebuild 건너뜀
- ✅ 후보군 120개 로드
- ✅ `scan_count=120`으로 미너비니 실행
- ✅ KIS_ENV mismatch 경고만 출력

### 시나리오 2: LIVE 실거래 (기존 동작 유지)
```bash
export PB1_CANDIDATE_ONLY=0
export LIVE_TRADING_ENABLED=1
export STRATEGY_ENV=live
export KIS_ENV=live  # 반드시 일치해야 함
export DRY_RUN=0
```

**예상 결과**:
- ✅ KIS_ENV != STRATEGY_ENV 시 에러 발생 (엄격한 검증)
- ✅ 후보군 또는 유니버스로 정상 거래

---

## 🔧 트러블슈팅

### Q1: `CRITICAL: scan_count=195 exceeds 150`
- **원인**: 후보군이 로드되지 않아 유니버스(195)를 스캔 중
- **해결**: 
  1. `PB1_WATCHLIST_ENABLED=1` 확인
  2. DB에 후보군 존재 확인: `SELECT COUNT(*) FROM pb1_watchlist WHERE as_of_date = CURRENT_DATE`
  3. 후보군이 없으면 먼저 candidate 모드로 빌드

### Q2: `KIS_ENV != STRATEGY_ENV` 에러 (DIAG에서도 발생)
- **원인**: LIVE 모드 플래그가 잘못 설정됨
- **해결**:
  1. `LIVE_TRADING_ENABLED=0` 확인
  2. `DRY_RUN=1` 확인
  3. `PB1_CANDIDATE_ONLY=1` 확인

### Q3: 후보군이 없음 (로드 실패)
- **원인**: DB에 오늘 날짜 후보군이 없음
- **해결**:
  ```bash
  # candidate 모드로 먼저 실행하여 후보군 생성
  export PB1_CANDIDATE_ONLY=0  # 빌드 허용
  python -m trader.pb1_runner --mode candidate
  
  # 그 후 trade-tick 실행
  export PB1_CANDIDATE_ONLY=1  # 빌드 금지, 로드만
  python -m trader.pb1_runner --mode trade-tick
  ```

---

## 📝 관련 문서

- [LIVE_TRADING_SETUP_COMPLETE.md](./LIVE_TRADING_SETUP_COMPLETE.md): 실거래 설정
- [CANDIDATE_POOL_IMPLEMENTATION.md](../CANDIDATE_POOL_IMPLEMENTATION.md): 후보군 구현
- [DIAG_MINERVINI_ONLY_GUIDE.md](../DIAG_MINERVINI_ONLY_GUIDE.md): DIAG 미너비니 가이드

---

## 🎓 핵심 개념

### Candidate-Only 철학
1. **DB 재생성 금지**: 이미 있는 데이터는 재사용
2. **후보군 우선**: 유니버스보다 후보군(작은 집합)을 우선 사용
3. **DIAG 유연성**: 테스트 모드에서는 환경 불일치 허용
4. **LIVE 엄격성**: 실거래에서는 모든 검증 엄격히 적용

### Scan Universe 우선순위
```
if PB1_CANDIDATE_ONLY=1 and watchlist exists:
    scan_codes = watchlist  # 120개
else if watchlist exists:
    scan_codes = watchlist  # 일반 모드에서도 우선
else:
    scan_codes = universe   # 195개
```

---

**마지막 업데이트**: 2026-02-02  
**작성자**: Copilot (CEO 송제혁님 요구사항 기반)
