# DIAG 모드: 후보군 기반 Minervini 필터 전용 실행 가이드

## ✅ 개요

DIAG 모드에서 **후보군(Candidate Pool) 120개**만 대상으로 **Minervini 필터를 적용**하고 **즉시 종료**하는 패치입니다.

- **주문/entry/exit 로직 절대 실행 안 함** ✅
- **universe 195 스캔 절대 타지 않음** ✅
- **KIS_ENV vs STRATEGY_ENV mismatch 허용** (DIAG+Minervini-only 모드에서만)

---

## ✅ 실행 방법

### 1. 환경변수 설정

```bash
export STRATEGY_MODE=DIAG
export PB1_DIAG_MINERVINI_ONLY=1
export STRATEGY_ENV=live

# 옵션 (후보군 강제 사용)
export PB1_FORCE_CANDIDATE_POOL=1

# KIS_ENV는 아무거나 (mismatch 허용됨)
export KIS_ENV=practice  # 또는 live
```

### 2. 실행

#### 로컬 실행

```bash
python -m trader.pb1_runner
```

#### GitHub Actions 실행

워크플로우에서 다음 환경변수를 추가:

```yaml
env:
  STRATEGY_MODE: DIAG
  PB1_DIAG_MINERVINI_ONLY: 1
  STRATEGY_ENV: live
  PB1_FORCE_CANDIDATE_POOL: 1
  KIS_ENV: practice  # mismatch 허용됨
```

---

## ✅ 완료 기준

실행 후 다음이 확인되어야 합니다:

### 1. 로그 확인

```
[PB1][DIAG_MINERVINI_ONLY] mode=DIAG diag_minervini_only=1 -> run minervini filter only and exit
[DIAG_MINERVINI_ONLY][START] env=live strategy=best_k_meta as_of=2026-02-02
[DIAG_MINERVINI_ONLY][POOL] loaded=120 as_of=2026-02-01 reason=hit
[MINERVINI_RUNNER][START] as_of=2026-02-02 env=live codes=120
[MINERVINI_RUNNER][RESULT] input=120 passed=15 rejected=105
[MINERVINI_RUNNER][REPORT] path=/path/to/runtime/reports/minervini/2026-02-02/minervini_report.json passed=15 rejected=105
[DIAG_MINERVINI_ONLY][TOP_CANDIDATES] path=/path/to/runtime/top_candidates.json count=15
[DIAG_MINERVINI_ONLY][SUCCESS] input=120 passed=15 rejected=105
[PB1][DIAG_MINERVINI_ONLY][EXIT] code=0 reason=minervini_only_complete
```

### 2. 리포트 파일 확인

```bash
# Minervini 리포트
cat runtime/reports/minervini/YYYY-MM-DD/minervini_report.json

# Top candidates
cat runtime/top_candidates.json
```

예시:

**minervini_report.json**
```json
{
  "input_members": 120,
  "candidates": [
    {"code": "005930", "name": "삼성전자", "score": 85.5},
    {"code": "000660", "name": "SK하이닉스", "score": 78.2}
  ],
  "rejected": [
    {"code": "035720", "name": "카카오", "reasons": ["RS_BELOW", "VCP_LOW"]}
  ],
  "reason_counts": {
    "RS_BELOW": 45,
    "VCP_LOW": 38,
    "TREND_FAIL": 22
  }
}
```

**top_candidates.json**
```json
[
  {"code": "005930", "score": 85.5, "rs_pctile": 92.3},
  {"code": "000660", "score": 78.2, "rs_pctile": 88.1}
]
```

---

## ✅ 주요 특징

### 1. 후보군 우선 사용

- DB의 `pb1_candidate_pool` (size=120) 로드
- 유니버스 195는 **절대 사용 안 함** ✅

### 2. entry/exit 로직 절대 실행 안 함

- 토큰 발급, 잔고조회, 주문 로직 **모두 스킵**
- Minervini 필터 적용 후 **즉시 종료**

### 3. KIS_ENV mismatch 허용

```
STRATEGY_MODE=DIAG + PB1_DIAG_MINERVINI_ONLY=1 
→ KIS_ENV != STRATEGY_ENV 허용 (경고만 출력)

LIVE 모드
→ KIS_ENV != STRATEGY_ENV이면 FAIL (기존 유지)
```

---

## ✅ 환경변수 레퍼런스

| 변수 | 필수 | 기본값 | 설명 |
|------|------|--------|------|
| `STRATEGY_MODE` | ✅ | - | `DIAG`로 설정 필수 |
| `PB1_DIAG_MINERVINI_ONLY` | ✅ | `0` | `1`로 설정하면 Minervini-only 모드 활성화 |
| `STRATEGY_ENV` | ✅ | `live` | 후보군이 저장된 환경 (일반적으로 `live`) |
| `PB1_FORCE_CANDIDATE_POOL` | ⚪ | `0` | `1`로 설정하면 후보군 강제 사용 (universe로 새는 버그 방지) |
| `KIS_ENV` | ⚪ | - | DIAG+Minervini-only에서는 mismatch 허용됨 |

---

## ✅ 트러블슈팅

### 1. 후보군이 없다고 나옴

```
[DIAG_MINERVINI_ONLY][FAIL] no candidate pool reason=missing
```

**해결책:**
```bash
# 후보군 생성 (주말에 실행)
python -m trader.candidate_pool_builder --build pool --env live
```

### 2. 벤치마크 데이터 부족

```
[MINERVINI_RUNNER][BENCHMARK_FAIL] rows=50 min_required=200
```

**해결책:**
- KRX OHLCV 데이터 확인 (DB `price_daily` 테이블)
- 벤치마크 종목 (229200 KODEX코스피200) 데이터 보충

### 3. 여전히 universe 195를 스캔함

```
ENTRY[PIPE][START] universe=195
```

**해결책:**
```bash
# 후보군 강제 사용 플래그 활성화
export PB1_FORCE_CANDIDATE_POOL=1
```

---

## ✅ 파일 구조

```
trader/
  minervini_runner.py           # ✅ 신규: Minervini 독립 실행 모듈
  pb1_runner.py                 # ✅ 수정: DIAG Minervini-only 진입점 추가
  candidate_pool_builder.py     # 기존: 후보군 로드 함수 사용
  minervini/
    report.py                   # 기존: 리포트 생성 함수 사용

runtime/
  reports/
    minervini/
      YYYY-MM-DD/
        minervini_report.json   # ✅ Minervini 리포트
  top_candidates.json           # ✅ 통과 후보 리스트
```

---

## ✅ 구현 요약

### 1. `trader/minervini_runner.py` (신규)

- `run_minervini_for_codes()`: 종목 리스트에 Minervini 필터 적용
- `run_diag_minervini_only()`: DIAG 모드 전용 진입점

### 2. `trader/pb1_runner.py` (수정)

- `main()` 함수 시작 부분에 DIAG Minervini-only 분기 추가
- KIS_ENV vs STRATEGY_ENV mismatch 허용 로직 추가

### 3. 환경변수

- `PB1_DIAG_MINERVINI_ONLY=1`: Minervini-only 모드 활성화
- `PB1_FORCE_CANDIDATE_POOL=1`: 후보군 강제 사용

---

## ✅ 다음 단계

1. **로컬 테스트**
   ```bash
   export STRATEGY_MODE=DIAG
   export PB1_DIAG_MINERVINI_ONLY=1
   export STRATEGY_ENV=live
   python -m trader.pb1_runner
   ```

2. **리포트 확인**
   ```bash
   cat runtime/reports/minervini/$(date +%Y-%m-%d)/minervini_report.json
   cat runtime/top_candidates.json
   ```

3. **GitHub Actions 통합**
   - 워크플로우에 환경변수 추가
   - 장 종료 후 실행하여 다음날 입력 데이터로 활용

---

## ✅ 참고

- 후보군 TTL: 7일 (CANDIDATE_POOL_TTL_DAYS)
- 후보군 최소 크기: 40개 (CANDIDATE_POOL_MIN_SIZE)
- Minervini RS 최소값: 70% (RS_MIN_PCTILE)
- VCP 최소 점수: 60 (VCP_MIN_SCORE)
