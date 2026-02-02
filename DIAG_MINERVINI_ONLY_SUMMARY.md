# ✅ DIAG 모드: 후보군 기반 Minervini 필터 전용 패치 완료

## 📋 요약

DIAG 모드에서 **후보군 120개만 대상으로 Minervini 필터를 적용**하고 **즉시 종료**하는 기능을 구현했습니다.

---

## ✅ 구현 내용

### 1. 신규 파일

#### `trader/minervini_runner.py` (신규)
- `run_minervini_for_codes()`: 종목 리스트에 Minervini 필터 적용
- `run_diag_minervini_only()`: DIAG 모드 전용 진입점
- OHLCV 로드, RS/VCP 계산, 리포트 생성 통합

### 2. 수정 파일

#### `trader/pb1_runner.py` (수정)
- `main()` 함수에 DIAG Minervini-only 진입점 추가
- 환경변수 파싱:
  - `PB1_DIAG_MINERVINI_ONLY=1`
  - `PB1_FORCE_CANDIDATE_POOL=1`
- KIS_ENV vs STRATEGY_ENV mismatch 허용 (DIAG+Minervini-only 모드에서만)

---

## ✅ 주요 특징

### 1. 후보군 기반 입력
- DB `pb1_candidate_pool` (size=120) 로드
- **유니버스 195는 절대 사용 안 함** ✅

### 2. entry/exit 로직 완전 스킵
- 토큰 발급, 잔고조회, 주문 로직 **모두 스킵**
- Minervini 필터만 적용 후 **즉시 종료**

### 3. KIS_ENV mismatch 허용
```
DIAG + PB1_DIAG_MINERVINI_ONLY=1
→ KIS_ENV != STRATEGY_ENV 허용 (경고만)

LIVE 모드
→ KIS_ENV != STRATEGY_ENV이면 FAIL (기존 유지)
```

### 4. 리포트 자동 생성
- `runtime/reports/minervini/YYYY-MM-DD/minervini_report.json`
- `runtime/top_candidates.json`

---

## ✅ 실행 방법

### 환경변수 설정

```bash
export STRATEGY_MODE=DIAG
export PB1_DIAG_MINERVINI_ONLY=1
export STRATEGY_ENV=live
export PB1_FORCE_CANDIDATE_POOL=1  # 옵션
export KIS_ENV=practice  # mismatch 허용됨
```

### 실행

```bash
python -m trader.pb1_runner
```

### GitHub Actions

```yaml
env:
  STRATEGY_MODE: DIAG
  PB1_DIAG_MINERVINI_ONLY: 1
  STRATEGY_ENV: live
  PB1_FORCE_CANDIDATE_POOL: 1
  KIS_ENV: practice
```

---

## ✅ 출력 확인

### 1. 로그

```
[PB1][DIAG_MINERVINI_ONLY] mode=DIAG diag_minervini_only=1 -> run minervini filter only and exit
[DIAG_MINERVINI_ONLY][POOL] loaded=120 as_of=2026-02-01 reason=hit
[MINERVINI_RUNNER][RESULT] input=120 passed=15 rejected=105
[DIAG_MINERVINI_ONLY][SUCCESS] input=120 passed=15 rejected=105
[PB1][DIAG_MINERVINI_ONLY][EXIT] code=0 reason=minervini_only_complete
```

### 2. 리포트 파일

```bash
cat runtime/reports/minervini/2026-02-02/minervini_report.json
cat runtime/top_candidates.json
```

---

## ✅ 환경변수 레퍼런스

| 변수 | 필수 | 기본값 | 설명 |
|------|------|--------|------|
| `STRATEGY_MODE` | ✅ | - | `DIAG`로 설정 필수 |
| `PB1_DIAG_MINERVINI_ONLY` | ✅ | `0` | `1`로 설정하면 Minervini-only 모드 활성화 |
| `STRATEGY_ENV` | ✅ | `live` | 후보군이 저장된 환경 |
| `PB1_FORCE_CANDIDATE_POOL` | ⚪ | `0` | 후보군 강제 사용 (universe로 새는 버그 방지) |
| `KIS_ENV` | ⚪ | - | DIAG+Minervini-only에서는 mismatch 허용 |

---

## ✅ 설계 흐름

```
main()
  ├─ PB1_DIAG_MINERVINI_ONLY=1 && STRATEGY_MODE=DIAG 체크
  │   ↓
  ├─ run_diag_minervini_only()
  │   ├─ load_candidate_pool() → 120개
  │   ├─ run_minervini_for_codes()
  │   │   ├─ OHLCV 로드 (ChainOHLCVProvider)
  │   │   ├─ RS 계산 (rank_rs)
  │   │   ├─ VCP 계산 (score_vcp, detect_vcp)
  │   │   ├─ 스코어 계산 (score_setup)
  │   │   ├─ 필터 적용 (RS, VCP, 트렌드, 유동성)
  │   │   └─ 리포트 생성 (run_minervini_report)
  │   ├─ top_candidates.json 저장
  │   └─ SystemExit(0)
  └─ (도달 안 함: entry/exit 로직 스킵)
```

---

## ✅ 파일 구조

```
trader/
  minervini_runner.py           # ✅ 신규
  pb1_runner.py                 # ✅ 수정
  candidate_pool_builder.py     # 기존 (load_candidate_pool 사용)
  minervini/
    report.py                   # 기존 (run_minervini_report 사용)
  setups/
    vcp_pro.py                  # 기존 (score_vcp, detect_vcp 사용)
  factors/
    rs_rank.py                  # 기존 (rank_rs 사용)
  strategies/
    pb1_minervini_v2.py         # 기존 (compute_features, score_setup 사용)

runtime/
  reports/
    minervini/
      YYYY-MM-DD/
        minervini_report.json   # ✅ 출력
  top_candidates.json           # ✅ 출력

DIAG_MINERVINI_ONLY_GUIDE.md   # ✅ 사용자 가이드
DIAG_MINERVINI_ONLY_SUMMARY.md # ✅ 이 파일
```

---

## ✅ 테스트 시나리오

### 1. 정상 실행

```bash
export STRATEGY_MODE=DIAG
export PB1_DIAG_MINERVINI_ONLY=1
export STRATEGY_ENV=live
python -m trader.pb1_runner
```

**기대 결과:**
- 후보군 120개 로드
- Minervini 필터 적용
- 리포트 2개 생성
- exit code 0

### 2. 후보군 없음

```bash
# 후보군 DB에 없는 경우
python -m trader.pb1_runner
```

**기대 결과:**
```
[DIAG_MINERVINI_ONLY][FAIL] no candidate pool reason=missing
exit code 1
```

### 3. LIVE 모드에서 mismatch

```bash
export STRATEGY_MODE=LIVE
export KIS_ENV=live
export STRATEGY_ENV=paper
python -m trader.pb1_runner
```

**기대 결과:**
```
[PB1][ENV][CRITICAL] KIS_ENV=live != STRATEGY_ENV=paper -> FAIL
ValueError: KIS_ENV (live) != STRATEGY_ENV (paper)
```

### 4. DIAG 모드에서 mismatch 허용

```bash
export STRATEGY_MODE=DIAG
export PB1_DIAG_MINERVINI_ONLY=1
export KIS_ENV=practice
export STRATEGY_ENV=live
python -m trader.pb1_runner
```

**기대 결과:**
```
[PB1][ENV][MISMATCH_ALLOWED] KIS_ENV=practice != STRATEGY_ENV=live (allowed in DIAG+Minervini-only mode)
(계속 실행)
```

---

## ✅ 트러블슈팅

### 1. 후보군 부족

**증상:**
```
[CANDIDATE_POOL][LOAD] miss reason=too_small as_of=2026-02-01 size=35 min=40
```

**해결:**
```bash
# 후보군 재생성
python -m trader.candidate_pool_builder --build pool --env live
```

### 2. 벤치마크 데이터 부족

**증상:**
```
[MINERVINI_RUNNER][BENCHMARK_FAIL] rows=50 min_required=200
```

**해결:**
- DB `price_daily` 테이블 확인
- KODEX코스피200 (229200) 데이터 보충

### 3. import 오류

**증상:**
```
ImportError: cannot import name 'run_diag_minervini_only' from 'trader.minervini_runner'
```

**해결:**
- Python 모듈 재로드: `python -c "import trader.minervini_runner"`
- 파일 권한 확인: `ls -l trader/minervini_runner.py`

---

## ✅ 다음 단계

1. **로컬 테스트**
   - 환경변수 설정
   - `python -m trader.pb1_runner` 실행
   - 리포트 파일 확인

2. **GitHub Actions 통합**
   - 워크플로우 파일 수정
   - 환경변수 추가
   - 장 종료 후 실행 스케줄 설정

3. **모니터링**
   - 리포트 내용 검증
   - 통과 종목 수 추이 관찰
   - 탈락 사유 통계 분석

---

## ✅ 참고 문서

- [DIAG_MINERVINI_ONLY_GUIDE.md](DIAG_MINERVINI_ONLY_GUIDE.md) - 상세 사용 가이드
- [CANDIDATE_POOL_IMPLEMENTATION.md](CANDIDATE_POOL_IMPLEMENTATION.md) - 후보군 시스템 설명
- [MINERVINI_DEBUG.md](docs/MINERVINI_DEBUG.md) - Minervini 디버깅 가이드

---

## ✅ 완료 체크리스트

- [x] `trader/minervini_runner.py` 생성
- [x] `trader/pb1_runner.py` 수정 (DIAG 진입점 추가)
- [x] KIS_ENV mismatch 허용 로직 추가
- [x] 환경변수 파싱 (`PB1_DIAG_MINERVINI_ONLY`, `PB1_FORCE_CANDIDATE_POOL`)
- [x] 리포트 저장 경로 설정
- [x] 사용자 가이드 문서 작성
- [x] import 오류 수정
- [x] 에러 체크 통과

---

**구현 완료!** 🎉

이제 DIAG 모드에서 후보군 기반 Minervini 필터만 실행할 수 있습니다.
