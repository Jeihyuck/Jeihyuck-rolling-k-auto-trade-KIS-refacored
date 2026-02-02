# DIAG Trade-Tick 완벽 설정 완료

## ✅ 패치 완료 (nullim 브랜치)

### 목표
- 유니버스/후보군을 새로 만들지 않음
- DB에 저장된 candidate pool(120개) 로드
- DIAG로 trade-tick 실행
- 미너비니 필터/스코어링까지 수행
- 주문은 절대 안 나감 (DRY_RUN)

---

## A. GitHub Actions 실행 설정값

**Run workflow** 입력값:

```
run_mode: trade
env: PAPER
candidate_force_rebuild: 0
live_trading_enabled: 0
dry_run: 1
```

✅ 정리: `trade / PAPER / 0 / 0 / 1`

---

## B. 패치 내용 (3개 파일)

### E-1: 워크플로우 if 조건 수정

**파일**: `.github/workflows/trade-runner.yml`

**변경**: trade-tick job이 `MODE=trade` 또는 `MODE=both`일 때 무조건 실행

```yaml
if: >
  always() && (
    (github.event_name == 'schedule' && github.event.schedule == '55 23 * * 0-4') ||
    (github.event_name == 'workflow_dispatch' &&
      (github.event.inputs.MODE == 'trade' || github.event.inputs.MODE == 'both'))
  )
```

**효과**: upstream(candidate_pool) 의존성 제거 → trade-tick이 스킵되지 않음

---

### E-2: 환경 변수 강제 주입

**파일**: `.github/workflows/trade-runner.yml`

**변경사항**:

```yaml
env:
  # 1) 환경 mismatch 방지 (가장 중요)
  KIS_ENV: PAPER
  STRATEGY_ENV: PAPER

  # 2) 주문 절대 금지
  DRY_RUN: "1"
  LIVE_TRADING_ENABLED: "0"
  DISABLE_LIVE_TRADING: "1"
  SIMULATION_MODE: "1"
  STRATEGY_MODE: "PAPER"

  # 3) universe / candidate rebuild 금지
  UNIVERSE_FORCE_REBUILD: "0"
  CANDIDATE_POOL_FORCE_REBUILD: "0"
  CANDIDATE_POOL_ENABLED: "1"
```

**효과**:
- `ValueError: KIS_ENV != STRATEGY_ENV` 에러 완전 차단
- 모든 주문 관련 키를 강제 설정하여 실거래 원천 차단
- rebuild 완전 금지로 DB candidate pool만 사용

---

### E-3: universe_count 고정 패치

**파일**: `trader/pb1_engine.py`

**변경**: candidate_pool_hit이면 universe_count를 120으로 고정

```python
# ✅ E-3: candidate_pool_hit이면 universe_count를 watchlist로 교체
if "candidate_pool_hit" in watchlist_source:
    universe_members = watchlist_members
    universe_count = len(watchlist_members)
    logger.info(
        "[E-3][CANDIDATE_POOL_HIT] universe_count fixed: %s -> %s",
        len(self._load_universe()), universe_count
    )
```

**효과**:
- `CRITICAL: universe_count=195 exceeds 150` 에러 완전 차단
- candidate pool 로드 시점부터 entry 파이프라인 전체가 120으로 인식
- 미너비니 필터가 끝까지 정상 실행

---

## C. 실행 방법

### 1. 로컬 확인 (이미 완료)
```bash
cd /workspaces/Jeihyuck-rolling-k-auto-trade-KIS-refacored
git status
# 변경사항 확인됨
```

### 2. Git 커밋 & 푸시 (이미 완료)
```bash
git add .github/workflows/trade-runner.yml trader/pb1_engine.py
git commit -m "diag: force trade-tick on PAPER, no rebuild, candidate pool fixed"
git push origin nullim
```

**Commit**: `90b7786`

### 3. GitHub Actions 실행

1. GitHub repo → **Actions** 탭
2. **Trade Runner (Unified: Candidate Pool + Trade Tick)** 선택
3. **Run workflow** 클릭
4. 입력값:
   - `run_mode`: **trade**
   - `env`: **PAPER**
   - `candidate_force_rebuild`: **0**
   - `live_trading_enabled`: **0**
   - `dry_run`: **1**
5. **Run workflow** 클릭

---

## D. 예상 로그 (정상 실행 시)

```
[CANDIDATE_POOL][LOAD] using latest=2026-02-02 size=120
[PB1][WATCHLIST] loaded=120 source=candidate_pool_hit
[E-3][CANDIDATE_POOL_HIT] universe_count fixed: 195 -> 120
[ENTRY][SCAN_UNIVERSE] source=candidate_pool scan_count=120 (universe_count=120 watchlist_count=120)
```

**핵심 지표**:
- ✅ `source=candidate_pool_hit`
- ✅ `universe_count=120` (NOT 195)
- ✅ `scan_count=120`
- ✅ CRITICAL 에러 없음
- ✅ 미너비니 필터/스코어링 끝까지 실행

---

## E. 문제 해결 완료 요약

| 문제 | 원인 | 해결 |
|------|------|------|
| trade-tick이 스킵됨 | workflow if 조건에 upstream 의존성 | E-1: if 조건 단순화 |
| KIS_ENV != STRATEGY_ENV | 환경 변수 불일치 | E-2: PAPER 강제 고정 |
| universe_count=195 exceeds 150 | candidate pool 로드 후에도 universe_count가 195 유지 | E-3: candidate_pool_hit 시 universe_count=120 고정 |
| 유니버스/후보군 재생성 | FORCE_REBUILD=1 | E-2: FORCE_REBUILD=0 강제 |
| 실거래 주문 우려 | LIVE_TRADING_ENABLED=1 | E-2: 모든 주문 키를 0/1로 강제 |

---

## F. 최종 확인 체크리스트

- [x] E-1: trade-tick job if 조건 수정 (upstream 의존성 제거)
- [x] E-2: KIS_ENV=PAPER, STRATEGY_ENV=PAPER 강제
- [x] E-2: 주문 절대 금지 (모든 키 강제)
- [x] E-2: UNIVERSE_FORCE_REBUILD=0 강제
- [x] E-3: candidate_pool_hit → universe_count=120 고정
- [x] Git commit & push (90b7786)

---

## G. 실행 후 확인사항

1. **Actions 로그**에서 확인:
   - `[CANDIDATE_POOL][LOAD]` 로그
   - `[E-3][CANDIDATE_POOL_HIT] universe_count fixed` 로그
   - `universe_count=120` (NOT 195)
   - CRITICAL 에러 없음

2. **미너비니 필터** 완료 확인:
   - `[MINERVINI][FILTER]` 로그
   - `[MINERVINI][SCORE]` 로그
   - 최종 후보 선정 로그

3. **주문 0** 확인:
   - 로그에 `[ORDER]` 또는 `[TRADE]`가 없어야 함
   - `DRY_RUN=1`, `LIVE_TRADING_ENABLED=0` 로그 확인

---

## H. 추가 안전장치

**코드에 강제 안전장치를 넣었으므로**:
- 수동 실행 시마다 안전하게 덮어씀
- push해도 기존 설정과 섞이지 않음
- 항상 `trade / PAPER / 0 / 0 / 1` 보장

---

**작업 완료 시각**: 2026-02-02  
**Commit**: `90b7786`  
**Branch**: `nullim`  
**Status**: ✅ Ready to Run
