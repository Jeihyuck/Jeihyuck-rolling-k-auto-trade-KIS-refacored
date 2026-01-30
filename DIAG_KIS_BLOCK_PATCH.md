# ✅ DIAG 모드 KIS HTTP 호출 완전 차단 패치

## 🎯 목표

1. **장중이면 LIVE, 장 종료면 자동 DIAG** (이미 AUTO가 하고 있음 — 유지)
2. **DIAG에서는 KIS HTTP를 절대 호출하지 않음**
3. **DIAG에서도 PB1 full path 실행** (단, 주문/실매매는 DRY_RUN 유지)
4. **watchlist_builder가 KIS 일봉을 "직접" 부르는 경로를 DIAG에서만 제거** (=우회)
5. **LIVE 로직/데이터 소스/호출 흐름은 절대 변경하지 않음**

---

## 📝 변경 사항 요약

### 1. 신규 파일: `trader/diag_utils.py`

**목적**: DIAG 모드 판정 함수 제공

```python
def is_diag_mode() -> bool:
    """
    DIAG 판단:
    - STRATEGY_MODE=DIAG 이거나
    - KIS_HTTP_ENABLED=0 이면 무조건 DIAG 취급
    
    LIVE는 절대 건드리지 않기 위해, 'DIAG일 때만' 우회 로직을 활성화한다.
    """
    mode = (os.getenv("STRATEGY_MODE") or "").upper().strip()
    kis_http = (os.getenv("KIS_HTTP_ENABLED") or "").strip()
    return mode == "DIAG" or kis_http == "0"
```

**특징**:
- ✅ LIVE에는 절대 영향 없음 (DIAG일 때만 True 반환)
- ✅ 재사용 가능한 유틸리티 함수

---

### 2. 수정: `trader/data/ohlcv_provider.py`

**위치**: `KISOHLCVProvider.get_ohlcv()` 메소드

**변경 전**:
```python
# KIS fallback
fallback_allowed = ALLOW_KIS_DAILY_FALLBACK or (days <= 120)
if not fallback_allowed:
    logger.warning("[OHLCV][DB][NO_FALLBACK] symbol=%s days=%d", symbol, days)
    return OHLCVResult(pd.DataFrame(), {...})

# KIS 호출
try:
    candles = self.kis.get_daily_candles(symbol, count=max(days, 120))
    ...
```

**변경 후**:
```python
# KIS fallback
# ✅ DIAG 모드에서는 절대로 KIS 호출하지 않음 (LIVE 환경만 허용)
if is_diag_mode():
    logger.warning("[OHLCV][DIAG][KIS_BLOCKED] symbol=%s days=%d -> returning empty (no KIS in DIAG)", symbol, days)
    return OHLCVResult(pd.DataFrame(), {"provider": self.name, "source": "db", "error": "diag_mode_kis_blocked", "volume_missing": True})

fallback_allowed = ALLOW_KIS_DAILY_FALLBACK or (days <= 120)
if not fallback_allowed:
    logger.warning("[OHLCV][DB][NO_FALLBACK] symbol=%s days=%d", symbol, days)
    return OHLCVResult(pd.DataFrame(), {...})

# KIS 호출 (LIVE만)
...
```

**효과**:
- ✅ DIAG 모드에서 `watchlist_builder` → `ohlcv_provider` → `KIS API` 경로 완전 차단
- ✅ LIVE 모드에서는 기존 fallback 로직 100% 유지
- ✅ DB 조회 실패 시에도 KIS로 넘어가지 않고 빈 DataFrame 반환

---

### 3. 기존 검증: `trader/pb1_engine.py`

**DIAG_FULL_EXEC 로직 확인** (라인 4276-4310):

```python
# ✅ DIAG_FULL_EXEC: entry gate 우회
diag_full_exec = bool(getattr(self, "diag_full_exec", False))

if not entry_allowed:
    # ✅ DIAG_FULL_EXEC: entry gate 우회
    if diag_full_exec and entry_reason in ("entry_cutoff", "window_blocked", "phase_manage", "entry_disabled"):
        logger.warning(
            "[PB1][DIAG_FULL_EXEC] override entry gate reason=%s -> allow entry pipeline (dry_run=%s)",
            entry_reason,
            self.dry_run
        )
        entry_allowed = True
        entry_reason = "diag_full_exec_override"
```

**상태**: ✅ 이미 구현 완료
- DIAG && PB1_DIAG_FULL_EXEC && dry_run=True → entry_gate 우회 허용
- LIVE에서는 절대 영향 없음

---

### 4. 기존 검증: `.github/workflows/trade-runner.yml`

**Resolve market-aware runtime mode** 스텝 (라인 300-320):

```yaml
- name: Resolve market-aware runtime mode
  id: market_mode
  run: |
    ...
    mode = "LIVE" if in_market else "DIAG"
    kis_http = "1" if in_market else "0"
    ...
    with open(os.environ["GITHUB_ENV"], "a", encoding="utf-8") as f:
        f.write(f"EFFECTIVE_STRATEGY_MODE={mode}\n")
        f.write(f"KIS_HTTP_ENABLED={kis_http}\n")
```

**Run PB1** 스텝 (라인 338-348):

```yaml
- name: Run PB1 (AUTO -> LIVE in-market, DIAG after-hours)
  env:
    STRATEGY_MODE: ${{ env.EFFECTIVE_STRATEGY_MODE }}
    KIS_HTTP_ENABLED: ${{ env.KIS_HTTP_ENABLED }}
    PB1_DIAG_FULL_EXEC: "1"
    PB1_WINDOW_OVERRIDE: "day"
    PB1_ALLOW_OUTSIDE_WINDOWS: "1"
  run: |
    python -m trader.pb1_runner
```

**상태**: ✅ 이미 구현 완료
- 장중이면 `LIVE + KIS_HTTP_ENABLED=1`
- 장 종료면 `DIAG + KIS_HTTP_ENABLED=0 + PB1_DIAG_FULL_EXEC=1`
- 5분 제약 제거됨 (schedule 주석 처리)

---

## 🔍 최종 검증 체크리스트

### DIAG 실행 시 반드시 만족해야 하는 로그

#### 환경 변수
- ✅ `STRATEGY_MODE=DIAG`
- ✅ `KIS_HTTP_ENABLED=0`
- ✅ `PB1_DIAG_FULL_EXEC=1`

#### Watchlist 로그
- ✅ `[OHLCV][DIAG][KIS_BLOCKED] symbol=XXX days=XXX -> returning empty (no KIS in DIAG)`
- ❌ `inquire-daily-itemchartprice` 같은 KIS endpoint 호출 경고가 watchlist_builder 경로에서 더 이상 나오면 **실패**

#### Entry Pipeline 로그
- ✅ `[PB1][DIAG_FULL_EXEC] override entry gate reason=entry_cutoff -> allow entry pipeline (dry_run=True)`
- ✅ 후보 선정/스코어/필터/리밸런싱/리포트/DB 기록이 돌아가야 함
- ✅ 주문은 DRY_RUN이라 체결 없음

---

## 🚀 DIAG에서 "정상 풀패스"의 정의

1. **엔진이 끝까지 실행되고 종료**
2. **주문은 DRY_RUN이라 체결 없지만,**
3. **후보 선정/스코어/필터/리밸런싱/리포트/DB 기록이 돌아가야 함**

---

## 🔧 테스트 방법

### 로컬 테스트 (DIAG 모드)

```bash
export STRATEGY_MODE=DIAG
export KIS_HTTP_ENABLED=0
export PB1_DIAG_FULL_EXEC=1
export DRY_RUN=1

python -m trader.pb1_runner
```

**예상 로그**:
```
[AUTO] ... -> STRATEGY_MODE=DIAG ... KIS_HTTP_ENABLED=0
[OHLCV][DIAG][KIS_BLOCKED] symbol=005930 days=120 -> returning empty (no KIS in DIAG)
[PB1][DIAG_FULL_EXEC] override entry gate reason=entry_cutoff -> allow entry pipeline (dry_run=True)
[WATCHLIST][BUILD][DONE] as_of=2026-01-30 final_members=X
[PB1][BUY][EVAL] candidates=X
...
```

### GitHub Actions 테스트

**Workflow Dispatch 수동 실행**:
1. GitHub Actions 탭 → "Trade Runner (DB-Only SSOT) [5m tick]" 선택
2. "Run workflow" 클릭
3. DIAG_FULL_REHEARSAL: `1`
4. DIAG_REHEARSAL_KIS_CALLS: `0`
5. "Run workflow" 실행

**예상 동작**:
- `Resolve market-aware runtime mode` 스텝에서 자동으로 `STRATEGY_MODE=DIAG` 설정
- `KIS_HTTP_ENABLED=0` 설정
- `PB1_DIAG_FULL_EXEC=1` 환경변수로 entry gate 우회
- Watchlist 빌드 시 KIS 호출 없이 DB/FDR만 사용
- Entry pipeline 실행 (주문은 DRY_RUN)

---

## 📊 영향 범위

### ✅ 변경 영향 있는 부분 (DIAG Only)

1. **trader/data/ohlcv_provider.py**
   - DIAG 모드에서 KIS fallback 차단
   - DB 조회 실패 시 빈 DataFrame 반환

### ❌ 변경 영향 없는 부분 (LIVE 100% 보존)

1. **watchlist_builder.py**: 변경 없음 (ohlcv_provider를 통해서만 데이터 조회)
2. **pb1_engine.py**: DIAG_FULL_EXEC 로직 이미 존재 (기존 유지)
3. **trade-runner.yml**: 이미 AUTO 모드로 LIVE/DIAG 자동 전환 구현됨
4. **LIVE 모드의 모든 로직**: 100% 기존과 동일

---

## 🎉 결론

이 패치는 **DIAG 모드에서만** KIS HTTP 호출을 차단하여:

1. ✅ **DIAG 풀패스 실행 가능** (watchlist 생성 → 후보 선정 → 리밸런싱 → 리포트)
2. ✅ **LIVE 환경에는 절대 영향 없음** (기존 로직 100% 유지)
3. ✅ **GitHub Actions에서 자동으로 장중/장후 모드 전환**
4. ✅ **5분 제약 제거** (schedule 주석 처리로 해결)

**Git 브랜치**: `fix/diag-fdr-watchlist-only`

**커밋 메시지**:
```
feat: DIAG 모드에서 KIS HTTP 호출 완전 차단 (watchlist FDR/DB only)

✅ 변경사항:
1. trader/diag_utils.py 신규 생성
2. trader/data/ohlcv_provider.py 수정
```

**다음 단계**:
1. 로컬에서 DIAG 모드 테스트
2. GitHub Actions에서 workflow_dispatch 테스트
3. 검증 완료 후 main 브랜치로 병합
