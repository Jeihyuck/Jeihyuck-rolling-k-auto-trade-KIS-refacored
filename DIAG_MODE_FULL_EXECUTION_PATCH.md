# DIAG 모드 완전 실행 패치 완료

## 🎯 핵심 문제 해결

### 문제점
- **장외면 무조건 스킵**: DIAG 모드에서도 `if not trading_day: return`으로 엔진이 실행되지 않음
- **"DIAG 동작 없음"**: 로그에 `[NO_TRADE] Outside trading session ... Skip` 만 남고 종료

### 해결책
- **LIVE만 스킵, DIAG는 계속**: 장외 스킵 로직을 `mode == "LIVE"` 조건으로 한정
- **KIS API만 차단**: `KISBlockedError`로 KIS 호출만 막고 로직은 끝까지 실행
- **DIAG 요약 로그**: 실행 결과를 `[DIAG_SUMMARY]`로 명확하게 출력

---

## ✅ 적용된 패치

### 1. pb1_runner.py: 장외 스킵을 LIVE에만 적용

**Before** (line 1055):
```python
if not trading_day:
    # LIVE, DIAG 구분 없이 무조건 return
    if loop_mode:
        logger.info("[PB1][LOOP] non-trading-day -> skip")
    else:
        logger.info("[PB1][SKIP] non-trading-day -> skip")
    return [], False, {}, phase_for_log, "NONTRADING_DAY_EXIT"
```

**After**:
```python
if not trading_day:
    if mode == "DIAG":
        # ✅ DIAG는 계속 실행 (KIS API만 차단)
        logger.info("[PB1][DIAG][NONTRADING_DAY] mode=DIAG continue execution (KIS blocked)")
        # nontrading_smoke 실행 후 엔진도 계속 진행
        # ✅ return 하지 않음
    else:
        # LIVE만 스킵
        logger.info("[PB1][SKIP] non-trading-day -> skip (LIVE mode)")
        return [], False, {}, phase_for_log, "NONTRADING_DAY_EXIT"
```

### 2. pb1_runner.py: DIAG 요약 로그 추가

**Before** (line 1662):
```python
result_status = result.status if result else "UNKNOWN"
return touched_files, did_work, metrics, phase_for_log, result_status
```

**After**:
```python
result_status = result.status if result else "UNKNOWN"

# ✅ DIAG 모드 실행 요약 로그
if mode == "DIAG":
    logger.info(
        "[DIAG_SUMMARY] status=%s phase=%s did_work=%s metrics=%s",
        result_status,
        phase_for_log,
        did_work,
        metrics,
    )
    if engine_runner:
        logger.info(
            "[DIAG_SUMMARY][ENGINE] top_candidates=%s current_code=%s",
            len(engine_runner.top_candidates) if hasattr(engine_runner, 'top_candidates') else 0,
            engine_runner.current_code if hasattr(engine_runner, 'current_code') else None,
        )

return touched_files, did_work, metrics, phase_for_log, result_status
```

### 3. pb1_engine.py: KISBlockedError import

```python
from trader.kis_wrapper import KisAPI, KISBlockedError
```

---

## 🔍 DIAG 모드 실행 검증

### 성공 시 반드시 보이는 로그 (8개)

1. **모드 전환 확인**
   ```
   [PB1][MODE] mode_env=AUTO resolved=DIAG fixed_in_env=True
   [AUTO_MODE] mode=AUTO resolved=DIAG is_market_open=False
   ```

2. **장외 계속 실행 확인**
   ```
   [PB1][DIAG][NONTRADING_DAY] mode=DIAG continue execution (KIS blocked)
   ```

3. **엔진 시작 확인**
   ```
   [PB1][RUN-START] ... STRATEGY_MODE=DIAG ...
   ```

4. **유니버스/워치리스트 로드**
   ```
   [PB1][WATCHLIST][LOAD] env=... strategy=... as_of=... members=...
   ```
   또는
   ```
   [WATCHLIST][BUILD][DONE] n=... stored=...
   ```

5. **후보 선정 (선택적, 엔진 동작에 따라)**
   ```
   [ENTRY][PIPE][START] ...
   [CANDIDATE] code=... score=... reason=...
   ```

6. **주문 계획 (선택적)**
   ```
   [ORDER_PLAN] budget=... qty=...
   ```

7. **DIAG 요약 (필수)**
   ```
   [DIAG_SUMMARY] status=... phase=... did_work=...
   ```

8. **엔진 상태 (필수)**
   ```
   [DIAG_SUMMARY][ENGINE] top_candidates=... current_code=...
   ```

### 실패 시 보이는 로그

```
[PB1][SKIP] non-trading-day -> skip (LIVE mode)
[PB1][EXIT] reason=nontrading_day_exit mode=LIVE
```

→ **이 로그가 보이면 DIAG 모드가 아닌 LIVE 모드로 실행된 것**

---

## 🚀 실행 방법

### 장외 시간에 DIAG 모드 테스트

```bash
# 1. AUTO 모드로 실행 (장외면 자동 DIAG)
export STRATEGY_MODE=AUTO
export PB1_LOOP_ENABLED=0  # 한 번만 실행
python -m trader.pb1_runner

# 2. 강제 DIAG 모드 (언제든지)
export STRATEGY_MODE=DIAG
export PB1_LOOP_ENABLED=0
python -m trader.pb1_runner
```

### 로그 확인 포인트

```bash
# 필수 로그 확인
grep "\[PB1\]\[DIAG\]\[NONTRADING_DAY\]" logs/*.log
grep "\[DIAG_SUMMARY\]" logs/*.log

# KIS API 차단 확인
grep "KISBlockedError" logs/*.log
grep "KIS API blocked in DIAG mode" logs/*.log
```

---

## 📊 예상 동작 흐름

### DIAG 모드 (장외)

```
00:00 → GitHub Actions 시작
00:00 → AUTO 모드 → DIAG 결정
00:00 → [AUTO_MODE] mode=AUTO resolved=DIAG
00:00 → run_once() 실행 시작
00:00 → [PB1][DIAG][NONTRADING_DAY] mode=DIAG continue execution
00:00 → nontrading_smoke 실행 (선택)
00:00 → 유니버스/워치리스트 로드 (DB)
00:00 → 엔진 실행 시작
00:00 → 후보 선정/스코어링 (DB OHLCV 기반)
00:00 → 돌파 조건 판단
00:00 → 주문서 생성 계획
00:00 → KIS API 호출 시도 → KISBlockedError 발생
00:00 → [DIAG_SUMMARY] status=... phase=... did_work=...
00:00 → 종료
```

### LIVE 모드 (장중)

```
09:00 → AUTO 모드 → LIVE 결정
09:00 → [AUTO_MODE] mode=AUTO resolved=LIVE
09:00 → 루프 시작 (UNTIL_CLOSE)
09:00~15:12 → 30초마다 run_once() 실행
  → 후보 선정
  → 돌파 감지
  → KIS API 호출 (실주문)
15:12 → deadline 도달 → 종료
```

---

## 🎯 다음 단계

### 1. DIAG 모드 검증
- [ ] 장외 시간에 수동 실행
- [ ] `[DIAG_SUMMARY]` 로그 확인
- [ ] `KISBlockedError` 발생 확인
- [ ] 엔진이 끝까지 실행되었는지 확인

### 2. LIVE 모드 검증
- [ ] 장중 시간에 수동 실행
- [ ] UNTIL_CLOSE 루프 동작 확인
- [ ] 실주문 정상 실행 확인

### 3. 자동 시동 활성화
- [ ] GitHub Actions `schedule` 주석 해제
- [ ] 며칠간 모니터링
- [ ] DIAG/LIVE 자동 전환 확인

---

## 📝 변경된 파일

1. `trader/pb1_runner.py`
   - `run_once()`: 장외 스킵을 LIVE에만 적용
   - `run_once()`: DIAG 요약 로그 추가

2. `trader/pb1_engine.py`
   - `KISBlockedError` import 추가

3. `CONTINUOUS_LOOP_IMPLEMENTATION.md`
   - DIAG 모드 완전 실행 설명 추가
   - 검증 체크리스트 추가

---

**구현 완료일**: 2026-01-30  
**작성자**: GitHub Copilot  
**버전**: v1.1 (DIAG 완전 실행 패치)
