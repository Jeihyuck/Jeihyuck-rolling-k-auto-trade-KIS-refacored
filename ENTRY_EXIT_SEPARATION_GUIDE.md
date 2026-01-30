# PB1 엔진 EXIT/ENTRY 분리 및 진단 강화 가이드

## 개요

이 문서는 PB1 엔진의 EXIT/ENTRY PASS 완전 분리, Minervini 필터 진단 강화, dedupe 키 개선 작업의 내용과 검증 방법을 설명합니다.

## 주요 변경사항

### 1. ENTRY Pipeline 계측 로그 (8종 필수) ✅

**변경 위치**: `trader/pb1_engine.py::run()`, `_compute_candidates()`

**목적**: "매수 0건" 문제 진단을 위한 완전한 파이프라인 가시성 확보

**추가된 로그**:

1. **[ENTRY][CANDIDATES]** (line ~4346)
   ```python
   logger.info(
       "[ENTRY][CANDIDATES] trace=%s start universe=%s data_ok=%s dt_minervini=%.2f",
       trace_id, len(members), data_ok_count, dt_minervini
   )
   ```
   - Minervini 실행 직후 초기 카운트
   - `dt_minervini`: Minervini 실행 시간 (초)

2. **[MINERVINI][ENTER]** (line ~1371)
   ```python
   logger.info(
       "[MINERVINI][ENTER] trace=%s universe=%s rs_min=%.0f vcp_lookback=%s",
       trace_id, before_minervini, rs_min_percentile * 100, VCP_LOOKBACK
   )
   ```
   - Minervini 필터 호출 확인
   - `t_minervini_enter = time_module.monotonic()` 시작 시간 기록

3. **[MINERVINI][APPLY]** (line ~1612)
   ```python
   logger.info(
       "[MINERVINI][APPLY] universe=%s rs_min_pctile=%.0f vcp_lookback=%s lookback_days=%s/%s",
       before_minervini, rs_min_percentile * 100, VCP_LOOKBACK, RS_LOOKBACK_DAYS, RS_LOOKBACK2_DAYS
   )
   ```
   - Minervini 필터 설정 기록

4. **[MINERVINI][RESULT]** (line ~1621)
   ```python
   logger.info(
       "[MINERVINI][RESULT] before=%s after_rs=%s dropped_rs=%s after_vcp=%s dropped_vcp=%s both_pass=%s skipped=%s degraded=%s sample=%s",
       before_minervini, after_rs, rs_fail_count, after_vcp, vcp_fail_count, both_pass, skipped_count, degraded_count, sample_codes
   )
   ```
   - RS/VCP 필터 통과 결과 상세

5. **[MINERVINI][EXIT]** (line ~1633)
   ```python
   logger.info(
       "[MINERVINI][EXIT] passed=%s failed=%s dt=%.2f",
       both_pass, before_minervini - both_pass, dt_minervini_total
   )
   ```
   - Minervini 최종 요약 + 시간 측정
   - `dt_minervini_total = time_module.monotonic() - t_minervini_enter`

6. **[ENTRY][PB1_FILTER]** (line ~4366)
   ```python
   logger.info(
       "[ENTRY][PB1_FILTER] trace=%s before=%s after=%s dt=%.2f tier=%s relax_passes=%s",
       trace_id, data_ok_count, setup_ok_count, dt_pb1_filter, selected_tier, relax_passes_used
   )
   ```
   - PB1 필터 성능 측정
   - `dt_pb1_filter = time_module.monotonic() - t_pb1_start`

7. **[ORDER][BUILD]** (line ~4917)
   ```python
   logger.info(
       "[ORDER][BUILD] trace=%s count=%s symbols=%s",
       trace_id, len(orderable_candidates), ",".join(order_symbols[:10])
   )
   ```
   - 주문 생성 단계
   - `dt_order_build = time_module.monotonic() - t_order_build`

8. **[ORDER][SUBMIT]** (line ~4948)
   ```python
   logger.info(
       "[ORDER][SUBMIT] trace=%s submitted=%s failed=%s dt_build=%.2f dt_submit=%.2f",
       trace_id, submitted_count, failed_count, dt_order_build, dt_order_submit
   )
   ```
   - 주문 제출 결과
   - `dt_order_submit = time_module.monotonic() - t_order_submit`

**추가 진단 로그**:

- **[ENTRY][NO_BUY]** (line ~4737)
  ```python
  logger.warning(
      "[ENTRY][NO_BUY] trace=%s reason=%s candidates=%s setup_ok=%s after_risk=%s cash=%s drop_top3=%s",
      trace_id, no_buy_reason, len(candidates), len(setup_ok_codes), after_risk_check_count, available_cash_krw, drop_reason_counter.most_common(3)
  )
  ```
  - `orderable_candidates`가 0일 때 이유 분석
  - **Reason Codes**:
    - `EMPTY_AFTER_MINERVINI`: Minervini 후 후보 0
    - `EMPTY_AFTER_PB1_FILTER`: PB1 필터 후 후보 0
    - `OHLCV_INSUFFICIENT`: data_ok 후보 0
    - `ORDER_SUBMIT_BLOCKED`: entry_allowed=False
    - `CASH_INSUFFICIENT`: available_cash_krw <= 0
    - `EMPTY_AFTER_RANK`: after_risk_check_count == 0
    - `DROP:xxx`: drop_reason_counter 최빈값

**시간 측정 변수**:
- `t_minervini_enter`, `dt_minervini`: Minervini 필터 시간
- `t_pb1_start`, `dt_pb1_filter`: PB1 필터 시간
- `t_order_build`, `dt_order_build`: 주문 생성 시간
- `t_order_submit`, `dt_order_submit`: 주문 제출 시간

**trace_id 형식**:
```python
trace_id = f"{run_id}:{self._today}:{timestamp_ms}"
```
- 모든 로그에 `trace=%s` 파라미터 포함
- 동일 실행의 모든 로그를 추적 가능

---

### 2. EXIT/ENTRY PASS 완전 분리 ✅

**변경 위치**: `trader/pb1_engine.py::run()`

**목적**: 매도(EXIT)와 매수(ENTRY)가 동일 tick에서 서로 간섭하지 않고 독립적으로 실행되도록 보장

**구현 내용**:
- EXIT PASS와 ENTRY PASS를 완전히 분리된 try-except 블록으로 실행
- EXIT PASS 실패 시에도 ENTRY PASS는 반드시 실행
- 각 PASS의 시작/종료 로그 추가

**추가된 로그**:
```
[PASS][EXIT][START] phase=... existing_positions=...
[PASS][EXIT][END] sells=... skipped_dup=...
[PASS][ENTRY][START] phase=... entry_allowed=... slots_remaining=...
[PASS][ENTRY][END] buys=... skipped_dup=... candidates=...
```

### 3. Dedupe 키 스키마 개선 ✅

**변경 위치**: `trader/pb1_engine.py::_client_order_key()`

**목적**: EXIT와 ENTRY의 dedupe 키가 완전히 분리되어 교차 간섭 방지

**기존 키 형식**:
```
{env}:{strategy}:{date}:{code}:{SIDE}
예: live:pb1_pullback_close:2026-01-30:323280:SELL
```

**신규 키 형식**:
```
{env}:{strategy}:{date}:{code}:{ACTION}:{SIDE}:{stage}:{window}:{mode}

예시:
- EXIT: live:pb1_pullback_close:2026-01-30:323280:EXIT:SELL:TP1:day:1
- ENTRY: live:pb1_pullback_close:2026-01-30:005930:ENTRY:BUY:PB1:day:1
```

**효과**:
- ACTION 필드(EXIT/ENTRY)로 완전 분리
- 동일 종목에 대해 SELL과 BUY가 서로 다른 키 공간 사용
- stage, window, mode까지 포함하여 더 세밀한 구분 가능

### 3. ENTRY 후보 0일 때 이유 로그 추가 ✅

**변경 위치**: `trader/pb1_engine.py::run()`

**목적**: 매수 후보가 0개일 때 명확한 원인 진단

**추가된 로그**:
```python
[ENTRY][CANDIDATES] start universe=195 data_ok=...
[ENTRY][NO_BUY] reason=... candidates=... setup_ok=... after_risk=... drop_top3=...
```

**가능한 이유 코드**:
- `minervini_empty`: Minervini 필터 통과 종목 없음
- `pb1_conditions_empty`: PB1 pullback 조건 통과 종목 없음
- `ohlcv_missing`: OHLCV 데이터 부족
- `entry_disabled`: ENTRY 비활성화 상태
- `cash_insufficient`: 예수금 부족
- `risk_filter`: 리스크 관리 필터 탈락
- `drop:{reason}`: 기타 탈락 사유 (가장 빈번한 사유 표시)

### 4. Minervini 필터 로그 강화 및 DEGRADED 모드 지원 ✅

**변경 위치**: `trader/pb1_engine.py::_compute_candidates()`

**목적**: Minervini 필터가 제대로 작동하는지 확인하고, 데이터 부족 시 정책을 명확히 함

**환경변수**:
- `MINERVINI_DEBUG=1`: 상세 디버그 로그 활성화
- `MINERVINI_DEGRADED_OK=0|1`: 데이터 부족 시 스킵 허용 여부
  - `0` (기본값): STRICT 모드 - 벤치마크 데이터 부족 시 후보 비우기
  - `1`: DEGRADED 모드 - 벤치마크 데이터 부족해도 RS 계산 시도

**추가된 로그**:
```python
[MINERVINI][SKIP] reason=insufficient_benchmark benchmark_rows=... min_required=126 pass_through=...
[MINERVINI][STRICT] benchmark data insufficient -> clear candidates (set MINERVINI_DEGRADED_OK=1 to allow)
[MINERVINI][APPLY] universe=... rs_min_pctile=... vcp_lookback=... lookback_days=.../...
[MINERVINI][RESULT] before=... after_rs=... dropped_rs=... after_vcp=... dropped_vcp=... both_pass=... skipped=... degraded=... sample=[...]
```

### 5. NO_TRADE 디버그 모드 지원 ✅

**변경 위치**: `trader/pb1_engine.py::_place_entry()`, `_place_entry_close()`

**목적**: 실제 주문 없이 후보 생성 과정만 확인

**환경변수**:
- `NO_TRADE=1`: 주문 전송 스킵, 로그만 출력

**추가된 로그**:
```python
[TRADE][DECISION][BUY] ... no_trade=True
[TRADE][SKIP][NO_TRADE] code=... qty=... reason=NO_TRADE_MODE
```

## 검증 방법

### 1. 주문 없이 후보/필터만 확인 (STRICT 모드)

```bash
export NO_TRADE=1
export MINERVINI_DEBUG=1
export MINERVINI_DEGRADED_OK=0
python -m trader.pb1_runner --env live --strategy best_k_meta --once --window day --phase entry
```

**기대 로그**:
- `[ENTRY][CANDIDATES] start universe=...`
- `[MINERVINI][APPLY] universe=...`
- `[MINERVINI][RESULT] before=... after_rs=... dropped_rs=...`
- `[ENTRY][NO_BUY] reason=...` (후보가 0일 경우)

**해석**:
- Minervini RESULT 로그가 나오면 필터가 정상 작동 중
- NO_BUY 로그의 reason으로 후보 0 원인 파악 가능

### 2. DEGRADED 모드로 데이터 문제 분리

```bash
export NO_TRADE=1
export MINERVINI_DEBUG=1
export MINERVINI_DEGRADED_OK=1
python -m trader.pb1_runner --env live --strategy best_k_meta --once --window day --phase entry
```

**해석**:
- DEGRADED_OK=0에서 후보 0개, DEGRADED_OK=1에서 후보 생성
  → **데이터 부족이 원인 확정**
- 둘 다 0개
  → **PB1 조건이 너무 빡셈 또는 universe 자체 문제**

### 3. EXIT/ENTRY 동시 실행 확인 (모의 환경)

```bash
export NO_TRADE=0
export MINERVINI_DEBUG=1
python -m trader.pb1_runner --env live --strategy best_k_meta --once --window day --phase entry
```

**기대 로그**:
```
[PASS][EXIT][START] ...
[PASS][EXIT][END] sells=... skipped_dup=...
[PASS][ENTRY][START] ...
[PASS][ENTRY][END] buys=... skipped_dup=... candidates=...
```

**검증 포인트**:
- EXIT와 ENTRY 로그가 **둘 다** 출력되어야 함
- EXIT에서 예외 발생해도 ENTRY는 반드시 실행되어야 함

### 4. Dedupe 키 분리 확인

EXIT와 ENTRY가 같은 종목에 대해 독립적으로 동작하는지 확인:

```bash
# 로그에서 client_order_key 확인
grep "client_order_key" logs/pb1_*.log | tail -20
```

**기대 결과**:
- EXIT: `...:323280:EXIT:SELL:TP1:day:1`
- ENTRY: `...:323280:ENTRY:BUY:PB1:day:1`

키가 완전히 다르므로 교차 간섭 없음을 확인

## PR 체크리스트

- [x] EXIT PASS 실패 시에도 ENTRY PASS 로그가 항상 실행됨
- [x] ENTRY 후보 0일 때 [ENTRY][NO_BUY] reason 로그 출력
- [x] Minervini [RESULT] 로그가 반드시 출력됨 (APPLY 이후)
- [x] dedupe 키에 ACTION(ENTRY/EXIT) 포함되어 BUY/SELL 교차 간섭 없음
- [x] NO_TRADE=1로 후보 생성만 빠르게 확인 가능
- [x] MINERVINI_DEGRADED_OK=0 기본값 (STRICT 모드 유지)

## 안전장치

1. **EXIT 우선 실행**: 리스크 관리를 위해 EXIT PASS를 항상 먼저 실행
2. **ENTRY 독립성**: EXIT PASS 예외/dup가 ENTRY PASS를 막지 않음
3. **Dedupe 완전 분리**: EXIT/ENTRY가 서로 다른 키 공간 사용
4. **STRICT 기본값**: MINERVINI_DEGRADED_OK=0 기본값으로 운영 안정성 확보
5. **NO_TRADE 지원**: 프로덕션 영향 없이 디버깅 가능

## 트러블슈팅

### Q1: Minervini RESULT 로그가 안 나옵니다

**원인**: `_compute_candidates()` 호출이 스킵되거나 예외 발생

**해결**:
1. `MINERVINI_DEBUG=1` 설정하고 재실행
2. `[ENTRY][CANDIDATES] start` 로그 확인
3. 예외 스택 트레이스 확인

### Q2: 후보가 항상 0개입니다

**진단**:
```bash
# STRICT vs DEGRADED 비교
MINERVINI_DEGRADED_OK=0 python -m trader.pb1_runner ... 2>&1 | grep NO_BUY
MINERVINI_DEGRADED_OK=1 python -m trader.pb1_runner ... 2>&1 | grep NO_BUY
```

- STRICT=0, DEGRADED=0 → **Minervini 조건 또는 데이터 문제**
- STRICT>0, DEGRADED>0 → **PB1 pullback 조건 너무 빡셈**

### Q3: EXIT와 ENTRY가 서로 간섭합니다

**확인**:
```bash
grep "client_order_key" logs/*.log | grep -E "(EXIT|ENTRY)"
```

ACTION 필드가 제대로 분리되어 있는지 확인

## 마이그레이션 노트

기존 시스템에서 마이그레이션 시 주의사항:

1. **Dedupe 키 변경**: 기존 주문 키와 호환되지 않음 (의도된 동작)
2. **MINERVINI_DEGRADED_OK**: 기존 동작 유지하려면 `=1` 설정 필요
3. **EXIT/ENTRY 로그**: 로그 파싱 스크립트가 있다면 새 로그 형식 반영 필요

## 참고

- 관련 이슈: [링크 필요시 추가]
- 설계 문서: 이 파일의 개요 섹션 참조
- 환경변수 설정: `.env` 파일 또는 런타임 `export` 사용
