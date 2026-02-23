# Fix B: as_of 일관성 통합 수정 완료

**수정 날짜**: 2026-02-23  
**핵심 버그**: GUARD가 오늘 날짜(`trade_date`)로 final30을 검사하지만, 실제 데이터는 과거 날짜(`actual_as_of`)에 있어서 발생하는 SystemExit(2)

---

## 1️⃣ **Step 1-2: AsOfContext 구조체 생성** ✅

**파일**: `trader/time_utils.py`

```python
class AsOfContext:
    """
    Trade 엔진 입력의 as_of 컨텍스트를 단일 구조로 통일.
    
    - trade_date: 오늘 거래일 (예: 2026-02-23)
    - requested_as_of: derived_as_of (요청 기준, 예: 2026-02-20)
    - actual_as_of: DB/폴백 확정 (반드시 사용, 예: 2026-02-19) ✅
    - reason: override / intraday_use_prev_close / fallback 등
    """
```

**사용처**:
- `trader/pb1_engine.py`의 `run()` 메서드에서 계산/로깅용으로 사용 가능
- `trader/pb1_runner.py`에서 `universe_context.as_of_date` = actual_as_of로 자동 설정

---

## 2️⃣ **Step 3-4: Fix B 본체 - final30 GUARD/LOAD as_of 교체** ✅

**파일**: `trader/pb1_engine.py` (라인 5341-5391)

### 변경 전:
```python
as_of_final = get_as_of_date()  # ❌ 오늘 날짜 (예: 2026-02-23)
```

### 변경 후:
```python
# ✅ FIX B: actual_as_of를 universe_context에서 추출
if self._universe_context and self._universe_context.as_of_date:
    as_of_final = self._universe_context.as_of_date  # ✅ watchlist lock된 날짜 (예: 2026-02-19)
    reason = f"universe_context (source={...})"
else:
    as_of_final = get_as_of_date()
    reason = "fallback_today"

logger.info(
    "[PB1][ASOF][CONTEXT] as_of_final=%s reason=%s universe_context_available=%s",
    as_of_final,
    reason,
    bool(self._universe_context),
)
```

**효과**:
- TRADE 모드에서: final30 = 2026-02-19 (watchlist lock된 as_of)
- GUARD 검사: 2026-02-19 기준 (데이터 있음 ✅)
- 로그: `[PB1][ASOF][CONTEXT] as_of_final=2026-02-19 reason=universe_context (source=watchlist_final)`

---

## 3️⃣ **Step 5: final30 Missing 시 Fallback 구현** ✅

**파일**: `trader/pb1_engine.py` (메서드 `_load_entry_final30_or_abort()`)

### 변경 후:
```python
def _load_entry_final30_or_abort(self, as_of: str) -> list[str]:
    """
    ✅ FIX B-ENTRY: final30 로드 (없으면 watchlist_final_locked로 대체)
    """
    final30_codes = load_final30(self.env, as_of) or []
    if final30_codes:
        logger.info("[FINAL30][LOAD] as_of=%s count=%s source=final30_snapshot", as_of, len(final30_codes))
        return final30_codes
    
    # ✅ final30 스냅샷 없으면 watchlist_final(universe_context)로 대체
    if self._universe_context and self._universe_context.members:
        fallback_codes = [m.get("code") for m in self._universe_context.members if m.get("code")]
        fallback_source = self._universe_context.meta.get("source", "unknown") if self._universe_context.meta else "unknown"
        logger.warning(
            "[PB1][ENTRY][FINAL30_FALLBACK] as_of=%s final30_snapshot_missing -> using watchlist source=%s count=%s",
            as_of,
            fallback_source,
            len(fallback_codes),
        )
        return fallback_codes
    
    # 둘 다 없으면 abort
    logger.error("[PB1][ENTRY][GUARD] final30_missing -> abort as_of=%s env=%s (no watchlist_final fallback available)", ...)
    raise SystemExit(2)
```

**효과**:
- final30 스냅샷 없어도: watchlist_final로 대체하여 진입 가능
- 로그: `[PB1][ENTRY][FINAL30_FALLBACK] ... source=watchlist_final count=30`
- Abort 방지 ✅

---

## 4️⃣ **Fix C: LIVE 플래그 정합성 검증** ✅

**파일**: `trader/pb1_runner.py`

### 변경 1 (라인 1095-1120): 충돌 감지 시 경고 + 강등
```python
if violations:
    logger.error("="*80)
    logger.error("[PB1][LIVE][CONFLICT] CRITICAL: intended_live=True but conflicts detected:")
    for v in violations:
        logger.error(f"  - {v}")
    logger.error("  ACTION: Downgrading mode to DIAG, blocking all orders, calculation mode only")
    logger.error("="*80)
    intended_live = False  # ✅ LIVE 플래그 강등
```

### 변경 2 (라인 1169-1178): mode 강등
```python
# ✅ FIX C: LIVE 플래그 충돌 감지 후 mode 강등
if mode == "LIVE" and not intended_live:
    logger.error("="*80)
    logger.error("[PB1][LIVE][DOWNGRADE] Downgrading mode from LIVE to DIAG due to LIVE flag conflicts")
    logger.error("="*80)
    mode = "DIAG"
```

**효과**:
- 충돌 상황에서 RuntimeError 대신 DIAG 모드로 운영
- 주문 완전 차단 (order_allowed=0)
- 계산은 계속 수행 (calc_allowed=1)

---

## 5️⃣ **Fix D: Universe as_of 로깅 개선** ✅

**파일**: `trader/pb1_runner.py` (함수 `_load_universe_context()`)

```python
logger.warning(
    "[PB1][UNIVERSE][DB_ONLY] fallback_current run_id=%s requested_as_of=%s actual_as_of=%s members=%s",
    fallback.get("run_id"),
    as_of,  # ← requested_as_of
    universe_actual_as_of,  # ← actual_as_of (fallback된 날짜)
    fallback.get("members_count"),
)
```

**효과**:
- requested_as_of vs actual_as_of 명확히 구분 로깅
- universe fallback 추적 용이

---

## 6️⃣ **최종 흐름 검증** ✅

### 시나리오: TRADE_INPUT=final30, AS_OF_OVERRIDE=2026-02-20

**pb1_runner 실행**:
```
1. requested_as_of = 2026-02-20 (derive_as_of)
2. watchlist_final load → actual_as_of=2026-02-19 (fallback)
3. UniverseContext(as_of_date=2026-02-19, meta={requested_as_of=2026-02-20, actual_as_of=2026-02-19})
```

**pb1_engine 실행**:
```
4. as_of_final = self._universe_context.as_of_date = 2026-02-19
5. _load_entry_final30_or_abort(as_of=2026-02-19)
   - final30 snapshot 찾기: load_final30(env, "2026-02-19") → NOT FOUND
   - fallback: universe_context.members (watchlist_final 30개)
   - 로그: [PB1][ENTRY][FINAL30_FALLBACK] source=watchlist_final count=30
6. scan_members = final30_codes (30개) → 정상 진입
```

**로그 예시**:
```
[TRADE][WATCHLIST_FINAL][LOCK] env=practice strategy=pb1_watchlist_final requested_as_of=2026-02-20 actual_as_of=2026-02-19 n=30
[PB1][ASOF][CONTEXT] as_of_final=2026-02-19 reason=universe_context (source=watchlist_final)
[FINAL30][LOAD] as_of=2026-02-19 count=30 source=final30_snapshot  (또는)
[PB1][ENTRY][FINAL30_FALLBACK] as_of=2026-02-19 final30_snapshot_missing -> using watchlist source=watchlist_final count=30
[ENTRY][PIPE][START] ... scan_count=30 source=final30 ...
```

---

## 7️⃣ **테스트 체크리스트** ✅

- [x] `python -m py_compile` 통과 (syntax 에러 없음)
- [x] `AsOfContext` import 및 기본 기능 동작 확인
- [x] as_of 관련 로그 라인 위치 확인됨
  - [x] 5368줄: Fix B (as_of_final 결정)
  - [x] 4440줄: Fix B-ENTRY (_load_entry_final30_or_abort with fallback)
  - [x] 1095줄: Fix C (LIVE 플래그 검증)
- [ ] 실제 runtime 테스트 (로컬 실행 또는 Actions)

---

## 📌 **주의사항**

1. **pb1_runner 초기화 순서**:
   - `intended_live` 결정 (1087줄)
   - LIVE 플래그 충돌 감지 및 강등 (1095-1120줄)
   - mode 결정 및 강등 (1153-1178줄)
   - **이 순서 변경 금지!**

2. **universe_context 전달 필수**:
   - pb1_runner에서 PB1Engine 초기화 시 반드시 `universe_context=universe_ctx` 전달
   - 없으면 fallback으로 `get_as_of_date()` (오늘 날짜) 사용

3. **로그 검증 포인트**:
   - `[TRADE][WATCHLIST_FINAL][LOCK]` 로그에서 `actual_as_of` 확인
   - `[PB1][ASOF][CONTEXT]` 로그에서 as_of_final과 reason 확인
   - `[FINAL30][LOAD]` 또는 `[PB1][ENTRY][FINAL30_FALLBACK]` 중 하나 나타남

---

## ✨ **완성 상태**

| 항목 | 상태 |
|------|------|
| Fix B (as_of 일관성) | ✅ 완료 |
| Step 5 (final30 fallback) | ✅ 완료 |
| Fix C (LIVE 플래그) | ✅ 완료 |
| Fix D (universe as_of) | ✅ 완료 |
| Syntax 검증 | ✅ 통과 |
| 런타임 테스트 | ⏳ 대기 중 |

