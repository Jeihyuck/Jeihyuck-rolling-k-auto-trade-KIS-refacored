# 후보군 스캔 입력 완전 적용 패치

## 🎯 문제 상황

**증상**: 후보군 120개를 DB에서 성공적으로 로드했지만 실제 매수 엔진은 universe=195로 계속 스캔하여 guard 에러 발생하고 매매 불발

**원인**: 
- `_load_today_watchlist_members()`에서 후보군 120개를 로드 성공
- 하지만 엔트리 파이프라인 시작 시 `members = self._load_universe()`로 195 universe를 다시 로드하여 덮어씀
- `_compute_candidates()` 내부에서는 guard만 로그를 찍고 실제로는 195 universe를 계속 사용

## ✅ 해결 방안

### 1. scan_members 변수 도입 (pb1_engine.py)

**변경 위치**: [trader/pb1_engine.py](trader/pb1_engine.py#L4791-L4835)

```python
# ✅ universe_members는 보유/리포트/정산용으로만 로드
universe_members = self._load_universe()

# ✅ scan_members: 후보군 hit시 watchlist, 아니면 universe
scan_members = universe_members
scan_source = "universe"

watchlist_enabled = os.getenv("PB1_WATCHLIST_ENABLED", "1") == "1"
if watchlist_enabled and self.phase in {"prep", "entry"}:
    watchlist_members, watchlist_reason = self._load_today_watchlist_members()
    if "candidate_pool_hit" in watchlist_reason or "candidate_pool_autobuilt" in watchlist_reason:
        scan_members = watchlist_members
        scan_source = watchlist_reason
        logger.info(
            "[ENTRY][SCAN_INPUT] source=%s scan_count=%s (NOT universe=%s)",
            scan_source,
            len(scan_members),
            len(universe_members),
        )
```

**효과**:
- `universe_members`: 195개 전체 유지 (보유 종목 조회, 리포트, 정산용)
- `scan_members`: 후보군 hit시 120개로 제한 (엔트리 스캔 전용)
- 후보군과 universe가 명확히 분리됨

### 2. [ENTRY][PIPE][START] 로그 변경

**변경 전**:
```
[ENTRY][PIPE][START] trace=... universe=195 slots=... tick_budget=...
```

**변경 후**:
```
[ENTRY][PIPE][START] trace=... scan_count=120 source=candidate_pool_hit slots=... tick_budget=...
```

**판정 기준**:
- ✅ `scan_count=120 source=candidate_pool_hit` → 성공
- ❌ `universe=195` → 실패 (패치 미적용)

### 3. _compute_candidates()에서 scan_members 사용

**변경 위치**: [trader/pb1_engine.py](trader/pb1_engine.py#L4898-L4916)

```python
candidates = self._compute_candidates(scan_members)

logger.info(
    "[ENTRY][CANDIDATES] trace=%s start scan_count=%s source=%s data_ok=%s dt_minervini=%.2f",
    trace_id,
    len(scan_members),
    scan_source,
    data_ok_count,
    dt_minervini,
)
```

**효과**:
- prefilter, minervini, OHLCV fetch가 모두 `scan_members` 기준으로 동작
- universe=195를 참조하는 경로 완전 제거

### 4. 후보군 guard를 자동 교정에서 경고로 변경

**변경 전** (line 1489):
```python
if CANDIDATE_POOL_ENABLED and universe_count > 150:
    logger.error(
        "[CANDIDATE_POOL][GUARD] CRITICAL: universe_count=%s exceeds 150, "
        "candidate pool system should have reduced this! "
        "Check _load_today_watchlist_members logic.",
        universe_count
    )
```

**변경 후**:
```python
if CANDIDATE_POOL_ENABLED and scan_count > 150:
    logger.warning(
        "[CANDIDATE_POOL][GUARD] scan_count=%s exceeds 150, "
        "this should have been reduced by candidate pool. "
        "Proceeding with large universe (performance may be slow).",
        scan_count
    )
```

**효과**:
- guard가 터지면 **실제 스캔이 중단되지 않고** 계속 진행 (degraded mode)
- 에러 대신 경고만 출력 (후보군이 없어도 전체 universe로 fallback 가능)

### 5. 환경변수 검증 (pb1_runner.py)

**변경 위치**: [trader/pb1_runner.py](trader/pb1_runner.py#L2089-L2119)

```python
# ✅ 환경변수 검증: KIS_ENV vs STRATEGY_ENV 일치 확인
kis_env = os.getenv("KIS_ENV", "").lower()
strategy_env = os.getenv("STRATEGY_ENV", "").lower()

if kis_env and strategy_env and kis_env != strategy_env:
    logger.error(
        "[PB1][ENV][CRITICAL] KIS_ENV=%s != STRATEGY_ENV=%s -> FAIL",
        kis_env,
        strategy_env,
    )
    raise ValueError(f"KIS_ENV ({kis_env}) != STRATEGY_ENV ({strategy_env})")

# ✅ KIS_ENV가 없으면 STRATEGY_ENV로 설정
if not kis_env and strategy_env:
    os.environ["KIS_ENV"] = strategy_env
    logger.info("[PB1][ENV][AUTO] KIS_ENV not set -> using STRATEGY_ENV=%s", strategy_env)

# ✅ STRATEGY_ENV가 없으면 KIS_ENV로 설정
if not strategy_env and kis_env:
    os.environ["STRATEGY_ENV"] = kis_env
    logger.info("[PB1][ENV][AUTO] STRATEGY_ENV not set -> using KIS_ENV=%s", kis_env)
```

**효과**:
- `KIS_ENV != STRATEGY_ENV` 상태에서 즉시 fail (잘못된 universe/후보군 로드 방지)
- 하나만 설정되어 있으면 자동으로 동기화

## 📊 성공 판정 기준

### ✅ 성공 (후보군 스캔 입력 적용됨)

```log
[CANDIDATE_POOL][USAGE] candidates_universe_size=120 as_of=2026-02-02 (NOT 195 universe)
[ENTRY][SCAN_INPUT] source=candidate_pool_hit scan_count=120 (NOT universe=195)
[ENTRY][PIPE][START] trace=... scan_count=120 source=candidate_pool_hit slots=...
[PB1][CANDIDATES][PREFILTER] scan_count=120 -> filtered=50
[ENTRY][CANDIDATES] trace=... start scan_count=120 source=candidate_pool_hit data_ok=...
```

### ❌ 실패 (여전히 universe=195 사용)

```log
[CANDIDATE_POOL][USAGE] candidates_universe_size=120 ...  # 후보군 로드는 성공
[ENTRY][PIPE][START] trace=... universe=195 slots=...     # ❌ 아직 universe 사용
[CANDIDATE_POOL][GUARD] CRITICAL: universe_count=195 ...  # guard 터짐
```

## 🔧 로컬 테스트 방법

```bash
# 1. 환경변수 확인
echo "STRATEGY_ENV=$STRATEGY_ENV"
echo "KIS_ENV=$KIS_ENV"
# 둘 다 "live"여야 함

# 2. 후보군 확인
STRATEGY_ENV=live AS_OF=2026-02-02 python scripts/verify_candidate_pool.py

# 3. 로그 확인
tail -f logs/trader.log | grep -E "SCAN_INPUT|PIPE\]\[START\]|PREFILTER|CANDIDATES\]"
```

**기대 출력**:
```
[ENTRY][SCAN_INPUT] source=candidate_pool_hit scan_count=120 (NOT universe=195)
[ENTRY][PIPE][START] trace=... scan_count=120 source=candidate_pool_hit ...
[PB1][CANDIDATES][PREFILTER] scan_count=120 -> filtered=50
```

## 📝 주요 변경 파일

| 파일 | 변경 내용 | 라인 |
|------|----------|------|
| [trader/pb1_engine.py](trader/pb1_engine.py#L4791-L4835) | `scan_members` 변수 도입, `universe_members`와 분리 | 4791-4835 |
| [trader/pb1_engine.py](trader/pb1_engine.py#L4898-L4916) | `_compute_candidates(scan_members)` 전달 | 4898-4916 |
| [trader/pb1_engine.py](trader/pb1_engine.py#L1489-L1501) | guard를 error→warning으로 변경 | 1489-1501 |
| [trader/pb1_engine.py](trader/pb1_engine.py#L1816-L1840) | 로그 출력을 `scan_count`로 변경 | 1816-1840 |
| [trader/pb1_runner.py](trader/pb1_runner.py#L2089-L2119) | `KIS_ENV` vs `STRATEGY_ENV` 검증 추가 | 2089-2119 |

## ⚠️ 다음 런 체크리스트

1. **환경변수**: `KIS_ENV=live`, `STRATEGY_ENV=live` 동일 여부 확인
2. **후보군 존재**: `verify_candidate_pool.py` 실행하여 120개 확인
3. **로그 판정**: `[ENTRY][PIPE][START] scan_count=120` 출력 확인
4. **시간 여유**: 15:15 cutoff 기준 최소 15분 전 시작 (15:00 권장)
5. **ATR 파라미터**: `PB1_MAX_ATR_PCT=8` 너무 타이트하면 완화 검토

## 🚀 다음 개선 사항 (Optional)

### 1. prefilter 강화 (시간 부족 시)
```python
# 120 → 30으로 더 공격적으로 줄이기
PB1_UNIVERSE_SCAN_LIMIT=30  # 기본값 50에서 감소
```

### 2. ATR 파라미터 완화 (setup_ok가 모두 컷되는 경우)
```python
PB1_MAX_ATR_PCT=10  # 8%에서 10%로 완화
```

### 3. 조기 종료 활성화 (충분한 후보 확보 시)
```python
PB1_CANDIDATE_EARLY_STOP=1  # 이미 활성화됨
PB1_EARLY_STOP_CANDIDATES=12  # 12개 확보 시 조기 종료
```

---

**패치 날짜**: 2026-02-02  
**작성자**: GitHub Copilot  
**상태**: ✅ READY FOR TESTING
