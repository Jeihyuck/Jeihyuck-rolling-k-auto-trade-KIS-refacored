# AUTO LIVE ARMING — 시간 기반 자동 Live Trading 시스템

## 개요

**AUTO LIVE ARMING**은 거래 시간대에 자동으로 주문을 허용하고, 그 외 시간대에는 자동으로 차단하는 시스템입니다.
더 이상 매일 수동으로 `FORCE_BLOCK_LIVE`, `LIVE_TRADING_ENABLED` 등의 환경변수를 조작할 필요가 없습니다.

### 핵심 원칙

1. **정책은 한 곳에서만 결정**: `compute_live_gate()` 함수가 단일 진실 공급원
2. **시간 기반 자동 정책**: 거래일/거래시간/전략모드를 기반으로 자동 결정
3. **긴급 제어 유지**: `KILL_SWITCH`, `FORCE_BLOCK_LIVE`, `FORCE_LIVE` 환경변수로 긴급 제어 가능
4. **투명성**: 모든 결정은 `reason`과 함께 로깅되어 디버깅 가능

### 작동 방식

```
08:50 (preopen)    → allow_live_gate: 0 / force_block_live: 1 / reason: WINDOW=preopen
09:00 (morning)    → allow_live_gate: 1 / force_block_live: 0 / reason: TIME_WINDOW_OK
13:00 (intraday)   → allow_live_gate: 1 / force_block_live: 0 / reason: TIME_WINDOW_OK
15:30 (after)      → allow_live_gate: 0 / force_block_live: 1 / reason: WINDOW=after
주말               → allow_live_gate: 0 / force_block_live: 1 / reason: NOT_TRADING_DAY
```

## 주요 구성요소

### 1. `trader/live_gate.py`

시간 기반 Live Gate 정책 엔진입니다.

#### `compute_live_gate()` 함수

```python
def compute_live_gate(
    now_kst: datetime,
    *,
    kis_env: str,           # "practice" 또는 "real"
    strategy_mode: str,     # "LIVE", "DIAG" 등
    dryrun: bool,           # Dry-run 모드 여부
    analysis_only: bool,    # 분석 전용 모드 (MINERVINI_ONLY 등)
) -> LiveGateStatus
```

**우선순위** (높음 → 낮음):
1. 긴급 제어 (`KILL_SWITCH`, `FORCE_BLOCK_LIVE`, `FORCE_LIVE`)
2. 분석 전용 / Dry-run / 전략 모드
3. 거래일 체크
4. 시간대 체크

**시간대 구분**:
- `preopen` (09:00 이전): 주문 차단
- `morning` (09:00-10:00): 주문 허용
- `intraday` (10:00-15:15): 주문 허용
- `close` (15:15-15:30): 주문 허용
- `after` (15:30 이후): 주문 차단

#### `LiveGateStatus` 객체

```python
@dataclass(frozen=True)
class LiveGateStatus:
    allow_live_gate: bool      # True이면 주문 허용
    force_block_live: bool     # True이면 강제 차단
    reason: str                # 결정 이유
    trading_day: bool          # 거래일 여부
    window: str                # 시간대
    now_kst: datetime          # 현재 KST 시각
```

### 2. `trader/config.py` 통합

`config.py`에서 `compute_live_gate()`를 호출하여 전역 상수를 생성합니다:

```python
from trader.live_gate import compute_live_gate, LiveGateStatus

LIVE_GATE_STATUS: LiveGateStatus = compute_live_gate(...)
ALLOW_LIVE_GATE: bool = LIVE_GATE_STATUS.allow_live_gate
FORCE_BLOCK_LIVE: bool = LIVE_GATE_STATUS.force_block_live
```

### 3. `trader/kis_wrapper.py` 주문 차단

주문 함수(`_order_cash`, `buy_stock_limit`, `sell_stock_limit`)에서 `ALLOW_LIVE_GATE`를 체크합니다:

```python
from trader.config import ALLOW_LIVE_GATE, FORCE_BLOCK_LIVE, LIVE_GATE_STATUS

if not ALLOW_LIVE_GATE or FORCE_BLOCK_LIVE:
    logger.warning("[ORDER][BLOCKED] reason=%s ...", LIVE_GATE_STATUS.reason)
    return {"blocked": True, "reason": LIVE_GATE_STATUS.reason, ...}
```

### 4. `trader/pb1_runner.py` 상태 로깅

러너 시작 시 Live Gate 상태를 로깅합니다:

```python
from trader.config import LIVE_GATE_STATUS

logger.info(
    "[LIVE_GATE_STATUS] allow_live_gate=%s force_block_live=%s reason=%s ...",
    int(LIVE_GATE_STATUS.allow_live_gate),
    int(LIVE_GATE_STATUS.force_block_live),
    LIVE_GATE_STATUS.reason,
    ...
)
```

### 5. GitHub Actions 워크플로우

`.github/workflows/unified-pipeline.yml`에서 시간대별로 `STRATEGY_MODE`를 설정합니다:

```yaml
schedule:
  - cron: "40 23 * * 0-4"      # KST 08:40 (prep)
  - cron: "0-30/5 0-6 * * 1-5" # KST 09:00-15:30 (trade)

env:
  STRATEGY_MODE: "LIVE"         # 거래 시간대
  # STRATEGY_MODE: "DIAG"       # prep 또는 시간 외
  KILL_SWITCH: "0"              # 긴급 정지 (필요시 1)
  FORCE_BLOCK_LIVE: "0"         # 긴급 차단 (필요시 1)
  OPEN_BUFFER_SEC: "5"          # 개장 직후 버퍼 (선택사항)
```

## 환경변수 가이드

### 정상 운영 (자동 정책)

```bash
STRATEGY_MODE=LIVE            # 거래 시간대에 자동으로 주문 허용
KILL_SWITCH=0                 # 긴급 정지 OFF
FORCE_BLOCK_LIVE=0            # 긴급 차단 OFF
OPEN_BUFFER_SEC=5             # 09:00 직후 5초간 주문 보류 (선택사항)
```

### 긴급 제어

#### 1. 긴급 정지 (KILL_SWITCH)

**모든 주문을 즉시 차단**합니다. 최우선 순위입니다.

```bash
KILL_SWITCH=1
```

- 사용 예: 시스템 이상, 시장 급변동, 긴급 점검
- 영향: 모든 조건 무시하고 무조건 차단
- 해제: `KILL_SWITCH=0`으로 변경 후 재시작

#### 2. 강제 차단 (FORCE_BLOCK_LIVE)

**주문을 강제로 차단**합니다. 테스트/검증 시 사용합니다.

```bash
FORCE_BLOCK_LIVE=1
```

- 사용 예: 테스트, dry-run 검증, 안전 확인
- 영향: `KILL_SWITCH` 다음 우선순위로 차단
- 해제: `FORCE_BLOCK_LIVE=0`으로 변경

#### 3. 강제 허용 (FORCE_LIVE)

**주문을 강제로 허용**합니다. ⚠️ **매우 조심스럽게 사용**하세요.

```bash
FORCE_LIVE=1                  # practice 환경에서는 즉시 허용
```

```bash
FORCE_LIVE=1
FORCE_LIVE_CONFIRM=YES        # real 환경에서는 추가 확인 필요
```

- 사용 예: 거래 시간 외 테스트, 긴급 주문
- 영향: 시간대/거래일 무시하고 주문 허용
- 위험: 장외 시간에 주문 실행 가능 → 체결 안 될 수 있음
- **real 환경에서는 `FORCE_LIVE_CONFIRM=YES` 필수**

### 기타 설정

```bash
OPEN_BUFFER_SEC=5             # 09:00 직후 N초간 주문 보류 (기본값: 0)
```

- 09:00:00 ~ 09:00:04 (5초 미만): 차단 (`reason=OPEN_BUFFER<5s`)
- 09:00:05 이후: 정상 허용
- 용도: 동시호가 체결 직후 급등락 리스크 회피

## 로그 해석

### 정상 허용

```
[LIVE_GATE] allow_live_gate=1 force_block_live=0 reason=TIME_WINDOW_OK 
trading_day=1 window=intraday now_kst=2026-02-25T13:00:00+09:00
```

→ 거래 시간대, 주문 허용됨

### 시간대 차단

```
[LIVE_GATE] allow_live_gate=0 force_block_live=1 reason=WINDOW=preopen 
trading_day=1 window=preopen now_kst=2026-02-25T08:59:00+09:00
```

→ 개장 전, 주문 차단됨

### 주말 차단

```
[LIVE_GATE] allow_live_gate=0 force_block_live=1 reason=NOT_TRADING_DAY 
trading_day=0 window=intraday now_kst=2026-02-22T13:00:00+09:00
```

→ 주말, 주문 차단됨

### 긴급 차단

```
[LIVE_GATE] allow_live_gate=0 force_block_live=1 reason=KILL_SWITCH 
trading_day=1 window=intraday now_kst=2026-02-25T13:00:00+09:00
```

→ `KILL_SWITCH=1`, 모든 주문 차단됨

## 단위 테스트

`trader/tests/test_live_gate.py`에 30개의 단위 테스트가 있습니다:

```bash
pytest trader/tests/test_live_gate.py -v
```

**테스트 커버리지**:
- 거래일 판정 (평일/주말)
- 시간대 구분 (preopen/morning/intraday/close/after)
- 정상 허용 시나리오
- 차단 시나리오 (시간대, 주말, 모드)
- 긴급 제어 (KILL_SWITCH, FORCE_BLOCK_LIVE, FORCE_LIVE)
- 특수 케이스 (OPEN_BUFFER_SEC)

## 마이그레이션 가이드

### 기존 시스템에서 마이그레이션

#### 1. 환경변수 중복 제거

**Before**:
```bash
FORCE_BLOCK_LIVE=1
DISABLE_LIVE_TRADING=1
LIVE_TRADING_ENABLED=0
STRATEGY_MODE=DIAG
```

**After**:
```bash
STRATEGY_MODE=DIAG            # 이것만으로 충분
# FORCE_BLOCK_LIVE, DISABLE_LIVE_TRADING, LIVE_TRADING_ENABLED는 불필요
```

#### 2. GitHub Actions 워크플로우 간소화

**Before**:
```yaml
env:
  FORCE_BLOCK_LIVE: "1"
  DISABLE_LIVE_TRADING: "1"
  LIVE_TRADING_ENABLED: "0"
  STRATEGY_MODE: "DIAG"
```

**After**:
```yaml
env:
  STRATEGY_MODE: "LIVE"       # 시간대별 자동 정책
  KILL_SWITCH: "0"            # 긴급용만
  FORCE_BLOCK_LIVE: "0"       # 긴급용만
```

#### 3. 수동 토글 제거

더 이상 매일 아침 `FORCE_BLOCK_LIVE=0`으로 변경하고, 저녁에 `FORCE_BLOCK_LIVE=1`로 변경할 필요 없습니다.
시간대에 따라 자동으로 처리됩니다.

## FAQ

### Q: 주문이 차단되는데 왜 그런가요?

A: 로그에서 `[LIVE_GATE_STATUS]`를 확인하세요. `reason` 필드가 차단 이유를 알려줍니다.

```
reason=WINDOW=preopen      → 개장 전
reason=WINDOW=after        → 장 마감 후
reason=NOT_TRADING_DAY     → 주말/공휴일
reason=MODE=DIAG           → STRATEGY_MODE가 DIAG
reason=DRYRUN              → DRY_RUN=1
reason=ANALYSIS_ONLY       → MINERVINI_ONLY=1
reason=KILL_SWITCH         → KILL_SWITCH=1
reason=FORCE_BLOCK_LIVE    → FORCE_BLOCK_LIVE=1
```

### Q: 거래 시간인데도 주문이 안 됩니다.

A: 다음을 확인하세요:

1. `STRATEGY_MODE=LIVE`인가? (`DIAG`면 차단됨)
2. `DRY_RUN=0`인가? (1이면 차단됨)
3. `KILL_SWITCH=0`인가? (1이면 차단됨)
4. `FORCE_BLOCK_LIVE=0`인가? (1이면 차단됨)

### Q: 거래 시간 외에 테스트하고 싶습니다.

A: `FORCE_LIVE=1`을 설정하세요. practice 환경에서는 즉시 허용됩니다.

```bash
FORCE_LIVE=1 python -m trader.pb1_runner
```

### Q: real 환경에서 FORCE_LIVE를 사용하려면?

A: 추가 확인이 필요합니다:

```bash
FORCE_LIVE=1 FORCE_LIVE_CONFIRM=YES python -m trader.pb1_runner
```

### Q: 09:00 직후 급등락 리스크를 피하고 싶습니다.

A: `OPEN_BUFFER_SEC`를 설정하세요:

```bash
OPEN_BUFFER_SEC=10  # 09:00:00 ~ 09:00:09까지 주문 보류
```

### Q: 공휴일은 자동으로 감지하나요?

A: 현재는 주말만 감지합니다. 공휴일 감지는 향후 업데이트 예정입니다.

고도화 옵션:
- 한국거래소 휴장일 DB/API 연동
- `trading_calendar` 라이브러리 활용
- 수동 관리되는 `holiday.csv` 참조

### Q: 이전 FORCE_BLOCK_LIVE 환경변수는 어떻게 되나요?

A: 긴급 제어용으로 계속 사용 가능합니다. 하지만 일상적인 운영에서는 사용하지 마세요.
시간 기반 정책이 자동으로 처리합니다.

## 운영 체크리스트

### 일일 점검

- [ ] 로그에서 `[LIVE_GATE_STATUS]` 확인
- [ ] `allow_live_gate` 값이 시간대별로 올바른지 확인
- [ ] `reason` 필드가 예상과 일치하는지 확인

### 주간 점검

- [ ] `KILL_SWITCH=0` 확인 (긴급 상황 아닌 경우)
- [ ] `FORCE_BLOCK_LIVE=0` 확인 (테스트 종료 후)
- [ ] GitHub Actions 워크플로우 실행 로그 검토

### 월간 점검

- [ ] 단위 테스트 실행: `pytest trader/tests/test_live_gate.py -v`
- [ ] 공휴일 목록 업데이트 (고도화 후)
- [ ] 시간대 정책 검토 및 조정

## 향후 업데이트 계획

### Phase 1 (현재)
- ✅ 시간 기반 자동 정책
- ✅ 긴급 제어
- ✅ 주말 감지
- ✅ 단위 테스트

### Phase 2 (예정)
- [ ] 한국거래소 공휴일 DB/API 연동
- [ ] 실시간 시장 상태 감지 (장 중단, 서킷 브레이커 등)
- [ ] 동적 시간대 조정 (여름/겨울 시간 변경 등)

### Phase 3 (예정)
- [ ] 전략별 시간대 커스터마이징
- [ ] 리스크 기반 동적 gate 조정
- [ ] A/B 테스트 지원

## 문의 및 지원

- 문서: 이 파일
- 테스트: `trader/tests/test_live_gate.py`
- 소스: `trader/live_gate.py`
- 이슈: GitHub Issues

---

**Last Updated**: 2026-02-25
**Version**: 1.0.0
