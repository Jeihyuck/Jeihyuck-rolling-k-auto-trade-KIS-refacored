# 한국장 거래 시스템 환경 변수 설정 가이드

## 개요
한국장 trade-am / trade-afternoon 일반화 수정 후 사용 가능한 환경 변수 목록입니다.

## RECONCILE_ONLY 관련

### PB1_RECONCILE_ONLY_GLOBAL_SKIP
- **기본값**: `0`
- **설명**: open order 존재 시 전체 entry engine을 skip할지 여부
- **권장값**: `0` (종목별 open order 체크로 충분)
- **사용처**: trade-am, trade-afternoon

```yaml
PB1_RECONCILE_ONLY_GLOBAL_SKIP: "0"
```

### PB1_FORCE_RECONCILE_ONLY
- **기본값**: `0`
- **설명**: 강제로 RECONCILE_ONLY 모드 활성화
- **권장값**: `0` (일반 운영 시)
- **사용처**: 긴급 상황, DB 복구 시

```yaml
PB1_FORCE_RECONCILE_ONLY: "0"
```

## Stale Open Order 정리

### PB1_EXPIRE_STALE_OPEN_ORDERS
- **기본값**: `1`
- **설명**: 오래된 open order 자동 정리 활성화
- **권장값**: `1` (활성화)
- **사용처**: trade-prep, trade-am, trade-afternoon

```yaml
PB1_EXPIRE_STALE_OPEN_ORDERS: "1"
```

### PB1_STALE_OPEN_ORDER_MAX_MINUTES
- **기본값**: `30`
- **설명**: open order가 stale로 간주되는 최대 시간 (분)
- **권장값**: `30` (30분)
- **사용처**: stale order cleanup

```yaml
PB1_STALE_OPEN_ORDER_MAX_MINUTES: "30"
```

### PB1_STALE_OPEN_ORDER_REPAIR_PRACTICE
- **기본값**: `1`
- **설명**: practice 환경에서도 stale order repair 활성화
- **권장값**: `1` (활성화)
- **사용처**: practice 환경

```yaml
PB1_STALE_OPEN_ORDER_REPAIR_PRACTICE: "1"
```

## DB 타임아웃 설정

### DB_LOCK_TIMEOUT_MS
- **기본값**: `5000`
- **설명**: PostgreSQL lock_timeout (밀리초)
- **권장값**: `5000` (5초)
- **사용처**: DB 연결

```yaml
DB_LOCK_TIMEOUT_MS: "5000"
```

### DB_STATEMENT_TIMEOUT_MS
- **기본값**: `15000`
- **설명**: PostgreSQL statement_timeout (밀리초)
- **권장값**: `15000` (15초)
- **사용처**: DB 연결

```yaml
DB_STATEMENT_TIMEOUT_MS: "15000"
```

### DB_IDLE_IN_TX_SESSION_TIMEOUT_MS
- **기본값**: `15000`
- **설명**: PostgreSQL idle_in_transaction_session_timeout (밀리초)
- **권장값**: `15000` (15초)
- **사용처**: DB 연결

```yaml
DB_IDLE_IN_TX_SESSION_TIMEOUT_MS: "15000"
```

### PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT
- **기본값**: `0`
- **설명**: order lookup timeout 시 fail-open 동작
- **권장값**: `1` (timeout 시 빈 배열 반환)
- **사용처**: trade-am, trade-afternoon

```yaml
PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT: "1"
```

## 시작 지연 감지

### PB1_MAX_START_DELAY_SEC_AM
- **기본값**: `1800`
- **설명**: trade-am 최대 허용 시작 지연 시간 (초)
- **권장값**: `1800` (30분)
- **사용처**: trade-am

```yaml
PB1_MAX_START_DELAY_SEC_AM: "1800"
```

### PB1_MAX_START_DELAY_SEC_AFTERNOON
- **기본값**: `1800`
- **설명**: trade-afternoon 최대 허용 시작 지연 시간 (초)
- **권장값**: `1800` (30분)
- **사용처**: trade-afternoon

```yaml
PB1_MAX_START_DELAY_SEC_AFTERNOON: "1800"
```

### PB1_WARN_ON_START_DELAY
- **기본값**: `1`
- **설명**: 시작 지연 시 경고 로그 출력
- **권장값**: `1` (활성화)
- **사용처**: trade-am, trade-afternoon

```yaml
PB1_WARN_ON_START_DELAY: "1"
```

### PB1_BLOCK_ENTRY_IF_START_DELAY_EXCEEDED
- **기본값**: `0`
- **설명**: 시작 지연이 허용 시간 초과 시 entry 차단
- **권장값**: `0` (차단 안 함, 경고만)
- **사용처**: trade-am, trade-afternoon

```yaml
PB1_BLOCK_ENTRY_IF_START_DELAY_EXCEEDED: "0"
```

## 레거시 설정 (제거됨)

### PB1_BLOCK_ENTRY_AFTER_EXIT ⚠️ DEPRECATED
- **상태**: 제거됨
- **이유**: 전체 entry 차단이 아닌 종목별 재매수 금지로 변경
- **대체**: 자동으로 당일 매도 종목만 재매수 금지

---

## 전체 권장 설정 (GitHub Actions Workflow)

```yaml
env:
  # RECONCILE_ONLY
  PB1_RECONCILE_ONLY_GLOBAL_SKIP: "0"
  PB1_FORCE_RECONCILE_ONLY: "0"
  
  # Stale Order Cleanup
  PB1_EXPIRE_STALE_OPEN_ORDERS: "1"
  PB1_STALE_OPEN_ORDER_MAX_MINUTES: "30"
  PB1_STALE_OPEN_ORDER_REPAIR_PRACTICE: "1"
  
  # DB Timeout
  DB_LOCK_TIMEOUT_MS: "5000"
  DB_STATEMENT_TIMEOUT_MS: "15000"
  DB_IDLE_IN_TX_SESSION_TIMEOUT_MS: "15000"
  PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT: "1"
  
  # Start Delay Detection
  PB1_MAX_START_DELAY_SEC_AM: "1800"
  PB1_MAX_START_DELAY_SEC_AFTERNOON: "1800"
  PB1_WARN_ON_START_DELAY: "1"
  PB1_BLOCK_ENTRY_IF_START_DELAY_EXCEEDED: "0"
```

---

## 로그 확인 포인트

수정 후 다음 로그가 나오면 **안 됩니다**:

```
❌ ENTRY_DECISION result=SKIP reason=BLOCKED_AFTER_EXIT
❌ [PB1][RECONCILE_ONLY] enabled=1 pending=1 open_orders=N action=skip_engine
   (단, PB1_FORCE_RECONCILE_ONLY=1이 아닌 경우)
❌ candidates_scanned=0 setup_ok=0 (entry disabled가 아닌데 이렇게 나오면 실패)
❌ accepted_sells=1인데 sells=0
❌ SELL fill 존재하는데 Realized PNL Today=0
❌ 모든 보유종목 Days Held=0
❌ tick_hard_timeout timeout_sec=90 last_stage=post_capital.exit_pass
```

수정 후 다음 로그가 나와야 **합니다**:

```
✅ [ENTRY][AFTER_EXIT][CODE_BLOCK] sold_this_tick=N sold_today=M action=block_sold_codes_only
✅ [ENTRY][SKIP][CODE_LEVEL] code=<code> reason=SAME_DAY_SELL_REENTRY_BLOCK
✅ [ENTRY][ALLOW][OTHER_CODE] code=<code> reason=not_sold_today
✅ [PB1][RECONCILE_ONLY][BYPASS] open_orders=N global_skip=0 action=continue_engine
✅ [ORDERS][STALE_REPAIR][DONE] expired=N remaining_open=M
✅ [PB1][POST_CAPITAL][EXIT_PASS][DONE] sells=<actual_count> sold_codes_count=N
```
