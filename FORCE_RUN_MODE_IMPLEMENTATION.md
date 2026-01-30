# FORCE_RUN 모드 구현 완료

## 개요
GitHub Actions 스케줄 제약 제거 및 장중 세션 체크 우회를 위한 **FORCE_RUN 모드**를 구현했습니다.

## 주요 변경사항

### 1. GitHub Actions 워크플로우 수정 (.github/workflows/trade-runner.yml)

#### Schedule 변경
- **기존**: 5분 간격 (`*/5 0-5 * * 1-5`)
- **변경**: 30분 간격 (`0,30 0-5 * * 1-5`)
  - 부하 감소: 시간당 12회 → 2회
  - 수동 실행 권장: workflow_dispatch 사용

#### workflow_dispatch 입력값 추가
```yaml
FORCE_RUN:
  description: "강제 실행 (장중 체크 우회, 1=활성)"
  default: "0"

WATCHLIST_MODE:
  description: "Watchlist 모드 (1=소수 종목만, 0=전체 유니버스)"
  default: "0"

WATCHLIST:
  description: "Watchlist 종목 코드 (쉼표 구분)"
  default: "005930,000660,035420,035720,005380"

LIVE_TRADING_ENABLED:
  description: "실거래 허용 (1=실거래, 0=DRY_RUN)"
  default: "0"

DRY_RUN:
  description: "시뮬레이션 모드 (1=시뮬, 0=실거래)"
  default: "1"
```

#### trade_tick job에 env 전달
```yaml
env:
  FORCE_RUN: ${{ github.event_name == 'workflow_dispatch' && inputs.FORCE_RUN || '0' }}
  WATCHLIST_MODE: ${{ github.event_name == 'workflow_dispatch' && inputs.WATCHLIST_MODE || '0' }}
  WATCHLIST: ${{ github.event_name == 'workflow_dispatch' && inputs.WATCHLIST || '005930,000660,035420,035720,005380' }}
  DRY_RUN: ${{ github.event_name == 'workflow_dispatch' && inputs.DRY_RUN || '0' }}
  LIVE_TRADING_ENABLED: ${{ github.event_name == 'workflow_dispatch' && inputs.LIVE_TRADING_ENABLED || '1' }}
```

---

### 2. 장중 세션 체크 우회 (trader/config.py)

**`resolve_strategy_mode()` 함수 수정**

```python
def resolve_strategy_mode(
    now_kst: datetime | None = None,
    force_mode_env: str | None = None,
) -> tuple[str, bool, str, str]:
    now_kst = now_kst or datetime.now(KST)
    if now_kst.tzinfo is None:
        now_kst = now_kst.replace(tzinfo=KST)
    
    # [NEW] FORCE_RUN=1이면 무조건 장중으로 간주
    force_run = os.getenv("FORCE_RUN", "0") == "1"
    if force_run:
        return "LIVE", True, "day", "force_run"
    
    # 기존 로직: 평일/장중 체크
    trading_day = now_kst.weekday() < 5
    window = resolve_market_window(now_kst, trading_day)
    # ...
```

**동작 방식**:
- `FORCE_RUN=1`이면:
  - `mode="LIVE"` (실거래 모드)
  - `trading_day=True` (장이 열린 것으로 간주)
  - `window="day"` (장중 시간)
  - `source="force_run"` (강제 모드 표시)
- 기존 시간/요일 체크를 완전히 우회

---

### 3. Watchlist 모드 지원 (trader/pb1_runner.py)

**`_load_universe_context()` 함수 수정**

```python
def _load_universe_context(...) -> UniverseContext:
    # [NEW] WATCHLIST_MODE=1이면 WATCHLIST env에서 직접 로딩
    watchlist_mode = os.getenv("WATCHLIST_MODE", "0") == "1"
    if watchlist_mode:
        watchlist_codes = os.getenv("WATCHLIST", "005930,000660,035420,035720,005380")
        codes = [c.strip() for c in watchlist_codes.split(",") if c.strip()]
        members = [{"code": code, "name": code} for code in codes]
        logger.info(
            "[PB1][WATCHLIST_MODE] bypassing universe -> using %d codes: %s",
            len(codes),
            codes[:10],
        )
        return UniverseContext(
            as_of_date=as_of,
            members=members,
            selected_path=None,
            meta={"source": "watchlist_env", "codes": codes},
            is_empty=False,
        )
    
    # 기존 로직: DB에서 universe 로딩
    repo = UniverseRepo(engine)
    members = repo.get_universe_members(...)
    # ...
```

**동작 방식**:
- `WATCHLIST_MODE=1`이면 DB 유니버스 빌드/로딩 건너뛰기
- `WATCHLIST` 환경변수에서 직접 종목 코드 파싱
- 예: `WATCHLIST="005930,000660,035420"` → 3개 종목만 거래

---

### 4. 진단 로깅 강화

#### 4.1 런타임 시작 로깅 (trader/pb1_runner.py `run_once()`)

```python
def run_once(...):
    # [NEW] FORCE_RUN, WATCHLIST_MODE 로깅
    force_run = os.getenv("FORCE_RUN", "0") == "1"
    watchlist_mode = os.getenv("WATCHLIST_MODE", "0") == "1"
    dry_run = os.getenv("DRY_RUN", "0") == "1"
    live_trading = os.getenv("LIVE_TRADING_ENABLED", "0") == "1"
    
    if force_run or watchlist_mode:
        logger.info(
            "[PB1][FORCE_RUN] FORCE_RUN=%s WATCHLIST_MODE=%s WATCHLIST=%s DRY_RUN=%s LIVE_TRADING=%s",
            force_run,
            watchlist_mode,
            os.getenv("WATCHLIST", "")[:100],
            dry_run,
            live_trading,
        )
```

**출력 예시**:
```
[PB1][FORCE_RUN] FORCE_RUN=True WATCHLIST_MODE=True WATCHLIST=005930,000660,035420 DRY_RUN=False LIVE_TRADING=True
```

#### 4.2 주문 직전 로깅 (trader/kis_wrapper.py `_order_cash()`)

```python
# [NEW] FORCE_RUN 모드에서 주문 직전 로깅 강화
log_body_masked = {
    k: (v if k not in ("CANO", "ACNT_PRDT_CD") else "***")
    for k, v in body.items()
}
dry_run = os.getenv("DRY_RUN", "0") == "1"
live_trading = os.getenv("LIVE_TRADING_ENABLED", "0") == "1"
force_run = os.getenv("FORCE_RUN", "0") == "1"

logger.info(
    "[ORDER_READY] code=%s side=%s qty=%s price=%s tr_id=%s ord_dvsn=%s DRY_RUN=%s LIVE=%s FORCE_RUN=%s body=%s",
    body.get("PDNO"),
    "SELL" if is_sell else "BUY",
    body.get("ORD_QTY"),
    body.get("ORD_UNPR"),
    tr_id,
    ord_dvsn,
    dry_run,
    live_trading,
    force_run,
    log_body_masked,
)
```

**출력 예시**:
```
[ORDER_READY] code=005930 side=BUY qty=10 price=85000 tr_id=VTTC0012U ord_dvsn=01 DRY_RUN=False LIVE=True FORCE_RUN=True body={...}
```

---

## 사용 방법

### 수동 실행 (GitHub Actions UI)

1. GitHub Repository → Actions 탭
2. "PB1 Trade Runner" 워크플로우 선택
3. "Run workflow" 클릭
4. 파라미터 입력:
   ```
   FORCE_RUN: 1
   WATCHLIST_MODE: 1
   WATCHLIST: 005930,000660,035420,035720,005380
   LIVE_TRADING_ENABLED: 0  (테스트용)
   DRY_RUN: 1              (안전모드)
   ```
5. "Run workflow" 실행

### 로컬 실행 (CLI)

```bash
# 강제 모드 + Watchlist 모드 + DRY_RUN
export FORCE_RUN=1
export WATCHLIST_MODE=1
export WATCHLIST="005930,000660,035420,035720,005380"
export DRY_RUN=1
export LIVE_TRADING_ENABLED=0
export STRATEGY_MODE=LIVE

python -m trader.pb1_runner --env practice --strategy best_k_meta
```

### 실거래 주의사항

**실거래 모드 활성화 조건**:
```bash
FORCE_RUN=1
DRY_RUN=0
LIVE_TRADING_ENABLED=1
DISABLE_LIVE_TRADING=0
STRATEGY_MODE=LIVE
```

⚠️ **경고**: 실거래 모드에서는 실제 주문이 KIS API로 전송됩니다!

---

## 안전장치

### 1. STRATEGY_MODE=LIVE 검증 (trader/pb1_runner.py)

```python
if os.getenv("STRATEGY_MODE") == "LIVE":
    violations = []
    if os.getenv("LIVE_TRADING_ENABLED") != "1":
        violations.append("LIVE_TRADING_ENABLED != '1'")
    if os.getenv("DISABLE_LIVE_TRADING") == "1":
        violations.append("DISABLE_LIVE_TRADING == '1'")
    if os.getenv("DRY_RUN") == "1":
        violations.append("DRY_RUN == '1'")
    if violations:
        raise RuntimeError(f"LIVE mode violations: {', '.join(violations)}")
```

### 2. 주문 차단 로직 (trader/kis_wrapper.py)

- `_assert_orders_allowed()`: 실거래 플래그 검증
- `_order_block_reason()`: 시간대별 주문 차단
- 민감 정보 마스킹: `CANO`, `ACNT_PRDT_CD`

---

## 테스트 시나리오

### 시나리오 1: 장외 시간 강제 실행 (DRY_RUN)

```bash
# 토요일 오후 3시에 실행 (평소에는 NO_TRADE)
FORCE_RUN=1 \
WATCHLIST_MODE=1 \
WATCHLIST="005930,000660" \
DRY_RUN=1 \
LIVE_TRADING_ENABLED=0 \
python -m trader.pb1_runner --env practice --strategy best_k_meta
```

**기대 결과**:
- `resolve_strategy_mode()` → `("LIVE", True, "day", "force_run")`
- 유니버스 2종목 (`005930`, `000660`)
- 주문 시뮬레이션만 (실제 API 호출 없음)
- 로그: `[ORDER_READY] ... DRY_RUN=True LIVE=False FORCE_RUN=True`

### 시나리오 2: 소수 종목 실거래 (매우 위험!)

```bash
# 장중 시간에만 실행할 것!
FORCE_RUN=1 \
WATCHLIST_MODE=1 \
WATCHLIST="005930" \
DRY_RUN=0 \
LIVE_TRADING_ENABLED=1 \
DISABLE_LIVE_TRADING=0 \
STRATEGY_MODE=LIVE \
python -m trader.pb1_runner --env real --strategy best_k_meta
```

**기대 결과**:
- 실제 KIS API 주문 전송
- 로그: `[ORDER_READY] ... DRY_RUN=False LIVE=True FORCE_RUN=True`
- 주문 응답: `{"rt_cd": "0", "output": {"ODNO": "..."}}`

---

## 파일 변경 목록

| 파일 | 변경 내용 | 라인 수 |
|------|----------|---------|
| `.github/workflows/trade-runner.yml` | schedule 30분화, workflow_dispatch 입력값 5개 추가, env 전달 | ~30 |
| `trader/config.py` | `resolve_strategy_mode()` FORCE_RUN 체크 추가 | +5 |
| `trader/pb1_runner.py` | `_load_universe_context()` WATCHLIST_MODE, `run_once()` 로깅 강화 | +35 |
| `trader/kis_wrapper.py` | `_order_cash()` 주문 직전 로깅 강화 (DRY_RUN/FORCE_RUN 표시) | +17 |

**총 변경**: 4개 파일, 약 87줄

---

## 검증 체크리스트

- [x] GitHub Actions 워크플로우 문법 검증 (YAML 유효성)
- [x] `resolve_strategy_mode()` FORCE_RUN=1 테스트 (로컬)
- [x] `_load_universe_context()` WATCHLIST_MODE=1 테스트
- [x] `run_once()` 로깅 출력 확인
- [x] `kis_wrapper._order_cash()` ORDER_READY 로그 확인
- [ ] GitHub Actions UI에서 workflow_dispatch 실행 (수동)
- [ ] 장외 시간 FORCE_RUN=1 DRY_RUN=1 실행
- [ ] WATCHLIST="005930" 단일 종목 시뮬레이션

---

## 참고 자료

- [PB1 Watchlist 캐시 구현](PB1_WATCHLIST_PATCH_SUMMARY.md)
- [GitHub Actions Workflow Syntax](https://docs.github.com/en/actions/using-workflows/workflow-syntax-for-github-actions)
- [KIS API 주문 TR_ID 매핑](trader/kis_wrapper.py#L2753)

---

## 작성일
2025-01-XX (FORCE_RUN 모드 구현 완료)
