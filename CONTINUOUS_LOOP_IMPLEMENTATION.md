# 연속 루프 구현 완료 (5분 크론 제약 제거)

## 📋 요약

GitHub Actions의 5분 크론을 제거하고, 한 번 실행으로 장중 연속 감시/매매가 가능하도록 시스템을 재설계했습니다.

---

## ✅ 구현된 기능

### 1. GitHub Actions Workflow 개선

**파일**: [.github/workflows/trade_intraday.yml](.github/workflows/trade_intraday.yml)

**변경사항**:
- ❌ **5분 크론 제거**: `*/5 * * * 1-5` → 수동 실행(`workflow_dispatch`)만 활성화
- ⏱️ **타임아웃 증가**: `timeout-minutes: 360` (6시간) - 장중 연속 운용 지원
- 🔒 **중복 실행 방지**: `concurrency` 설정으로 동시 실행 차단
- 🔄 **AUTO 모드**: `STRATEGY_MODE: AUTO` - 장중/장외 자동 전환

**주요 환경변수**:
```yaml
STRATEGY_MODE: AUTO                  # 장중=LIVE, 장외=DIAG
PB1_LOOP_ENABLED: "1"                # 루프 활성화
PB1_LOOP_INTERVAL_SEC: "30"          # 30초 간격 (조정 가능)
PB1_LOOP_MODE: "UNTIL_CLOSE"         # 장마감까지 계속
PB1_LOOP_GRACE_MIN: "3"              # 마감 직전 여유시간
KIS_BLOCK_ALL_ON_DIAG: "1"           # DIAG 시 KIS API 차단
```

---

### 2. AUTO 모드 결정 로직

**파일**: [trader/pb1_runner.py](trader/pb1_runner.py)

**새로운 함수**:
```python
def resolve_auto_strategy_mode(mode_env: str) -> str:
    """
    AUTO 모드 결정: 시간 기반으로 LIVE/DIAG 결정.
    
    - LIVE/DIAG 명시 시 그대로 사용
    - AUTO 시:
        - 장중(09:00~15:20, 월~금): LIVE
        - 장외(주말, 장시작 전, 장마감 후): DIAG
    """
```

**정책**:
- `STRATEGY_MODE=LIVE` → 강제 LIVE (테스트용)
- `STRATEGY_MODE=DIAG` → 강제 DIAG (테스트용)
- `STRATEGY_MODE=AUTO` → 시간 기반 자동 전환 ⭐
- DIAG 모드에서는 **루프 비활성화** (한 번만 실행)

---

### 3. UNTIL_CLOSE 루프 모드

**파일**: [trader/pb1_runner.py](trader/pb1_runner.py)

**기존 문제**:
```python
# 기존: 15분 하드코딩 → 장중에도 짧게 끊김
max_minutes = 15
```

**해결책**:
```python
def compute_loop_deadline(now: datetime) -> datetime:
    """장 마감(15:15) - grace_min(3분) = 15:12까지 루프"""
    grace_min = int(os.getenv("PB1_LOOP_GRACE_MIN", "3"))
    market_close = market_close_dt_kst(now)  # 15:15
    return market_close - timedelta(minutes=grace_min)
```

**루프 종료 조건** (UNTIL_CLOSE 모드):
1. `now >= deadline` (15:12 도달)
2. SIGTERM/SIGINT (수동 중단)
3. max_seconds 도달 (폴백)

---

### 4. DIAG 모드 KIS API 차단

**파일**: [trader/kis_wrapper.py](trader/kis_wrapper.py)

**새로운 예외**:
```python
class KISBlockedError(RuntimeError):
    """DIAG 모드에서 KIS API 차단."""
    pass
```

**차단 로직** (`_safe_request`):
```python
strategy_mode = os.getenv("STRATEGY_MODE", "").upper()
block_on_diag = os.getenv("KIS_BLOCK_ALL_ON_DIAG", "1") == "1"

if strategy_mode == "DIAG" and block_on_diag:
    raise KISBlockedError(f"KIS API blocked in DIAG mode: {method} {url}")
```

**정책**:
- `KIS_BLOCK_ALL_ON_DIAG=1` (기본): DIAG에서 모든 KIS API 차단
- `DIAG_ALLOW_KIS_MARKETDATA=1`: 시세 조회만 허용 (선택)
- **로직은 끝까지 실행**, KIS API만 차단되어 에러 발생

---

### 5. Watchlist Date 타입 에러 수정

**파일**: [trader/db/repos.py](trader/db/repos.py)

**문제**:
```
operator does not exist: date = character varying
```

**원인**: `as_of` 파라미터가 문자열로 전달되어 PostgreSQL DATE 컬럼과 비교 불가

**해결**:
```python
def load_watchlist(self, *, env: str, strategy: str, as_of: date | str):
    # ✅ 문자열 → date 객체 변환
    as_of_date = to_date(as_of)
    
    stmt = select(schema.pb1_watchlist).where(
        and_(
            schema.pb1_watchlist.c.env == env,
            schema.pb1_watchlist.c.strategy == strategy,
            schema.pb1_watchlist.c.as_of == as_of_date,  # ✅ DATE 타입
        )
    )
```

---

### 6. 시간 유틸리티 함수 추가

**파일**: [trader/time_utils.py](trader/time_utils.py)

**새로운 함수**:
```python
def is_market_open_kst(dt: datetime | None = None) -> bool:
    """장중 여부 판단 (AUTO 모드 결정용)"""
    dt = dt or now_kst()
    if not is_trading_weekday(dt):
        return False
    t = dt.time()
    return MARKET_OPEN <= t <= MARKET_CLOSE  # 09:00~15:20

def market_close_dt_kst(dt: datetime) -> datetime:
    """주어진 날짜의 장 마감 시각(15:15) 반환"""
    return dt.replace(hour=15, minute=15, second=0, microsecond=0)
```

---

## 🚀 사용 방법

### 1. 수동 실행 (확인용)

GitHub Actions > trade_intraday > "Run workflow" 클릭

```yaml
# 기본 설정 (AUTO 모드)
STRATEGY_MODE: AUTO              # 장중=LIVE, 장외=DIAG
PB1_LOOP_INTERVAL_SEC: "30"      # 30초 간격
PB1_LOOP_MODE: "UNTIL_CLOSE"     # 장마감까지
```

### 2. 자동 시동 (확인 후 활성화)

[.github/workflows/trade_intraday.yml](.github/workflows/trade_intraday.yml)의 주석 해제:

```yaml
on:
  schedule:
    - cron: "55 23 * * 0-4"  # KST 08:55 (UTC 23:55) 월~금
  workflow_dispatch:
```

**정책**:
- **크론은 "시동"만 담당** (하루 1회)
- **내부 루프가 장중 감시/매매 담당** (UNTIL_CLOSE)
- 5분마다 재시작 ❌ → 한 번 시작으로 6시간 연속 ✅

---

## 📊 동작 흐름

### 장중 (월~금 09:00~15:20)

```
08:55 → GitHub Actions 크론 시작
09:00 → AUTO 모드 → LIVE 결정
09:00 → 루프 시작 (PB1_LOOP_MODE=UNTIL_CLOSE)
09:00~15:12 → 30초마다 run_once() 실행
  - 후보 선정
  - 돌파 감지
  - 주문 제출
15:12 → deadline 도달 → 루프 종료
15:15 → 장 마감
```

### 장외 (주말, 장시작 전, 장마감 후)

```
23:00 → GitHub Actions 크론 시작 (토요일)
23:00 → AUTO 모드 → DIAG 결정
23:00 → 루프 비활성화 (run_loop=False)
23:00 → run_once() 1회만 실행
  - 후보 선정 (KIS API 차단 → 에러)
  - 로직은 끝까지 실행
  - SIM 주문 기록 (선택)
23:00 → 종료
```

---

## ⚙️ 환경변수 제어

### 필수 설정

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `STRATEGY_MODE` | `AUTO` | `LIVE` / `DIAG` / `AUTO` |
| `PB1_LOOP_ENABLED` | `0` | 루프 활성화 (`1`) |
| `PB1_LOOP_INTERVAL_SEC` | `60` | 루프 간격 (초) |
| `PB1_LOOP_MODE` | - | `UNTIL_CLOSE`: 장마감까지 |

### 고급 설정

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `PB1_LOOP_GRACE_MIN` | `3` | 마감 전 여유시간 (분) |
| `KIS_BLOCK_ALL_ON_DIAG` | `1` | DIAG에서 KIS API 차단 |
| `DIAG_ALLOW_KIS_MARKETDATA` | `0` | DIAG에서 시세만 허용 |
| `PB1_DIAG_RUN_FULL` | `1` | DIAG에서 로직 끝까지 실행 |
| `PB1_DIAG_WRITE_SIM_ORDERS` | `1` | DIAG에서 SIM 주문 기록 |

---

## 🔍 검증 포인트

### 1. AUTO 모드 결정 확인

로그에서 확인:
```
[PB1][MODE] mode_env=AUTO resolved=LIVE fixed_in_env=True
[AUTO_MODE] mode=AUTO resolved=LIVE is_market_open=True now=2026-01-30 09:05:00
```

### 2. 루프 deadline 확인

로그에서 확인:
```
[LOOP_DEADLINE] market_close=15:15:00 grace_min=3 deadline=15:12:00
[PB1][LOOP] mode=UNTIL_CLOSE interval=30 deadline=2026-01-30 15:12:00
```

### 3. DIAG 모드 KIS 차단 확인

로그에서 확인:
```
[PB1][DIAG] mode=DIAG -> disable loop (run once)
[NET][SAFE] KIS API blocked in DIAG mode: GET /uapi/domestic-stock/...
```

### 4. Watchlist 타입 에러 제거 확인

로그에서 에러 없음:
```
[WATCHLIST][LOAD] env=live strategy=best_k_meta as_of=2026-01-27 members=50
```

---

## 🎯 다음 단계

### 1. 초기 확인 (수동 실행)

- [ ] `workflow_dispatch`로 수동 실행
- [ ] 로그에서 AUTO 모드 결정 확인
- [ ] UNTIL_CLOSE 루프 정상 동작 확인
- [ ] 돌파 매수/매도 정상 실행 확인

### 2. 파라미터 조정

- [ ] `PB1_LOOP_INTERVAL_SEC` 조정 (30초 → 60초?)
- [ ] `PB1_LOOP_GRACE_MIN` 조정 (3분 → 5분?)
- [ ] `timeout-minutes` 조정 (360분 → 300분?)

### 3. 자동 시동 활성화

- [ ] `schedule` 주석 해제 (KST 08:55)
- [ ] 며칠간 모니터링
- [ ] 로그 분석 및 최적화

---

## 📝 변경 파일 목록

1. [.github/workflows/trade_intraday.yml](.github/workflows/trade_intraday.yml)
   - 5분 크론 제거
   - 타임아웃 증가 (360분)
   - AUTO 모드 환경변수 추가

2. [trader/pb1_runner.py](trader/pb1_runner.py)
   - `resolve_auto_strategy_mode()` 추가
   - `compute_loop_deadline()` 추가
   - `_resolve_loop_limits()` UNTIL_CLOSE 지원
   - `main()` AUTO 모드 결정 및 고정

3. [trader/kis_wrapper.py](trader/kis_wrapper.py)
   - `KISBlockedError` 예외 추가
   - `_safe_request()` DIAG 차단 로직 추가
   - `__all__` export 추가

4. [trader/db/repos.py](trader/db/repos.py)
   - `load_watchlist()` date 타입 강제 변환
   - `to_date()` 사용으로 VARCHAR 에러 방지

5. [trader/time_utils.py](trader/time_utils.py)
   - `is_market_open_kst()` 추가
   - `market_close_dt_kst()` 추가

---

## 🚨 주의사항

1. **첫 실행은 반드시 수동으로**: `workflow_dispatch`로 "잘 도는지" 확인 후 자동 시동
2. **DIAG 모드 에러는 정상**: KIS API 차단으로 인한 에러는 예상된 동작
3. **루프 간격 조정**: 초기 30초 → 안정화 후 60초 등 조정 가능
4. **타임아웃 여유**: 360분(6시간)은 충분하지만, 플랫폼 제한 고려

---

## 📚 참고

- CEO 요구사항: "5분 제약 없이 장중 연속 감시"
- 핵심 원칙: **크론은 시동만, 내부 루프가 매매 담당**
- 장외 정책: **DIAG 자동 전환 + KIS API 0 + 로직 끝까지 실행**

---

**구현 완료일**: 2026-01-30  
**작성자**: GitHub Copilot  
**버전**: v1.0
