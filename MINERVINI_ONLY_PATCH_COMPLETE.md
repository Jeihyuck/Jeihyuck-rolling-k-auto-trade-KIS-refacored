# ✅ MINERVINI_ONLY 패치 완료 - Analytics + DB 저장 + LIVE 연동

## 📋 패치 개요

MINERVINI_ONLY=1 모드에서 **잔고 확인 없이도 미너비니 필터가 끝까지 실행**되고, **TopN을 DB에 저장**하여 **내일 LIVE가 바로 사용**할 수 있도록 전체 파이프라인을 완성했습니다.

---

## 🔧 수정된 파일 (5개)

### 1. `trader/config.py`
**변경 내용**: MINERVINI analytics-only 관련 설정 추가

```python
# MINERVINI analytics-only controls
MINERVINI_BYPASS_BALANCE = _env_flag("MINERVINI_BYPASS_BALANCE", "1")
MINERVINI_TOPN_STRATEGY_KEY = os.getenv("MINERVINI_TOPN_STRATEGY_KEY", "pb1_minervini_topn")
MINERVINI_WRITE_TO_CANDIDATE_POOL = _env_flag("MINERVINI_WRITE_TO_CANDIDATE_POOL", "1")
PB1_CANDIDATE_POOL_KEY = os.getenv("PB1_CANDIDATE_POOL_KEY", "pb1_candidate_pool")
MINERVINI_TOPN_N = int(os.getenv("MINERVINI_TOPN_N", "10"))
```

**설정 설명**:
- `MINERVINI_BYPASS_BALANCE=1` (기본): analytics-only일 때 잔고 게이트 우회
- `MINERVINI_TOPN_STRATEGY_KEY`: TopN을 저장할 watchlist 키
- `MINERVINI_WRITE_TO_CANDIDATE_POOL=1` (기본): LIVE 연동을 위해 candidate_pool에도 저장
- `MINERVINI_TOPN_N=10`: TopN 개수

---

### 2. `trader/pb1_runner.py`
**변경 내용**: 잔고 게이트를 주문 게이트로 분리

#### 패치 위치 1: entry_allowed_this_tick 결정 로직
```python
# ✅ analytics-only 체크 미리 계산
from trader.config import MINERVINI_BYPASS_BALANCE
analytics_only = minervini_only and MINERVINI_BYPASS_BALANCE

if balance_state == BALANCE_STATE_UNKNOWN:
    if not analytics_only:
        entry_allowed_this_tick = False
        entry_block_reason = entry_block_reason or "balance_unknown"
```

#### 패치 위치 2: DEGRADED 조기 리턴 방지
```python
# ✅ analytics-only 모드에서는 잔고 게이트를 우회
analytics_only = minervini_only and MINERVINI_BYPASS_BALANCE

if balance_state == BALANCE_STATE_UNKNOWN and PB1_REQUIRE_BALANCE_FOR_ENTRY:
    if analytics_only:
        # analytics-only에서는 막지 않는다 (주문은 kis=None으로 이미 차단됨)
        logger.warning("[PB1][ENTRY_NOT_BLOCKED] analytics_only -> continue (no orders)")
    else:
        logger.warning("[PB1][DEGRADED] reason=balance_unknown -> skip trading")
        runs_repo.finish_run(run_record_id, status="DEGRADED", notes="balance_unknown")
        db_write_reasons.append("balance_degraded")
        _write_last_db_write(runtime_root_dir, run_id=str(run_record_id), reason="balance_degraded", now=now)
        return [], False, {}, phase_for_log, "DEGRADED_BALANCE_UNKNOWN"
```

**동작 원리**:
- `analytics_only=True`이면 필터/선정 파이프라인은 계속 실행
- 주문은 `kis=None`이므로 자동으로 차단됨
- 일반 모드는 기존 동작 유지 (balance_unknown이면 DEGRADED 반환)

---

### 3. `trader/db/repos.py`
**변경 내용**: 간단한 코드 리스트로 watchlist 저장하는 helper 함수 추가

```python
def save_watchlist_simple(
    engine: Engine,
    *,
    env: str,
    strategy: str,
    as_of: date,
    codes: List[str],
    meta: Dict[str, Any] | None = None,
) -> None:
    """
    간단한 코드 리스트로 watchlist 저장.
    
    Args:
        engine: DB 엔진
        env: 환경 (live/paper)
        strategy: 전략 키
        as_of: 기준 날짜
        codes: 종목 코드 리스트
        meta: 메타 정보 (전체 watchlist에 공통으로 저장)
    """
    as_of = to_date(as_of)
    repo = WatchlistRepo(engine)
    members = [
        {
            "code": str(code).zfill(6),
            "rank": i + 1,
            "score": None,
            "meta": meta or {},
        }
        for i, code in enumerate(codes)
    ]
    repo.save_watchlist(env=env, strategy=strategy, as_of=as_of, members=members)
```

**용도**: 미너비니 TopN을 간단하게 DB에 저장

---

### 4. `trader/minervini_runner.py`
**변경 내용**: `run_diag_minervini_only` 함수에 TopN DB 저장 로직 추가

```python
# ✅ TopN DB 저장 (MINERVINI_ONLY 전용)
from trader.config import (
    MINERVINI_TOPN_N,
    MINERVINI_TOPN_STRATEGY_KEY,
    MINERVINI_WRITE_TO_CANDIDATE_POOL,
    PB1_CANDIDATE_POOL_KEY,
)
from trader.db.repos import save_watchlist_simple

topn_codes = [c["code"] for c in top_candidates[:MINERVINI_TOPN_N]]

if topn_codes:
    # TopN Watchlist 저장
    topn_meta = {
        "source": "minervini",
        "topn_n": MINERVINI_TOPN_N,
        "params": {
            "rs_min_percentile": minervini_config.rs_min_percentile,
            "vcp_min_score": minervini_config.vcp_min_score,
            "min_dollar_vol_50d": minervini_config.min_dollar_vol_50d,
        },
        "sample": topn_codes[:5],
    }
    
    save_watchlist_simple(
        engine,
        env=env,
        strategy=MINERVINI_TOPN_STRATEGY_KEY,
        as_of=as_of,
        codes=topn_codes,
        meta=topn_meta,
    )
    logger.info(
        "[MINERVINI][TOPN][SAVED] key=%s as_of=%s size=%s sample=%s",
        MINERVINI_TOPN_STRATEGY_KEY, as_of, len(topn_codes), topn_codes[:5]
    )
    
    # LIVE 연동: candidate_pool에도 저장
    if MINERVINI_WRITE_TO_CANDIDATE_POOL:
        pool_meta = {**topn_meta, "alias_of": MINERVINI_TOPN_STRATEGY_KEY}
        save_watchlist_simple(
            engine,
            env=env,
            strategy=PB1_CANDIDATE_POOL_KEY,
            as_of=as_of,
            codes=topn_codes,
            meta=pool_meta,
        )
        logger.info(
            "[CANDIDATE_POOL][SAVED] key=%s as_of=%s size=%s",
            PB1_CANDIDATE_POOL_KEY, as_of, len(topn_codes)
        )
```

**동작**:
1. 미너비니 필터를 통과한 종목 중 상위 N개 선정
2. `pb1_minervini_topn` watchlist에 저장
3. `pb1_candidate_pool`에도 저장 (LIVE가 바로 로드)

---

### 5. `trader/kis_wrapper.py`
**변경 내용**: 모든 주문 함수에 MINERVINI_ONLY 안전장치 추가

```python
def buy_stock_market(self, pdno: str, qty: int) -> Optional[dict]:
    # ✅ MINERVINI_ONLY 안전장치: analytics-only일 때는 주문 금지
    if os.getenv("MINERVINI_ONLY", "0").strip().lower() in ("1", "true", "yes", "y", "on"):
        logger.warning("[MINERVINI_ONLY] orders are disabled -> skip buy_stock_market")
        return None
    ...

def sell_stock_market(self, pdno: str, qty: int) -> Optional[dict]:
    # ✅ MINERVINI_ONLY 안전장치
    if os.getenv("MINERVINI_ONLY", "0").strip().lower() in ("1", "true", "yes", "y", "on"):
        logger.warning("[MINERVINI_ONLY] orders are disabled -> skip sell_stock_market")
        return None
    ...

def buy_stock_limit(self, pdno: str, qty: int, price: int) -> Optional[dict]:
    # ✅ MINERVINI_ONLY 안전장치
    if os.getenv("MINERVINI_ONLY", "0").strip().lower() in ("1", "true", "yes", "y", "on"):
        logger.warning("[MINERVINI_ONLY] orders are disabled -> skip buy_stock_limit")
        return None
    ...

def sell_stock_limit(self, pdno: str, qty: int, price: int) -> Optional[dict]:
    # ✅ MINERVINI_ONLY 안전장치
    if os.getenv("MINERVINI_ONLY", "0").strip().lower() in ("1", "true", "yes", "y", "on"):
        logger.warning("[MINERVINI_ONLY] orders are disabled -> skip sell_stock_limit")
        return None
    ...
```

**목적**: 코드 레벨에서 MINERVINI_ONLY=1일 때 주문이 100% 차단되도록 보장

---

## ✅ 검증 기준 (성공 로그 패턴)

### Analytics-Only 실행 성공 패턴
```
[PB1][ENTRY_NOT_BLOCKED] analytics_only -> continue (no orders)
[MINERVINI][RUN] ...
[MINERVINI][TOPN] size=10 sample=...
[MINERVINI][TOPN][SAVED] key=pb1_minervini_topn as_of=YYYY-MM-DD size=10 sample=[...]
[CANDIDATE_POOL][SAVED] key=pb1_candidate_pool as_of=YYYY-MM-DD size=10
```

**절대 나오면 안 되는 로그**:
- `ENTRY_BLOCKED reason=balance_unknown` 뒤 즉시 종료 (return)
- 주문 ack/fill 로그 (analytics-only에서)

### LIVE 실행 성공 패턴 (내일)
```
[WATCHLIST][LOAD] as_of=YYYY-MM-DD members>=min_size
[CANDIDATE_POOL] enabled=1 size=10..
```

---

## 🚀 사용 방법

### 1. Analytics-Only 실행 (밤에 자동 실행)
```bash
export MINERVINI_ONLY=1
export MINERVINI_BYPASS_BALANCE=1
export MINERVINI_WRITE_TO_CANDIDATE_POOL=1
export KIS_HTTP_ENABLED=0  # 안전
export DRY_RUN=1  # 안전
export MINERVINI_TOPN_N=10

# 실행
python -m trader.pb1_runner ...
```

### 2. LIVE 실행 (다음날)
```bash
export MINERVINI_ONLY=0
export KIS_HTTP_ENABLED=1
export DRY_RUN=0
export DISABLE_LIVE_TRADING=0
export STRATEGY_MODE=LIVE

# 실행 (DB에서 candidate_pool 자동 로드)
python -m trader.pb1_runner ...
```

---

## 📊 데이터 흐름

```
밤 (Analytics-Only)
  ↓
유니버스 로드
  ↓
미너비니 필터 실행 (balance 불필요)
  ↓
TopN 선정 (score 순)
  ↓
DB 저장:
  - pb1_minervini_topn (as_of=밤 날짜)
  - pb1_candidate_pool (as_of=밤 날짜)
  ↓
다음날 아침 (LIVE)
  ↓
DB에서 candidate_pool 로드 (fallback 포함)
  ↓
진입 로직 실행 (balance 정상 조회)
  ↓
주문 실행
```

---

## 🔐 안전장치 요약

1. **Balance Gate Bypass**: `analytics_only=True`이면 잔고 게이트 통과
2. **Order Gate**: 모든 주문 함수에서 `MINERVINI_ONLY=1` 체크 → return None
3. **KIS API Init Skip**: `MINERVINI_ONLY=1`이면 KisAPI 초기화 생략
4. **DB 저장 전용**: analytics-only는 "필터 + 저장"만 수행

---

## 📝 환경변수 전체 목록

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `MINERVINI_ONLY` | 0 | 1이면 analytics-only 모드 |
| `MINERVINI_BYPASS_BALANCE` | 1 | analytics-only일 때 balance gate 우회 |
| `MINERVINI_TOPN_STRATEGY_KEY` | `pb1_minervini_topn` | TopN watchlist 저장 키 |
| `MINERVINI_WRITE_TO_CANDIDATE_POOL` | 1 | candidate_pool에도 저장 여부 |
| `PB1_CANDIDATE_POOL_KEY` | `pb1_candidate_pool` | candidate pool 키 |
| `MINERVINI_TOPN_N` | 10 | TopN 개수 |

---

## 🎯 핵심 개선 효과

1. ✅ **MINERVINI_ONLY=1에서 끝까지 실행**: balance 없어도 필터 완료
2. ✅ **TopN DB 저장**: 다음날 LIVE가 바로 사용 가능
3. ✅ **LIVE 연동 자동화**: candidate_pool에 저장되어 수동 작업 불필요
4. ✅ **주문 안전장치**: 코드 레벨에서 MINERVINI_ONLY 체크
5. ✅ **기존 로직 보존**: 일반 모드는 변경 없음

---

## 📌 추가 권장사항

### GitHub Actions Workflow

#### analytics-only (밤)
```yaml
- name: Run Analytics-Only
  env:
    MINERVINI_ONLY: 1
    MINERVINI_BYPASS_BALANCE: 1
    MINERVINI_WRITE_TO_CANDIDATE_POOL: 1
    KIS_HTTP_ENABLED: 0
    DRY_RUN: 1
  run: |
    python -m trader.pb1_runner
```

#### LIVE (아침)
```yaml
- name: Run LIVE
  env:
    MINERVINI_ONLY: 0
    KIS_HTTP_ENABLED: 1
    DRY_RUN: 0
    DISABLE_LIVE_TRADING: 0
    STRATEGY_MODE: LIVE
  run: |
    python -m trader.pb1_runner
```

---

## 🔍 디버깅 팁

### 1. DB 저장 확인
```sql
SELECT * FROM pb1_watchlist 
WHERE strategy IN ('pb1_minervini_topn', 'pb1_candidate_pool')
  AND as_of = '2026-02-03'
ORDER BY strategy, rank;
```

### 2. 로그 패턴 확인
```bash
# analytics-only 성공 확인
grep -E "BALANCE.*BYPASS|TOPN.*SAVED|CANDIDATE_POOL.*SAVED" logs/pb1_runner.log

# 주문 차단 확인
grep "MINERVINI_ONLY.*orders are disabled" logs/pb1_runner.log
```

### 3. 전체 흐름 추적
```bash
# 밤 실행 흐름
grep -E "MINERVINI_ONLY|BALANCE|TOPN|CANDIDATE_POOL" logs/pb1_runner.log | tail -50

# LIVE 실행 흐름
grep -E "WATCHLIST.*LOAD|CANDIDATE_POOL.*enabled" logs/pb1_runner.log | tail -50
```

---

## ✅ 패치 완료 체크리스트

- [x] config.py: MINERVINI analytics-only 설정 추가
- [x] pb1_runner.py: balance gate → order gate 분리
- [x] repos.py: save_watchlist_simple 함수 추가
- [x] minervini_runner.py: TopN DB 저장 로직 추가
- [x] kis_wrapper.py: 주문 함수에 MINERVINI_ONLY 안전장치 추가
- [x] 에러 없음 확인 (모든 파일 pylance 통과)

---

**패치 완료일**: 2026-02-03  
**패치 작성자**: GitHub Copilot
