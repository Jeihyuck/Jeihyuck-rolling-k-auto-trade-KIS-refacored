# 후보군 ENV 네임스페이스 버그 수정 (2026-02-02)

## 🐛 문제 상황

### 증상
```
[VERIFY][OK] env=live ... members=120  ✅ 검증 성공
[WATCHLIST][LOAD] env=*** ... members=0  ❌ trade-tick에서 0개
[CANDIDATE_POOL][MISS] -> rebuild_light_scan
[OHLCV][KIS][FAIL] net fail
```

- **검증 스크립트**: `env=live`로 120개 후보군을 정상 확인
- **trade-tick**: 같은 DB, 같은 날짜인데 `members=0`으로 로드 실패
- **결과**: 불필요한 rebuild → KIS API 네트워크 실패 연쇄

---

## 🔍 근본 원인

### 환경 변수 불일치 (KIS_ENV vs STRATEGY_ENV)

#### 후보군 빌드 (candidate_pool_builder.py)
```python
# env 결정 방식
env = os.getenv("STRATEGY_ENV", "PAPER")  # STRATEGY_ENV 사용
```
→ **후보군은 `STRATEGY_ENV=live`로 DB에 저장됨**

#### trade-tick (pb1_runner.py → pb1_engine.py)
```python
# PB1Engine 생성 시
PB1Engine(..., env=kis_env or "practice")  # KIS_ENV 사용

# 후보군 로드 시
load_candidate_pool(engine=self.engine, env=self.env)  # kis_env로 검색
```
→ **trade-tick은 `KIS_ENV` (practice/real)로 DB 검색**

### 결과: 환경 변수 불일치
- 후보군 저장: `env=live` (STRATEGY_ENV)
- 후보군 검색: `env=practice` (KIS_ENV)
- DB 쿼리 WHERE 절: `env='practice'` → **0개 반환**

---

## ✅ 해결책

### 1. pb1_engine.py 수정

**변경 내용**: 후보군 로드/저장 시 `STRATEGY_ENV`를 우선 사용

```python
# Before
pool_codes, pool_as_of, pool_reason = load_candidate_pool(
    engine=self.engine,
    env=self.env,  # ❌ KIS_ENV (practice/real)
    today=today,
)

# After
import os
pool_env = os.getenv("STRATEGY_ENV", self.env)  # ✅ STRATEGY_ENV 우선

pool_codes, pool_as_of, pool_reason = load_candidate_pool(
    engine=self.engine,
    env=pool_env,  # ✅ STRATEGY_ENV (live/paper)
    today=today,
)
```

**변경 파일**: [trader/pb1_engine.py](trader/pb1_engine.py#L4130-L4145)

**적용 위치**:
1. 후보군 로드 ([라인 4140](trader/pb1_engine.py#L4140))
2. 후보군 rebuild ([라인 4175](trader/pb1_engine.py#L4175))

---

### 2. verify_candidate_pool.py 개선

**변경 내용**: `KIS_ENV` vs `STRATEGY_ENV` 차이 경고 추가

```python
# ✅ KIS_ENV 경고 추가
kis_env = os.getenv("KIS_ENV")
if kis_env and kis_env != env:
    print(f"[VERIFY][WARN] ⚠️  KIS_ENV={kis_env} != STRATEGY_ENV={env}")
    print(f"[VERIFY][WARN] Candidate pool uses STRATEGY_ENV (not KIS_ENV)")
    print(f"[VERIFY][WARN] Ensure STRATEGY_ENV={env} is set correctly for trade-tick")
```

**변경 파일**: [scripts/verify_candidate_pool.py](scripts/verify_candidate_pool.py)

---

## 📊 수정 효과

### Before (버그)
```
[후보군 빌드]
STRATEGY_ENV=live → DB에 env='live'로 저장 ✅

[trade-tick]
KIS_ENV=practice → DB에서 env='practice'로 검색 ❌
members=0 → rebuild → KIS API fail 💥
```

### After (수정)
```
[후보군 빌드]
STRATEGY_ENV=live → DB에 env='live'로 저장 ✅

[trade-tick]
STRATEGY_ENV=live → DB에서 env='live'로 검색 ✅
members=120 → rebuild skip → 정상 매매 🚀
```

---

## 🎯 핵심 원칙

### 후보군은 항상 STRATEGY_ENV로 관리
- **저장**: `STRATEGY_ENV` (live/paper)
- **로드**: `STRATEGY_ENV` (live/paper)
- **KIS_ENV**: KIS API 계정만 제어 (practice/real)

### 환경 변수 역할 분리
| 변수 | 역할 | 값 | 사용처 |
|------|------|-----|---------|
| `STRATEGY_ENV` | 전략 환경 | live/paper | 후보군, 유니버스, DB 네임스페이스 |
| `KIS_ENV` | KIS 계정 | practice/real | KIS API 인증, 주문 실행 |

### 올바른 설정 예시
```bash
# 모의투자 (practice 계좌 + live 전략)
STRATEGY_ENV=live
KIS_ENV=practice

# 실거래 (real 계좌 + live 전략)
STRATEGY_ENV=live
KIS_ENV=real

# 백테스트 (paper 전략)
STRATEGY_ENV=paper
KIS_ENV=practice  # 또는 미설정
```

---

## ✅ 검증 방법

### 1. 검증 스크립트 실행
```bash
STRATEGY_ENV=live AS_OF=2026-02-02 python scripts/verify_candidate_pool.py
```

**기대 출력**:
```
[VERIFY] env=live (STRATEGY_ENV)
[VERIFY] kis_env=practice (KIS_ENV - not used for candidate pool)
[VERIFY][OK] ✅ Candidate pool found!
[VERIFY][OK] size=120
[VERIFY][OK] ✅ All checks passed!
```

### 2. trade-tick 로그 확인
```
[WATCHLIST][LOAD] env=live strategy=pb1_candidate_pool as_of=2026-02-02 members=120
[CANDIDATE_POOL][USAGE] candidates_universe_size=120
```

**실패 징후 (수정 전)**:
```
[WATCHLIST][LOAD] env=*** members=0  ❌
[CANDIDATE_POOL][MISS] -> rebuild  ❌
```

---

## 📝 GitHub Actions 설정

### workflow 환경 변수 (trade_tick job)

**필수 설정**:
```yaml
env:
  # ✅ 후보군 로드용 (STRATEGY_ENV)
  STRATEGY_ENV: live
  
  # ✅ KIS 계좌 종류 (KIS_ENV - secret에서 주입)
  # KIS_ENV: practice/real (secret에서 설정)
  
  # ✅ 후보군 설정
  CANDIDATE_POOL_ENABLED: "1"
  CANDIDATE_POOL_STRATEGY_KEY: "pb1_candidate_pool"
  
  # ✅ 재빌드 금지 (주중에는 DB만 사용)
  PB1_WATCHLIST_AUTOBUILD: "0"
  CANDIDATE_POOL_FORCE_REBUILD: "0"
```

---

## 🚨 주의사항

### 1. STRATEGY_ENV는 필수
- trade-tick 실행 시 반드시 `STRATEGY_ENV=live` 설정
- 미설정 시 기본값 `STRATEGY_ENV=paper`로 동작 → 후보군 못 찾음

### 2. KIS_ENV와 혼동 금지
- `KIS_ENV`: KIS API 계정 (practice/real)
- `STRATEGY_ENV`: 전략 환경 (live/paper)
- 둘은 **독립적**으로 설정 가능

### 3. 후보군 빌드 시에도 동일 env 사용
```bash
# 후보군 빌드 (주말)
STRATEGY_ENV=live python -m trader.candidate_pool_builder --build pool

# trade-tick (주중)
STRATEGY_ENV=live python -m trader.pb1_runner trade-tick
```

---

## 📖 관련 문서

- [LIVE_TRADING_SETUP_COMPLETE.md](docs/LIVE_TRADING_SETUP_COMPLETE.md) - 실거래 설정
- [TRADE_TICK_ENV_SETUP.md](docs/TRADE_TICK_ENV_SETUP.md) - 환경 변수 가이드
- [PB1_WATCHLIST_AUTO_PIPELINE.md](PB1_WATCHLIST_AUTO_PIPELINE.md) - 후보군 파이프라인

---

## 🎉 결론

### 수정 완료 ✅
1. ✅ `pb1_engine.py`: 후보군 로드/저장 시 `STRATEGY_ENV` 우선 사용
2. ✅ `verify_candidate_pool.py`: `KIS_ENV` vs `STRATEGY_ENV` 불일치 경고
3. ✅ 문서화: 환경 변수 역할 명확화

### 예상 효과
- **members=0 문제 해결**: trade-tick이 `env=live`로 120개 정상 로드
- **불필요한 rebuild 제거**: 후보군 있으면 rebuild 안 함
- **KIS API 실패 감소**: rebuild 안 하니 KIS 일봉 API 호출 안 함
- **안정적인 매매**: 120개 후보군으로 Minervini 입력 정상화

### 다음 실행 시 확인사항
```bash
# 1. 환경 변수 확인
echo $STRATEGY_ENV  # live
echo $KIS_ENV       # practice 또는 real

# 2. 검증 스크립트
STRATEGY_ENV=live AS_OF=2026-02-02 python scripts/verify_candidate_pool.py

# 3. trade-tick 로그에서 확인
# [WATCHLIST][LOAD] env=live ... members=120 ✅
```

**Good luck! 🚀**
