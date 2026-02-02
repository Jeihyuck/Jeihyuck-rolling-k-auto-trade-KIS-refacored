# 후보군 0개 버그 수정 완료 - 실행 가이드

## ✅ 수정 완료 (2026-02-02)

### 🐛 문제
- **증상**: 후보군이 DB에 120개 있는데도 trade-tick에서 0개로 로드
- **원인**: `STRATEGY_ENV=live`로 저장했는데 `KIS_ENV`로 검색
- **결과**: 불필요한 rebuild → KIS API 실패 연쇄

### ✅ 해결
1. **[pb1_engine.py](trader/pb1_engine.py#L4130-L4180)**: 후보군 로드 시 `STRATEGY_ENV` 우선 사용
2. **[verify_candidate_pool.py](scripts/verify_candidate_pool.py)**: KIS_ENV vs STRATEGY_ENV 경고 추가
3. **문서화**: [ENV_NAMESPACE_FIX.md](ENV_NAMESPACE_FIX.md) 작성

---

## 🚀 오늘 매매를 위한 실행 가이드

### 1️⃣ 환경 변수 설정 (필수)

**GitHub Actions Dispatch 입력값**:
```
MODE: trade
ENV: LIVE
FORCE_REBUILD: 0
LIVE_TRADING_ENABLED: 1
DRY_RUN: 0
```

**Workflow 환경 변수** (.github/workflows/trade-runner.yml):
```yaml
env:
  STRATEGY_ENV: live              # ✅ 후보군 로드용 (가장 중요!)
  STRATEGY_MODE: LIVE
  LIVE_TRADING_ENABLED: 1
  DRY_RUN: 0
  DISABLE_LIVE_TRADING: 0
  
  # 후보군 설정
  CANDIDATE_POOL_ENABLED: "1"
  CANDIDATE_POOL_STRATEGY_KEY: "pb1_candidate_pool"
  
  # 재빌드 금지
  PB1_WATCHLIST_AUTOBUILD: "0"
  CANDIDATE_POOL_FORCE_REBUILD: "0"
```

**Secrets 확인**:
- `KIS_ENV`: `practice` 또는 `real`
- `PBCORE_DB_URL`: PostgreSQL 연결 문자열
- KIS API 키들

---

### 2️⃣ 검증 (로컬에서 먼저 확인)

```bash
# 환경 변수 설정
export STRATEGY_ENV=live
export AS_OF=2026-02-02
export PBCORE_DB_URL="postgresql://..."

# 후보군 검증
python scripts/verify_candidate_pool.py
```

**기대 출력**:
```
[VERIFY] env=live (STRATEGY_ENV)
[VERIFY][OK] ✅ Candidate pool found!
[VERIFY][OK] size=120
[VERIFY][OK] ✅ All checks passed!
```

**⚠️ 만약 실패하면**:
- `size=0`: 후보군이 없거나 env/as_of 불일치
- `KIS_ENV != STRATEGY_ENV` 경고: 정상 (무시해도 됨)

---

### 3️⃣ GitHub Actions 실행

1. **Actions 탭** → **Trade Runner** 선택
2. **Run workflow** 클릭
3. 입력값 확인:
   - `MODE`: `trade`
   - `ENV`: `LIVE`
   - `FORCE_REBUILD`: `0`
   - `LIVE_TRADING_ENABLED`: `1`
   - `DRY_RUN`: `0`
4. **Run workflow** 실행

---

### 4️⃣ 로그 모니터링 (성공 확인)

**✅ 정상 로그**:
```
[VERIFY][OK] ✅ Candidate pool found!
[VERIFY][OK] size=120
[POOL][PRECHECK] ✅ Candidate pool verification passed

[WATCHLIST][LOAD] env=live strategy=pb1_candidate_pool as_of=2026-02-02 members=120
[CANDIDATE_POOL][USAGE] candidates_universe_size=120 (NOT 195 universe)
[PB1][ENTRY] candidates scanning from 120 members
```

**❌ 실패 징후 (즉시 중단!)**:
```
[VERIFY][FAIL] ❌ Candidate pool is EMPTY!
[WATCHLIST][LOAD] env=*** members=0
[CANDIDATE_POOL][MISS] -> rebuild_light_scan
[OHLCV][KIS][FAIL] net fail
```

---

## 🔍 문제 해결

### Q1: `members=0`이 뜨는 경우

**원인**: `STRATEGY_ENV` 미설정 또는 잘못된 값

**해결**:
```bash
# workflow 환경 변수에 추가
STRATEGY_ENV: live
```

### Q2: `[VERIFY][WARN] KIS_ENV != STRATEGY_ENV` 경고

**원인**: KIS_ENV와 STRATEGY_ENV가 다름 (정상 상황)

**설명**:
- `KIS_ENV`: KIS 계정 (practice/real)
- `STRATEGY_ENV`: 후보군 네임스페이스 (live/paper)
- 둘은 **독립적**이므로 다를 수 있음

**액션**: 경고 무시 (정상)

### Q3: rebuild가 계속 돌아가는 경우

**원인**: 재빌드 금지 설정 누락

**해결**:
```bash
CANDIDATE_POOL_FORCE_REBUILD=0
PB1_WATCHLIST_AUTOBUILD=0
```

---

## 📊 핵심 개념

### 환경 변수 역할

| 변수 | 역할 | 값 | 비고 |
|------|------|-----|------|
| `STRATEGY_ENV` | 후보군/DB 네임스페이스 | live/paper | **가장 중요!** |
| `KIS_ENV` | KIS API 계정 | practice/real | 주문 실행만 관여 |

### 올바른 조합

```bash
# 모의투자 (practice 계좌 + live 전략)
STRATEGY_ENV=live
KIS_ENV=practice

# 실거래 (real 계좌 + live 전략)
STRATEGY_ENV=live
KIS_ENV=real
```

---

## 📝 다음 단계

### 오늘 (2026-02-02)
1. ✅ GitHub Actions 실행
2. ✅ 로그에서 `members=120` 확인
3. ✅ 매매 정상 진행 확인

### 내일부터
- 주중: 같은 설정으로 계속 실행 (재빌드 없음)
- 다음 주말: 후보군 갱신 (candidate pool build)

---

## 📖 관련 문서

- **[ENV_NAMESPACE_FIX.md](ENV_NAMESPACE_FIX.md)**: 버그 상세 분석 및 수정 내용
- **[LIVE_TRADING_SETUP_COMPLETE.md](docs/LIVE_TRADING_SETUP_COMPLETE.md)**: 실거래 전체 설정
- **[TRADE_TICK_ENV_SETUP.md](docs/TRADE_TICK_ENV_SETUP.md)**: 환경 변수 가이드

---

## ✅ 최종 체크리스트

- [ ] `STRATEGY_ENV=live` 설정 확인
- [ ] 검증 스크립트 성공 (`members=120`)
- [ ] GitHub Actions 입력값 확인
- [ ] Secrets 설정 확인 (KIS_ENV, DB_URL, API 키)
- [ ] 로그에서 `[WATCHLIST][LOAD] ... members=120` 확인

**모두 체크되면 GO! 🚀**
