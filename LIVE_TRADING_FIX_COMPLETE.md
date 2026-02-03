# 🎯 LIVE 거래 전송 활성화 완료 (모의계좌 실제 주문 전송)

## ✅ 구현 완료 사항

### 1. AUTO 스텝 개선 (핵심 수정)

**문제**: AUTO가 LIVE 판정만 하고 LIVE 금지 플래그를 해제하지 않았음
- 기존: `DRY_RUN=1`, `LIVE_TRADING_ENABLED=0`, `DISABLE_LIVE_TRADING=1`, `SIMULATION_MODE=1` 고정
- 결과: "LIVE env violations" 가드가 의도적으로 실행 중단

**해결**: AUTO 스텝에서 장중일 때 LIVE 허용 플래그를 강제 세팅
```python
if in_market:
    # 가드/엔진이 STRATEGY_MODE를 보는 경우도 있어서 같이 맞춤
    f.write("STRATEGY_MODE=LIVE\n")
    
    # LIVE env violations 방지 3종 세트 (반드시 이 값이어야 함)
    f.write("DRY_RUN=0\n")
    f.write("LIVE_TRADING_ENABLED=1\n")
    f.write("DISABLE_LIVE_TRADING=0\n")
    
    # 시뮬 모드 OFF (주문을 진짜로 모의서버에 전송)
    f.write("SIMULATION_MODE=0\n")
else:
    # 장외 안전장치
    f.write("STRATEGY_MODE=DIAG\n")
    f.write("DRY_RUN=1\n")
    f.write("LIVE_TRADING_ENABLED=0\n")
    f.write("DISABLE_LIVE_TRADING=1\n")
    f.write("SIMULATION_MODE=1\n")
```

### 2. Job-level 환경변수 충돌 제거

**문제**: Job env에 충돌 플래그가 고정값으로 박혀 있었음
- `DRY_RUN: "1"`, `LIVE_TRADING_ENABLED: "0"`, `DISABLE_LIVE_TRADING: "1"`, `SIMULATION_MODE: "1"`, `STRATEGY_MODE: "PAPER"`

**해결**: Job env에서 충돌 플래그 완전 제거
- AUTO가 시장 상태에 따라 동적으로 세팅하도록 허용
- 고정값은 `KIS_ENV=PAPER`, `STRATEGY_ENV=PAPER`, API URL, 키, 계좌 정보만 유지

### 3. ENV 덤프 스텝 추가 (디버깅 강화)

LIVE env guard 직전에 환경변수 덤프 스텝 추가:
```bash
echo "[ENV] KIS_ENV=$KIS_ENV STRATEGY_ENV=$STRATEGY_ENV API_BASE_URL=$API_BASE_URL"
echo "[ENV] EFFECTIVE_STRATEGY_MODE=$EFFECTIVE_STRATEGY_MODE STRATEGY_MODE=${STRATEGY_MODE:-}"
echo "[ENV] DRY_RUN=${DRY_RUN:-} LIVE_TRADING_ENABLED=${LIVE_TRADING_ENABLED:-} DISABLE_LIVE_TRADING=${DISABLE_LIVE_TRADING:-}"
echo "[ENV] SIMULATION_MODE=${SIMULATION_MODE:-} KIS_HTTP_ENABLED=$KIS_HTTP_ENABLED MARKET_WINDOW=$MARKET_WINDOW PB1_PHASE_DEFAULT=$PB1_PHASE_DEFAULT"
echo "[ENV] PB1_WATCHLIST_TOPK=${PB1_WATCHLIST_TOPK:-} PB1_WATCHLIST_FINALN=${PB1_WATCHLIST_FINALN:-}"
echo "[ENV] PB1_EARLY_STOP_ENABLED=${PB1_EARLY_STOP_ENABLED:-}"
```

**장중 정상 출력 예시**:
```
DRY_RUN=0
LIVE_TRADING_ENABLED=1
DISABLE_LIVE_TRADING=0
SIMULATION_MODE=0
KIS_HTTP_ENABLED=1
STRATEGY_MODE=LIVE
```

### 4. LIVE env guard 개선

`SIMULATION_MODE=1` 체크 추가:
```bash
[ "${SIMULATION_MODE:-}" = "1" ] && violations+=("SIMULATION_MODE=1")
```

### 5. 후보 풀 120→50→30 설정

Job env에 설정 추가:
```yaml
PB1_WATCHLIST_TOPK: "50"     # 기존 20 → 50
PB1_WATCHLIST_FINALN: "30"   # 기존 10 → 30
```

### 6. Early Stop 절대 금지

**Job-level 설정**:
```yaml
PB1_EARLY_STOP_ENABLED: "0"   # 전체 스캔 강제
```

**코드 수정** ([trader/pb1_engine.py](trader/pb1_engine.py)):
- `PB1_EARLY_STOP_ENABLED` config를 import 추가
- Early stop 로직을 config 기반으로 수정
  ```python
  # 기존: early_stop = os.getenv("PB1_CANDIDATE_EARLY_STOP", "1") == "1"
  # 변경: early_stop = PB1_EARLY_STOP_ENABLED
  ```
- `PB1_EARLY_STOP_ENABLED=0`이면 early stop이 절대 실행되지 않음

---

## 📋 성공 판정 체크리스트

장중(예: 오전 10시대) 실행 시 다음이 모두 출력되어야 합니다:

### 1. AUTO 스텝 출력
```
[AUTO] now=... trading_day=True window=morning in_market=True -> STRATEGY_MODE=LIVE ... KIS_HTTP_ENABLED=1 live_ok=true
```

### 2. ENV 덤프 출력
```
[ENV] DRY_RUN=0
[ENV] LIVE_TRADING_ENABLED=1
[ENV] DISABLE_LIVE_TRADING=0
[ENV] SIMULATION_MODE=0
[ENV] KIS_HTTP_ENABLED=1
[ENV] STRATEGY_MODE=LIVE
[ENV] PB1_WATCHLIST_TOPK=50
[ENV] PB1_WATCHLIST_FINALN=30
[ENV] PB1_EARLY_STOP_ENABLED=0
```

### 3. LIVE env precheck 통과
```
[PRECHECK] LIVE env ok
```

### 4. 후보 풀 로그
```
WATCHLIST ... members=120
topk=50 / finaln=30
```

### 5. Early stop 없음
- `EARLY_STOP_CANDIDATES_SUFFICIENT` 로그 없어야 함
- 전체 스캔 완료 로그 확인

---

## 🔧 변경 파일 목록

1. [.github/workflows/trade-runner.yml](.github/workflows/trade-runner.yml)
   - AUTO 스텝: 장중 LIVE 허용 플래그 강제 세팅
   - Job env: 충돌 플래그 제거
   - ENV 덤프 스텝 추가
   - LIVE env guard: SIMULATION_MODE 체크 추가
   - PB1 watchlist: 50/30으로 변경
   - PB1_EARLY_STOP_ENABLED: 0으로 설정

2. [trader/pb1_engine.py](trader/pb1_engine.py)
   - `PB1_EARLY_STOP_ENABLED` import 추가
   - Early stop 로직을 config 기반으로 수정

3. [trader/config.py](trader/config.py)
   - 기존 `PB1_EARLY_STOP_ENABLED` 설정 확인 (수정 없음)
   - 기본값: `True` (job env에서 `0`으로 오버라이드)

---

## 🎯 핵심 요약

### 장중 실행 시 (오전 10시대)
```yaml
# 계정
KIS_ENV: PAPER
STRATEGY_ENV: PAPER
API_BASE_URL: https://openapivts...

# LIVE 전송 허용
STRATEGY_MODE: LIVE
KIS_HTTP_ENABLED: 1
DRY_RUN: 0
LIVE_TRADING_ENABLED: 1
DISABLE_LIVE_TRADING: 0
SIMULATION_MODE: 0  # ← 핵심! (1이면 가짜 주문)

# 후보 풀
Candidate pool: 120개 (DB 로드)
PB1 스캔: 120 → 50 → 30

# Early stop 금지
PB1_EARLY_STOP_ENABLED: 0  # 전체 스캔 강제
```

### 장외 실행 시 (오후/주말)
```yaml
# 안전장치 자동 활성화
STRATEGY_MODE: DIAG
DRY_RUN: 1
LIVE_TRADING_ENABLED: 0
DISABLE_LIVE_TRADING: 1
SIMULATION_MODE: 1
KIS_HTTP_ENABLED: 0
```

---

## ⚠️ 중요 사항

1. **SIMULATION_MODE=0이 핵심**
   - `SIMULATION_MODE=1`이면 주문이 "가짜"로 처리되어 체결이 절대 안 나옴
   - 모의계좌 실제 전송하려면 반드시 `0`이어야 함

2. **AUTO 스텝이 가장 중요**
   - AUTO가 시장 상태를 판정하고 LIVE 허용 플래그를 세팅
   - Job env에 고정값이 있으면 AUTO가 덮어씀

3. **스텝 순서 보장**
   - AUTO → ENV 덤프 → LIVE env guard → trade-tick
   - 순서가 바뀌면 가드가 잘못된 값을 체크함

4. **Early stop 절대 금지**
   - `PB1_EARLY_STOP_ENABLED=0`으로 전체 스캔 강제
   - 120개 후보 중 30개 찾을 때까지 스캔

---

## 🚀 다음 실행 시 확인 사항

1. **장중(오전 10시대) workflow 실행**
   ```bash
   gh workflow run trade-runner.yml -f MODE=both -f ENV=PAPER
   ```

2. **로그에서 확인**
   - AUTO 출력: `in_market=True -> STRATEGY_MODE=LIVE`
   - ENV 덤프: 모든 플래그가 올바른 값인지 확인
   - LIVE env precheck: 통과하는지 확인
   - trade-tick: 실제 주문 전송 로그 확인

3. **실패 시 디버깅**
   - ENV 덤프에서 어떤 값이 잘못되었는지 확인
   - 다른 스텝이 AUTO 이후에 값을 덮어쓰는지 확인
   - LIVE env guard violations 메시지 확인

---

## ✅ 완료

모든 요구사항이 구현되었습니다:
- ✅ 장중 모의계좌 실제 주문 전송 활성화
- ✅ AUTO 스텝 LIVE 허용 플래그 강제 세팅
- ✅ Job-level 충돌 플래그 제거
- ✅ ENV 덤프 스텝 추가
- ✅ 후보 풀 120→50→30 설정
- ✅ Early stop 절대 금지

다음 장중 실행 시 정상 동작할 것입니다! 🎉
