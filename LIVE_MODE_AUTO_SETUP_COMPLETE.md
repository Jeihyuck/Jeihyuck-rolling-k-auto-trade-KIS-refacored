# ✅ LIVE 모드 자동 설정 완료

## 변경 사항 요약

### 1. workflow_dispatch 입력값 수정
- **MODE**: `trade` 기본값 (안전하게)
- **FORCE_CANDIDATE**: `0` 기본값 (재생성 금지)
- **MINERVINI_ONLY**: `0` 기본값 (분석만 안함)
- **DRY_RUN**: `1` 기본값 (수동 테스트는 안전하게 드라이런)
- **DISABLE_LIVE_TRADING**: `1` 기본값 (수동 테스트는 LIVE 차단)
- **LIVE_TRADING_ENABLED**: `0` 기본값 (수동 테스트는 LIVE 금지)

### 2. FINAL ENV LOCK 스텝 추가
**위치**: trade_tick job의 "Guard: postgres-only" 직후

**동작**:
- **스케줄 실행**: 무조건 LIVE 모드 (모의계좌)
  - `STRATEGY_MODE=LIVE`
  - `KIS_ENV=practice`
  - `DRY_RUN=0`
  - `DISABLE_LIVE_TRADING=0`
  - `LIVE_TRADING_ENABLED=1`
  
- **수동 실행**: 입력값 반영 (테스트용)
  - `STRATEGY_MODE=DIAG` (기본)
  - 입력값에 따라 LIVE 차단 가능

### 3. ENV SNAPSHOT 스텝 추가
**위치**: FINAL ENV LOCK 직후

**목적**: 환경변수 값 확인 (스케줄 실행 시 LIVE 통과 확인)

## 체크리스트

### 0. 선행 조치: Repository/Environment Variables 정리

아래 변수가 GitHub Repository Settings에 있으면 **반드시 삭제**하세요:

- [ ] `DRY_RUN`
- [ ] `DISABLE_LIVE_TRADING`
- [ ] `LIVE_TRADING_ENABLED`
- [ ] `DIAGNOSTIC_MODE`
- [ ] `DIAGNOSTIC_ONLY`

**삭제 방법**:
1. GitHub Repository → Settings → Secrets and variables → Actions
2. Repository variables / Environment variables 탭에서 확인
3. 위 변수들이 있으면 모두 삭제

### 1. 코드 변경 확인

- [x] workflow_dispatch 입력값 수정 완료
- [x] FINAL ENV LOCK 스텝 추가 완료
- [x] ENV SNAPSHOT 스텝 추가 완료

### 2. 테스트 실행

#### 2-1. 수동 실행 테스트 (주말/장외시간)

**Actions → Trade Runner → Run workflow**

기본값 그대로 실행:
```
MODE: trade
FORCE_CANDIDATE: 0
MINERVINI_ONLY: 0
DRY_RUN: 1
DISABLE_LIVE_TRADING: 1
LIVE_TRADING_ENABLED: 0
```

**예상 결과**:
```
[FINAL-LOCK] event=workflow_dispatch
[ENV] STRATEGY_MODE=DIAG MODE=trade KIS_ENV=practice
[ENV] DRY_RUN=1 DISABLE_LIVE_TRADING=1 LIVE_TRADING_ENABLED=0
```

#### 2-2. 스케줄 실행 확인 (다음 평일 장중)

**Actions → Trade Runner → 스케줄 실행 로그 확인**

**예상 결과**:
```
[FINAL-LOCK] event=schedule
[ENV] STRATEGY_MODE=LIVE MODE=trade KIS_ENV=practice
[ENV] DRY_RUN=0 DISABLE_LIVE_TRADING=0 LIVE_TRADING_ENABLED=1
[ENV] API_BASE_URL=https://openapivts.koreainvestment.com:29443 KIS_HTTP_ENABLED=1
```

**중요**: 위 값들이 정확히 나와야 LIVE 인터락을 통과합니다.

### 3. 운영 원칙

1. **스케줄은 절대 믿지 않는다**
   - 스케줄 실행은 항상 FINAL LOCK에서 LIVE 값을 강제로 설정
   
2. **DRY_RUN 관련 키는 Repository Variables에 두지 않는다**
   - 있으면 다시 덮어써서 반복 실패
   
3. **"LIVE env violations"는 안전장치다**
   - 스케줄에서는 항상 통과하도록 설정됨

## 다음 스케줄 실행 시각

- **후보군 생성**: 매주 금요일 15:30 UTC (토요일 00:30 KST)
- **Trade Tick**: 월~금 23:55 UTC (화~토 08:55 KST)

## 수동 실행 예시

### 예시 1: 주말에 분석만 테스트
```
MODE: trade
FORCE_CANDIDATE: 0
MINERVINI_ONLY: 1
DRY_RUN: 1
DISABLE_LIVE_TRADING: 1
LIVE_TRADING_ENABLED: 0
```

### 예시 2: 주말에 주문흐름까지 리허설 (모의)
```
MODE: trade
FORCE_CANDIDATE: 0
MINERVINI_ONLY: 0
DRY_RUN: 0
DISABLE_LIVE_TRADING: 0
LIVE_TRADING_ENABLED: 1
```

## 문제 해결

### LIVE env violations 에러 발생 시

1. **Repository Variables 확인**
   - DRY_RUN, DISABLE_LIVE_TRADING, LIVE_TRADING_ENABLED 삭제
   
2. **ENV SNAPSHOT 로그 확인**
   - DRY_RUN=0, DISABLE_LIVE_TRADING=0, LIVE_TRADING_ENABLED=1 인지 확인
   
3. **FINAL ENV LOCK 로그 확인**
   - 스케줄 실행인데 event=workflow_dispatch로 나오면 GitHub 문제

### 스케줄이 안 돌아가는 경우

1. **Cron 시간 확인**
   - UTC 기준으로 설정되어 있음 (한국시간 -9시간)
   
2. **Repository 활성화 확인**
   - 최근 커밋이 없으면 스케줄이 비활성화될 수 있음

## 다음 단계

1. 이 문서를 참고하여 Repository Variables 정리
2. 수동 실행으로 FINAL ENV LOCK 동작 확인
3. 다음 평일 장중 스케줄 실행 로그 모니터링
4. ENV SNAPSHOT에서 DRY_RUN=0 확인

---
**작성일**: 2026-02-04
**버전**: v1.0
