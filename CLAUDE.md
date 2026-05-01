# CLAUDE.md — US Agent Branch

이 파일은 `us-agent` 브랜치에 대한 핵심 지침서입니다.
AI 코딩 어시스턴트 및 자동화 도구가 이 브랜치에서 작업할 때 반드시 준수해야 합니다.

---

## 1. 브랜치 정책

| 브랜치 | 목적 | 수정 가능 여부 |
|--------|------|--------------|
| `us-agent` | 미국 주식 모의투자 Multi-Agent 시스템 | ✅ 자유롭게 수정 |
| `nullim` | 한국 주식 원본 브랜치 | ❌ 절대 수정 금지 |

- `us-agent` 브랜치는 `nullim`의 복사본에서 시작.
- `nullim` 브랜치에 push/commit/cherry-pick 금지.
- PR은 반드시 `us-agent` → `us-agent`로 생성. `nullim`은 base 금지.

---

## 2. 주요 목표

**KIS 한국투자증권 해외주식 모의투자 API** 를 활용한 미국 주식 자동 매매 시스템 구축.

- 모의투자(paper trading) 전용 — 실제 주문 절대 불가
- Multi-Agent 구조로 스스로 실행/검증/오류분석/개선
- Harness Engineering으로 오프라인 검증 환경 보장

---

## 3. 하드 제약 (절대 위반 금지)

```
ALLOW_REAL_ORDER=0          # 실거래 완전 차단
US_PAPER_TRADING_ENABLED=1  # 모의투자 플래그 필수
DRY_RUN=1                   # 기본값: dry-run
KIS_ENV=practice            # KIS 모의투자 환경
```

- `ALLOW_REAL_ORDER=1` 설정 코드를 절대 작성하지 말 것.
- 오프라인 harness 실행 시 실제 KIS HTTP 호출 금지.
- `risk_gate.py`의 check 함수는 `os.getenv()`를 직접 호출 (모듈 상수 금지).

---

## 4. 아키텍처

```
trader/us/
├── config.py               # 환경 변수 / 상수
├── symbols.py              # 종목 레지스트리
├── market_calendar.py      # 미국 시장 시간대 / 공휴일
├── universe.py             # 매매 유니버스 (config/us_universe.yaml)
├── data_provider.py        # 가격 조회 추상 레이어
├── strategy/               # 매매 전략
│   ├── base.py
│   ├── us_pb1_pullback.py
│   ├── us_momentum.py
│   └── us_etf_trend.py
├── execution/              # KIS API 실행 레이어
│   ├── kis_us_registry.py  # TR 코드 레지스트리
│   ├── kis_us_client.py    # KIS 해외주식 API 클라이언트
│   ├── risk_gate.py        # 실행 전 risk check
│   ├── order_router.py     # dry-run / paper order 라우팅
│   ├── fills.py            # 체결 조회
│   └── reconcile.py        # 포지션 reconcile
├── runner/                 # 실행 스케줄러
│   ├── prep_runner.py      # 전략 실행 → intent 생성
│   ├── trade_open_runner.py
│   ├── trade_mid_runner.py
│   ├── trade_close_runner.py
│   ├── daily_report_runner.py
│   └── dispatcher.py       # 통합 진입점
├── agents/                 # Multi-Agent
│   ├── architect_agent.py  # 경계 위반 탐지 / task 라우팅
│   ├── strategy_agent.py   # 전략 실행
│   ├── execution_agent.py  # 주문 라우팅
│   ├── risk_agent.py       # risk gate 검사
│   ├── qa_harness_agent.py # pytest + harness 실행
│   ├── failure_triage_agent.py  # 장애 분류
│   ├── patch_planner_agent.py   # 패치 계획 생성
│   └── report_agent.py     # 일일 리포트
└── harness/                # Harness Engineering
    ├── manifest.yaml       # 시나리오 정의
    ├── runner.py           # 시나리오 실행기
    ├── log_parser.py       # 로그 마커 추출
    ├── validators.py       # 검증 규칙
    ├── failure_classifier.py  # 장애 분류 taxonomy
    └── scenarios/          # 개별 시나리오 스크립트
```

---

## 5. 필수 명령어

### 테스트
```bash
pytest -q tests/us
```

### Harness (오프라인 전체 시나리오)
```bash
python -m trader.us.harness.runner --scenario all --offline
```

### 전략 prep (오프라인)
```bash
python -m trader.us.runner.prep_runner --env practice --offline
```

### 개장 매수 (오프라인 dry-run)
```bash
python -m trader.us.runner.trade_open_runner --env practice --offline
```

### 마감 reconcile (오프라인)
```bash
python -m trader.us.runner.trade_close_runner --env practice --offline
```

### 전체 dispatcher
```bash
python -m trader.us.runner.dispatcher --mode all --env practice --offline
```

---

## 6. GitHub Actions 워크플로우

| 파일 | 트리거 | 역할 |
|------|--------|------|
| `us-agent.yml` | schedule(5회/일) + dispatch | 모의투자 자동 실행 |
| `us-harness.yml` | PR / schedule / dispatch | 오프라인 QA |
| `us-agent-auto-repair.yml` | 위 두 워크플로우 실패 시 | 장애 분류 + patch plan |

- `us-agent.yml`은 `us-agent` 브랜치에서만 실행 (`if: github.ref_name == 'us-agent'`).
- `us-harness.yml`은 KIS credentials 없이 실행 (오프라인 강제).
- `us-agent-auto-repair.yml`은 `PLAN_ONLY=1`로 동작 — 자동 코드 수정 없음.

---

## 7. DB 마이그레이션

```bash
psql $DATABASE_URL -f migrations/0038_us_agent_tables.sql
```

10개 테이블: `us_universe`, `us_watchlist`, `us_order_intents`, `us_orders`,
`us_fills`, `us_positions`, `us_reconcile_logs`, `us_agent_runs`,
`us_harness_runs`, `us_failure_events`

---

## 8. 환경 변수 참조

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `TRADING_REGION` | `"US"` | 거래 지역 |
| `KIS_ENV` | `"practice"` | KIS 환경 (practice/prod) |
| `US_PAPER_TRADING_ENABLED` | `"1"` | 모의투자 활성화 |
| `DRY_RUN` | `"1"` | dry-run 모드 |
| `ALLOW_REAL_ORDER` | `"0"` | 실거래 차단 |
| `KIS_APP_KEY` | — | KIS API 앱키 (secret) |
| `KIS_APP_SECRET` | — | KIS API 앱시크릿 (secret) |
| `KIS_ACCOUNT_NO` | — | KIS 계좌번호 (secret) |
| `KIS_TOKEN_FILE` | `/tmp/kis_token.json` | 토큰 캐시 경로 |
| `US_MAX_POSITION_COUNT` | `"10"` | 최대 보유 종목 수 |
| `US_MAX_SINGLE_NOTIONAL_USD` | `"2000"` | 종목당 최대 주문금액 |
| `US_MAX_DAILY_NOTIONAL_USD` | `"10000"` | 일일 최대 주문금액 |
| `US_MAX_POSITION_WEIGHT` | `"0.20"` | 종목 최대 비중 |
| `US_MIN_CASH_BUFFER_USD` | `"500"` | 최소 현금 유보 |
