# KODEX 레버리지 무한매수 전략 검증 보고서

기준 `dual-agent` SHA는 `d0a081f2df6d42b0c6593686af90547bbb4652df`이다. 이 작업 환경에는
`origin` remote가 구성되어 있지 않아 fetch 결과와 원격 최신성을 별도로 검증할 수 없었다.

## 결론

`KR_INF_REGIME_GUARD_V1` 구현은 기본 활성화되지만, 장기 실제 데이터 취득이 실행 환경의
HTTPS proxy 403으로 차단되어 **NO_DEPLOYABLE_POLICY**로 판정했다. 따라서 설정이 켜져 있어도
검증된 production policy가 생기기 전까지 신규 BUY는 fail-closed해야 한다. 수익 또는 승리를
보장하지 않는다. CI의 합성 fixture 결과를 실제 백테스트 결과로 표시하지 않았다.

## 구현 및 격리

- 독립 패키지 `trader/kr/infinite`에 설정, 모델, 8-state risk adapter, 수량/cap 전략,
  PostgreSQL repository/reconcile 경계, session integration, point-in-time replay를 구현했다.
- 기존 PB1 종료 뒤 exception-isolated hook만 호출한다. `[KR_INF][ERROR]`는 PB1 결과를 바꾸지 않는다.
- 활성 시 `exclude_owned()`가 122630만 기존 BUY 후보에서 제거하며 다른 종목은 보존한다.
- US, US tests, WSL US runner, scheduler 파일은 변경하지 않았다. CI가 금지 경로 diff를 검사한다.
- migration 0049는 독립 테이블/index에 `IF NOT EXISTS`만 사용한다.

## 기본 설정과 주문 게이트

ENABLED=1, REAL_ORDER=1, ALLOW_BUY=1, ALLOW_SELL=1, symbol=122630,
capital=15,000,000원, units=40, unit=375,000원이다. `STRATEGY_MODE=LIVE`, 실전 KIS,
`LIVE_TRADING_ENABLED=1`, `DRY_RUN=0`, `DISABLE_LIVE_TRADING=0`, `FORCE_BLOCK_LIVE=0`을
동시에 요구한다. Sleeve는 전역 환경변수를 변경하지 않는다.

## Canonical regime 연결

| 상태 | production action |
|---|---|
| KR_DEFENSE_CRASH | BLOCK_BUY_CRASH |
| KR_DEFENSE_RISK_OFF | BLOCK_BUY_RISK_OFF |
| KR_DEFENSE_CAUTION | BLOCK_BUY_CAUTION |
| KR_SHOCK_REBOUND_PENDING | BLOCK_BUY_REBOUND_PENDING |
| KR_SHOCK_REBOUND_CONFIRMED | ALLOW_RECOVERY_PROBE (일일/cap guard 적용) |
| KR_NORMAL | ALLOW_ROUTINE_BUY |
| KR_RISK_ON | ALLOW_ROUTINE_BUY |
| KR_STRONG_RISK_ON | ALLOW_ROUTINE_BUY, 매도 조건 우선 |

UNKNOWN, BLOCKED, stale/future/date mismatch, account kill switch, execution-policy mismatch는 차단한다.
KOSPI local state/policy가 진입 기준이며 KOSDAQ 단독 악화는 veto가 아니다.

## 후보 비교와 선택

요청된 routine/price-gap/ATR/caution/recovery-probe 매수, 6/8/10/12% 및 staged/trailing
매도, no-stop/-10/-12/-15% 방어, bear cap 8/12/16/20 후보를 실제 데이터로 순위화하려 했으나
데이터를 한 행도 받지 못했다. 선택 기준 7(비용 반영 OOS 수익)과 2022 자금 소진/MDD를 검증할
수 없으므로 모든 후보를 탈락시켰다. 임의 수치나 2022 결과를 만들지 않았다.

## Replay 방법과 비용

Replay 엔진은 수정 OHLC, 실제 입력 행의 거래일, 전일까지의 20/50일 자료로 신호를 만들고 다음
거래일 수정 시가에 정수 수량으로 체결한다. 매수 0.015%, 매도 0.015%, 매도세 0.18%, 양방향
각 0.10% slippage를 반영한다. 수정가격에 분배/분할 효과가 포함된 것으로 취급한다. 누락/0 가격은
건너뛴다. fixture는 look-ahead와 비용 계산 검사용일 뿐 실제 결과가 아니다.

## 실제 replay 시도 및 2022

- 요청: `price_daily → 기존 KIS OHLCV provider → 사용자 CSV` auto loader로 2010-01-01부터 최신 완료 일봉.
- 결과: 신규 auto loader가 `price_daily`를 먼저 조회했으나 DB URL이 없었고, 기존 KIS OHLCV
  provider fallback은 KIS credentials/network가 없어 실패했으며 사용자 CSV도 제공되지 않았다.
  Loader exit code는 2, row count는 0, empty checksum은
  `4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945`이다. 실제 기간,
  전체/OOS/work-forward, 2018/2020/2022/회복,
  buy-and-hold, ±20% sensitivity 지표는 모두 `null`이다.
- 2022 손익/MDD/자금 조기 소진: **검증 불가**, 조작하지 않음.

## 테스트 및 남은 위험

신규 43개 테스트는 기본 설정, exact 8-state mapping/행동, KOSPI 독립성, stale/unknown/kill
switch, fee-aware cap, partial fill, pending/restart/idempotency, reconcile/ownership conflict,
sell-first, live gate, replay 비용/look-ahead 경계를 검증한다. DB/KIS 운영 integration은 실제 계좌와
PostgreSQL 없이는 end-to-end 검증할 수 없다. 실제 장기 replay가 성공하여 deployable policy를
고정하기 전에는 주문 운영 승인을 해서는 안 된다. Scheduler 변경은 없다.

## PR97 재검토 상태

세션 hook의 빈 기본값 호출은 제거하고 PB1 tick이 사용하는 snapshot, KIS balance/quote,
orderable cash와 router를 직접 주입하도록 변경했다. 동일 PostgreSQL connection에서 advisory lock을
획득·해제하고, 귀속된 부분체결/terminal order evidence 기반 reconcile을 추가했다. 다만 이 환경에는
실제 DB/KIS credentials와 장기 수정 OHLCV가 없으므로 실제 broker ACK→fill E2E 및 실제 replay는
여전히 완료하지 못했다. 따라서 `deployable_buy_policy=False`, PR Draft 유지, Ready 전환 금지가
올바른 상태다.

추가로 주문 route 결과를 ACK_PENDING/REJECTED/RECONCILE_PENDING으로 구분하고, 결과 불명
예외는 pending을 보존한다. 실제 fill만 수수료 포함 buy notional과 unit을 증가시키며 fill identity로
중복을 제거한다. 전량매도와 broker 잔고 0이 함께 확인된 cycle은 별도 history table에 보존하고,
완료 다음 거래일 새 UUID와 0원 cap으로 시작한다. 검증 명령 결과는 `tests/kr/infinite` 43 passed,
`tests/kr` 149 passed, runner/order/reconcile/regime 선택 회귀 59 passed이다.

Practice는 더 이상 `KIS_NOT_LIVE`로 차단하지 않고 기존 KIS wrapper의 선택된 practice endpoint로
전달된다. 실전/모의 모두 공통 DRY_RUN, DISABLE_LIVE_TRADING, FORCE_BLOCK_LIVE,
LIVE_TRADING_ENABLED, STRATEGY_MODE gate를 통과해야 한다. 다만 실제 replay 미완료 때문에
`deployable_buy_policy=False`가 최종 BUY를 차단하므로 기본 practice BUY 1건 증거는 아직 없다.
