# Dual Agent Boundary Rules

dual-agent는 nullim을 복사해서 만든 한국장 + 미국장 통합 운영 후보 브랜치다.

## KR trading area

한국장 코드는 기존 nullim 구조를 유지한다.

- 기존 trader/*.py
- trader/kis_wrapper.py
- trader/pb1_runner.py
- 기존 한국장 workflow
- 기존 한국장 DB tables
- 기존 한국장 orders/positions/signals/pb1 관련 코드

미국장 작업 중 위 파일은 수정하지 않는다.

## US trading area

미국장 코드는 아래 경로에만 둔다.

- trader/us/**
- tests/us/**
- config/us_*.yaml
- migrations/*us_agent*.sql
- .github/workflows/us-*.yml

## DB boundary

한국장 테이블과 미국장 테이블은 분리한다.

KR:
- existing domestic tables

US:
- us_ prefix tables only

미국장 코드에서 한국장 테이블을 직접 접근하지 않는다.
한국장 코드에서 us_ 테이블을 직접 접근하지 않는다.

## Shared secrets

KIS 모의투자 계좌는 한국/미국 공용으로 사용할 수 있다.
GitHub Secrets는 기존 공용 secrets를 그대로 사용한다.

- KIS_APP_KEY
- KIS_APP_SECRET
- KIS_REST_URL
- CANO
- ACNT_PRDT_CD
- KIS_ENV
- PBCORE_DB_URL

KIS_US_* secret은 만들지 않는다.

## Forbidden

- 미국장 작업 중 trader/pb1_runner.py 수정 금지
- 미국장 작업 중 trader/kis_wrapper.py 수정 금지
- 미국장 작업 중 기존 한국장 workflow 수정 금지
- 한국장 workflow에 TRADING_REGION=US 넣지 말 것
- 한국장 workflow에 US_AGENT_ENABLED 넣지 말 것
- 미국장 workflow에서 한국장 runner 호출 금지
- 미국장 코드에서 한국장 orders/positions/signals 테이블 직접 접근 금지
- 한국장 코드에서 us_ 테이블 접근 금지
