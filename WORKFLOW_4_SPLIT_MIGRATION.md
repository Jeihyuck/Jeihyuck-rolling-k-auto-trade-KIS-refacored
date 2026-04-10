# Workflow 4-Split Migration Summary

## 목표

기존 unified 중심 스케줄을 해체하고, DB를 단일 진실원본으로 사용하는 4개 active scheduled workflow 구조로 전환한다.

## 최종 active workflow

1. .github/workflows/trade-prep.yml
2. .github/workflows/trade-am.yml
3. .github/workflows/trade-pm.yml
4. .github/workflows/trade-close.yml

## 스케줄

1. prep: 0 23 * * 0-4 (KST 08:00, 월-금)
2. am: 7 0 * * 1-5 (KST 09:07, 월-금, 13:00까지 loop)
3. pm: 0 4 * * 1-5 (KST 13:00, 월-금, 15:10까지 loop)
4. close: 15 6 * * 1-5 (KST 15:15, 월-금, 장마감까지 exit loop)

## 변경 사항

1. prep workflow는 python -m trader.prep_runner만 실행한다.
2. intraday AM/PM workflow는 python -m trader.trade_tick만 실행한다.
3. close workflow도 python -m trader.trade_tick만 실행하되, PB1_PHASE_DEFAULT=exit와 PB1_ENTRY_ENABLED=0를 강제한다.
4. unified-pipeline.yml의 schedule은 제거하고 workflow_dispatch 전용 수동 디버그 wrapper로 축소했다.
5. intraday workflow(trade-am, trade-pm, trade-close)는 동일 concurrency group pb1-practice-intraday-serial 을 사용한다.
6. prep는 독립 group을 사용해도 되지만, intraday 3종은 반드시 직렬 실행된다.
7. 모든 active workflow는 PBCORE_DB_URL postgres-only guard를 통과해야 한다.
8. 모든 active workflow는 SQLITE_DISABLED=1, BOTSTATE_DISABLED=1, DB_DISABLE_PREPARED_STATEMENTS=1을 사용한다.
9. workflow 간 파일/artifact continuity를 가정하지 않고, 필요한 상태는 DB에서 다시 읽는다.

## SSOT 원칙

1. final30_scored, candidate pool, ledger/order/fill 상태는 DB에서 재조회한다.
2. runtime, ledger, signals 파일은 보조 미러로 취급하며, cross-VM continuity의 전제로 사용하지 않는다.
3. SQLite 및 bot_state continuity는 허용하지 않는다.

## 검증 포인트

1. prep는 scripts/verify_prep_log.py로 DONE_CORE 기반 PREP contract를 검증한다.
2. AM/PM/close는 TRADE FINAL30 DB load, READY OK, RUN SUMMARY 로그를 확인한다.
3. close는 BUY 로그가 없음을 추가로 확인한다.
4. 각 workflow 종료 시 SQLite artifact scan을 수행한다.
