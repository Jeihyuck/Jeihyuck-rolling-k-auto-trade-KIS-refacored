# Workflow 4-Split Manual Checklist

## 배포 전 확인

1. GitHub Actions에서 active scheduled workflow가 정확히 4개인지 확인한다.
2. unified-pipeline.yml에 schedule 트리거가 없는지 확인한다.
3. repository secrets에 PBCORE_DB_URL, KIS_APP_KEY, KIS_APP_SECRET, KIS_REST_URL, CANO, ACNT_PRDT_CD가 모두 설정되어 있는지 확인한다.
4. PBCORE_DB_URL이 postgres 스킴인지 확인한다.

## Prep 점검

1. trade-prep workflow를 수동 실행한다.
2. 로그에 [PREP][DONE_CORE][DONE] 또는 [FINAL30_SCORED][CORE_SAVE][DONE]가 있는지 확인한다.
3. scripts/verify_prep_log.py 검증 step이 성공하는지 확인한다.
4. artifact에 prep.log가 업로드되는지 확인한다.

## AM 점검

1. trade-am workflow를 수동 실행한다.
2. 로그에 [TRADE][FINAL30][DB_EXACT_LOAD]가 있는지 확인한다.
3. 로그에 [TRADE][READY][OK]가 있는지 확인한다.
4. 로그에 [RUN_SUMMARY][RESULT]가 있는지 확인한다.

## PM 점검

1. trade-pm workflow를 수동 실행한다.
2. 로그에 [TRADE][FINAL30][DB_EXACT_LOAD]가 있는지 확인한다.
3. 로그에 [TRADE][READY][OK]가 있는지 확인한다.
4. 로그에 [RUN_SUMMARY][RESULT]가 있는지 확인한다.

## Close 점검

1. trade-close workflow를 수동 실행한다.
2. 환경 변수에 PB1_PHASE_DEFAULT=exit와 PB1_ENTRY_ENABLED=0가 반영되는지 확인한다.
3. 로그에 [TRADE][FINAL30][DB_EXACT_LOAD]가 있는지 확인한다.
4. 로그에 [TRADE][READY][OK]가 있는지 확인한다.
5. 로그에 [RUN_SUMMARY][RESULT]가 있는지 확인한다.
6. 로그에 [TRADE][ORDER][BUY]가 없음을 확인한다.

## 공통 점검

1. 모든 workflow에서 DB migrate step이 성공하는지 확인한다.
2. 모든 workflow에서 SQLite artifact scan이 통과하는지 확인한다.
3. actions/checkout에 특정 개인 브랜치 ref 하드코딩이 없는지 확인한다.
4. workflow 간 artifact 전달이나 workspace continuity에 의존하는 step이 없는지 확인한다.