# Pullback Reversal Dip Strategy Update

이전 눌림목 로직(진입 후 고점 대비 -1.5%/-3.0% 분할매수)에서 다음과 같이 변경되었습니다.

## 핵심 변화
- **패턴 필수 진입 조건**: 신고가(최근 `PULLBACK_LOOKBACK`일 내 최고가) 달성 후 `PULLBACK_DAYS`일 이상 연속 하락한 상태에서 어제 고가를 돌파(반등)하려는 흐름이 확인돼야 신규 진입/추가 매수가 가능합니다. 신호 미충족 시 돌파·눌림 모두 스킵합니다.
- **반등 확인 가격**: 어제(마지막 하락일) 고가에 `PULLBACK_REVERSAL_BUFFER_PCT`(기본 0.2%)를 더한 가격을 되돌림 기준선으로 사용하며, 이 가격을 회복해야 “reversing”으로 간주합니다.
- **상태 저장**: 패턴 탐지 결과를 포지션 상태(`pullback_peak_price`, `pullback_reversal_price`, `pullback_reason`)에 기록해 장중 재평가와 리포트에 활용합니다.

## 코드 적용 위치
- `CONFIG`에 새 파라미터(`USE_PULLBACK_ENTRY`, `PULLBACK_LOOKBACK`, `PULLBACK_DAYS`, `PULLBACK_REVERSAL_BUFFER_PCT`)를 추가해 `.env`로 조정 가능하게 했습니다.
- `_detect_pullback_reversal` 함수가 일봉을 스캔해 “신고가 → 3일 하락 → 반등” 패턴을 찾아 `setup/reversing/reversal_price/peak_price` 정보를 반환합니다.
- 진입 루프와 추가 매수 로직에서 `USE_PULLBACK_ENTRY`가 켜져 있으면 위 패턴이 성립(`setup` & `reversing`)해야만 주문이 실행됩니다.

## 기대 효과
- 단순 intraday 하락폭 기반 물타기 대신, 추세 고점 이후 정상 조정·반등 구간에 한정해 진입하도록 제한하여 의미 없는 추락 구간 물타기를 줄입니다.
- 파라미터를 통해 하락 일수·반등 여유폭을 손쉽게 튜닝할 수 있어 시장 환경에 맞춘 보수/공격적 운용이 가능합니다.
