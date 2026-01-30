# Minervini 필터 디버그 가이드

## 목표
Minervini 필터가 실제로 매수 후보를 골라내는지 10분 내로 확정 판정

## 판정 기준

### PASS (정상 동작)
- `[MINERVINI][ENTER]` 또는 `[MINERVINI][APPLY]` 로그가 뜸
- `[MINERVINI][RESULT]`에서 `before > both_pass`이며, `both_pass`가 0~수십 수준으로 줄어듦
- 예: `before=195 after_rs=150 dropped_rs=45 after_vcp=120 dropped_vcp=75 both_pass=85`

### FAIL (미호출)
- `[MINERVINI][ENTER]` 또는 `[MINERVINI][APPLY]` 로그가 한 번도 안 뜸
- → 코드상 필터 호출 경로가 없거나 조건으로 스킵됨

### FAIL (무력화)
- `[MINERVINI][APPLY]` 로그는 뜨나 `before ≈ both_pass` (거의 줄지 않음)
- `[MINERVINI][SKIP]` 또는 `[MINERVINI][DEGRADED]` 경고가 뜸
- 벤치마크 데이터 부족 또는 개별 종목 OHLCV 부족

### FAIL (계산 불가)
- 예외 발생하거나 `benchmark_rows`/`stock_rows`가 `min_required` 미만

## 디버그 모드 활성화

```bash
export MINERVINI_DEBUG=1
```

디버그 모드에서는 다음 추가 로그가 출력됩니다:
- 각 종목의 필터 진입/결과 (`[MINERVINI][FILTER][ENTER/RESULT]`)
- 필터 실패 종목 상세 (`[MINERVINI][FILTER_FAIL]`)
- 상위 후보 샘플 3개의 RS/VCP 점수 (`[MINERVINI][SAMPLE]`)

## Codespaces 즉시 실행 커맨드

### A) DRY RUN으로 후보만 확인 (매수 없음, 권장)
```bash
export TZ=Asia/Seoul
export MINERVINI_DEBUG=1
export NO_TRADE=1
python -m trader.pb1_runner --env live --strategy best_k_meta --once --window morning --phase entry
```

### B) 실제 환경 시뮬레이션 (매수 없음)
```bash
export MINERVINI_DEBUG=1
export DRY_RUN=1
python -m trader.pb1_runner --env practice --strategy best_k_meta --once --window morning --phase entry
```

### C) 로그만 확인 (기존 실행 결과)
```bash
grep -E '\[MINERVINI\]' logs/pb1_*.log | tail -100
```

## 예상 로그 출력

### 정상 동작 시
```
INFO:[MINERVINI][APPLY] universe=195 rs_min_pctile=80 vcp_lookback=120 lookback_days=63/126
INFO:[MINERVINI][RESULT] before=195 after_rs=150 dropped_rs=45 after_vcp=120 dropped_vcp=75 both_pass=85 sample=['005930','000660','035720','035420','051910']
INFO:[MINERVINI][SAMPLE] code=005930 rs_pct=95.2 vcp_ok=True vcp_score=10.0 score=105.2
INFO:[MINERVINI][CANDIDATES] n=85 codes=005930, 000660, ...
```

### 벤치마크 부족 시
```
WARNING:[MINERVINI][SKIP] reason=insufficient_benchmark benchmark_rows=50 min_required=126
WARNING:[MINERVINI][DEGRADED] RS calculation may be unreliable due to insufficient benchmark data
```

### 필터 실패 상세 (DEBUG 모드)
```
INFO:[MINERVINI][FILTER][ENTER] c=52000.00 ma50=50000.00 ma150=48000.00 ma200=45000.00 rs_pct=75.00 vcp_ok=False rs_min=80.00
INFO:[MINERVINI][FILTER][RESULT] ok=False reasons=['rs_below_min', 'vcp_fail']
INFO:[MINERVINI][FILTER_FAIL] code=123456 reasons=['rs_below_min', 'vcp_fail'] rs_pct=75.0 vcp_ok=False
```

## 로그 해석

### 주요 로그 키워드

| 로그 | 의미 |
|------|------|
| `[MINERVINI][APPLY]` | 필터 적용 시작 (파라미터 출력) |
| `[MINERVINI][RESULT]` | 필터 적용 결과 (전/후 카운트) |
| `[MINERVINI][SKIP]` | 필터 스킵 (데이터 부족 등) |
| `[MINERVINI][DEGRADED]` | 필터 무력화/완화 |
| `[MINERVINI][FILTER][ENTER]` | 개별 종목 필터 진입 (DEBUG) |
| `[MINERVINI][FILTER_FAIL]` | 개별 종목 필터 실패 (DEBUG) |
| `[MINERVINI][SAMPLE]` | 상위 후보 샘플 (DEBUG) |

### 필터 적용 전후 카운트

- `before`: 데이터가 있는 전체 종목 수
- `after_rs`: RS 필터 통과 종목 수
- `dropped_rs`: RS 필터 탈락 종목 수
- `after_vcp`: VCP 필터 통과 종목 수
- `dropped_vcp`: VCP 필터 탈락 종목 수
- `both_pass`: RS + VCP 모두 통과한 종목 수

### 주요 실패 사유

- `rs_below_min`: RS percentile이 최소값(기본 80) 미만
- `vcp_fail`: VCP 패턴 미충족
- `trend_template_fail`: 가격 > MA50 > MA150 > MA200 조건 미충족
- `ma200_not_rising`: MA200이 상승 추세가 아님
- `illiquid`: 유동성 부족 (50일 평균 거래대금 < 20억)
- `missing_ma`: 이동평균 계산 불가 (데이터 부족)

## 환경 변수

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `MINERVINI_DEBUG` | `0` | `1`로 설정 시 상세 로그 출력 |
| `RS_MIN_PCTILE` | `80` | RS 최소 백분위수 (0-100) |
| `VCP_MIN_SCORE` | `70` | VCP 최소 점수 |
| `VCP_LOOKBACK` | `120` | VCP 패턴 탐지 lookback 일수 |
| `RS_LOOKBACK_DAYS` | `63` | RS 계산 단기 lookback |
| `RS_LOOKBACK2_DAYS` | `126` | RS 계산 장기 lookback |

## 트러블슈팅

### Q1. `[MINERVINI][APPLY]` 로그가 안 뜨는 경우
- 후보 생성 단계(`_compute_candidates`)가 실행되지 않음
- `--phase entry` 또는 `--window morning` 옵션 확인
- 유니버스가 비어있는지 확인 (`[PB1][ENTRY_BLOCKED] reason=universe_empty`)

### Q2. `before ≈ both_pass` (거의 탈락하지 않음)
- RS/VCP 조건이 너무 완화되어 있거나
- 벤치마크 데이터 부족으로 RS 계산 실패 → `[MINERVINI][SKIP]` 로그 확인
- `RS_MIN_PCTILE`, `VCP_MIN_SCORE` 값 확인

### Q3. 벤치마크 데이터 부족 경고 (`insufficient_benchmark`)
- DB에 `229200` (KOSDAQ150) OHLCV 데이터가 없거나 부족
- 일봉 데이터 최소 126일 필요
- 데이터 백필 필요: `python -m trader.backfill_ohlcv --code 229200 --days 200`

### Q4. `[MINERVINI][CANDIDATES]` 로그에서 `n=0`
- 모든 종목이 필터에서 탈락
- `[MINERVINI][REJECT_SUMMARY]` 로그에서 주요 탈락 사유 확인
- `runtime/reports/minervini/<date>/minervini_report.json` 파일에서 상세 탈락 사유 확인

## 관련 파일

- `trader/strategies/pb1_minervini_v2.py`: Minervini 필터 로직
  - `evaluate_filters()`: 트렌드/RS/VCP 필터 적용
  - `detect_vcp()`: VCP 패턴 탐지
  - `compute_features()`: 이동평균/ATR 등 계산
- `trader/pb1_engine.py`: 후보 생성 및 필터 적용
  - `_compute_candidates()`: 후보 생성 (RS/VCP 점수 계산)
  - `_apply_thresholds()`: 필터 적용
- `trader/minervini/report.py`: Minervini 리포트 생성
  - `run_minervini_report()`: 후보/탈락 종목 리포트

## 빠른 체크리스트

1. [ ] `MINERVINI_DEBUG=1` 설정
2. [ ] 디버그 커맨드 실행 (A, B 중 선택)
3. [ ] `[MINERVINI][APPLY]` 로그 확인 → 없으면 FAIL(미호출)
4. [ ] `[MINERVINI][RESULT]` 로그 확인
   - `before > both_pass` 이고 적절히 감소 → PASS
   - `before ≈ both_pass` → FAIL(무력화)
5. [ ] `[MINERVINI][SKIP]` 또는 `[MINERVINI][DEGRADED]` 경고 확인
6. [ ] `runtime/reports/minervini/<date>/minervini_report.json` 확인 (상세 분석)

## 다음 단계

필터가 정상 동작하는 것을 확인한 후:
1. RS/VCP 임계값 조정 (`RS_MIN_PCTILE`, `VCP_MIN_SCORE`)
2. 백테스트로 과거 성과 검증
3. 실전 투자 전 충분한 페이퍼 트레이딩
