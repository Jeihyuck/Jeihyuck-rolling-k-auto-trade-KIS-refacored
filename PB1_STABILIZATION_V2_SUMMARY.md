# PB1 LIVE 안정화 v2.0 - 변경 요약

## 🎯 최종 목표
PB1 LIVE에서 실제 매수까지 정상 도달하도록 안정성과 성능을 강화

## ✅ 완료된 변경사항

### 1️⃣ OHLCV 로딩 전략 개선
- **변경**: 기본 윈도우를 260일 → **200일**로 축소
- **위치**: 
  - `trader/pb1_engine.py::_fetch_daily()`
  - `trader/config.py::PB1_OHLCV_DAYS_BASE`
- **효과**: DB 커버리지 부족으로 인한 대량 탈락 방지

### 2️⃣ ATR Risk Gate 고정
- **상태**: ✅ 이미 올바르게 구현됨
- **구현**:
  ```python
  # ATR은 항상 ratio (0~1)로 저장
  atr_ratio = atr_value / close
  atr_pct = atr_ratio * 100  # 로그용
  
  # 비교는 ratio끼리
  if atr_ratio > atr_max_ratio:  # 0.08 (8%)
      reject()
  ```
- **로그**:
  ```
  [PB1][RISK_GATE] code=XXX atr=XXXX close=XXXX atr_ratio=0.0754 atr_pct=7.54% atr_max=8.00%
  ```

### 3️⃣ 52주 고저 안전화
- **변경**: 252일 미만 데이터일 때 `None` 반환 (기존: `float("nan")`)
- **위치**:
  - `trader/strategies/pb1_minervini_v2.py::compute_features()`
  - `trader/strategies/pb1_minervini_close.py::compute_features()`
- **필터 처리**:
  ```python
  # 52주 고저: None이면 스킵 (탈락시키지 않음)
  if hi_52w is not None and not (np.isfinite(hi_52w) and c >= hi_52w * 0.75):
      reasons.append("too_far_from_52w_high")
  if lo_52w is not None and not (np.isfinite(lo_52w) and c >= lo_52w * 1.30):
      reasons.append("not_enough_off_52w_low")
  ```

### 4️⃣ None-safe 필터 처리
- **원칙**: 데이터 부족 = 스킵 (탈락 아님)
- **적용 항목**:
  - ✅ `hi_52w`, `lo_52w`: None일 때 해당 조건 스킵
  - ✅ VCP, Score, MA200 slope: 데이터 부족 시 FAIL (기존 유지)

### 5️⃣ KIS API Rate Limit 방지
- **상태**: ✅ 이미 구현됨
- **구현**:
  - `price_cache` TTL: **2초**
  - Token bucket rate limiter (per-endpoint)
  - HTTP 500 에러 시 즉시 fallback
- **효과**: 초당 거래건수 초과 방지, 재시도 최소화

### 6️⃣ PRICE Fallback 정책 명문화
- **상태**: ✅ 이미 구현됨
- **우선순위**:
  1. `ask`/`bid` (정상)
  2. `prpr` (현재가) + slippage
  3. 마지막 체결가 (DB/캐시)
  4. ❌ 없으면 skip
- **로그**:
  ```
  [PB1][PRICE][FALLBACK] code=XXXX used=prpr_slippage px=YYYY
  ```

### 7️⃣ SIZING 로그 강화
- **상태**: ✅ 이미 구현됨
- **로그**:
  ```
  [PB1][SIZING] code=XXX px=YYY qty=Z ok=1 reason=['ok']
  ```

### 8️⃣ 데이터 품질 로그 추가
- **새로운 로그**:
  ```
  [PB1][OHLCV][WINDOW] code=XXX days=200 rows=185 hi_52w_available=0
  ```

## 📊 주요 상수 변경

| 상수 | 기존 | 변경 | 설명 |
|------|------|------|------|
| OHLCV 윈도우 | 260일 | **200일** | PB1_OHLCV_DAYS_BASE |
| 52주 고저 계산 | `float("nan")` | **None** | 252일 미만 시 |
| ATR 비교 단위 | ratio | **ratio** | 변경 없음 (확정) |
| Price cache TTL | 2초 | **2초** | 변경 없음 |

## 🔍 검증 체크리스트

- ✅ OHLCV 200일 로딩 정상 작동
- ✅ 52주 고저 None 처리 정상
- ✅ ATR Risk Gate ratio 비교 정상
- ✅ None-safe 필터 정상 작동
- ✅ KIS API 캐싱 정상 작동
- ✅ Price fallback 정상 작동
- ✅ Syntax 에러 없음

## 📌 다음 단계

1. **실전 테스트**: LIVE 환경에서 후보 선정 → 매수 전 과정 검증
2. **로그 모니터링**:
   - `[PB1][OHLCV][WINDOW]` → 200일 데이터 확인
   - `[PB1][RISK_GATE]` → ATR 6~9% 종목 정상 통과/탈락
   - `[PB1][PRICE][FALLBACK]` → KIS 500 발생 시에도 진행
3. **성공 기준**:
   - 후보 5~10개 유지
   - 실제 체결 로그 확인
   - 로그만 보고 "왜 이 종목 샀는지" 이해 가능

## 🎓 핵심 원칙 (재확인)

> **"이 지시서는 성능 최적화가 아니라 '실전에서 안 죽는 PB1'을 만드는 것이 목적이다."**
> 
> **"데이터 부족은 실패가 아니라 스킵이며, 살릴 수 있는 종목은 최대한 살린다."**

---
작성일: 2026-01-29
버전: v2.0
