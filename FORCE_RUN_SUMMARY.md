# FORCE_RUN 모드 구현 요약

## 구현 완료 ✅

**YML 시간 제약 제거 + Watch list 축소 + 강제 매매 모드** 5가지 작업을 모두 완료했습니다.

---

## 변경된 파일

1. **`.github/workflows/trade-runner.yml`** (스케줄 + 입력값)
   - Schedule: 5분 → 30분 간격
   - workflow_dispatch 입력값 5개 추가 (FORCE_RUN, WATCHLIST_MODE, WATCHLIST, LIVE_TRADING_ENABLED, DRY_RUN)

2. **`trader/config.py`** (장중 세션 우회)
   - `resolve_strategy_mode()`: FORCE_RUN=1이면 무조건 LIVE 모드

3. **`trader/pb1_runner.py`** (Watchlist 모드 + 로깅)
   - `_load_universe_context()`: WATCHLIST_MODE=1이면 env에서 직접 종목 로딩
   - `run_once()`: FORCE_RUN/WATCHLIST_MODE 시작 로깅

4. **`trader/kis_wrapper.py`** (주문 직전 로깅)
   - `_order_cash()`: ORDER_READY 로그에 DRY_RUN/FORCE_RUN 상태 표시

---

## 사용법 (GitHub Actions 수동 실행)

1. GitHub → Actions → "PB1 Trade Runner"
2. "Run workflow" 클릭
3. 파라미터 입력:
   - **FORCE_RUN**: `1` (장중 체크 우회)
   - **WATCHLIST_MODE**: `1` (소수 종목만)
   - **WATCHLIST**: `005930,000660,035420,035720,005380` (5종목)
   - **LIVE_TRADING_ENABLED**: `0` (테스트용)
   - **DRY_RUN**: `1` (안전모드)

---

## 주요 동작

| 모드 | FORCE_RUN | WATCHLIST_MODE | 동작 |
|------|-----------|----------------|------|
| **정상** | 0 | 0 | 평일 09:00~15:30만 실행, DB 유니버스 사용 |
| **강제** | 1 | 0 | 언제든지 실행, DB 유니버스 사용 |
| **축소** | 0 | 1 | 평일 09:00~15:30만 실행, WATCHLIST 5종목 |
| **강제+축소** | 1 | 1 | **언제든지 실행, WATCHLIST 5종목** ⭐ |

---

## 로그 예시

### 시작 시
```
[PB1][FORCE_RUN] FORCE_RUN=True WATCHLIST_MODE=True WATCHLIST=005930,000660,035420 DRY_RUN=True LIVE_TRADING=False
[PB1][WATCHLIST_MODE] bypassing universe -> using 3 codes: ['005930', '000660', '035420']
```

### 주문 직전
```
[ORDER_READY] code=005930 side=BUY qty=10 price=85000 tr_id=VTTC0012U ord_dvsn=01 DRY_RUN=True LIVE=False FORCE_RUN=True body={...}
```

---

## 안전장치

- **DRY_RUN=1**: 시뮬레이션만, 실제 주문 없음 (기본값)
- **LIVE_TRADING_ENABLED=0**: 실거래 비활성화 (기본값)
- **STRATEGY_MODE 검증**: LIVE 모드 시 플래그 불일치하면 에러

---

## 검증 완료

- ✅ YAML 문법 유효성 (trade-runner.yml)
- ✅ Python 구문 오류 없음 (config.py, pb1_runner.py, kis_wrapper.py)
- ✅ FORCE_RUN 로직 구현
- ✅ WATCHLIST_MODE 로직 구현
- ✅ 진단 로깅 강화

---

## 다음 단계

1. **로컬 테스트** (선택):
   ```bash
   FORCE_RUN=1 WATCHLIST_MODE=1 WATCHLIST="005930" DRY_RUN=1 \
   python -m trader.pb1_runner --env practice --strategy best_k_meta
   ```

2. **GitHub Actions 수동 실행** (권장):
   - Actions 탭에서 workflow_dispatch로 파라미터 입력 후 실행

3. **실거래 전 필수 체크**:
   - [ ] DRY_RUN=1로 충분히 테스트
   - [ ] 로그에서 주문 파라미터 확인
   - [ ] 종목 코드/수량/가격 검증
   - [ ] LIVE_TRADING_ENABLED=1, DRY_RUN=0으로 변경

---

**작성**: 2025-01-XX  
**참고**: [FORCE_RUN_MODE_IMPLEMENTATION.md](FORCE_RUN_MODE_IMPLEMENTATION.md)
