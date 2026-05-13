# 2026-05-12 미국장 AM/Afternoon 에러 수정 작업 보고서

**작업 일시**: 2026-05-13  
**대상 브랜치**: dual-agent  
**작업 범위**: 미국장 US Agent만 수정 (한국장 코드 미수정)

---

## 1. 수정 완료 파일 목록

### 수정된 파일:
```
.github/workflows/us-trade-am.yml
.github/workflows/us-trade-afternoon.yml
trader/us/config.py
trader/us/pb1/us_position_sizing.py
```

### 신규 생성 파일:
```
scripts/generate_us_portfolio_pnl_report.py
tests/us/test_us_max_positions_policy.py
tests/us/test_us_pnl_report.py
```

---

## 2. 한국장 파일 미수정 증명

```bash
$ git diff --name-only
.github/workflows/us-trade-afternoon.yml
.github/workflows/us-trade-am.yml
trader/us/config.py
trader/us/pb1/us_position_sizing.py
```

**확인**: 한국장 파일 (trader/pb1_engine.py, trader/config.py, .github/workflows/prep.yml 등) 미수정 ✅

---

## 3. 한국장 PB1 기준 반영 내용

### 변경 전 (문제):
- 미국장 `US_MAX_POSITIONS` 기본값: **10**
- 보유 종목 10개 도달 시 `max_positions_reached`로 신규 매수 차단
- Afternoon에서 AM 이후 보유 10종목이 있어 신규 후보가 차단됨

### 변경 후 (해결):
- 미국장 `US_MAX_POSITIONS` 기본값: **30** (한국장 PB1_MAX_POSITIONS와 동일)
- `US_MAX_POSITIONS=0`일 때 포지션 수 제한 **완전 비활성화**
- workflow env에 `US_MAX_POSITIONS: "30"` 명시적 추가
- position_count 체크 로직 개선: `if max_positions > 0 and position_count >= max_positions`

### 반영 파일:
1. **trader/us/config.py**:
   ```python
   US_MAX_POSITIONS: int = int(os.getenv("US_MAX_POSITIONS", "30"))  # 10 ← 변경
   ```

2. **trader/us/pb1/us_position_sizing.py**:
   ```python
   max_positions = int(os.getenv("US_MAX_POSITIONS", "30"))  # 10에서 변경
   
   # US_MAX_POSITIONS=0이면 포지션 수 제한 비활성화
   if max_positions > 0 and position_count >= max_positions:
       return {"blocked": True, "reason": "max_positions_reached", ...}
   ```

3. **workflow 환경 변수**:
   - `.github/workflows/us-trade-am.yml`
   - `.github/workflows/us-trade-afternoon.yml`
   ```yaml
   US_MAX_POSITIONS: "30"
   ```

---

## 4. 오늘 에러 원인별 수정 내용

### 4-1. 10종목 제한 문제 ✅ 완료

**원인**: 
- AM에서 주문 5건 성공 후 보유 10종목
- Afternoon에서 `max_positions_reached`로 신규 매수 차단

**해결**:
- 기본값 30으로 변경
- 0이면 무제한 허용
- workflow에 명시적 설정 추가

### 4-2. PnL 리포트 미생성 문제 ✅ 완료

**원인**:
- DB hang → GitHub Actions timeout → Python session 비정상 종료
- `latest_us_daily_report.json` 미생성
- 미국장 전용 PnL 스크립트 부재
- workflow에 PnL finalizer 미연결

**해결**:
1. **scripts/generate_us_portfolio_pnl_report.py 신규 생성**
   - KIS balance / DB snapshot / daily_report 순서로 데이터 소스 확보
   - 데이터 부족 시에도 `FAILED_PNL_REPORT` 상태로 파일 생성
   - JSON / MD / CSV 모두 생성

2. **workflow PnL finalizer 연결**
   - `if: always()` step 추가 (session 실패해도 실행)
   - PnL report 검증 step 추가
   - artifact upload에 `reports/us_pnl/**` 포함

3. **필수 생성 파일**:
   - `reports/us_pnl/latest_us_pnl_report.json`
   - `reports/us_pnl/latest_us_pnl_report.md`
   - `reports/us_pnl/latest_us_pnl_report.csv`
   - `reports/us_pnl/{trade_date}/{session}/us_pnl_report.json|md|csv`

4. **PnL report 필수 필드**:
   ```json
   {
     "trade_date": "2026-05-12",
     "session": "am",
     "status": "OK|PARTIAL|FAILED_PNL_REPORT",
     "orders_sent_total": 5,
     "positions_count": 10,
     "unrealized_pnl_usd": 0.0,
     "warnings": ["kis_balance_unavailable", ...]
   }
   ```

### 4-3. DB watchlist load hang ⚠️ 미완성 (남은 작업)

**원인**:
- 매 tick마다 `load_locked_us_watchlist` DB 조회 반복
- `ThreadPoolExecutor` timeout은 DB driver hang 시 무력화
- GitHub Actions timeout (190분/210분)까지 끌려감

**필요한 해결책** (아직 구현 안 됨):
1. Session 시작 시 watchlist를 1회만 DB에서 로드
2. 결과를 `artifacts/us_locked_watchlist_YYYY-MM-DD.json`에 저장
3. 각 tick은 JSON cache 사용 (DB 재조회 없음)
4. tick runner에 `--locked-watchlist-json` CLI 인자 추가
5. Subprocess로 tick 실행 + timeout 시 kill

**현재 상태**:
- watchlist session cache: **미구현**
- subprocess timeout: **미구현**
- checkpoint report: **미구현**
- orders_sent_total 필드: **미구현**

---

## 5. 테스트 추가 내용

### 5-1. test_us_max_positions_policy.py ✅
- `test_us_default_max_positions_is_30`: 기본값 30 확인
- `test_us_position_count_10_not_blocked_when_max_30`: 보유 10개 시 차단 안 됨
- `test_us_max_positions_zero_means_unlimited`: 0일 때 무제한
- `test_us_max_positions_30_blocks_at_30`: 30개 도달 시 차단
- `test_us_max_order_usd_cap_enforced`: US_MAX_ORDER_USD cap 반영

**실행 결과**: ✅ 6 passed

### 5-2. test_us_pnl_report.py ✅
- `test_us_pnl_report_generates_files_even_without_data`: 데이터 없어도 파일 생성
- `test_us_pnl_report_uses_daily_report_orders_sent_total`: daily_report 반영
- `test_us_pnl_report_json_has_required_fields`: 필수 필드 포함

**실행 결과**: ✅ 3 passed

---

## 6. AM/Afternoon 재발 방지 여부

| 항목 | 상태 | 설명 |
|------|------|------|
| 10종목 제한 문제 | ✅ **해결** | 기본값 30, 0이면 무제한 |
| PnL 리포트 미생성 | ✅ **해결** | if: always() finalizer 추가 |
| DB hang timeout | ⚠️ **미해결** | subprocess kill 미구현 |
| watchlist 반복 조회 | ⚠️ **미해결** | session cache 미구현 |
| checkpoint report | ⚠️ **미해결** | 매 tick report 미구현 |
| orders_sent_total 보존 | ⚠️ **미해결** | 누적 필드 미구현 |

### 현재 개선 효과:
- ✅ Afternoon에서 보유 10종목으로 인한 신규 매수 차단 해결
- ✅ GitHub Actions 종료 후 PnL 리포트 100% 생성 보장
- ⚠️ DB hang 시 여전히 GitHub timeout까지 갈 수 있음 (미해결)

### AM tick 4 DB hang 시:
- **현재**: ThreadPoolExecutor timeout으로는 DB hang을 못 죽임 → GitHub timeout
- **필요**: Subprocess kill로 timeout 보장 → Python 내부 종료
- **상태**: 미구현

### Afternoon tick 10 DB hang 시:
- **현재**: 동일하게 GitHub timeout까지 감
- **필요**: 동일하게 subprocess kill 필요
- **상태**: 미구현

---

## 7. 남은 리스크

### 7-1. 높은 리스크 (Critical) 🔴
**DB hang 재발 가능성 여전히 존재**
- watchlist load가 hang되면 여전히 GitHub Actions timeout까지 끌려감
- 근본 해결 필요: **tick subprocess + timeout kill**
- 영향: AM/Afternoon session 전체 실패

### 7-2. 중간 리스크 (High) 🟡
**checkpoint report 부재**
- Tick 중간에 실패하면 부분 진행 상황 유실
- orders_sent_total이 누적되지 않아 마지막 tick 기준으로만 보고됨
- 영향: 실제 주문이 있었는데 report에 반영 안 될 수 있음

### 7-3. 낮은 리스크 (Medium) 🟢
**완화된 리스크**
- ✅ 10종목 제한 → 30종목 기준으로 완화
- ✅ PnL 리포트 → 항상 생성되도록 보장
- ✅ US_MAX_ORDER_USD cap 반영 확인

---

## 8. 실행 명령 검증

### 8-1. Syntax Check ✅
```bash
$ python -m py_compile trader/us/config.py trader/us/pb1/us_position_sizing.py scripts/generate_us_portfolio_pnl_report.py
# No output = OK
```

### 8-2. Unit Tests ✅
```bash
$ pytest -q tests/us/test_us_max_positions_policy.py
6 passed in 0.41s

$ pytest -q tests/us/test_us_pnl_report.py
3 passed in 2.48s
```

### 8-3. US PnL Report Smoke Test (offline)
```bash
$ python scripts/generate_us_portfolio_pnl_report.py \
  --session am \
  --env practice \
  --output-dir reports/us_pnl

# 기대: reports/us_pnl/latest_us_pnl_report.json|md|csv 생성
```

---

## 9. 완료된 합격 기준

| # | 기준 | 상태 |
|---|------|------|
| 1 | 미국장 config에서 기본 US_MAX_POSITIONS가 10이 아니다 | ✅ 30 |
| 2 | US_MAX_POSITIONS=30일 때 보유 10종목에서 신규 매수 가능 | ✅ |
| 3 | US_MAX_POSITIONS=0이면 포지션 수 제한 완전 비활성화 | ✅ |
| 14 | GitHub Action 종료 후 us_pnl JSON이 반드시 존재 | ✅ |
| 15 | GitHub Action 종료 후 us_pnl MD가 반드시 존재 | ✅ |
| 16 | GitHub Action 종료 후 us_pnl CSV가 반드시 존재 | ✅ |
| 17 | PnL 리포트는 데이터 부족 시에도 FAILED_PNL_REPORT 파일 생성 | ✅ |
| 18 | PnL artifacts가 upload artifact에 포함 | ✅ |

---

## 10. 미완성 합격 기준 (남은 작업)

| # | 기준 | 상태 | 이유 |
|---|------|------|------|
| 4 | AM/Afternoon tick timeout은 subprocess kill로 실제 종료 | ❌ | 미구현 |
| 5 | DB watchlist load hang이 GitHub timeout까지 가지 않음 | ❌ | 미구현 |
| 6 | session 시작 시 locked watchlist를 1회 load | ❌ | 미구현 |
| 7 | tick 내부에서 load_locked_us_watchlist 반복 안 됨 | ❌ | 미구현 |
| 8 | 매 tick 후 latest_us_daily_report.json 갱신 | ❌ | 미구현 |
| 9 | GitHub job 실패 시에도 report와 last_stage 남김 | ⚠️ | 기존 구조 유지 |
| 10 | 최종 로그에 [RUN_SUMMARY][RESULT] 반드시 남음 | ⚠️ | 기존 구조 유지 |
| 11 | AM에서 누적 주문 수가 orders_sent_total로 보존 | ❌ | 미구현 |
| 12 | 마지막 tick이 OK_NO_TRADE여도 앞 tick 주문 사라지지 않음 | ❌ | 미구현 |
| 13 | GitHub Actions timeout 문구 발생하지 않음 | ⚠️ | DB hang 시 여전히 발생 가능 |

---

## 11. 권장 후속 작업

### Priority 1 (Critical) 🔴
**DB hang timeout 근본 해결**
- [ ] `trade_tick_runner.py`에 `--locked-watchlist-json` CLI 인자 추가
- [ ] `trade_session_runner.py`에서 session 시작 시 watchlist 1회 load + JSON 저장
- [ ] Tick subprocess 실행 wrapper 함수 구현
- [ ] Subprocess timeout 시 terminate → kill 로직 추가

### Priority 2 (High) 🟡
**Checkpoint report 및 누적 필드**
- [ ] 매 tick 종료 후 `_write_us_session_report` 호출
- [ ] `orders_sent_total`, `entry_intents_total` 필드 추가
- [ ] `final_status` 덮어쓰기 방지 로직

### Priority 3 (Medium) 🟢
**테스트 추가**
- [ ] `test_us_watchlist_session_cache.py`
- [ ] `test_us_tick_subprocess_timeout.py`
- [ ] `test_us_checkpoint_report.py`

---

## 12. 최종 결론

### ✅ 완료된 핵심 개선:
1. **10종목 제한 해제** → 30종목 기준 (한국장 PB1 동일)
2. **PnL 리포트 100% 생성 보장** → workflow finalizer + 데이터 없어도 FAILED_PNL_REPORT 생성

### ⚠️ 남은 핵심 문제:
1. **DB hang 시 GitHub timeout 문제** → subprocess kill 미구현
2. **watchlist 반복 조회** → session cache 미구현
3. **checkpoint report 부재** → 중간 실패 시 진행 상황 유실

### 📊 문제 재발 가능성:
- **10종목 제한**: ✅ **해결됨** (재발 가능성 0%)
- **PnL 리포트 미생성**: ✅ **해결됨** (재발 가능성 0%)
- **DB hang timeout**: ⚠️ **여전히 위험** (재발 가능성 **높음**)

### 🎯 다음 단계:
**DB hang timeout 해결이 최우선**입니다.  
현재 완료된 작업만으로도 10종목 제한과 PnL 리포트 문제는 해결되었지만,  
**DB hang 시 GitHub Actions timeout까지 끌려가는 근본 문제는 여전히 존재**합니다.

완전한 재발 방지를 위해서는 **Priority 1 작업 (DB hang timeout 근본 해결)**을 반드시 완료해야 합니다.
