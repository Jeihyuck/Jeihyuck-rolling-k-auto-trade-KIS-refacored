# 🔧 가격조회 최적화 패치 가이드

## 📋 개요

KIS API 초당 거래건수 초과 오류(EGW002)를 방지하기 위해 가격조회에 **TTL 캐시**, **레이트 리미터**, **서킷 브레이커**를 적용한 패치입니다.

## ✅ 주요 개선사항

### 1. **kis_wrapper.py** 수정

#### 🆕 `get_price_snapshot()` - 단일 진입점
- **TTL 캐시**: 동일 종목에 대한 중복 호출 방지 (기본 2초)
- **레이트 리미터**: 초당 호출수 제한 (기본 3 QPS)
- **서킷 브레이커**: EGW002 발생 시 일정 시간 호출 차단 (기본 15초)
- **Inflight 공유**: 동시 호출 시 결과 공유로 중복 제거

#### 🔧 `_inquire_price_once()` 개선
- 반환값을 `Tuple[Optional[float], str]`로 변경
- 성공: `(price, "OK")`
- 레이트 리밋: `(None, "RATE_LIMIT")`
- HTTP 오류: `(None, "HTTP_FAIL")`
- 파싱 실패: `(None, "PARSE_FAIL")`

#### ♻️ `get_last_price()` 리팩토링
- `get_price_snapshot()`을 사용하여 중복 호출 방지
- ask/bid가 없으면 prpr 사용, 추가 재조회 없음

### 2. **pb1_engine.py** 수정

#### 🚫 유니버스 스캔 단계에서 가격조회 금지
- 198개 종목을 OHLCV(일봉) 데이터로만 필터링
- ATR, RS, VCP 필터 후 최종 후보와 보유 종목만 가격 조회

#### 🎯 가격 계산 로직 단일화
- `get_price_snapshot(code)` 한 번만 호출
- ask/bid 없으면 prpr 사용, 재조회 없음
- 가격 없으면 해당 종목 스킵, 다음 틱에서 재시도

#### ⚡ 레이트리밋 서킷 처리
```python
# _mark_price() 함수에서 서킷 체크
if _price_cache.is_circuit_open():
    return None  # 서킷 열려있으면 조회 건너뜀

# RATE_LIMIT 오류 시 서킷 오픈
if "rate_limit" in exc_str or "egw002" in exc_str:
    _price_cache.open_circuit()
```

#### 📊 가격 조회 상한 설정
- `PB1_MAX_PRICE_FETCH_PER_TICK`: 틱당 최대 가격조회 횟수 (기본 30)
- 초과 시 나머지 종목은 다음 틱으로 이월

## 🔧 환경변수 설정

### 필수 설정 (.env 또는 GitHub Actions)

```bash
# === 가격조회 캐시 및 레이트 리미터 ===
PRICE_TTL_SEC=2                 # 캐시 TTL (초) - 동일 종목 재조회 금지 시간
PRICE_QPS=3                     # 초당 최대 호출수 (Queries Per Second)
PRICE_BURST=3                   # 버스트 허용 수 (동시 호출 허용량)
PRICE_CIRCUIT_SEC=15            # 서킷 브레이커 유지 시간 (초)

# === PB1 엔진 가격조회 제한 ===
PB1_MAX_PRICE_FETCH_PER_TICK=30 # 틱당 가격조회 최대 횟수
```

### 권장 튜닝 방법

#### 🐢 보수적 설정 (안전우선)
```bash
PRICE_TTL_SEC=3
PRICE_QPS=2
PRICE_BURST=2
PRICE_CIRCUIT_SEC=20
PB1_MAX_PRICE_FETCH_PER_TICK=20
```

#### ⚡ 공격적 설정 (속도우선)
```bash
PRICE_TTL_SEC=1
PRICE_QPS=5
PRICE_BURST=5
PRICE_CIRCUIT_SEC=10
PB1_MAX_PRICE_FETCH_PER_TICK=50
```

#### 🎯 균형 설정 (기본값)
```bash
PRICE_TTL_SEC=2
PRICE_QPS=3
PRICE_BURST=3
PRICE_CIRCUIT_SEC=15
PB1_MAX_PRICE_FETCH_PER_TICK=30
```

## 📈 기대 효과

### Before (패치 전)
```
[PRICE][FAIL] status=500 rt_cd=1 msg_cd=EGW00201 msg1=초당 거래건수를 초과하였습니다
[PRICE_ONCE_EX] J/005930 FHKST01010100 → HTTP 500
[RETRY] attempt=1/5 sleep=1.20 err=HTTP 500
```
- ❌ 같은 종목 중복 조회
- ❌ 재시도마다 추가 호출
- ❌ 서킷 없어서 계속 실패

### After (패치 후)
```
[PRICE][CACHE_HIT] code=005930 ttl=2s
[PB1][PRICE][CIRCUIT_OPEN] skip price fetch until circuit closes
[PB1][PRICE][FALLBACK_PRPR] code=005930 prpr=62000 (no ask/bid)
```
- ✅ 캐시로 중복 호출 차단
- ✅ 서킷으로 과부하 방지
- ✅ prpr 폴백으로 정상 매매

### 호출량 감소 예시
| 항목 | 패치 전 | 패치 후 | 감소율 |
|------|---------|---------|--------|
| 유니버스 스캔 (198종목) | 198회 | 0회 | **100%** |
| 후보 종목 가격조회 | 20회×3 재시도 = 60회 | 20회 | **67%** |
| 중복 호출 (2초 내) | 10회 | 0회 (캐시) | **100%** |
| **총 호출량** | **268회** | **20회** | **93%** |

## 🔍 모니터링 및 로그

### 정상 동작 로그
```
[PRICE][CACHE_HIT] code=005930 ttl=2s
[PB1][PRICE][OK] code=005930 ask=62100 bid=62000
[PB1][TICK_SUMMARY] candidates_ok=15 priced_ok=15 intents_created=5
```

### 서킷 동작 로그
```
[PRICE][RATE_LIMITED] code=005930 msg_cd=EGW00201 -> open circuit 15s
[PB1][PRICE][CIRCUIT_OPEN] skip price fetch until circuit closes (until=1738123456)
[PRICE][CIRCUIT_CLOSED] circuit closed after 15s
```

### 폴백 동작 로그
```
[PB1][PRICE][FALLBACK_PRPR] code=005930 prpr=62000 (no ask/bid)
[PB1][PRICE][FALLBACK] code=005930 source=balance_prpr
[PB1][PRICE][FALLBACK] code=005930 source=ohlcv_close
```

## ⚠️ 주의사항

### 1. 캐시 TTL이 너무 길면
- ❌ 가격 변동이 큰 구간에서 부정확한 가격 사용 가능
- ✅ 권장: 1~3초 (시장 상황에 따라 조절)

### 2. QPS가 너무 낮으면
- ❌ 틱 처리 시간 증가, 기회 손실 가능
- ✅ 권장: 최소 2 QPS 유지

### 3. 서킷 시간이 너무 길면
- ❌ 서킷 열린 동안 신규 진입 불가
- ✅ 권장: 10~20초 (레이트 리밋 복구 시간 고려)

### 4. PB1_MAX_PRICE_FETCH_PER_TICK가 너무 낮으면
- ❌ 후보 종목 다수일 때 일부만 처리
- ✅ 권장: 예상 후보 수 × 1.5

## 🧪 테스트 방법

### 1. 캐시 동작 확인
```python
# 같은 종목 2초 내 재조회 시 캐시 히트 확인
kis.get_price_snapshot("005930")  # API 호출
kis.get_price_snapshot("005930")  # [CACHE_HIT] 로그 확인
```

### 2. 서킷 브레이커 테스트
```python
# 환경변수로 서킷 강제 오픈 시간 단축
PRICE_CIRCUIT_SEC=5

# 레이트 리밋 발생 시 5초간 조회 차단 확인
```

### 3. 폴백 동작 확인
```python
# ask/bid 없는 종목 조회 시 prpr 사용 확인
# [PB1][PRICE][FALLBACK_PRPR] 로그 확인
```

## 📚 참고 자료

- [KIS OpenAPI 문서](https://apiportal.koreainvestment.com/)
- [레이트 리미터 구현](./trader/kis_wrapper.py#L255-L280)
- [서킷 브레이커 구현](./trader/kis_wrapper.py#L286-L343)
- [PB1 엔진 가격조회](./trader/pb1_engine.py#L2187-L2240)

---

**적용 날짜**: 2026-01-29  
**작성자**: GitHub Copilot  
**버전**: v1.0
