# 🔧 KIS API 가격조회 최적화 패치 적용 완료

## 📋 패치 개요
KIS OpenAPI의 초당 거래건수 제한(EGW002) 오류를 해결하기 위한 종합 최적화 패치가 완료되었습니다.

## ✅ 적용된 변경사항

### 1. 환경변수 설정 (.env.price_limit)
```env
PRICE_TTL_SEC=2          # 동일 종목 가격조회 TTL 2초
PRICE_QPS=3              # 현재가 API 초당 3건으로 제한
PRICE_BURST=3            # 버스트 허용 3
PRICE_CIRCUIT_SEC=15     # EGW002 발생 시 15초간 가격조회 감쇠
```

**권장사항**: QPS는 보수적으로 3부터 시작하고, 안정화 후 4~5로 조정

### 2. trader/kis_wrapper.py 수정

#### 추가된 클래스 및 기능:
- **_TokenBucket**: Thread-safe 토큰 버킷 레이트 리미터
- **_PriceCache**: TTL 캐시 + inflight 요청 공유 + 서킷 브레이커
- **get_price_snapshot()**: 가격조회의 단일 진입점 (Single Source of Truth)
  - TTL 캐시로 동일 종목 중복 조회 방지
  - Inflight deduplication으로 동시 요청 공유
  - 레이트 리미터로 초당 호출 제한
  - EGW002 감지 시 서킷 브레이커 작동

#### 핵심 변경:
```python
# 기존: 여러 곳에서 개별 API 호출
quote1 = self.get_price_quote(code)
quote2 = self.get_quote_safe(code)  # 중복!

# 변경: 단일 진입점 사용
snapshot = self.get_price_snapshot(code, market="J")  # 한 번만!
ask, bid, prpr = _extract_px_from_snapshot(snapshot)
```

### 3. trader/pb1_engine.py 수정

#### 추가된 헬퍼 함수:
- **_extract_px_from_snapshot()**: 스냅샷에서 ask/bid/prpr 추출

#### 수정된 함수:
- **_mark_price()**: 단일 스냅샷 조회로 변경, ask/bid 없으면 prpr로 fallback (재호출 금지)
- **_size_positions()**: get_quote_safe → get_price_snapshot 변경
- **_run_price_probe()**: 단일 스냅샷 조회로 변경
- **get_quote_snapshot()**: 내부적으로 get_price_snapshot 사용하여 중복 호출 제거

#### 서킷 브레이커 통합:
```python
# run() 메서드 시작 부분
if _price_cache.is_circuit_open():
    logger.warning("[PB1][DEGRADED] price circuit open -> skip new entries this tick")
    entry_allowed = False
    entry_reason = "price_circuit_open"
```

### 4. 가격조회 정책 변경

#### AS-IS (기존):
```
유니버스 198개 종목
  → 각 종목마다 2~3회 가격 API 호출
  → ask/bid 없으면 재조회
  = 총 400~600회 API 호출 → EGW002 발생!
```

#### TO-BE (변경 후):
```
유니버스 198개 종목
  → OHLCV만 조회 (가격조회 없음)
  
후보 종목 2~15개
  → get_price_snapshot() 1회씩만 호출
  → TTL 캐시로 2초 내 재조회 차단
  → 레이트 리미터로 초당 3건 제한
  = 총 2~15회 API 호출 (95% 이상 감소!)
```

## 📊 기대 효과

### 1. API 호출 횟수 감소
- **틱당 가격조회**: 198회 → 2~15회 (92~99% 감소)
- **종목별 중복 호출**: 2~3회 → 1회 (50~67% 감소)

### 2. 오류 방지
- **EGW002 발생률**: 거의 0으로 감소
- **서킷 브레이커**: 문제 발생 시 자동 감쇠 (15초)

### 3. 성능 개선
- **캐시 히트**: `[PRICE][CACHE_HIT]` 로그로 확인 가능
- **Inflight 공유**: `[PRICE][INFLIGHT_JOIN]` 로그로 확인 가능
- **레이트 제한**: `[PRICE][RATE_WAIT]` 로그로 대기 시간 확인

## 🔍 모니터링 로그

### 정상 작동 시:
```
[PRICE][CACHE_HIT] code=005930 ttl=2s
[PRICE][INFLIGHT_JOIN] code=035720
```

### 레이트 제한 작동 시:
```
[PRICE][RATE_WAIT] code=000660 sleep=0.33s
```

### EGW002 감지 시:
```
[PRICE][RATE_LIMITED] code=035420 msg_cd=EGW00201 -> open circuit 15s
[PRICE][CIRCUIT_OPEN] skip inquire-price key=('J', '005930') until=1738150425
[PB1][DEGRADED] price circuit open -> skip new entries this tick
```

## 🚀 배포 가이드

### 1. 환경변수 설정
GitHub Actions Secrets 또는 `.env`에 추가:
```bash
PRICE_TTL_SEC=2
PRICE_QPS=3
PRICE_BURST=3
PRICE_CIRCUIT_SEC=15
```

### 2. 코드 배포
```bash
git add trader/kis_wrapper.py trader/pb1_engine.py .env.price_limit
git commit -m "feat: KIS 가격조회 최적화 - TTL 캐시 + 레이트리미터 + 서킷브레이커"
git push
```

### 3. 모니터링
배포 후 로그에서 다음을 확인:
- `[PRICE][CACHE_HIT]` 빈도 증가
- `[PRICE][RATE_LIMITED]` 감소/소멸
- `inquire-price` 호출 횟수 대폭 감소

### 4. 튜닝 (선택)
안정화 후 QPS 조정:
```env
PRICE_QPS=4  # 또는 5까지 점진적 증가
```

## 🔧 추가 최적화 팁

### 200일 이동평균 사용 검토
현재 260일(1년) 사용 중 → 200일로 변경 시 OHLCV 로딩 20% 감소:
```python
# trader/config.py
PB1_OHLCV_DAYS_BASE = 200  # 기존 260
```
⚠️ 주의: 이는 전략 성과에 영향을 줄 수 있으므로 백테스트 필요

### 캐시 TTL 조정
더 공격적인 캐싱:
```env
PRICE_TTL_SEC=5  # 2초 → 5초로 증가
```

## 📝 참고 사항

### 제한 사항
- 현재가 조회는 여전히 실시간 데이터가 아닌 최대 2초(TTL) 지연 가능
- 극단적 변동성 구간에서는 캐시 TTL을 1초로 줄이는 것도 고려

### 호환성
- 기존 코드와 100% 호환
- 점진적 배포 가능 (kis_wrapper만 먼저 배포 가능)

---

## 🎯 핵심 원칙

> **"종목별 가격조회는 틱당 1회, 유니버스 스캔 시 가격조회 금지"**

이 원칙만 지켜도 EGW002 오류는 거의 발생하지 않습니다.
