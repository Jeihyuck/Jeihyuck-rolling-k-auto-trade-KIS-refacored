import requests, os, json, time, logging
from settings import APP_KEY, APP_SECRET, API_BASE_URL, CANO, ACNT_PRDT_CD, KIS_ENV
from datetime import datetime
import pytz
import threading

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# helpers
# -----------------------------------------------------------------------------

def safe_strip(val):
    if val is None:
        return ''
    if isinstance(val, str):
        return val.replace('\n', '').replace('\r', '').strip()
    return str(val).strip()

logger.info(f"[환경변수 체크] APP_KEY={repr(APP_KEY)}")
logger.info(f"[환경변수 체크] CANO={repr(CANO)}")
logger.info(f"[환경변수 체크] ACNT_PRDT_CD={repr(ACNT_PRDT_CD)}")
logger.info(f"[환경변수 체크] API_BASE_URL={repr(API_BASE_URL)}")
logger.info(f"[환경변수 체크] KIS_ENV={repr(KIS_ENV)}")


# -----------------------------------------------------------------------------
# KIS API Wrapper
# -----------------------------------------------------------------------------
class KisAPI:
    _token_cache = {"token": None, "expires_at": 0, "last_issued": 0}
    _cache_path = "kis_token_cache.json"
    _token_lock = threading.Lock()

    def __init__(self):
        self.CANO = safe_strip(CANO)
        self.ACNT_PRDT_CD = safe_strip(ACNT_PRDT_CD)
        self.token = self.get_valid_token()
        logger.info(f"[생성자 체크] CANO={repr(self.CANO)}, ACNT_PRDT_CD={repr(self.ACNT_PRDT_CD)}")

    # ------------------------------------------------------------------
    # token
    # ------------------------------------------------------------------
    def get_valid_token(self):
        with KisAPI._token_lock:
            now = time.time()
            # in-memory cache
            if self._token_cache["token"] and now < self._token_cache["expires_at"] - 300:
                return self._token_cache["token"]

            # file cache
            if os.path.exists(self._cache_path):
                try:
                    with open(self._cache_path, "r") as f:
                        cache = json.load(f)
                    if "access_token" in cache and now < cache.get("expires_at", 0) - 300:
                        self._token_cache.update({
                            "token": cache["access_token"],
                            "expires_at": cache.get("expires_at", 0),
                            "last_issued": cache.get("last_issued", 0),
                        })
                        logger.info(f"[토큰캐시] 파일캐시 사용: {cache['access_token'][:10]}... 만료:{cache.get('expires_at')}")
                        return cache["access_token"]
                except Exception as e:
                    logger.warning(f"[토큰캐시 읽기오류] {e}")

            # throttle: 1/min issue
            if now - self._token_cache["last_issued"] < 61:
                logger.warning("[토큰] 1분 이내 재발급 시도 차단, 기존 토큰 재사용")
                if self._token_cache["token"]:
                    return self._token_cache["token"]
                raise Exception("토큰 발급 제한(1분 1회), 잠시 후 재시도 필요")

            token, expires_in = self._issue_token_and_expire()
            expires_at = now + int(expires_in)
            self._token_cache.update({
                "token": token,
                "expires_at": expires_at,
                "last_issued": now,
            })
            try:
                with open(self._cache_path, "w") as f:
                    json.dump({
                        "access_token": token,
                        "expires_at": expires_at,
                        "last_issued": now,
                    }, f)
            except Exception as e:
                logger.warning(f"[토큰캐시 쓰기오류] {e}")

            logger.info("[토큰캐시] 새 토큰 발급 및 캐시")
            return token

    def _issue_token_and_expire(self):
        url = f"{API_BASE_URL}/oauth2/tokenP"
        headers = {"content-type": "application/json"}
        data = {"grant_type": "client_credentials", "appkey": APP_KEY, "appsecret": APP_SECRET}
        resp = requests.post(url, json=data, headers=headers, timeout=5).json()
        if "access_token" in resp:
            logger.info(f"[🔑 토큰발급] 성공: {resp}")
            return resp["access_token"], resp["expires_in"]
        logger.error(f"[🔑 토큰발급 실패]: {resp.get('error_description')}")
        raise Exception(f"토큰 발급 실패: {resp.get('error_description')}")

    def _headers(self, tr_id):
        return {
            "authorization": f"Bearer {self.get_valid_token()}",
            "appkey": APP_KEY,
            "appsecret": APP_SECRET,
            "tr_id": tr_id,
            "custtype": "P",
            "content-type": "application/json",
        }

    # ------------------------------------------------------------------
    # quotations / orders
    # ------------------------------------------------------------------
    def get_current_price(self, code):
        tried = []
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
        headers = self._headers("FHKST01010100")
        for market_div in ["J", "U"]:  # J: 주식, U: ETF/ETN 등 (백업)
            for code_fmt in [code, f"A{code}", code[1:] if str(code).startswith("A") else code]:
                params = {"fid_cond_mrkt_div_code": market_div, "fid_input_iscd": code_fmt}
                for _ in range(3):
                    try:
                        resp = requests.get(url, headers=headers, params=params, timeout=5).json()
                        tried.append((market_div, code_fmt, resp.get("rt_cd"), resp.get("msg1")))
                        if resp.get("rt_cd") == "0" and "output" in resp:
                            return float(resp["output"].get("stck_prpr"))
                    except Exception as e:
                        logger.error(f"[현재가조회오류][{code}] {e}")
                        time.sleep(0.7)
        raise Exception(f"현재가 조회 실패({code}): tried={tried}")

    def buy_stock(self, code, qty, price=None):
        tr_id = "VTTC0012U" if KIS_ENV == "practice" else "TTTC0012U"
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
        headers = self._headers(tr_id)
        if price is None:
            price = self.get_current_price(code)
        data = {
            "CANO": safe_strip(self.CANO),
            "ACNT_PRDT_CD": safe_strip(self.ACNT_PRDT_CD),
            "PDNO": str(code).strip(),
            "ORD_DVSN": "00",  # 지정가
            "ORD_QTY": str(int(float(qty))).strip(),
            "ORD_UNPR": str(int(float(price))).strip(),
        }
        logger.info(f"[매수주문 요청파라미터] {data}")
        for _ in range(3):
            try:
                resp = requests.post(url, headers=headers, json=data, timeout=5).json()
                if resp.get("rt_cd") == "0":
                    logger.info(f"[매수 체결 응답] {resp}")
                    return resp.get("output")
                elif resp.get("msg1") == "모의투자 장종료 입니다.":
                    logger.warning("⏰ [KIS] 장운영시간 외 주문시도 — 주문 무시(정상)")
                    return None
                elif "초과" in (resp.get("msg1") or ""):
                    logger.warning(f"⏰ [KIS] API 사용량 초과(Throttle) — 주문 무시(정상): {resp.get('msg1')}")
                    return None
                else:
                    logger.error(f"[ORDER_FAIL] {resp}")
            except Exception as e:
                logger.error(f"[매수주문 예외][{code}] {e}")
                time.sleep(0.8)
        raise Exception(f"매수주문 실패({code}): {resp.get('msg1', resp)}")

    def sell_stock(self, code, qty, price=None):
        tr_id = "VTTC0013U" if KIS_ENV == "practice" else "TTTC0013U"
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
        headers = self._headers(tr_id)
        if price is None:
            price = self.get_current_price(code)
        data = {
            "CANO": safe_strip(self.CANO),
            "ACNT_PRDT_CD": safe_strip(self.ACNT_PRDT_CD),
            "PDNO": str(code).strip(),
            "ORD_DVSN": "00",
            "ORD_QTY": str(int(float(qty))).strip(),
            "ORD_UNPR": str(int(float(price))).strip(),
        }
        logger.info(f"[매도주문 요청파라미터] {data}")
        for _ in range(3):
            try:
                resp = requests.post(url, headers=headers, json=data, timeout=5).json()
                if resp.get("rt_cd") == "0":
                    logger.info(f"[매도 체결 응답] {resp}")
                    return resp.get("output")
                elif resp.get("msg1") == "모의투자 장종료 입니다.":
                    logger.warning("⏰ [KIS] 장운영시간 외 매도 주문시도 — 주문 무시(정상)")
                    return None
                elif "초과" in (resp.get("msg1") or ""):
                    logger.warning(f"⏰ [KIS] API 사용량 초과(Throttle) — 주문 무시(정상): {resp.get('msg1')}")
                    return None
                else:
                    logger.error(f"[SELL_ORDER_FAIL] {resp}")
            except Exception as e:
                logger.error(f"[매도주문 예외][{code}] {e}")
                time.sleep(0.8)
        raise Exception(f"매도주문 실패({code}): {resp.get('msg1', resp)}")

    # ------------------------------------------------------------------
    # balances (with robust pagination)
    # ------------------------------------------------------------------
    def _balance_params(self, ctx_fk: str = "", ctx_nk: str = ""):
        return {
            "CANO": safe_strip(self.CANO),
            "ACNT_PRDT_CD": safe_strip(self.ACNT_PRDT_CD),
            "AFHR_FLPR_YN": "N",
            "UNPR_YN": "N",
            "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N",
            "PRCS_DVSN": "01",
            "OFL_YN": "N",
            "INQR_DVSN": "02",
            # 페이지네이션 토큰 (직전 응답의 ctx 값 그대로 재전송)
            "CTX_AREA_FK100": safe_strip(ctx_fk),
            "CTX_AREA_NK100": safe_strip(ctx_nk),
        }

    def _select_holdings_list(self, resp: dict):
        """KIS 모의/실계좌에서 페이지별로 holdings 위치가 달라질 수 있어 방어적으로 선택."""
        # output1: 보유종목(가장 일반적)
        val = resp.get("output1")
        if isinstance(val, list) and val and isinstance(val[0], dict) and "pdno" in val[0]:
            return "output1", val
        # 일부 환경에서 output2가 종목일 때도 존재 (희귀)
        val = resp.get("output2")
        if isinstance(val, list) and val and isinstance(val[0], dict) and "pdno" in val[0]:
            return "output2", val
        # 구형/다른 엔드포인트
        val = resp.get("output")
        if isinstance(val, list) and val and isinstance(val[0], dict) and "pdno" in val[0]:
            return "output", val
        return None, []

    def get_balance(self):
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-balance"
        headers = self._headers("VTTC8434R" if KIS_ENV == "practice" else "TTTC8434R")

        all_rows = []
        seen_keys = set()
        ctx_fk = ""
        ctx_nk = ""

        for page in range(1, 200):  # 안전장치: 최대 200페이지
            params = self._balance_params(ctx_fk, ctx_nk)
            logger.info(f"[보유잔고 전체조회 요청파라미터] {params}")
            resp = None
            for _ in range(3):
                try:
                    resp = requests.get(url, headers=headers, params=params, timeout=7).json()
                    logger.info(f"[잔고조회 RAW 응답] {json.dumps(resp, ensure_ascii=False, indent=2)}")
                    break
                except Exception as e:
                    logger.error(f"[잔고전체조회 예외]{e}")
                    time.sleep(1.0)

            if not isinstance(resp, dict):
                logger.error(f"[잔고조회 실패] 잘못된 응답형식: {type(resp)}")
                break

            if resp.get("rt_cd") != "0":
                logger.error(f"[잔고조회 실패] {resp}")
                break

            # 보유종목이 들어있는 키를 판별
            which, rows = self._select_holdings_list(resp)
            if rows:
                # 중복 제거(같은 종목이 다음 페이지로 넘어오는 경우 방지)
                added = 0
                for r in rows:
                    key = (r.get("pdno"), r.get("pchs_avg_pric"), r.get("hldg_qty"))
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)
                    all_rows.append(r)
                    added += 1
                logger.info(f"[잔고조회] {which}에서 {len(rows)}개 수신(신규 {added}개) 누적 {len(all_rows)}개")
            else:
                # holdings가 이 페이지엔 없고 요약(output2)만 있을 수 있음
                logger.info("[잔고조회] 이 페이지에 보유종목 리스트 없음 (요약 페이지만 수신)")

            # 다음 페이지 토큰
            ctx_fk_next = safe_strip(resp.get("ctx_area_fk100", ""))
            ctx_nk_next = safe_strip(resp.get("ctx_area_nk100", ""))

            # 더 없으면 종료
            if not ctx_fk_next and not ctx_nk_next:
                break

            # 다음 반복 준비
            ctx_fk, ctx_nk = ctx_fk_next, ctx_nk_next
            # 과도한 QPS 방지
            time.sleep(0.25)

        return all_rows

    # ------------------------------------------------------------------
    # market hours
    # ------------------------------------------------------------------
    def is_market_open(self):
        KST = pytz.timezone('Asia/Seoul')
        now = datetime.now(KST)
        if now.weekday() >= 5:
            return False
        open_time = now.replace(hour=9, minute=0, second=0, microsecond=0)
        close_time = now.replace(hour=15, minute=30, second=0, microsecond=0)  # 정규장 15:30
        return open_time <= now <= close_time

