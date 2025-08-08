import requests, os, json, time, logging
from settings import APP_KEY, APP_SECRET, API_BASE_URL, CANO, ACNT_PRDT_CD, KIS_ENV
from datetime import datetime
import pytz
import threading

logger = logging.getLogger(__name__)


def safe_strip(val):
    if val is None:
        return ''
    if isinstance(val, str):
        return val.replace('\n', '').replace('\r', '').strip()
    return str(val).strip()


# 환경 변수 로깅(디버깅용)
logger.info(f"[환경변수 체크] APP_KEY={repr(APP_KEY)}")
logger.info(f"[환경변수 체크] CANO={repr(CANO)}")
logger.info(f"[환경변수 체크] ACNT_PRDT_CD={repr(ACNT_PRDT_CD)}")
logger.info(f"[환경변수 체크] API_BASE_URL={repr(API_BASE_URL)}")
logger.info(f"[환경변수 체크] KIS_ENV={repr(KIS_ENV)}")


class KisAPI:
    """한국투자증권 OpenAPI 래퍼
    - 토큰 캐시(파일 + 메모리)
    - 현재가 조회
    - 현금 매수/매도
    - 잔고/예수금 조회(페이지네이션 완전 반영)
    """

    _token_cache = {"token": None, "expires_at": 0, "last_issued": 0}
    _cache_path = "kis_token_cache.json"
    _token_lock = threading.Lock()

    def __init__(self):
        self.CANO = safe_strip(CANO)
        self.ACNT_PRDT_CD = safe_strip(ACNT_PRDT_CD)
        self.token = self.get_valid_token()
        logger.info(f"[생성자 체크] CANO={repr(self.CANO)}, ACNT_PRDT_CD={repr(self.ACNT_PRDT_CD)}")

    # -------------------- 인증/토큰 --------------------
    def get_valid_token(self):
        with KisAPI._token_lock:
            now = time.time()
            # 메모리 캐시 유효
            if self._token_cache["token"] and now < self._token_cache["expires_at"] - 300:
                return self._token_cache["token"]

            # 파일 캐시 유효
            if os.path.exists(self._cache_path):
                try:
                    with open(self._cache_path, "r") as f:
                        cache = json.load(f)
                    if "access_token" in cache and now < cache["expires_at"] - 300:
                        self._token_cache.update({
                            "token": cache["access_token"],
                            "expires_at": cache["expires_at"],
                            "last_issued": cache.get("last_issued", 0),
                        })
                        logger.info(f"[토큰캐시] 파일캐시 사용: {cache['access_token'][:10]}... 만료:{cache['expires_at']}")
                        return cache["access_token"]
                except Exception as e:
                    logger.warning(f"[토큰캐시 파일 로드 실패] {e}")

            # 1분 이내 재발급 방지(가이드)
            if now - self._token_cache["last_issued"] < 61:
                logger.warning("[토큰] 1분 이내 재발급 시도 차단, 기존 토큰 재사용")
                if self._token_cache["token"]:
                    return self._token_cache["token"]
                raise Exception("토큰 발급 제한(1분 1회), 잠시 후 재시도 필요")

            # 신규 발급
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
                logger.warning(f"[토큰캐시 파일 저장 실패] {e}")
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

    # -------------------- 시세 --------------------
    def get_current_price(self, code):
        tried = []
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
        headers = self._headers("FHKST01010100")
        for market_div in ["J", "U"]:
            for code_fmt in [code, f"A{code}", code[1:] if code.startswith("A") else code]:
                params = {
                    "fid_cond_mrkt_div_code": market_div,
                    "fid_input_iscd": code_fmt,
                }
                for _ in range(3):
                    try:
                        resp = requests.get(url, headers=headers, params=params, timeout=5).json()
                        tried.append((market_div, code_fmt, resp.get("rt_cd"), resp.get("msg1")))
                        if resp.get("rt_cd") == "0" and "output" in resp:
                            return float(resp["output"]["stck_prpr"])  # 현재가
                    except Exception as e:
                        logger.error(f"[현재가조회오류][{code}] {e}")
                        time.sleep(1)
        raise Exception(f"현재가 조회 실패({code}): tried={tried}")

    # -------------------- 주문 --------------------
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
            "ORD_DVSN": "00",  # 지정가(00)
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
                msg = resp.get("msg1", "")
                if msg == "모의투자 장종료 입니다.":
                    logger.warning("⏰ [KIS] 장운영시간 외 주문시도 — 주문 무시(정상)")
                    return None
                if "초과" in msg:
                    logger.warning(f"⏰ [KIS] API 사용량 초과(Throttle) — 주문 무시(정상): {msg}")
                    return None
                logger.error(f"[ORDER_FAIL] {resp}")
            except Exception as e:
                logger.error(f"[매수주문 예외][{code}] {e}")
                time.sleep(1)
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
                msg = resp.get("msg1", "")
                if msg == "모의투자 장종료 입니다.":
                    logger.warning("⏰ [KIS] 장운영시간 외 매도 주문시도 — 주문 무시(정상)")
                    return None
                if "초과" in msg:
                    logger.warning(f"⏰ [KIS] API 사용량 초과(Throttle) — 주문 무시(정상): {msg}")
                    return None
                logger.error(f"[SELL_ORDER_FAIL] {resp}")
            except Exception as e:
                logger.error(f"[매도주문 예외][{code}] {e}")
                time.sleep(1)
        raise Exception(f"매도주문 실패({code}): {resp.get('msg1', resp)}")

    # -------------------- 잔고/예수금 --------------------
    def get_cash_balance(self):
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-balance"
        headers = self._headers("VTTC8434R" if KIS_ENV == "practice" else "TTTC8434R")
        params = {
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
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }
        logger.info(f"[잔고조회 요청파라미터] {params}")
        for _ in range(3):
            try:
                resp = requests.get(url, headers=headers, params=params, timeout=5).json()
                logger.info(f"[잔고조회 응답] {resp}")
                if resp.get("rt_cd") == "0" and resp.get("output2"):
                    try:
                        cash = int(resp["output2"][0]["dnca_tot_amt"])  # 예수금
                        logger.info(f"[CASH_BALANCE] 현재 예수금: {cash:,}원")
                        return cash
                    except Exception as e:
                        logger.error(f"[CASH_BALANCE_PARSE_FAIL] {e}")
                        return 0
                logger.error(f"[CASH_BALANCE_PARSE_FAIL] {resp}")
            except Exception as e:
                logger.error(f"[잔고조회 예외]{e}")
                time.sleep(1)
        return 0

    def get_balance(self):
        """보유 종목 전체 조회(페이지네이션 완전 반영)
        - 한국투자 API는 1페이지 최대 20건을 리턴하므로 ctx_area_* 포인터로 반복 조회 필요
        - output2, output1, output 순서로 보유 종목 배열을 찾아 누적
        - 마지막 페이지는 ctx_area_nk100이 빈값으로 반환됨
        """
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-balance"
        headers = self._headers("VTTC8434R" if KIS_ENV == "practice" else "TTTC8434R")
        base_params = {
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
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }

        results = []
        ctx_area_fk100 = ""
        ctx_area_nk100 = ""

        for page in range(60):  # 안전장치(최대 60페이지 ≒ 1200종목)
            params = dict(base_params)
            params["CTX_AREA_FK100"] = ctx_area_fk100
            params["CTX_AREA_NK100"] = ctx_area_nk100

            logger.info(f"[보유잔고 전체조회 요청파라미터] {params}")
            resp = requests.get(url, headers=headers, params=params, timeout=7).json()
            logger.info(f"[잔고조회 RAW 응답] {json.dumps(resp, ensure_ascii=False, indent=2)}")

            if resp.get("rt_cd") != "0":
                logger.error(f"[잔고조회 실패] {resp}")
                break

            # 페이지 데이터 파싱
            items = []
            if resp.get("output2") and isinstance(resp["output2"], list):
                items = resp["output2"]
                logger.info(f"[잔고조회] output2(보유종목리스트) {len(items)}개")
            elif resp.get("output1") and isinstance(resp["output1"], list):
                items = resp["output1"]
                logger.info(f"[잔고조회] output1(보유종목리스트) {len(items)}개")
            elif resp.get("output") and isinstance(resp["output"], list):
                items = resp["output"]
                logger.info(f"[잔고조회] output(보유종목리스트) {len(items)}개")
            else:
                logger.warning(f"[잔고조회 결과없음] output2/output1/output 모두 비어있음. resp={resp}")
                break

            # 누적
            results.extend(items)

            # 다음 페이지 포인터 추출
            ctx_area_fk100 = (resp.get("ctx_area_fk100") or "").strip()
            ctx_area_nk100 = (resp.get("ctx_area_nk100") or "").strip()

            # 마지막 페이지: 다음 포인터가 비어있음
            if not ctx_area_nk100:
                break

            time.sleep(0.2)  # 서버 부하/쿨다운

        logger.info(f"[보유잔고 API 결과 종목수] {len(results)}개")
        return results

    # -------------------- 장 운영시간 --------------------
    def is_market_open(self):
        KST = pytz.timezone('Asia/Seoul')
        now = datetime.now(KST)
        if now.weekday() >= 5:  # 토,일 휴장
            return False
        open_time = now.replace(hour=9, minute=0, second=0, microsecond=0)
        close_time = now.replace(hour=15, minute=30, second=0, microsecond=0)  # 정규장 15:30
        return open_time <= now <= close_time
