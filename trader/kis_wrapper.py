# kis_wrapper.py
import os
import json
import time
import random
import logging
import threading
from datetime import datetime

import requests
import pytz

from settings import APP_KEY, APP_SECRET, API_BASE_URL, CANO, ACNT_PRDT_CD, KIS_ENV

logger = logging.getLogger(__name__)

# -------------------------------
# 유틸
# -------------------------------
def safe_strip(val):
    if val is None:
        return ""
    if isinstance(val, str):
        return val.replace("\n", "").replace("\r", "").strip()
    return str(val).strip()


def _json_dumps(body: dict) -> str:
    """
    HashKey/주문 본문 모두 동일 직렬화 문자열을 사용하기 위해 고정 직렬화.
    - 공백 제거(separators)
    - 키 순서 보존(sort_keys=False)
    - 한글 그대로(ensure_ascii=False)
    """
    return json.dumps(body, ensure_ascii=False, separators=(",", ":"), sort_keys=False)


logger.info(f"[환경변수 체크] APP_KEY={repr(APP_KEY)}")
logger.info(f"[환경변수 체크] CANO={repr(CANO)}")
logger.info(f"[환경변수 체크] ACNT_PRDT_CD={repr(ACNT_PRDT_CD)}")
logger.info(f"[환경변수 체크] API_BASE_URL={repr(API_BASE_URL)}")
logger.info(f"[환경변수 체크] KIS_ENV={repr(KIS_ENV)}")


class KisAPI:
    """
    - 토큰 캐시 + 파일 캐시
    - HashKey 생성 및 주문 API 호출 시 필수 헤더 준수
    - 시장가/IOC/최유리 Fallback 및 지수형 백오프
    - 보유수량 사전 검증
    """
    _token_cache = {"token": None, "expires_at": 0, "last_issued": 0}
    _cache_path = "kis_token_cache.json"
    _token_lock = threading.Lock()

    def __init__(self):
        self.CANO = safe_strip(CANO)
        self.ACNT_PRDT_CD = safe_strip(ACNT_PRDT_CD)
        self.env = safe_strip(KIS_ENV or "practice").lower()
        self.session = requests.Session()
        self.token = self.get_valid_token()
        logger.info(f"[생성자 체크] CANO={repr(self.CANO)}, ACNT_PRDT_CD={repr(self.ACNT_PRDT_CD)}, ENV={self.env}")

    # -------------------------------
    # 토큰
    # -------------------------------
    def get_valid_token(self):
        with KisAPI._token_lock:
            now = time.time()
            if self._token_cache["token"] and now < self._token_cache["expires_at"] - 300:
                return self._token_cache["token"]

            if os.path.exists(self._cache_path):
                try:
                    with open(self._cache_path, "r", encoding="utf-8") as f:
                        cache = json.load(f)
                    if "access_token" in cache and now < cache["expires_at"] - 300:
                        self._token_cache.update(
                            {"token": cache["access_token"], "expires_at": cache["expires_at"], "last_issued": cache.get("last_issued", 0)}
                        )
                        logger.info(f"[토큰캐시] 파일캐시 사용: {cache['access_token'][:10]}... 만료:{cache['expires_at']}")
                        return cache["access_token"]
                except Exception as e:
                    logger.warning(f"[토큰캐시 읽기 실패] {e}")

            # 1분 내 재발급 차단
            if now - self._token_cache["last_issued"] < 61:
                logger.warning("[토큰] 1분 이내 재발급 시도 차단, 기존 토큰 재사용")
                if self._token_cache["token"]:
                    return self._token_cache["token"]
                raise Exception("토큰 발급 제한(1분 1회), 잠시 후 재시도 필요")

            token, expires_in = self._issue_token_and_expire()
            expires_at = now + int(expires_in)
            self._token_cache.update({"token": token, "expires_at": expires_at, "last_issued": now})
            try:
                with open(self._cache_path, "w", encoding="utf-8") as f:
                    json.dump({"access_token": token, "expires_at": expires_at, "last_issued": now}, f, ensure_ascii=False)
            except Exception as e:
                logger.warning(f"[토큰캐시 쓰기 실패] {e}")
            logger.info("[토큰캐시] 새 토큰 발급 및 캐시")
            return token

    def _issue_token_and_expire(self):
        token_path = "/oauth2/tokenP" if self.env == "practice" else "/oauth2/token"
        url = f"{API_BASE_URL}{token_path}"
        headers = {"content-type": "application/json"}
        data = {"grant_type": "client_credentials", "appkey": APP_KEY, "appsecret": APP_SECRET}
        try:
            resp = self.session.post(url, json=data, headers=headers, timeout=(3.0, 7.0))
            j = resp.json()
        except Exception as e:
            logger.error(f"[🔑 토큰발급 예외] {e}")
            raise
        if "access_token" in j:
            logger.info(f"[🔑 토큰발급] 성공: {j}")
            return j["access_token"], j.get("expires_in", 86400)
        logger.error(f"[🔑 토큰발급 실패] {j.get('error_description', j)}")
        raise Exception(f"토큰 발급 실패: {j.get('error_description', j)}")

    # -------------------------------
    # 헤더/HashKey
    # -------------------------------
    def _headers(self, tr_id: str, hashkey: str | None = None):
        h = {
            "authorization": f"Bearer {self.get_valid_token()}",
            "appkey": APP_KEY,
            "appsecret": APP_SECRET,
            "tr_id": tr_id,
            "custtype": "P",  # 개인
            "content-type": "application/json; charset=utf-8",
        }
        if hashkey:
            h["hashkey"] = hashkey
        return h

    def _create_hashkey(self, body_dict: dict) -> str:
        """
        HashKey API: /uapi/hashkey
        - 헤더: appkey, appsecret, content-type
        - 바디: 주문에 사용할 원본 JSON 문자열과 동일해야 함
        """
        url = f"{API_BASE_URL}/uapi/hashkey"
        headers = {
            "content-type": "application/json; charset=utf-8",
            "appkey": APP_KEY,
            "appsecret": APP_SECRET,
        }
        body_str = _json_dumps(body_dict)
        try:
            r = self.session.post(url, headers=headers, data=body_str.encode("utf-8"), timeout=(3.0, 5.0))
            j = r.json()
        except Exception as e:
            logger.error(f"[HASHKEY 예외] {e}")
            raise
        hk = j.get("HASH") or j.get("hash") or j.get("hashkey")
        if not hk:
            logger.error(f"[HASHKEY 실패] resp={j}")
            raise Exception(f"HashKey 생성 실패: {j}")
        return hk

    # -------------------------------
    # 시세/장운영
    # -------------------------------
    def get_current_price(self, code: str) -> float:
        tried = []
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
        headers = self._headers("FHKST01010100")
        for market_div in ["J", "UN"]:  # J: KRX, UN: 통합
            for code_fmt in [code, f"A{code}" if not code.startswith("A") else code, code[1:] if code.startswith("A") else code]:
                params = {"FID_COND_MRKT_DIV_CODE": market_div, "FID_INPUT_ISCD": code_fmt}
                try:
                    resp = self.session.get(url, headers=headers, params=params, timeout=(3.0, 5.0))
                    data = resp.json()
                except Exception as e:
                    tried.append((market_div, code_fmt, f"EXC:{e}"))
                    continue
                tried.append((market_div, code_fmt, data.get("rt_cd"), data.get("msg1")))
                if resp.status_code == 200 and data.get("rt_cd") == "0" and "output" in data:
                    return float(data["output"]["stck_prpr"])
        raise Exception(f"현재가 조회 실패({code}): tried={tried}")

    def is_market_open(self) -> bool:
        kst = pytz.timezone("Asia/Seoul")
        now = datetime.now(kst)
        if now.weekday() >= 5:
            return False
        open_time = now.replace(hour=9, minute=0, second=0, microsecond=0)
        close_time = now.replace(hour=15, minute=20, second=0, microsecond=0)
        return open_time <= now <= close_time

    # -------------------------------
    # 잔고/보유수량 맵
    # -------------------------------
    def get_cash_balance(self) -> int:
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-balance"
        tr_id = "VTTC8434R" if self.env == "practice" else "TTTC8434R"
        headers = self._headers(tr_id)
        params = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": self.ACNT_PRDT_CD,
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
        try:
            resp = self.session.get(url, headers=headers, params=params, timeout=(3.0, 7.0))
            j = resp.json()
        except Exception as e:
            logger.error(f"[잔고조회 예외] {e}")
            return 0
        logger.info(f"[잔고조회 응답] {j}")
        if j.get("rt_cd") == "0" and "output2" in j and j["output2"]:
            try:
                cash = int(j["output2"][0]["dnca_tot_amt"])
                logger.info(f"[CASH_BALANCE] 현재 예수금: {cash:,}원")
                return cash
            except Exception as e:
                logger.error(f"[CASH_BALANCE_PARSE_FAIL] {e}")
                return 0
        logger.error(f"[CASH_BALANCE_PARSE_FAIL] {j}")
        return 0

    def get_positions(self) -> list[dict]:
        """
        잔고의 output1 배열(보유 종목 리스트) 반환
        """
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-balance"
        tr_id = "VTTC8434R" if self.env == "practice" else "TTTC8434R"
        headers = self._headers(tr_id)
        params = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": self.ACNT_PRDT_CD,
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
        try:
            resp = self.session.get(url, headers=headers, params=params, timeout=(3.0, 7.0))
            j = resp.json()
        except Exception as e:
            logger.error(f"[포지션조회 예외] {e}")
            return []
        arr = j.get("output1") or []
        return arr

    def get_balance_map(self) -> dict:
        """
        { '종목코드(pdno)': 주문가능수량(int) } 맵 생성
        """
        pos = self.get_positions()
        mp = {}
        for row in pos:
            try:
                pdno = safe_strip(row.get("pdno"))
                qty = int(float(row.get("ord_psbl_qty", "0")))
                if pdno:
                    mp[pdno] = qty
            except Exception:
                continue
        logger.info(f"[보유수량맵] {len(mp)}종목")
        return mp

    # -------------------------------
    # 주문 공통
    # -------------------------------
    def _order_cash(self, body: dict, *, is_sell: bool) -> dict | None:
        """
        /uapi/domestic-stock/v1/trading/order-cash
        - TR_ID: (모의) 매도 VTTC0011U / 매수 VTTC0012U
                 (실전) 매도 TTTC0011U / 매수 TTTC0012U
        - HashKey 필수(POST)
        - 지수형 백오프 + Fallback(시장가 -> IOC시장가 -> 최유리)
        """
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
        tr_id = (
            ("VTTC0011U" if self.env == "practice" else "TTTC0011U")
            if is_sell
            else ("VTTC0012U" if self.env == "practice" else "TTTC0012U")
        )

        # Fallback 시도 순서
        ord_dvsn_chain = ["01", "13", "03"]  # 시장가, IOC시장가, 최유리
        last_err = None

        for idx, ord_dvsn in enumerate(ord_dvsn_chain, start=1):
            body["ORD_DVSN"] = ord_dvsn
            # 시장가/최유리 류는 주문단가 0 고정
            body["ORD_UNPR"] = "0"

            # SLL_TYPE(매도유형): 미입력시 01 일반매도
            if is_sell and not body.get("SLL_TYPE"):
                body["SLL_TYPE"] = "01"

            # 거래소 구분(선택) - 모의는 KRX만
            body.setdefault("EXCG_ID_DVSN_CD", "KRX")

            # HashKey 생성
            hk = self._create_hashkey(body)
            headers = self._headers(tr_id, hk)

            # 로깅(민감정보 제외)
            log_body = dict(body)
            log_body_masked = {k: (v if k not in ("CANO", "ACNT_PRDT_CD") else "***") for k, v in log_body.items()}
            logger.info(f"[주문요청] tr_id={tr_id} ord_dvsn={ord_dvsn} body={log_body_masked}")

            # 지수형 백오프 파라미터
            attempt = 0
            while attempt < 3:  # 각 방식 최대 3회 네트워크 재시도
                attempt += 1
                try:
                    resp = self.session.post(url, headers=headers, data=_json_dumps(body).encode("utf-8"), timeout=(3.0, 7.0))
                    data = resp.json()
                except Exception as e:
                    backoff = min(0.6 * (1.7 ** (attempt - 1)), 5.0) + random.uniform(0, 0.35)
                    logger.error(f"[ORDER_NET_EX] ord_dvsn={ord_dvsn} attempt={attempt} ex={e} → sleep {backoff:.2f}s")
                    time.sleep(backoff)
                    last_err = e
                    continue

                if resp.status_code == 200 and data.get("rt_cd") == "0":
                    logger.info(f"[ORDER_OK] ord_dvsn={ord_dvsn} output={data.get('output')}")
                    return data

                msg_cd = data.get("msg_cd", "")
                msg1 = data.get("msg1", "")
                # 게이트웨이/내부 오류 패턴 → 백오프 후 재시도
                if msg_cd in ("IGW00008",) or "MCA" in msg1 or resp.status_code >= 500:
                    backoff = min(0.6 * (1.7 ** (attempt - 1)), 5.0) + random.uniform(0, 0.35)
                    logger.error(f"[ORDER_FAIL_GATEWAY] ord_dvsn={ord_dvsn} attempt={attempt} resp={data} → sleep {backoff:.2f}s")
                    time.sleep(backoff)
                    last_err = data
                    continue

                # 비즈니스 오류는 즉시 리턴
                logger.error(f"[ORDER_FAIL_BIZ] ord_dvsn={ord_dvsn} resp={data}")
                return None

            # 다음 Fallback 방식 시도
            logger.warning(f"[ORDER_FALLBACK] ord_dvsn={ord_dvsn} 실패 → 다음 방식 시도")

        # 모두 실패
        raise Exception(f"주문 실패: {last_err}")

    # -------------------------------
    # 매수/매도 래퍼
    # -------------------------------
    def buy_stock_market(self, pdno: str, qty: int) -> dict | None:
        """
        시장가 매수: ORD_DVSN=01, ORD_UNPR=0
        """
        body = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": self.ACNT_PRDT_CD,
            "PDNO": safe_strip(pdno),
            "ORD_QTY": str(int(qty)),
            "ORD_DVSN": "01",  # 시장가 (실제 호출 전 Fallback 체인에서 재설정)
            "ORD_UNPR": "0",
        }
        return self._order_cash(body, is_sell=False)

    def sell_stock_market(self, pdno: str, qty: int) -> dict | None:
        """
        시장가 매도: ORD_DVSN=01, ORD_UNPR=0
        - 보유수량 사전 검증
        """
        # 사전 검증
        bal_map = self.get_balance_map()
        ord_psbl = int(bal_map.get(safe_strip(pdno), 0))
        if ord_psbl <= 0:
            logger.error(f"[SELL_PRECHECK] 보유 없음 pdno={pdno}")
            return None
        if qty > ord_psbl:
            logger.warning(f"[SELL_PRECHECK] 수량 보정: req={qty} -> ord_psbl={ord_psbl}")
            qty = ord_psbl

        body = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": self.ACNT_PRDT_CD,
            "PDNO": safe_strip(pdno),
            "SLL_TYPE": "01",   # 일반매도(미입력 시 01)
            "ORD_QTY": str(int(qty)),
            "ORD_DVSN": "01",   # 시장가 (실제 호출 전 Fallback 체인에서 재설정)
            "ORD_UNPR": "0",
        }
        return self._order_cash(body, is_sell=True)

    # (선택) 지정가 주문이 필요할 때 사용
    def buy_stock_limit(self, pdno: str, qty: int, price: int) -> dict | None:
        body = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": self.ACNT_PRDT_CD,
            "PDNO": safe_strip(pdno),
            "ORD_QTY": str(int(qty)),
            "ORD_DVSN": "00",   # 지정가
            "ORD_UNPR": str(int(price)),
            "EXCG_ID_DVSN_CD": "KRX",
        }
        # 지정가/POST도 hashkey 필수
        hk = self._create_hashkey(body)
        tr_id = "VTTC0012U" if self.env == "practice" else "TTTC0012U"
        headers = self._headers(tr_id, hk)
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
        try:
            resp = self.session.post(url, headers=headers, data=_json_dumps(body).encode("utf-8"), timeout=(3.0, 7.0))
            data = resp.json()
        except Exception as e:
            logger.error(f"[BUY_LIMIT_NET_EX] {e}")
            raise
        if resp.status_code == 200 and data.get("rt_cd") == "0":
            logger.info(f"[BUY_LIMIT_OK] output={data.get('output')}")
            return data
        logger.error(f"[BUY_LIMIT_FAIL] {data}")
        return None

    def sell_stock_limit(self, pdno: str, qty: int, price: int) -> dict | None:
        # 보유수량 체크
        bal_map = self.get_balance_map()
        ord_psbl = int(bal_map.get(safe_strip(pdno), 0))
        if ord_psbl <= 0:
            logger.error(f"[SELL_LIMIT_PRECHECK] 보유 없음 pdno={pdno}")
            return None
        if qty > ord_psbl:
            logger.warning(f"[SELL_LIMIT_PRECHECK] 수량 보정: req={qty} -> ord_psbl={ord_psbl}")
            qty = ord_psbl

        body = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": self.ACNT_PRDT_CD,
            "PDNO": safe_strip(pdno),
            "SLL_TYPE": "01",
            "ORD_QTY": str(int(qty)),
            "ORD_DVSN": "00",   # 지정가
            "ORD_UNPR": str(int(price)),
            "EXCG_ID_DVSN_CD": "KRX",
        }
        hk = self._create_hashkey(body)
        tr_id = "VTTC0011U" if self.env == "practice" else "TTTC0011U"
        headers = self._headers(tr_id, hk)
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
        try:
            resp = self.session.post(url, headers=headers, data=_json_dumps(body).encode("utf-8"), timeout=(3.0, 7.0))
            data = resp.json()
        except Exception as e:
            logger.error(f"[SELL_LIMIT_NET_EX] {e}")
            raise
        if resp.status_code == 200 and data.get("rt_cd") == "0":
            logger.info(f"[SELL_LIMIT_OK] output={data.get('output')}")
            return data
        logger.error(f"[SELL_LIMIT_FAIL] {data}")
        return None
