# rolling_k_auto_trade_api/kis_wrapper.py
"""
KisAPI wrapper (class) - 토큰관리, hashkey, balance, order, fill/시세 조회.
- 견고성: 요청 재시도, 방어적 로깅, 예외 처리.
- 표준 메서드 이름: get_cash_balance, order_cash, get_current_price, get_price_data 추가.
"""

from __future__ import annotations
import os
import time
import logging
import requests
from requests.adapters import HTTPAdapter, Retry
from typing import Optional, Dict, Any, List

logger = logging.getLogger("trader.kis_wrapper")
logger.setLevel(logging.INFO)
if not logger.handlers:
    fmt = logging.Formatter('[%(asctime)s][%(levelname)s][%(name)s] %(message)s')
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

# ------------------------
# env / defaults
# ------------------------
APP_KEY = os.getenv("KIS_APP_KEY")
APP_SECRET = os.getenv("KIS_APP_SECRET")
CANO = os.getenv("CANO")
ACNT_PRDT_CD = os.getenv("ACNT_PRDT_CD", "01")
KIS_ENV = os.getenv("KIS_ENV", "practice").lower()

API_BASE_URL = os.getenv("API_BASE_URL", "")  # prefer explicit
if not API_BASE_URL:
    API_BASE_URL = os.getenv("KIS_REST_URL", "") or (
        "https://openapivts.koreainvestment.com:29443" if KIS_ENV == "practice"
        else "https://openapi.koreainvestment.com:9443"
    )

# tr_id settings (※ 최신 문서에 따라 조정 가능)
if KIS_ENV == "practice":
    BUY_TR_ID = "VTTC0012U"
    SELL_TR_ID = "VTTC0011U"
    BALANCE_TR_ID = "VTTC8434R"
    CUSTTYPE = "P"
else:
    BUY_TR_ID = "TTTC0012U"
    SELL_TR_ID = "TTTC0011U"
    BALANCE_TR_ID = "TTTC8434R"
    CUSTTYPE = "E"

# endpoints
ORDER_PATH = "/uapi/domestic-stock/v1/trading/order-cash"
ORDER_URL = f"{API_BASE_URL}{ORDER_PATH}"
BALANCE_URL = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-balance"
FILL_URL = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-psbl-order"  # (체결/가능 조회와 혼용 주의)
TOKEN_URL = f"{API_BASE_URL}/oauth2/tokenP"
HASHKEY_URL = f"{API_BASE_URL}/uapi/hashkey"
PRICE_URL = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"

# requests session with retries
session = requests.Session()
retries = Retry(
    total=3,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=frozenset(["HEAD", "GET", "OPTIONS", "POST"])
)
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)
session.mount("http://", adapter)


class KisAPI:
    def __init__(self,
                 app_key: Optional[str] = None,
                 app_secret: Optional[str] = None,
                 cano: Optional[str] = None,
                 acnt_prdt_cd: Optional[str] = None,
                 api_base_url: Optional[str] = None,
                 env: Optional[str] = None):
        self.app_key = app_key or APP_KEY
        self.app_secret = app_secret or APP_SECRET
        self.cano = cano or CANO
        self.acnt_prdt_cd = acnt_prdt_cd or ACNT_PRDT_CD
        self.api_base_url = api_base_url or API_BASE_URL
        self.env = (env or KIS_ENV).lower()

        self.access_token: Optional[str] = None
        self._token_expires_at = 0.0

        logger.info("[환경변수 체크] APP_KEY=%s", (self.app_key[:6] + "...") if self.app_key else None)
        logger.info("[환경변수 체크] CANO='%s'", self.cano)
        logger.info("[환경변수 체크] ACNT_PRDT_CD='%s'", self.acnt_prdt_cd)
        logger.info("[환경변수 체크] API_BASE_URL='%s'", self.api_base_url)
        logger.info("[환경변수 체크] KIS_ENV='%s'", self.env)

    # ------------------------
    # token management
    # ------------------------
    def _request_token(self) -> Dict[str, Any]:
        url = TOKEN_URL
        payload = {"grant_type": "client_credentials", "appkey": self.app_key, "appsecret": self.app_secret}
        headers = {"Content-Type": "application/json; charset=utf-8"}
        logger.info("[🔑 토큰발급] 요청 중...")
        resp = session.post(url, json=payload, headers=headers, timeout=10)
        logger.debug("[TOKEN_RESP] %s %s", resp.status_code, resp.text)
        resp.raise_for_status()
        j = resp.json()
        self.access_token = j.get("access_token")
        expires_in = int(j.get("expires_in", 3600))
        self._token_expires_at = time.time() + expires_in - 30
        logger.info("[🔑 토큰발급] 성공: expires_in=%s", expires_in)
        return j

    def refresh_token(self) -> str:
        j = self._request_token()
        return j.get("access_token", "")

    def get_valid_token(self) -> str:
        if not self.access_token or time.time() > self._token_expires_at:
            self._request_token()
        return self.access_token

    # ------------------------
    # headers
    # ------------------------
    def _auth_headers(self) -> Dict[str, str]:
        return {
            "authorization": f"Bearer {self.get_valid_token()}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "content-type": "application/json; charset=utf-8",
        }

    def balance_headers(self) -> Dict[str, str]:
        h = self._auth_headers()
        h["tr_id"] = BALANCE_TR_ID
        h["custtype"] = CUSTTYPE
        return h

    def order_headers(self, tr_id: str, hashkey: Optional[str] = None) -> Dict[str, str]:
        h = self._auth_headers()
        h["tr_id"] = tr_id
        h["custtype"] = CUSTTYPE
        if hashkey:
            h["hashkey"] = hashkey
        return h

    # ------------------------
    # hashkey
    # ------------------------
    def create_hashkey(self, payload: dict) -> Optional[str]:
        try:
            resp = session.post(HASHKEY_URL, json=payload, headers={
                "Content-Type": "application/json; charset=utf-8",
                "appkey": self.app_key, "appsecret": self.app_secret
            }, timeout=7)
            resp.raise_for_status()
            j = resp.json()
            return j.get("HASH") or j.get("hash")
        except Exception as e:
            logger.warning("[HASHKEY_FAIL] %s", e)
            return None

    # ------------------------
    # balance
    # ------------------------
    def inquire_balance(self, code: Optional[str] = None) -> Dict[str, Any]:
        params = {
            "CANO": self.cano,
            "ACNT_PRDT_CD": self.acnt_prdt_cd,
            "AFHR_FLPR_YN": "N",
            "UNPR_YN": "N",
            "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N",
            "PRCS_DVSN": "02",
            "OFL_YN": "N",
            "INQR_DVSN": "01",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": ""
        }
        if code:
            params["PDNO"] = code
        headers = self.balance_headers()
        try:
            resp = session.get(BALANCE_URL, params=params, headers=headers, timeout=10)
            logger.debug("[KIS_BALANCE_RESP] %s %s", resp.status_code, resp.text)
            if resp.status_code == 500:
                logger.warning("[BALANCE_BUG] 500 -> returning minimal")
                return {"qty": 0, "eval_amt": 0}
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.warning("[BALANCE_FAIL] %s", e)
            return {"qty": 0, "eval_amt": 0}

    def inquire_cash_balance(self) -> int:
        j = self.inquire_balance()
        try:
            cash = int(j["output2"][0].get("prvs_rcdl_excc_amt", 0))
            logger.info("[CASH_BALANCE] %s", cash)
            return cash
        except Exception:
            return 0

    # ✅ 표준 이름으로 제공 (kis_api 래퍼가 이 이름을 우선 탐색)
    def get_cash_balance(self) -> Dict[str, Any]:
        cash = self.inquire_cash_balance()
        return {"cash": cash}

    # ------------------------
    # orders
    # ------------------------
    def send_order(self, code: str, qty: int, price: int = 0, side: str = "sell",
                   ord_dvsn: str = "00", sll_type: Optional[str] = None) -> Optional[Dict[str, Any]]:
        body = {
            "CANO": self.cano,
            "ACNT_PRDT_CD": self.acnt_prdt_cd,
            "PDNO": code,
            "ORD_DVSN": ord_dvsn,
            "ORD_QTY": str(qty),
            "ORD_UNPR": str(price),
        }
        if sll_type:
            body["SLL_TYPE"] = sll_type

        hashkey = self.create_hashkey(body) or ""
        tr_id = BUY_TR_ID if side.lower() == "buy" else SELL_TR_ID
        headers = self.order_headers(tr_id=tr_id, hashkey=hashkey)

        logger.info("[주문요청] tr_id=%s side=%s ord_dvsn=%s body=%s", tr_id, side, ord_dvsn, body)
        try:
            resp = session.post(ORDER_URL, headers=headers, json=body, timeout=10)
            logger.debug("[ORDER_RESP] %s %s", resp.status_code, resp.text)
            try:
                rj = resp.json()
            except Exception:
                rj = {"status_code": resp.status_code, "text": resp.text}
            if resp.status_code >= 400 or (isinstance(rj, dict) and rj.get("rt_cd") not in (None, "0")):
                logger.error("[ORDER_FAIL_BIZ] side=%s ord_dvsn=%s resp=%s", side, ord_dvsn, rj)
                return None
            logger.info("[ORDER_OK] %s", rj)
            return rj
        except requests.exceptions.SSLError as e:
            logger.error("[ORDER_NET_EX] ord_dvsn=%s ex=%s", ord_dvsn, e)
            return None
        except Exception as e:
            logger.error("[ORDER_NET_EX] ord_dvsn=%s ex=%s", ord_dvsn, e)
            return None

    # ✅ kis_api.send_order_wrapper 와 1:1 매핑되는 표준 시그니처
    def order_cash(self,
                   code: str,
                   qty: int,
                   side: str,
                   price: float = 0.0,
                   order_type: Optional[str] = None,
                   tr_id: Optional[str] = None,
                   rqest_id: Optional[str] = None,
                   extra: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        side: 'BUY'|'SELL'
        order_type: KIS 주문유형 코드(e.g., '01': 시장가, '00': 지정가 등). 미지정 시 기본값 유지.
        price: 지정가일 때 사용(정수 변환).
        """
        side_norm = side.strip().lower()
        if order_type is None:
            # 기본 지정가("00") 또는 시장가("01")는 상위 로직에서 넘겨주는 것을 권장
            ord_dvsn = "00" if price else "01"
        else:
            ord_dvsn = order_type

        return self.send_order(
            code=code,
            qty=int(qty),
            price=int(price or 0),
            side=("buy" if side_norm == "buy" else "sell"),
            ord_dvsn=ord_dvsn,
        )

    # ------------------------
    # fills / possible order (placeholder)
    # ------------------------
    def inquire_filled_order(self, ord_no: str) -> Dict[str, Any]:
        params = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "ORD_UNQ_NO": ord_no}
        headers = self._auth_headers()
        resp = session.get(FILL_URL, params=params, headers=headers, timeout=10)
        resp.raise_for_status()
        return resp.json()

    # ------------------------
    # price (현재가/시세)
    # ------------------------
    def get_current_price(self, code: str) -> float:
        """
        단건 현재가 조회. 시장코드 'J'(코스닥), 'U'(코스피)를 시도하고,
        'A' 접두어 유무 모두 시도하여 성공값 반환.
        """
        tried: List[tuple] = []
        code_variants = [code]
        if not code.startswith("A"):
            code_variants.append("A" + code)
        else:
            code_variants.append(code[1:])

        for market_div in ("J", "U"):
            for code_fmt in code_variants:
                params = {"fid_cond_mrkt_div_code": market_div, "fid_input_iscd": code_fmt}
                headers = self._auth_headers()
                try:
                    resp = session.get(PRICE_URL, headers=headers, params=params, timeout=7)
                    data = resp.json()
                    tried.append((market_div, code_fmt, resp.status_code, data.get("rt_cd")))
                    if resp.status_code == 200 and data.get("rt_cd") == "0" and "output" in data:
                        return float(data["output"]["stck_prpr"])
                except Exception as e:
                    tried.append((market_div, code_fmt, "EX", str(e)))
                    continue
        raise RuntimeError(f"[PRICE_FAIL] code={code} tried={tried}")

    def get_price_data(self,
                       code: str,
                       interval: str = "1m",
                       lookback: int = 120,
                       market: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        최소 보장: 현재가 1포인트 반환 구조.
        - 분봉/일봉 시계열은 추후 /quotations/inquire-time-itemchartprice 등으로 확장 가능.
        """
        from datetime import datetime, timezone
        price = self.get_current_price(code)
        return [{"ts": datetime.now(timezone.utc).isoformat(), "price": price}]


__all__ = ["KisAPI"]
