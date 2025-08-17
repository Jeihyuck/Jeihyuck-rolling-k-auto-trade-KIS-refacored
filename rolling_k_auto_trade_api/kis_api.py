# rolling_k_auto_trade_api/kis_api.py
from dotenv import load_dotenv
load_dotenv()

import os
import time
import logging
import requests
from typing import Optional, Dict, Any

# ─────────────── 환경 변수 및 계좌 설정 ───────────────
REST_DOMAIN  = os.getenv("KIS_REST_URL", "https://openapi.koreainvestment.com:9443")
APP_KEY      = os.getenv("KIS_APP_KEY")
APP_SECRET   = os.getenv("KIS_APP_SECRET")
ACCESS_TOKEN = os.getenv("KIS_ACCESS_TOKEN")  # 최초 .env, 이후 자동 갱신

CANO         = os.getenv("CANO")              # 8자리 계좌번호
ACNT_PRDT_CD = os.getenv("ACNT_PRDT_CD", "01")

KIS_ENV = os.getenv("KIS_ENV", "practice").lower()

if KIS_ENV == "practice":
    DOMAIN        = "https://openapivts.koreainvestment.com:29443"
    ORDER_PATH    = "/uapi/domestic-stock/v1/trading/order-cash"
    BALANCE_TR_ID = "VTTC8434R"
    BUY_TR_ID     = "VTTC0012U"
    SELL_TR_ID    = "VTTC0011U"
    CUSTTYPE      = "P"  # 개인
else:
    DOMAIN        = "https://openapi.koreainvestment.com:9443"
    ORDER_PATH    = "/uapi/domestic-stock/v1/trading/order-cash"
    BALANCE_TR_ID = "TTTC8434R"
    BUY_TR_ID     = "TTTC0012U"
    SELL_TR_ID    = "TTTC0011U"
    CUSTTYPE      = "E"  # 법인 등

ORDER_URL   = f"{DOMAIN}{ORDER_PATH}"
BALANCE_URL = f"{DOMAIN}/uapi/domestic-stock/v1/trading/inquire-balance"
FILL_URL    = f"{DOMAIN}/uapi/domestic-stock/v1/trading/inquire-psbl-order"
TOKEN_URL   = f"{DOMAIN}/oauth2/tokenP"

# ─────────────── 로깅 설정 ───────────────
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
_fmt = logging.Formatter('[%(asctime)s][%(levelname)s][%(name)s] %(message)s')
fh = logging.FileHandler('kis_api.log', encoding='utf-8')
sh = logging.StreamHandler()
fh.setFormatter(_fmt)
sh.setFormatter(_fmt)
if not logger.handlers:
    logger.addHandler(fh)
    logger.addHandler(sh)

# ─────────────── 토큰 관리 ───────────────
_token_expires_at = 0  # UNIX epoch seconds

def refresh_token() -> str:
    """KIS OAuth2 토큰 재발급"""
    global ACCESS_TOKEN, _token_expires_at
    url     = f"{REST_DOMAIN}/oauth2/tokenP"
    headers = {"Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-cache"}
    payload = {"grant_type": "client_credentials", "appkey": APP_KEY, "appsecret": APP_SECRET}
    logger.info("[TOKEN] 재발급 요청")
    resp = requests.post(url, headers=headers, json=payload, timeout=7)
    logger.debug("[TOKEN_RESP] %s %s", resp.status_code, resp.text)
    resp.raise_for_status()
    j = resp.json()
    ACCESS_TOKEN      = j["access_token"]
    _token_expires_at = time.time() + int(j.get("expires_in", 3600)) - 30
    logger.info("[TOKEN] 재발급 성공, expires_in=%s", j.get("expires_in"))
    return ACCESS_TOKEN

def get_valid_token() -> str:
    if not ACCESS_TOKEN or time.time() > _token_expires_at:
        return refresh_token()
    return ACCESS_TOKEN

# ─────────────── 내부 유틸 함수 ───────────────
def _create_hashkey(payload: dict) -> str:
    """주문 payload -> hashkey"""
    url = f"{REST_DOMAIN}/uapi/hashkey"
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=7)
    logger.debug("[HASHKEY_RESP] %s %s", resp.status_code, resp.text)
    resp.raise_for_status()
    return resp.json()["HASH"]

def _balance_headers():
    """잔고조회 헤더 세팅"""
    return {
        "authorization": f"Bearer {get_valid_token()}",
        "content-type": "application/json; charset=utf-8",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": BALANCE_TR_ID,
        "custtype": CUSTTYPE,
    }

# ─────────────── API Wrappers ───────────────

def inquire_cash_balance() -> int:
    """
    예수금(출금가능금액) 조회: 잔고조회 API에서 output2에서 추출
    실패시 0원 반환
    """
    params = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "AFHR_FLPR_YN": "N",
        "UNPR_YN": "N",
        "UNPR_DVSN": "01",
        "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N",
        "PRCS_DVSN": "01",
        "OFL_YN": "N",
        "INQR_DVSN": "02",
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": ""
    }
    headers = _balance_headers()
    logger.debug(f"[KIS_BALANCE_CASH] URL={BALANCE_URL} Params={params} tr_id={headers['tr_id']}")
    try:
        resp = requests.get(BALANCE_URL, params=params, headers=headers, timeout=7)
        logger.debug(f"[KIS_BALANCE_CASH_RESP] {resp.status_code} {resp.text}")
        if resp.status_code != 200:
            logger.error("[CASH_BALANCE_FAIL] status=%s body=%s", resp.status_code, resp.text)
            return 0
        j = resp.json()
        cash = int(j["output2"][0].get("prvs_rcdl_excc_amt", 0))
        logger.info(f"[CASH_BALANCE] 현재 예수금: {cash:,}원")
        return cash
    except Exception as e:
        logger.error(f"[CASH_BALANCE_PARSE_FAIL] {e}")
        return 0

def inquire_balance(code: str = None) -> dict:
    params = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
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
    headers = _balance_headers()
    logger.debug(f"[KIS_BALANCE] URL={BALANCE_URL} Params={params} tr_id={headers['tr_id']}")
    try:
        resp = requests.get(BALANCE_URL, params=params, headers=headers, timeout=7)
        logger.debug(f"[KIS_BALANCE_RESP] {resp.status_code} {resp.text}")
        if resp.status_code == 500:
            logger.warning(f"[BALANCE_BUG] {code} 500응답 -> 0주 처리")
            return {"qty": 0, "eval_amt": 0}
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logger.warning(f"[BALANCE_FAIL] {code} | {e}")
        return {"qty": 0, "eval_amt": 0}

def send_order(code: str, qty: int, price: int, side: str) -> Optional[dict]:
    body = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "PDNO": code,
        "ORD_DVSN": "00",  # 지정가
        "ORD_QTY": str(qty),
        "ORD_UNPR": str(price),
    }
    hashkey = _create_hashkey(body)
    tr_id = BUY_TR_ID if side == "buy" else SELL_TR_ID
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": tr_id,
        "custtype": CUSTTYPE,
        "hashkey": hashkey,
        "Authorization": f"Bearer {get_valid_token()}",
    }
    resp = requests.post(ORDER_URL, headers=headers, json=body, timeout=7)
    try:
        resp_json = resp.json()
    except Exception:
        logger.error("[ORDER_RESP_PARSE_FAIL] status=%s body=%s", resp.status_code, resp.text)
        return None
    if resp.status_code >= 400 or resp_json.get("rt_cd") != "0":
        logger.error("[ORDER_FAIL] %s", resp.text)
        return None
    return resp_json

def inquire_filled_order(ord_no: str) -> dict:
    """체결 조회"""
    params = {"CANO": CANO, "ACNT_PRDT_CD": ACNT_PRDT_CD, "ORD_UNQ_NO": ord_no}
    headers = {
        "authorization": f"Bearer {get_valid_token()}",
        "content-type": "application/json; charset=utf-8",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
    }
    logger.debug(f"[KIS_FILL] URL={FILL_URL} Params={params}")
    resp = requests.get(FILL_URL, params=params, headers=headers, timeout=7)
    logger.debug(f"[KIS_FILL_RESP] {resp.status_code} {resp.text}")
    resp.raise_for_status()
    return resp.json()

# ─────────────── 호환성/유틸 (다른 모듈에서 기대하는 이름들) ───────────────

def get_cash_balance() -> int:
    """레거시/다른 모듈 호환 이름"""
    return inquire_cash_balance()

def get_price_data(code: str) -> Dict[str, Any]:
    """
    단순 호환 함수: 다른 모듈들이 import 하여 사용할 수 있게 제공.
    (실제 시세는 KIS의 시세 API를 사용해 추가 구현 가능)
    현재는 안전하게 실패하지 않도록 기본 틀만 제공.
    """
    try:
        # TODO: 실제 시세 API로 대체 가능
        # 예: 시세 조회 endpoint 호출 후 {'price': ..., 'time': ...} 반환
        return {"code": code, "price": None, "timestamp": None}
    except Exception as e:
        logger.warning("[GET_PRICE_FAIL] %s %s", code, e)
        return {"code": code, "price": None, "timestamp": None}

# 명시적 내보내기 (필요 시)
__all__ = [
    "refresh_token", "get_valid_token",
    "inquire_cash_balance", "get_cash_balance",
    "inquire_balance", "send_order",
    "inquire_filled_order", "get_price_data",
]

