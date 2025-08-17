# kis_api.py
from dotenv import load_dotenv
load_dotenv()

import os
import time
import logging
import requests

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
    CUSTTYPE      = "P"
else:
    DOMAIN        = "https://openapi.koreainvestment.com:9443"
    ORDER_PATH    = "/uapi/domestic-stock/v1/trading/order-cash"
    BALANCE_TR_ID = "TTTC8434R"
    BUY_TR_ID     = "TTTC0012U"
    SELL_TR_ID    = "TTTC0011U"
    CUSTTYPE      = "E"

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
_token_expires_at = 0

def refresh_token() -> str:
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

def send_order(code: str, qty: int, price: int, side: str) -> dict | None:
    body = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "PDNO": code,
        "ORD_DVSN": "00",  
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
    resp_json = resp.json()
    if resp.status_code >= 400 or resp_json.get("rt_cd") != "0":
        logger.error("[ORDER_FAIL] %s", resp.text)
        return None
    return resp_json

def inquire_filled_order(ord_no: str) -> dict:
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

# ─────────────── FastAPI 연동을 위한 하위호환 래퍼 추가됨 ───────────────

from .kis_wrapper import KisAPI
_kis = KisAPI()

def get_price_data(code: str):
    """시세 조회: FastAPI와 realtime_executor 호출 호환용"""
    try:
        price = _kis.get_current_price(code)
        return {"code": code, "price": float(price) if price is not None else None}
    except Exception as e:
        logger.error(f"[get_price_data ERROR] code={code} {e}")
        return {"code": code, "price": None}

def send_order_wrapper(side: str, code: str, qty: int, price: float | None = None):
    """주문 래퍼: FastAPI/realtime_executor 용"""
    try:
        if side.upper() == "BUY":
            return _kis.buy_stock(code, qty)
        else:
            return _kis.sell_stock_market(code, qty)
    except Exception as e:
        logger.error(f"[send_order ERROR] side={side}, code={code}, qty={qty}, price={price} {e}")
        return None

def get_cash_balance_wrapper():
    """예수금 조회 래퍼: FastAPI/realtime_executor 용"""
    try:
        cash = inquire_cash_balance()
        return {"cash": cash}
    except Exception as e:
        logger.error(f"[get_cash_balance ERROR] {e}")
        return {"cash": 0}

