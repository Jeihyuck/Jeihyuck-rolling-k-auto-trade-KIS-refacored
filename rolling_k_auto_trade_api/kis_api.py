# File: rolling_k_auto_trade_api/kis_api.py

from dotenv import load_dotenv
load_dotenv()

import os
import time
import json
import logging
import requests

import logging
import os

LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "kis_api.log")
os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# 콘솔 핸들러
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# 파일 핸들러
file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setLevel(logging.DEBUG)

# 포맷 정의
formatter = logging.Formatter("[%(asctime)s][%(levelname)s][%(name)s] %(message)s")
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# 핸들러 등록 (중복 방지)
if not logger.hasHandlers():
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

# ─────────────────────────────
# 환경 변수 및 기본 설정
# ─────────────────────────────
REST_DOMAIN  = os.getenv("KIS_REST_URL", "https://openapi.koreainvestment.com:9443")
APP_KEY      = os.getenv("KIS_APP_KEY")
APP_SECRET   = os.getenv("KIS_APP_SECRET")
ACCOUNT      = os.getenv("KIS_ACCOUNT")
ACCESS_TOKEN = os.getenv("KIS_ACCESS_TOKEN")
KIS_ENV      = os.getenv("KIS_ENV", "practice").lower()

# ─────────────────────────────
# KIS 주문 URL 설정 (모의 vs 실전)
# ─────────────────────────────
if KIS_ENV == "practice":
    DOMAIN = "https://openapivts.koreainvestment.com:29443"
else:
    DOMAIN = "https://openapi.koreainvestment.com:9443"

ORDER_PATH   = "/uapi/domestic-stock/v1/trading/order-cash"
ORDER_URL    = f"{DOMAIN}{ORDER_PATH}"
BALANCE_URL  = f"{DOMAIN}/uapi/domestic-stock/v1/trading/inquire-balance"
FILL_URL     = f"{DOMAIN}/uapi/domestic-stock/v1/trading/inquire-psbl-order"
TOKEN_URL    = f"{DOMAIN}/oauth2/tokenP"

_token_expires_at = 0
logger = logging.getLogger(__name__)

# ─────────────────────────────
# 토큰 재발급
# ─────────────────────────────
def refresh_token():
    global ACCESS_TOKEN, _token_expires_at

    url = TOKEN_URL
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Cache-Control": "no-cache",
    }
    payload = {
        "grant_type": "client_credentials",
        "appkey":     APP_KEY,
        "appsecret":  APP_SECRET,
    }
    logger.info("[TOKEN] 재발급 요청")

    resp = requests.post(url, headers=headers, json=payload, timeout=5)
    logger.debug("[TOKEN_RESP] %s %s", resp.status_code, resp.text)

    if resp.status_code >= 400:
        logger.error("[TOKEN_FAIL] status=%s body=%s", resp.status_code, resp.text)
        resp.raise_for_status()

    j = resp.json()
    ACCESS_TOKEN = j["access_token"]
    _token_expires_at = time.time() + int(j.get("expires_in", 3600)) - 30
    logger.info("[TOKEN] 재발급 성공, expires_in=%s", j.get("expires_in"))
    return ACCESS_TOKEN

def get_valid_token():
    if not ACCESS_TOKEN or time.time() > _token_expires_at:
        return refresh_token()
    return ACCESS_TOKEN

# ─────────────────────────────
# 해시키 생성
# ─────────────────────────────
def _create_hashkey(payload: dict) -> str:
    url = f"{REST_DOMAIN}/uapi/hashkey"
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "appkey":     APP_KEY,
        "appsecret":  APP_SECRET,
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=5)
    resp.raise_for_status()
    return resp.json()["HASH"]

# ─────────────────────────────
# 주문 요청
# ─────────────────────────────
def send_order(code: str, qty: int, side: str = "buy") -> dict:
    """현금 주문 (시장가)"""
    token = get_valid_token()

    # ✅ 환경에 따라 주문 구분코드와 TR_ID 결정
    KIS_ENV = os.getenv("KIS_ENV", "production").lower()
    if KIS_ENV == "practice":
        tr_id = "VTTC0807U" if side == "buy" else "VTTC0801U"
        custtype = "P"
        ord_dvsn_cd = "00"  # ✅ 모의투자 시장가 주문
        ord_unpr = "10000"
    else:
        tr_id = "TTTC0802U" if side == "buy" else "TTTC0801U"
        custtype = "E"
        ord_dvsn_cd = "01"  # 실전 시장가 주문
        ord_unpr = "0"

    body = {
        "CANO": ACCOUNT[:8],
        "ACNT_PRDT_CD": "01",
        "PDNO": code,
        "ORD_DVSN_CD": ord_dvsn_cd,
        "ORD_QTY": str(qty),
        "ORD_UNPR": ord_unpr
    }

    hashkey = _create_hashkey(body)

    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": tr_id,
        "custtype": custtype,
        "hashkey": hashkey,
        "Authorization": f"Bearer {token}",
    }

    logger.info(f"[KIS_ORDER_ENV] KIS_ENV={KIS_ENV}, TR_ID={tr_id}, ORD_DVSN_CD={ord_dvsn_cd}, UNPR={ord_unpr}")
    logger.info(f"[KIS_ORDER_ENV] KIS_ENV={KIS_ENV}, ORDER_URL={ORDER_URL}")
    logger.info(f"[ORDER_PARAM] TR_ID={tr_id}, ORD_DVSN_CD={ord_dvsn_cd}, ORD_UNPR={ord_unpr}")
    logger.debug("[KIS_ORDER] Headers=%s", headers)
    logger.debug("[KIS_ORDER] Body=%s", json.dumps(body, ensure_ascii=False))

    try:
        resp = requests.post(ORDER_URL, headers=headers, json=body, timeout=5)
        resp.raise_for_status()
    except requests.exceptions.ConnectionError as ce:
        logger.warning(f"[RETRY_ORDER] ConnectionError: {ce}, retrying in 2 sec...")
        time.sleep(2)
    # 한 번 재시도
        resp = requests.post(ORDER_URL, headers=headers, json=body, timeout=5)

    logger.debug("[KIS_ORDER_RESP] %s %s", resp.status_code, resp.text)

    if resp.status_code >= 400:
        raise requests.HTTPError(
            f"{resp.status_code} {resp.reason} | Body: {resp.text}",
            response=resp
        )

    return resp.json()



# ─────────────────────────────
# 잔고 조회
# ─────────────────────────────
def inquire_balance(code: str) -> dict:
    token = get_valid_token()
    params = {"CANO": ACCOUNT[:8], "ACNT_PRDT_CD": "01", "PDNO": code}
    headers = {"Authorization": f"Bearer {token}"}
    logger.debug(f"[KIS_BALANCE] URL={BALANCE_URL}, Params={params}")
    resp = requests.get(BALANCE_URL, params=params, headers=headers, timeout=5)
    logger.debug(f"[KIS_BALANCE_RESP] Status={resp.status_code}, Body={resp.text}")
    resp.raise_for_status()
    return resp.json()

# ─────────────────────────────
# 체결 조회
# ─────────────────────────────
def inquire_filled_order(ord_no: str) -> dict:
    token = get_valid_token()
    params = {"CANO": ACCOUNT[:8], "ACNT_PRDT_CD": "01", "ORD_UNQ_NO": ord_no}
    headers = {"Authorization": f"Bearer {token}"}
    logger.debug(f"[KIS_FILL] URL={FILL_URL}, Params={params}")
    resp = requests.get(FILL_URL, params=params, headers=headers, timeout=5)
    logger.debug(f"[KIS_FILL_RESP] Status={resp.status_code}, Body={resp.text}")
    resp.raise_for_status()
    return resp.json()


