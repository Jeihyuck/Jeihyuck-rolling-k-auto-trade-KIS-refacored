from dotenv import load_dotenv
load_dotenv()

import os
import time
import json
import logging
import requests

REST_DOMAIN = os.getenv("KIS_REST_URL", "https://openapi.koreainvestment.com:9443")
APP_KEY     = os.getenv("KIS_APP_KEY")
APP_SECRET  = os.getenv("KIS_APP_SECRET")
ACCOUNT     = os.getenv("KIS_ACCOUNT")        # 계좌번호 (8자리 이상)
ACCESS_TOKEN= os.getenv("KIS_ACCESS_TOKEN")   # 초기 액세스 토큰

KIS_ENV = os.getenv("KIS_ENV", "practice").lower()
if KIS_ENV == "practice":
    DOMAIN = os.getenv("KIS_REST_URL", "https://openapivts.koreainvestment.com:29443")
    ORDER_PATH = "/uapi/domestic-stock/v1/trading/order-cash"
else:
    DOMAIN = os.getenv("KIS_REST_URL", "https://openapi.koreainvestment.com:9443")
    ORDER_PATH = "/uapi/domestic-stock/v1/trading/order-cash"

ORDER_URL   = os.getenv("KIS_ORDER_URL",   f"{DOMAIN}{ORDER_PATH}")
BALANCE_URL = os.getenv("KIS_BALANCE_URL", f"{DOMAIN}/uapi/domestic-stock/v1/trading/inquire-balance")
FILL_URL    = os.getenv("KIS_FILL_URL",    f"{DOMAIN}/uapi/domestic-stock/v1/trading/inquire-psbl-order")
TOKEN_URL   = os.getenv("KIS_TOKEN_URL",   f"{DOMAIN}/oauth2/tokenP")

# 로거 설정 (콘솔+파일)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s][%(levelname)s][%(name)s] %(message)s')
file_handler = logging.FileHandler('kis_api.log', encoding='utf-8')
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

_token_expires_at = 0

def refresh_token():
    """만료된 토큰을 재발급하고 ACCESS_TOKEN / 만료시각 갱신"""
    global ACCESS_TOKEN, _token_expires_at
    url = f"{REST_DOMAIN}/oauth2/tokenP"
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
    resp = requests.post(url, headers=headers, json=payload, timeout=7)
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

def _create_hashkey(payload: dict) -> str:
    url = f"{REST_DOMAIN}/uapi/hashkey"
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "appkey":  APP_KEY,
        "appsecret": APP_SECRET,
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=7)
    logger.debug("[HASHKEY_RESP] %s %s", resp.status_code, resp.text)
    resp.raise_for_status()
    return resp.json()["HASH"]

def send_order(code: str, qty: int, price: int) -> dict:
    """
    KIS 엑셀 명세 100% 일치 지정가 매수 주문 함수
    :param code: 종목코드 (ex. '005930')
    :param qty: 수량
    :param price: 지정가
    """
    cano = str(ACCOUNT)[:8]
    acnt_prdt_cd = "01"

    # TR_ID 및 기타 헤더 설정
    if KIS_ENV == "practice":
        tr_id = "VTTC0011U"
        custtype = "P"
    else:
        tr_id = "TTTC0011U"
        custtype = "E"

    # 엑셀 명세에 맞는 필드명/값/타입/순서
    body = {
        "CANO": cano,                      # 종합계좌번호
        "ACNT_PRDT_CD": acnt_prdt_cd,      # 계좌상품코드
        "PDNO": code,                      # 종목코드
        "ORD_DVSN": "00",                 # 지정가 (엑셀 명세)
        "ORD_QTY": str(qty),               # 주문수량
        "ORD_UNPR": str(price),             # 주문단가
    }

    hashkey = _create_hashkey(body)
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": tr_id,
        "custtype": custtype,
        "hashkey": hashkey,
        "Authorization": f"Bearer {get_valid_token()}",
    }

    logger.info(f"[KIS_ORDER_BODY] {body}")
    resp = requests.post(ORDER_URL, headers=headers, json=body, timeout=7)
    logger.debug("[KIS_ORDER_RESP] %s %s", resp.status_code, resp.text)
    if resp.status_code >= 400:
        logger.error("[KIS_ORDER_FAIL] code=%s, error=%s | Body=%s", code, resp.status_code, resp.text)
        resp.raise_for_status()
    return resp.json()

def inquire_balance(code: str) -> dict:
    token = get_valid_token()
    if not BALANCE_URL:
        raise ValueError("KIS_BALANCE_URL 환경변수가 설정되지 않았습니다.")

    params = {"CANO": ACCOUNT[:8], "ACNT_PRDT_CD": "01", "PDNO": code}
    headers = {"Authorization": f"Bearer {token}"}
    logger.debug(f"[KIS_BALANCE] URL={BALANCE_URL}, Params={params}")
    resp = requests.get(BALANCE_URL, params=params, headers=headers, timeout=7)
    logger.debug(f"[KIS_BALANCE_RESP] Status={resp.status_code}, Body={resp.text}")
    resp.raise_for_status()
    return resp.json()

def inquire_filled_order(ord_no: str) -> dict:
    token = get_valid_token()
    if not FILL_URL:
        raise ValueError("KIS_FILL_URL 환경변수가 설정되지 않았습니다.")

    params = {"CANO": ACCOUNT[:8], "ACNT_PRDT_CD": "01", "ORD_UNQ_NO": ord_no}
    headers = {"Authorization": f"Bearer {token}"}
    logger.debug(f"[KIS_FILL] URL={FILL_URL}, Params={params}")
    resp = requests.get(FILL_URL, params=params, headers=headers, timeout=7)
    logger.debug(f"[KIS_FILL_RESP] Status={resp.status_code}, Body={resp.text}")
    resp.raise_for_status()
    return resp.json()

