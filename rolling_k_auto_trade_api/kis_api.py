# kis_api.py (patched for compatibility and robustness)
from dotenv import load_dotenv
load_dotenv()

import os
import time
import logging
from typing import Optional, Dict, Any
import requests

# ─────────────── 환경 변수 및 계좌 설정 ───────────────
REST_DOMAIN  = os.getenv("KIS_REST_URL", "https://openapi.koreainvestment.com:9443")
APP_KEY      = os.getenv("KIS_APP_KEY")
APP_SECRET   = os.getenv("KIS_APP_SECRET")
ACCESS_TOKEN = os.getenv("KIS_ACCESS_TOKEN")  # 최초 .env, 이후 자동 갱신

CANO         = os.getenv("CANO")              # 8자리 계좌번호
ACNT_PRDT_CD = os.getenv("ACNT_PRDT_CD", "01")

KIS_ENV = os.getenv("KIS_ENV", "practice").lower()

# environment-specific endpoints / tr ids
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
    CUSTTYPE      = "E"  # 법인(혹은 E)

ORDER_URL   = f"{DOMAIN}{ORDER_PATH}"
BALANCE_URL = f"{DOMAIN}/uapi/domestic-stock/v1/trading/inquire-balance"
FILL_URL    = f"{DOMAIN}/uapi/domestic-stock/v1/trading/inquire-psbl-order"
# NOTE: token endpoint: in some code we used REST_DOMAIN, in others DOMAIN.
# refresh_token() below uses REST_DOMAIN (original behavior kept).
TOKEN_URL   = f"{REST_DOMAIN}/oauth2/tokenP"

# ─────────────── 로깅 설정 ───────────────
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
_fmt = logging.Formatter('[%(asctime)s][%(levelname)s][%(name)s] %(message)s')
fh = logging.FileHandler('kis_api.log', encoding='utf-8')
sh = logging.StreamHandler()
fh.setFormatter(_fmt)
sh.setFormatter(_fmt)
# prevent adding duplicate handlers if module reloaded
if not logger.handlers:
    logger.addHandler(fh)
    logger.addHandler(sh)

# ─────────────── 토큰 관리 ───────────────
_token_expires_at = 0  # UNIX epoch seconds

def refresh_token() -> str:
    """KIS OAuth2 토큰 재발급 (환경변수 APP_KEY/APP_SECRET 필요)"""
    global ACCESS_TOKEN, _token_expires_at
    if not APP_KEY or not APP_SECRET:
        logger.error("[TOKEN] APP_KEY / APP_SECRET 미설정. 토큰 재발급 불가.")
        raise RuntimeError("APP_KEY or APP_SECRET not set in environment")
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
    """캐시된 토큰이 만료되었으면 재발급"""
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

# try importing optional kis_wrapper if project has been refactored to use it
try:
    # if your code expects KisAPI class from kis_wrapper, this will set it;
    # if kis_wrapper does not exist (older codebase), we keep KisAPI=None and continue.
    from .kis_wrapper import KisAPI  # type: ignore
except Exception:
    KisAPI = None
    logger.debug("Optional module rolling_k_auto_trade_api.kis_wrapper not found — continuing without it")

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
        "INQR_DVSN": "02",     # 전체잔고
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": ""
    }
    headers = _balance_headers()
    logger.debug(f"[KIS_BALANCE_CASH] URL={BALANCE_URL} Params={params} tr_id={headers.get('tr_id')}")
    try:
        resp = requests.get(BALANCE_URL, params=params, headers=headers, timeout=7)
        logger.debug(f"[KIS_BALANCE_CASH_RESP] {resp.status_code} {resp.text}")
        if resp.status_code != 200:
            logger.error("[CASH_BALANCE_FAIL] status=%s body=%s", resp.status_code, resp.text)
            return 0
        j = resp.json()
        # defensive parsing: output2 may be missing
        out2 = j.get("output2") or j.get("output") or []
        if out2 and isinstance(out2, list):
            cash_val = out2[0].get("prvs_rcdl_excc_amt") or out2[0].get("prvs_rcdl_excc_amt2") or 0
            try:
                cash = int(cash_val)
            except Exception:
                cash = 0
        else:
            cash = 0
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
    logger.debug(f"[KIS_BALANCE] URL={BALANCE_URL} Params={params} tr_id={headers.get('tr_id')}")
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

def send_order(code: str, qty: int, price: Optional[int] = None, side: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    주문 전송.
    - 기존 코드에서 여러 방식으로 호출되어 왔으므로 price/side를 optional로 둠.
    - side 가 'buy' 혹은 'sell' 로 들어오면 BUY_TR_ID / SELL_TR_ID 사용.
    - 가격이 None 이면 '0'을 넣어 지정가(혹은 시장가 규약에 맞게 호출하는 상위 코드로 변경 권장).
    """
    # normalize side
    if side:
        s = side.lower()
    else:
        s = "buy"  # 기본값 (안전상)
    ord_unpr = str(price) if price is not None else "0"

    body = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "PDNO": code,
        "ORD_DVSN": "00",  # 지정가(필요시 상위 호출부에서 변경)
        "ORD_QTY": str(qty),
        "ORD_UNPR": ord_unpr,
    }

    try:
        hashkey = _create_hashkey(body)
    except Exception as e:
        logger.error("[ORDER_HASH_FAIL] %s", e)
        return None

    tr_id = BUY_TR_ID if s == "buy" else SELL_TR_ID
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": tr_id,
        "custtype": CUSTTYPE,
        "hashkey": hashkey,
        "Authorization": f"Bearer {get_valid_token()}",
    }

    try:
        resp = requests.post(ORDER_URL, headers=headers, json=body, timeout=7)
    except requests.RequestException as e:
        logger.exception("[ORDER_NET_EX] tr_id=%s ex=%s", tr_id, e)
        return None

    try:
        resp_json = resp.json()
    except Exception:
        logger.error("[ORDER_RESP_NOT_JSON] status=%s body=%s", resp.status_code, resp.text)
        return None

    if resp.status_code >= 400 or resp_json.get("rt_cd") not in (None, "0", 0):
        # some API variants return rt_cd="0" on success, some different shapes exist
        logger.error("[ORDER_FAIL] status=%s resp=%s", resp.status_code, resp.text)
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

# ─────────────── Compatibility wrappers (for older code) ───────────────
def get_cash_balance() -> int:
    """기존 코드 호환용: get_cash_balance() 호출을 inquire_cash_balance 로 연결"""
    return inquire_cash_balance()

def get_price_data(code: str) -> Dict[str, Any]:
    """
    기존 코드(예: realtime_executor)에서 data['output'][0]['stck_prpr'] 형태로 접근하므로,
    가능한 한 그 형태를 반환하도록 시도합니다.
    - 시세조회 endpoint를 호출하고, 응답에 'output'이 있으면 그대로 반환.
    - 아니면 최소한 {'output': [{'stck_prpr': <가격값> }]} 형태를 반환하도록 방어.
    """
    # endpoint (KIS 예시)
    url = f"{DOMAIN}/uapi/domestic-stock/v1/quotations/inquire-price"
    params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}
    headers = {
        "authorization": f"Bearer {get_valid_token()}",
        "content-type": "application/json; charset=utf-8",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
    }
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=7)
        logger.debug("[PRICE_RESP] %s %s", resp.status_code, resp.text)
        resp.raise_for_status()
        j = resp.json()
        # If API already returns 'output' with the expected field, return it.
        if isinstance(j, dict) and "output" in j and isinstance(j["output"], list):
            return j
        # Try to extract a price field from likely shapes
        # common KIS field names: 'stck_prpr', 'prpr', 'last_price', etc.
        price = None
        # scan through dict values for plausible price
        for candidate in ("stck_prpr", "prpr", "last_price", "jnprc", "price"):
            # check nested outputs
            if "output1" in j and isinstance(j["output1"], list) and j["output1"]:
                if candidate in j["output1"][0]:
                    price = j["output1"][0].get(candidate)
                    break
            if candidate in j:
                price = j.get(candidate)
                break
            # sometimes API returns single dict with fields
            for val in j.values():
                if isinstance(val, dict) and candidate in val:
                    price = val[candidate]
                    break
            if price is not None:
                break
        if price is None:
            # fallback: try parse numeric in text
            price = 0
        return {"output": [{"stck_prpr": str(price)}]}
    except Exception as e:
        logger.warning("[PRICE_FAIL] code=%s ex=%s", code, e)
        # safe fallback shape so callers don't KeyError
        return {"output": [{"stck_prpr": "0"}]}

# export list for clarity
__all__ = [
    "refresh_token", "get_valid_token",
    "inquire_cash_balance", "inquire_balance", "send_order",
    "inquire_filled_order", "get_cash_balance", "get_price_data",
]

