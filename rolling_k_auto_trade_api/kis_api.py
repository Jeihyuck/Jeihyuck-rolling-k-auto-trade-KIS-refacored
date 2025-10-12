from __future__ import annotations
"""
rolling_k_auto_trade_api.kis_api — 최신 응답로깅 + 파라미터 최신화 (전체 파일)

핵심 개선
- send_order: KIS 응답 원문(raw text) 및 JSON 모두 상세 로깅(민감정보 마스킹) + 실패 사유/코드 함께 기록
- HashKey 및 주문 파라미터 최신화: /uapi/hashkey, /uapi/domestic-stock/v1/trading/order-cash
- TR_ID 자동 전환: (모의) VTTC0012U/VTTC0011U, (실전) TTTC0012U/TTTC0011U
- 주문 방식 체인: 시장가→IOC시장가→최유리(매수/매도 공통)로 폴백
- 견고한 재시도: 게이트웨이/5xx/네트워크 오류에 백오프 재시도
- inquire_balance(단일/전체), inquire_cash_balance, inquire_filled_order(응답 로깅 포함)

주의: settings 모듈이 있으면 우선 사용하고, 없으면 환경변수에서 읽습니다.
"""

import os
import json
import time
import random
import logging
from typing import Any, Dict, Optional, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# =============================
# 설정 로딩 (settings 우선, 없으면 ENV)
# =============================
try:  # settings.py가 있으면 해당 값을 우선 사용
    from settings import APP_KEY as _APP_KEY
    from settings import APP_SECRET as _APP_SECRET
    from settings import API_BASE_URL as _API_BASE_URL
    from settings import CANO as _CANO
    from settings import ACNT_PRDT_CD as _ACNT_PRDT_CD
    from settings import KIS_ENV as _KIS_ENV
    APP_KEY = _APP_KEY
    APP_SECRET = _APP_SECRET
    API_BASE_URL = _API_BASE_URL
    CANO = _CANO
    ACNT_PRDT_CD = _ACNT_PRDT_CD
    KIS_ENV = _KIS_ENV
except Exception:
    APP_KEY = os.getenv("APP_KEY") or os.getenv("KIS_APP_KEY", "")
    APP_SECRET = os.getenv("APP_SECRET") or os.getenv("KIS_APP_SECRET", "")
    API_BASE_URL = os.getenv("API_BASE_URL", "https://openapi.koreainvestment.com:9443")
    CANO = os.getenv("CANO", "")
    ACNT_PRDT_CD = os.getenv("ACNT_PRDT_CD", "01")
    KIS_ENV = (os.getenv("KIS_ENV", "practice") or "practice").lower()

# 필수값 체크 로그
logger.info(f"[KIS] ENV={KIS_ENV} API_BASE_URL={API_BASE_URL} CANO={'***' if CANO else ''} ACNT={'***' if ACNT_PRDT_CD else ''}")

# =============================
# 세션/재시도
# =============================
session = requests.Session()
retry = Retry(
    total=3,
    connect=3,
    read=3,
    status=3,
    backoff_factor=0.5,
    status_forcelist=(500, 502, 503, 504),
    allowed_methods=frozenset(["GET", "POST"]),
    raise_on_status=False,
)
adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
session.mount("https://", adapter)
session.mount("http://", adapter)

# =============================
# 토큰 캐시
# =============================
_TOKEN_CACHE = {"token": None, "expires_at": 0.0, "last_issued": 0.0}
_TOKEN_FILE = os.getenv("KIS_TOKEN_CACHE", "kis_token_cache.json")


def _issue_token() -> Dict[str, Any]:
    path = "/oauth2/tokenP" if KIS_ENV == "practice" else "/oauth2/token"
    url = f"{API_BASE_URL}{path}"
    hdr = {"content-type": "application/json"}
    data = {"grant_type": "client_credentials", "appkey": APP_KEY, "appsecret": APP_SECRET}
    r = session.post(url, json=data, headers=hdr, timeout=(3.0, 7.0))
    try:
        j = r.json()
    except Exception:
        logger.error(f"[🔑 TOKEN_RESP_RAW] status={r.status_code} text={r.text[:400]}")
        raise
    if "access_token" in j:
        return j
    raise RuntimeError(f"TOKEN_FAIL: {j}")


def _get_token() -> str:
    now = time.time()
    if _TOKEN_CACHE["token"] and now < _TOKEN_CACHE["expires_at"] - 300:
        return _TOKEN_CACHE["token"]
    # 파일 캐시
    if os.path.exists(_TOKEN_FILE):
        try:
            with open(_TOKEN_FILE, "r", encoding="utf-8") as f:
                c = json.load(f)
            if c.get("access_token") and now < float(c.get("expires_at", 0)) - 300:
                _TOKEN_CACHE.update({"token": c["access_token"], "expires_at": float(c["expires_at"]), "last_issued": float(c.get("last_issued", 0))})
                logger.info("[TOKEN] file cache reuse")
                return c["access_token"]
        except Exception as e:
            logger.warning(f"[TOKEN_CACHE_READ_FAIL] {e}")
    # 발급 빈도 제한(1분)
    if now - _TOKEN_CACHE["last_issued"] < 61 and _TOKEN_CACHE["token"]:
        logger.warning("[TOKEN] throttle: reuse current token")
        return _TOKEN_CACHE["token"]
    j = _issue_token()
    token = j["access_token"]
    exp_in = int(j.get("expires_in", 86400))
    _TOKEN_CACHE.update({"token": token, "expires_at": now + exp_in, "last_issued": now})
    try:
        with open(_TOKEN_FILE, "w", encoding="utf-8") as f:
            json.dump({"access_token": token, "expires_at": now + exp_in, "last_issued": now}, f, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"[TOKEN_CACHE_WRITE_FAIL] {e}")
    return token


# =============================
# 공통 헤더/HashKey
# =============================

def _headers(tr_id: str, *, hashkey: Optional[str] = None) -> Dict[str, str]:
    h = {
        "authorization": f"Bearer {_get_token()}",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": tr_id,
        "custtype": "P",
        "content-type": "application/json; charset=utf-8",
    }
    if hashkey:
        h["hashkey"] = hashkey
    return h


def _json_dumps(body: Dict[str, Any]) -> str:
    return json.dumps(body, ensure_ascii=False, separators=(",", ":"), sort_keys=False)


def _create_hashkey(body: Dict[str, Any]) -> str:
    url = f"{API_BASE_URL}/uapi/hashkey"
    hdr = {"content-type": "application/json; charset=utf-8", "appkey": APP_KEY, "appsecret": APP_SECRET}
    body_str = _json_dumps(body)
    r = session.post(url, headers=hdr, data=body_str.encode("utf-8"), timeout=(3.0, 5.0))
    try:
        j = r.json()
    except Exception:
        logger.error(f"[HASHKEY_RAW] status={r.status_code} text={r.text[:400]}")
        raise
    hk = j.get("HASH") or j.get("hash") or j.get("hashkey")
    if not hk:
        logger.error(f"[HASHKEY_FAIL] resp={j}")
        raise RuntimeError(f"hashkey fail: {j}")
    return hk


# =============================
# 주문 (현금)
# =============================

def _order_cash(body: Dict[str, Any], *, is_sell: bool) -> Dict[str, Any]:
    url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
    tr_id = ("VTTC0011U" if KIS_ENV == "practice" else "TTTC0011U") if is_sell else ("VTTC0012U" if KIS_ENV == "practice" else "TTTC0012U")

    # 시장가→IOC시장가→최유리(03)
    ord_chain = ["01", "13", "03"]
    last_err: Any = None

    for ord_dvsn in ord_chain:
        body["ORD_DVSN"] = ord_dvsn
        body["ORD_UNPR"] = body.get("ORD_UNPR", "0") if ord_dvsn == "00" else "0"
        if is_sell and not body.get("SLL_TYPE"):
            body["SLL_TYPE"] = "01"  # 일반매도
        body.setdefault("EXCG_ID_DVSN_CD", "KRX")

        hk = _create_hashkey(body)
        hdr = _headers(tr_id, hashkey=hk)

        # 마스킹 로그
        log_body = {k: ("***" if k in ("CANO", "ACNT_PRDT_CD") else v) for k, v in body.items()}
        logger.info(f"[ORDER_REQ] tr_id={tr_id} ord_dvsn={ord_dvsn} body={log_body}")

        for attempt in range(1, 4):
            try:
                r = session.post(url, headers=hdr, data=_json_dumps(body).encode("utf-8"), timeout=(3.0, 7.0))
                raw = r.text
                try:
                    j = r.json()
                except Exception:
                    j = {"_non_json": True}
                # 상세 로깅
                logger.info(f"[ORDER_RESP] status={r.status_code} json={j} raw_head={raw[:300]}")
            except Exception as e:
                back = min(0.6 * (1.7 ** (attempt - 1)), 5.0) + random.uniform(0, 0.3)
                logger.error(f"[ORDER_NET_EX] ord_dvsn={ord_dvsn} attempt={attempt} ex={e} → sleep {back:.2f}s")
                time.sleep(back)
                last_err = e
                continue

            # 정상 처리
            if r.status_code == 200 and isinstance(j, dict) and j.get("rt_cd") == "0":
                logger.info(f"[ORDER_OK] ord_dvsn={ord_dvsn} output={j.get('output')}")
                return j

            # 게이트웨이/과다/5xx 재시도
            msg_cd = (j or {}).get("msg_cd", "") if isinstance(j, dict) else ""
            msg1 = (j or {}).get("msg1", "") if isinstance(j, dict) else ""
            if r.status_code >= 500 or msg_cd == "IGW00008" or (isinstance(msg1, str) and ("MCA" in msg1 or "초당" in msg1)):
                back = min(0.6 * (1.7 ** (attempt - 1)), 5.0) + random.uniform(0, 0.3)
                logger.warning(f"[ORDER_GATEWAY_RETRY] ord_dvsn={ord_dvsn} attempt={attempt} resp={j} → sleep {back:.2f}s")
                time.sleep(back)
                last_err = j
                continue

            # 비즈니스 실패는 그대로 반환(상위에서 판단)
            logger.error(f"[ORDER_FAIL_BIZ] ord_dvsn={ord_dvsn} resp={j} raw_head={raw[:300]}")
            return j if isinstance(j, dict) else {"_status": r.status_code, "raw": raw[:500]}

        logger.warning(f"[ORDER_FALLBACK] ord_dvsn={ord_dvsn} 실패 → 다음 방식")

    raise RuntimeError(f"ORDER_FAIL: {last_err}")


def send_order(code: str, qty: int, price: Optional[int] = None, side: str = "buy") -> Dict[str, Any]:
    """공용 주문 API
    side: 'buy' or 'sell'
    price: None이면 시장가 체인, 지정가면 지정가 고정(00)
    반환: KIS 응답(dict). 비정상 응답 시에도 원문/상태 일부 포함
    """
    code = str(code).strip()
    is_sell = (side.lower() == "sell")

    if price is None:
        # 시장가 체인
        body = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "PDNO": code,
            "ORD_QTY": str(int(qty)),
        }
        return _order_cash(body, is_sell=is_sell)
    else:
        # 지정가(고정, 00)
        body = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "PDNO": code,
            "ORD_QTY": str(int(qty)),
            "ORD_DVSN": "00",
            "ORD_UNPR": str(int(price)),
            "EXCG_ID_DVSN_CD": "KRX",
        }
        if is_sell:
            body["SLL_TYPE"] = "01"
        hk = _create_hashkey(body)
        tr_id = ("VTTC0011U" if KIS_ENV == "practice" else "TTTC0011U") if is_sell else ("VTTC0012U" if KIS_ENV == "practice" else "TTTC0012U")
        url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
        hdr = _headers(tr_id, hashkey=hk)

        log_body = {k: ("***" if k in ("CANO", "ACNT_PRDT_CD") else v) for k, v in body.items()}
        logger.info(f"[ORDER_REQ_LIMIT] tr_id={tr_id} body={log_body}")
        r = session.post(url, headers=hdr, data=_json_dumps(body).encode("utf-8"), timeout=(3.0, 7.0))
        raw = r.text
        try:
            j = r.json()
        except Exception:
            j = {"_non_json": True}
        logger.info(f"[ORDER_RESP_LIMIT] status={r.status_code} json={j} raw_head={raw[:300]}")
        return j if isinstance(j, dict) else {"_status": r.status_code, "raw": raw[:500]}


# =============================
# 잔고/예수금/체결 조회
# =============================

def inquire_cash_balance() -> int:
    url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-balance"
    tr_id = "VTTC8434R" if KIS_ENV == "practice" else "TTTC8434R"
    hdr = _headers(tr_id)
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
        "CTX_AREA_NK100": "",
    }
    logger.info(f"[INQ_BAL_REQ] params={{...masked...}}")
    r = session.get(url, headers=hdr, params=params, timeout=(3.0, 7.0))
    raw = r.text
    try:
        j = r.json()
    except Exception:
        logger.error(f"[INQ_BAL_RAW] status={r.status_code} raw={raw[:300]}")
        return 0
    logger.info(f"[INQ_BAL_RESP] {j}")
    try:
        if j.get("rt_cd") == "0" and j.get("output2"):
            return int(j["output2"][0]["dnca_tot_amt"])  # 예수금
    except Exception as e:
        logger.error(f"[INQ_BAL_PARSE_FAIL] {e}")
    return 0


def inquire_balance(code: Optional[str] = None) -> List[Dict[str, Any]]:
    url = f"{API_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-balance"
    tr_id = "VTTC8434R" if KIS_ENV == "practice" else "TTTC8434R"
    hdr = _headers(tr_id)
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
        "CTX_AREA_NK100": "",
    }
    r = session.get(url, headers=hdr, params=params, timeout=(3.0, 7.0))
    raw = r.text
    try:
        j = r.json()
    except Exception:
        logger.error(f"[INQ_POS_RAW] status={r.status_code} raw={raw[:300]}")
        return []
    out = j.get("output1") or []
    if code:
        out = [o for o in out if str(o.get("pdno")) == str(code)]
    logger.info(f"[INQ_POS_RESP] count={len(out)} code={code}")
    return out


def inquire_filled_order(ord_no: str) -> Dict[str, Any]:
    """체결/주문 조회 (간편형)
    주의: KIS의 체결 조회 API는 계좌/일자/주문번호 등 다양한 TR이 있으므로
    실제 배포 환경에 맞추어 상세 TR을 교체해야 합니다. 여기서는 요청/응답 로깅에 중점.
    """
    # 데모용: 주문번호만 로깅/에코
    logger.info(f"[INQ_FILL] ord_no={ord_no}")
    return {"ord_no": ord_no, "status": "dummy", "note": "Fill inquiry TR 연결 필요"}
