# rolling_k_auto_trade_api/kis_api.py
"""
Compatibility wrapper module exporting functions used elsewhere in the project:
- refresh_token
- get_valid_token
- _create_hashkey
- _balance_headers
- inquire_cash_balance
- inquire_balance
- send_order
- inquire_filled_order

Internally uses KisAPI from kis_wrapper.py (singleton).
"""

import os
import logging
from typing import Optional, Dict, Any

from .kis_wrapper import KisAPI

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    fmt = logging.Formatter('[%(asctime)s][%(levelname)s][%(name)s] %(message)s')
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

# create a module-level KisAPI instance (singleton)
_kis = KisAPI(
    app_key=os.getenv("KIS_APP_KEY"),
    app_secret=os.getenv("KIS_APP_SECRET"),
    cano=os.getenv("CANO"),
    acnt_prdt_cd=os.getenv("ACNT_PRDT_CD", "01"),
    api_base_url=os.getenv("API_BASE_URL") or os.getenv("KIS_REST_URL"),
    env=os.getenv("KIS_ENV", "practice"),
)


def refresh_token() -> str:
    """강제 토큰 재발급"""
    logger.info("[TOKEN] 재발급 요청")
    return _kis.refresh_token()


def get_valid_token() -> str:
    return _kis.get_valid_token()


def _create_hashkey(payload: dict) -> Optional[str]:
    return _kis.create_hashkey(payload)


def _balance_headers() -> Dict[str, str]:
    return _kis.balance_headers()


def inquire_cash_balance() -> int:
    """
    예수금(출금가능금액) 조회: 잔고조회 API에서 output2에서 추출
    실패시 0원 반환
    """
    try:
        cash = _kis.inquire_cash_balance()
        logger.info(f"[CASH_BALANCE] 현재 예수금: {cash:,}원")
        return cash
    except Exception as e:
        logger.error("[CASH_BALANCE_PARSE_FAIL] %s", e)
        return 0


def inquire_balance(code: str = None) -> Dict[str, Any]:
    try:
        return _kis.inquire_balance(code)
    except Exception as e:
        logger.warning("[BALANCE_FAIL] %s | %s", code, e)
        return {"qty": 0, "eval_amt": 0}


def send_order(code: str, qty: int, price: int, side: str) -> Optional[Dict[str, Any]]:
    """
    code, qty, price, side ('buy'|'sell')
    Returns response dict on success, None on failure.
    """
    try:
        return _kis.send_order(code=code, qty=qty, price=price, side=side)
    except Exception as e:
        logger.error("[SEND_ORDER_FAIL] %s", e)
        return None


def inquire_filled_order(ord_no: str) -> Dict[str, Any]:
    try:
        return _kis.inquire_filled_order(ord_no)
    except Exception as e:
        logger.error("[INQUIRE_FILLED_FAIL] %s", e)
        return {}

