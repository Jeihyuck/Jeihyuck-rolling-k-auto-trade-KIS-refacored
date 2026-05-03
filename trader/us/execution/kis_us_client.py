# -*- coding: utf-8 -*-
"""KIS 해외주식 모의투자 API Client.

국내 kis_wrapper와 분리된 해외주식 전용 클라이언트.
endpoint/tr_id는 kis_us_registry에서만 관리.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any

from trader.us import config as us_cfg
from trader.us.execution.kis_us_registry import (
    KIS_VTS_BASE_URL,
    TOKEN_PATH,
    get_tr_info,
    get_order_exchange_code_for_api,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Token cache (in-process)
# ---------------------------------------------------------------------------
_TOKEN_CACHE: dict[str, Any] = {
    "access_token": None,
    "expires_at": None,
}


class KisUSClientError(Exception):
    """KIS US API 오류."""


class KisUSClient:
    """KIS 해외주식 모의투자 API 전용 클라이언트.

    Args:
        env: "practice" 만 허용 (실거래 차단)
        offline: True이면 모든 HTTP 호출 금지 (harness/test 용)
    """

    def __init__(self, env: str = "practice", offline: bool = False) -> None:
        if env.lower() != "practice":
            raise RuntimeError(
                f"[US_CLIENT][BLOCKED] reason=env_not_practice env={env!r}"
            )
        self._env = env
        self._offline = offline
        self._base_url = KIS_VTS_BASE_URL
        self._app_key = us_cfg.KIS_APP_KEY
        self._app_secret = us_cfg.KIS_APP_SECRET
        self._cano = us_cfg.CANO
        self._acnt_prdt_cd = us_cfg.ACNT_PRDT_CD

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def get_access_token(self) -> str:
        """액세스 토큰 반환 (캐시 / 갱신)."""
        self._assert_not_offline("get_access_token")
        now = datetime.utcnow()
        if _TOKEN_CACHE["access_token"] and _TOKEN_CACHE["expires_at"]:
            if now < _TOKEN_CACHE["expires_at"] - timedelta(minutes=5):
                return _TOKEN_CACHE["access_token"]  # type: ignore

        token = self._request_new_token()
        _TOKEN_CACHE["access_token"] = token
        _TOKEN_CACHE["expires_at"] = now + timedelta(hours=23)
        return token

    def _request_new_token(self) -> str:
        import requests  # lazy import
        url = self._base_url + TOKEN_PATH
        payload = {
            "grant_type": "client_credentials",
            "appkey": self._app_key,
            "appsecret": self._app_secret,
        }
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        token = data.get("access_token")
        if not token:
            raise KisUSClientError(f"[US_AUTH][FAIL] no access_token in response: {data}")
        logger.info("[US_AUTH][OK] token obtained")
        return token

    # ------------------------------------------------------------------
    # Market Data
    # ------------------------------------------------------------------

    def get_us_price(self, symbol: str, exchange: str) -> dict:
        """해외주식 현재가 조회."""
        self._assert_not_offline("get_us_price")
        tr = get_tr_info("us_price")
        headers = self._build_headers(tr["tr_id"])
        params = {
            "AUTH": "",
            "EXCD": self._resolve_quote_excd(exchange),
            "SYMB": symbol,
        }
        return self._get(tr["path"], headers=headers, params=params)

    def get_us_daily_price(self, symbol: str, exchange: str, count: int = 120) -> list[dict]:
        """해외주식 기간별 시세 (일봉)."""
        self._assert_not_offline("get_us_daily_price")
        tr = get_tr_info("us_daily_price")
        headers = self._build_headers(tr["tr_id"])
        today = datetime.now().strftime("%Y%m%d")
        past = (datetime.now() - timedelta(days=count * 2)).strftime("%Y%m%d")
        params = {
            "AUTH": "",
            "EXCD": self._resolve_quote_excd(exchange),
            "SYMB": symbol,
            "GUBN": "0",   # 0: 일, 1: 주, 2: 월
            "BYMD": today,
            "MODP": "0",
        }
        result = self._get(tr["path"], headers=headers, params=params)
        return (result.get("output2") or [])[:count]

    # ------------------------------------------------------------------
    # Account
    # ------------------------------------------------------------------

    def get_us_balance(self) -> dict:
        """해외주식 잔고 조회."""
        self._assert_not_offline("get_us_balance")
        tr = get_tr_info("us_balance")
        headers = self._build_headers(tr["tr_id"])
        params = {
            "CANO": self._cano,
            "ACNT_PRDT_CD": self._acnt_prdt_cd,
            "OVRS_EXCG_CD": "NASD",
            "TR_CRCY_CD": "USD",
            "CTX_AREA_FK200": "",
            "CTX_AREA_NK200": "",
        }
        return self._get(tr["path"], headers=headers, params=params)

    def get_us_orderable_cash(
        self,
        symbol: str = "AAPL",
        exchange: str = "NASDAQ",
        price: float = 100.0,
    ) -> dict:
        """해외주식 주문 가능 현금 조회.

        Args:
            symbol:   종목 코드 (ITEM_CD). 모의투자에서 필수.
            exchange: 거래소 코드. get_order_exchange_code_for_api() 로 변환.
            price:    호가 (OVRS_ORD_UNPR). 소수점 2자리 문자열.
        """
        self._assert_not_offline("get_us_orderable_cash")
        tr = get_tr_info("us_orderable_cash")
        headers = self._build_headers(tr["tr_id"])
        excg_code = get_order_exchange_code_for_api(exchange)
        params = {
            "CANO": self._cano,
            "ACNT_PRDT_CD": self._acnt_prdt_cd,
            "OVRS_EXCG_CD": excg_code,
            "OVRS_ORD_UNPR": f"{price:.2f}",
            "ITEM_CD": symbol,
        }
        return self._get(tr["path"], headers=headers, params=params)

    # ------------------------------------------------------------------
    # Orders
    # ------------------------------------------------------------------

    def place_us_buy_order(
        self,
        symbol: str,
        exchange: str,
        qty: int,
        price: float,
        order_type: str = "LIMIT",
    ) -> dict:
        """해외주식 매수 주문 (모의투자)."""
        us_cfg.assert_us_paper_order_allowed()
        self._assert_not_offline("place_us_buy_order")
        tr = get_tr_info("us_buy_order")
        return self._place_order(tr, symbol, exchange, qty, price, order_type)

    def place_us_sell_order(
        self,
        symbol: str,
        exchange: str,
        qty: int,
        price: float,
        order_type: str = "LIMIT",
    ) -> dict:
        """해외주식 매도 주문 (모의투자)."""
        us_cfg.assert_us_paper_order_allowed()
        self._assert_not_offline("place_us_sell_order")
        tr = get_tr_info("us_sell_order")
        return self._place_order(tr, symbol, exchange, qty, price, order_type)

    def _place_order(
        self,
        tr: dict,
        symbol: str,
        exchange: str,
        qty: int,
        price: float,
        order_type: str,
    ) -> dict:
        headers = self._build_headers(tr["tr_id"])
        excg_code = get_order_exchange_code_for_api(exchange)
        body = {
            "CANO": self._cano,
            "ACNT_PRDT_CD": self._acnt_prdt_cd,
            "OVRS_EXCG_CD": excg_code,
            "PDNO": symbol,
            "ORD_QTY": str(qty),
            "OVRS_ORD_UNPR": f"{price:.2f}",
            "ORD_SVR_DVSN_CD": "0",
            "ORD_DVSN": "00",  # 지정가
        }
        return self._post(tr["path"], headers=headers, body=body)

    # ------------------------------------------------------------------
    # Fills
    # ------------------------------------------------------------------

    def get_us_fills_today(self) -> list[dict]:
        """당일 체결 내역 조회."""
        self._assert_not_offline("get_us_fills_today")
        tr = get_tr_info("us_fills_today")
        headers = self._build_headers(tr["tr_id"])
        today = datetime.now().strftime("%Y%m%d")
        params = {
            "CANO": self._cano,
            "ACNT_PRDT_CD": self._acnt_prdt_cd,
            "PDNO": "",
            "ORD_STRT_DT": today,
            "ORD_END_DT": today,
            "SLL_BUY_DVSN": "00",
            "CCLD_NCCS_DVSN": "00",
            "OVRS_EXCG_CD": "NASD",
            "SORT_SQN": "DS",
            "CTX_AREA_FK200": "",
            "CTX_AREA_NK200": "",
        }
        result = self._get(tr["path"], headers=headers, params=params)
        return result.get("output") or []

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _build_headers(self, tr_id: str) -> dict:
        token = self.get_access_token() if not self._offline else "OFFLINE_TOKEN"
        return {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "appkey": self._app_key,
            "appsecret": self._app_secret,
            "tr_id": tr_id,
            "custtype": "P",
        }

    def _resolve_quote_excd(self, exchange: str) -> str:
        from trader.us.symbols import get_quote_exchange_code
        return get_quote_exchange_code(exchange)

    def _get(self, path: str, headers: dict, params: dict) -> dict:
        import requests
        url = self._base_url + path
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        self._check_rt_cd(data)
        return data

    def _post(self, path: str, headers: dict, body: dict) -> dict:
        import requests
        url = self._base_url + path
        resp = requests.post(url, headers=headers, json=body, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        self._check_rt_cd(data)
        return data

    def _check_rt_cd(self, data: dict) -> None:
        rt_cd = data.get("rt_cd")
        if rt_cd and rt_cd != "0":
            msg = data.get("msg1", "unknown error")
            raise KisUSClientError(f"[US_KIS][FAIL] rt_cd={rt_cd} msg={msg!r}")

    def _assert_not_offline(self, method_name: str) -> None:
        if self._offline:
            raise RuntimeError(
                f"[US_CLIENT][OFFLINE_BLOCK] method={method_name!r} "
                "HTTP calls not allowed in offline mode"
            )
