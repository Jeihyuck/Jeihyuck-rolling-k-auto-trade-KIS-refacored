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
import random
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


class KisUSTemporaryError(KisUSClientError):
    """KIS US API 일시적 오류 (재시도 가능)."""


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
        self._last_request_time: dict[str, float] = {}  # endpoint별 rate limiting
        self.stats = {
            "get_retry_count": 0,
            "post_retry_count": 0,
            "http_fail_final_count": 0,
        }

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
        
        # Final safety guard: signal-only mode
        if os.getenv("US_KIS_ORDER_ALLOWED") == "0":
            raise RuntimeError(
                "[US_KIS_ORDER_BLOCKED] US_KIS_ORDER_ALLOWED=0 — KIS order API disabled in signal-only mode"
            )
        
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
        
        # Final safety guard: signal-only mode
        if os.getenv("US_KIS_ORDER_ALLOWED") == "0":
            raise RuntimeError(
                "[US_KIS_ORDER_BLOCKED] US_KIS_ORDER_ALLOWED=0 — KIS order API disabled in signal-only mode"
            )
        
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

    def get_us_fills_today(self, trade_date: str | None = None) -> list[dict]:
        """당일 체결 내역 조회.
        
        Args:
            trade_date: YYYY-MM-DD 형식. None이면 NY 기준 오늘.
            
        Returns:
            체결 내역 list
        """
        self._assert_not_offline("get_us_fills_today")
        tr = get_tr_info("us_fills_today")
        headers = self._build_headers(tr["tr_id"])
        
        # trade_date 처리: YYYY-MM-DD → YYYYMMDD
        if trade_date:
            ord_dt = trade_date.replace("-", "")
        else:
            # NY 기준 today
            from zoneinfo import ZoneInfo
            from datetime import datetime as dt
            ny_tz = ZoneInfo("America/New_York")
            ord_dt = dt.now(tz=ny_tz).strftime("%Y%m%d")
        
        # Schema fallback: ORD_DT first, then ORD_RANGE
        schemas = ["ORD_DT", "ORD_RANGE"]
        errors = []

        for schema in schemas:
            params = self._build_us_fills_params(ord_dt, schema)
            logger.info(
                "[US_FILLS][REQUEST] schema=%s ord_dt=%s endpoint=inquire-ccnl",
                schema,
                ord_dt,
            )

            try:
                result = self._get(
                    tr["path"],
                    headers=headers,
                    params=params,
                    suppress_final_log=True,
                )
                fills = result.get("output") or []
                logger.info(
                    "[US_FILLS][FETCHED] count=%d status=OK schema=%s",
                    len(fills),
                    schema,
                )
                return fills

            except Exception as exc:
                error_msg = str(exc)
                errors.append({"schema": schema, "error": error_msg})

                if "INPUT_FIELD_NAME" in error_msg:
                    logger.warning(
                        "[US_FILLS][SCHEMA_RETRY] failed_schema=%s msg=%s",
                        schema,
                        error_msg,
                    )
                    continue

                if "EGW002" in error_msg or "RATE" in error_msg.upper():
                    logger.warning(
                        "[US_FILLS][ERROR][TEMP] schema=%s msg=%s",
                        schema,
                        error_msg,
                    )
                    raise KisUSTemporaryError(f"KIS temporary error: {error_msg}") from exc

                logger.warning(
                    "[US_FILLS][SCHEMA_RETRY] failed_schema=%s msg=%s",
                    schema,
                    error_msg,
                )
                continue

        logger.error(
            "[US_FILLS][ERROR][CONTRACT] all_schemas_failed errors=%s",
            errors,
        )
        raise KisUSClientError(
            f"KIS fills contract error: all schemas failed: {errors}"
        )

    def _build_us_fills_params(self, ord_dt: str, schema: str) -> dict:
        """US fills inquiry params를 schema에 따라 생성.
        
        Args:
            ord_dt: YYYYMMDD 형식 날짜
            schema: "ORD_DT" 또는 "ORD_RANGE"
            
        Returns:
            KIS fills inquiry params dict
        """
        base = {
            "CANO": self._cano,
            "ACNT_PRDT_CD": self._acnt_prdt_cd,
            "PDNO": "",
            "SLL_BUY_DVSN": "00",
            "CCLD_NCCS_DVSN": "00",
            "OVRS_EXCG_CD": "NASD",
            "SORT_SQN": "DS",
            "CTX_AREA_FK200": "",
            "CTX_AREA_NK200": "",
        }

        if schema == "ORD_DT":
            base["ORD_DT"] = ord_dt
        elif schema == "ORD_RANGE":
            base["ORD_STRT_DT"] = ord_dt
            base["ORD_END_DT"] = ord_dt
        else:
            raise ValueError(f"unknown fills schema: {schema}")

        return base

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

    def _get_rate_limit_for_path(self, path: str) -> tuple[float, float]:
        """경로별 rate limit 반환 (min_sec, max_sec)."""
        if "dailyprice" in path:
            return (0.4, 0.7)
        elif "/quotations/price" in path:
            return (0.3, 0.5)
        elif "order" in path.lower():
            return (1.0, 1.0)
        else:
            return (0.7, 0.7)  # trading 기본

    def _apply_rate_limit(self, path: str) -> None:
        """API 호출 전 rate limiting 적용."""
        endpoint_key = path.split("?")[0]  # query string 제거
        last_time = self._last_request_time.get(endpoint_key, 0.0)
        min_sec, max_sec = self._get_rate_limit_for_path(path)
        interval = random.uniform(min_sec, max_sec)
        
        elapsed = time.time() - last_time
        if elapsed < interval:
            sleep_time = interval - elapsed
            logger.debug(f"[US_KIS][RATE_LIMIT] path={path!r} sleep={sleep_time:.3f}s")
            time.sleep(sleep_time)
        
        self._last_request_time[endpoint_key] = time.time()

    def _is_temporary_error(self, err: Exception, data: dict | None = None) -> bool:
        """일시적 오류인지 판단 (재시도 가능)."""
        import requests
        
        # HTTP 상태 코드
        if isinstance(err, requests.exceptions.HTTPError):
            if err.response is not None and err.response.status_code in (500, 429, 502, 503, 504):
                return True
        
        # Timeout
        if isinstance(err, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)):
            return True
        
        # KIS 메시지
        if isinstance(err, KisUSClientError):
            err_msg = str(err).lower()
            if any(word in err_msg for word in ["초과", "egw002", "timeout", "temporarily"]):
                return True
        
        # rt_cd 체크
        if data:
            msg1 = data.get("msg1", "").lower()
            if any(word in msg1 for word in ["초과", "egw002", "일시적"]):
                return True
        
        return False

    def _get(self, path: str, headers: dict, params: dict, *, suppress_final_log: bool = False) -> dict:
        """GET 요청 with retry/backoff.
        
        Args:
            path: API path
            headers: HTTP headers
            params: Query parameters
            suppress_final_log: True이면 최종 실패 시 HTTP_FAIL_FINAL 로그 생략
        """
        import requests
        
        max_attempts = 5
        backoff_schedule = [0.7, 1.5, 3.0, 5.0]  # seconds
        
        for attempt in range(1, max_attempts + 1):
            try:
                self._apply_rate_limit(path)
                
                url = self._base_url + path
                resp = requests.get(url, headers=headers, params=params, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                self._check_rt_cd(data)
                return data
            
            except Exception as err:
                is_temp = self._is_temporary_error(err, None)
                
                if is_temp and attempt < max_attempts:
                    self.stats["get_retry_count"] += 1
                    backoff_sec = backoff_schedule[min(attempt - 1, len(backoff_schedule) - 1)]
                    jitter = random.uniform(0, 0.3 * backoff_sec)
                    sleep_time = backoff_sec + jitter
                    
                    logger.warning(
                        f"[US_KIS][RETRY] attempt={attempt}/{max_attempts} "
                        f"path={path!r} error={err!r} backoff={sleep_time:.2f}s"
                    )
                    time.sleep(sleep_time)
                    continue
                
                # 최종 실패
                if not suppress_final_log:
                    self.stats["http_fail_final_count"] += 1
                    logger.error(
                        f"[US_KIS][HTTP_FAIL_FINAL] attempt={attempt}/{max_attempts} "
                        f"path={path!r} error={err!r} temporary={is_temp}"
                    )
                if is_temp:
                    raise KisUSTemporaryError(f"GET {path} failed after {attempt} attempts: {err}") from err
                else:
                    raise
        
        raise KisUSTemporaryError(f"GET {path} exhausted {max_attempts} attempts")

    def _post(self, path: str, headers: dict, body: dict) -> dict:
        """POST 요청 with retry/backoff."""
        import requests
        
        max_attempts = 5
        backoff_schedule = [0.7, 1.5, 3.0, 5.0]  # seconds
        
        for attempt in range(1, max_attempts + 1):
            try:
                self._apply_rate_limit(path)
                
                url = self._base_url + path
                resp = requests.post(url, headers=headers, json=body, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                self._check_rt_cd(data)
                return data
            
            except Exception as err:
                is_temp = self._is_temporary_error(err, None)
                
                if is_temp and attempt < max_attempts:
                    self.stats["post_retry_count"] += 1
                    backoff_sec = backoff_schedule[min(attempt - 1, len(backoff_schedule) - 1)]
                    jitter = random.uniform(0, 0.3 * backoff_sec)
                    sleep_time = backoff_sec + jitter
                    
                    logger.warning(
                        f"[US_KIS][RETRY] attempt={attempt}/{max_attempts} "
                        f"path={path!r} error={err!r} backoff={sleep_time:.2f}s"
                    )
                    time.sleep(sleep_time)
                    continue
                
                # 최종 실패
                self.stats["http_fail_final_count"] += 1
                logger.error(
                    f"[US_KIS][HTTP_FAIL_FINAL] attempt={attempt}/{max_attempts} "
                    f"path={path!r} error={err!r} temporary={is_temp}"
                )
                if is_temp:
                    raise KisUSTemporaryError(f"POST {path} failed after {attempt} attempts: {err}") from err
                else:
                    raise
        
        raise KisUSTemporaryError(f"POST {path} exhausted {max_attempts} attempts")

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
