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


def _extract_input_field_name(error_msg: str) -> str:
    """Extract missing field name from KIS INPUT_FIELD_NAME error.
    
    Args:
        error_msg: KIS error message
        
    Returns:
        Extracted field name or empty string
    """
    marker = "INPUT_FIELD_NAME"
    if marker not in error_msg:
        return ""
    tail = error_msg.split(marker, 1)[-1]
    cleaned = tail.replace("'", "").replace('"', "").replace(":", "").strip()
    tokens = cleaned.split()
    return tokens[0] if tokens else ""


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
            "temp_error_count": 0,
            "temp_recovered_count": 0,
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
        """해외주식 잔고 조회 (다중 거래소).
        
        기본적으로 NASD, NYSE, AMEX 3개 거래소를 조회하여 병합.
        환경변수 US_BALANCE_EXCHANGES로 커스터마이징 가능.
        
        Returns:
            {
                "rt_cd": "0",
                "output1": merged_output1_list,
                "output2": merged_summary_dict,
                "queried_exchanges": ["NASD", "NYSE", "AMEX"],
                "exchange_result_counts": {"NASD": 4, "NYSE": 1, "AMEX": 0},
                "raw_by_exchange": {...}
            }
        """
        self._assert_not_offline("get_us_balance")
        
        exchanges_env = os.getenv("US_BALANCE_EXCHANGES", "NASD,NYSE,AMEX")
        exchanges = [e.strip().upper() for e in exchanges_env.split(",") if e.strip()]
        if not exchanges:
            exchanges = ["NASD", "NYSE", "AMEX"]
        
        logger.info("[US_BALANCE] querying exchanges=%s", exchanges)
        
        merged_output1: list[dict] = []
        exchange_result_counts: dict[str, int] = {}
        raw_by_exchange: dict[str, Any] = {}
        merged_output2: dict = {}
        
        for exchange_code in exchanges:
            try:
                exchange_data = self._get_us_balance_single_exchange(exchange_code)
                raw_by_exchange[exchange_code] = exchange_data
                
                # output1 병합
                ex_output1 = exchange_data.get("output1", [])
                if isinstance(ex_output1, dict):
                    ex_output1 = [ex_output1]
                elif not isinstance(ex_output1, list):
                    ex_output1 = []
                
                # 각 row에 exchange 태깅
                for row in ex_output1:
                    if isinstance(row, dict):
                        if "ovrs_excg_cd" not in row:
                            row["ovrs_excg_cd"] = exchange_code
                        merged_output1.append(row)
                
                exchange_result_counts[exchange_code] = len(ex_output1)
                
                # output2 병합 (첫 번째 유효한 것 사용)
                if not merged_output2:
                    ex_output2 = exchange_data.get("output2")
                    if isinstance(ex_output2, list) and ex_output2:
                        merged_output2 = ex_output2[0] if isinstance(ex_output2[0], dict) else {}
                    elif isinstance(ex_output2, dict):
                        merged_output2 = ex_output2
                
                logger.info(
                    "[US_BALANCE][EXCHANGE][DONE] exchange=%s count=%d",
                    exchange_code,
                    len(ex_output1),
                )
            
            except Exception as exc:
                logger.warning(
                    "[US_BALANCE][EXCHANGE][ERROR] exchange=%s error=%s",
                    exchange_code,
                    exc,
                )
                exchange_result_counts[exchange_code] = 0
                # 일부 거래소 실패 시 계속 진행 (다른 거래소 결과가 있으면 OK)
                continue
        
        # symbol 중복 병합
        merged_output1 = self._merge_duplicate_symbols(merged_output1)
        
        total_count = sum(exchange_result_counts.values())
        logger.info(
            "[US_BALANCE][MERGED] raw_count=%d unique_symbols=%d symbols=%s",
            total_count,
            len(merged_output1),
            ",".join([row.get("ovrs_pdno", row.get("pdno", "?")) for row in merged_output1 if isinstance(row, dict)]),
        )
        
        return {
            "rt_cd": "0",
            "output1": merged_output1,
            "output2": merged_output2,
            "queried_exchanges": exchanges,
            "exchange_result_counts": exchange_result_counts,
            "raw_by_exchange": raw_by_exchange,
        }
    
    def _get_us_balance_single_exchange(
        self,
        exchange_code: str,
        max_pages: int = 10,
    ) -> dict:
        """단일 거래소에 대한 잔고 조회 (pagination 처리).
        
        Args:
            exchange_code: "NASD", "NYSE", "AMEX" 등
            max_pages: 최대 페이지네이션 수
        
        Returns:
            KIS raw response (output1 list, output2 dict/list)
        """
        tr = get_tr_info("us_balance")
        headers = self._build_headers(tr["tr_id"])
        
        logger.info("[US_BALANCE][EXCHANGE][START] exchange=%s", exchange_code)
        
        all_output1: list[dict] = []
        output2: Any = None
        ctx_fk = ""
        ctx_nk = ""
        
        for page in range(1, max_pages + 1):
            params = {
                "CANO": self._cano,
                "ACNT_PRDT_CD": self._acnt_prdt_cd,
                "OVRS_EXCG_CD": exchange_code,
                "TR_CRCY_CD": "USD",
                "CTX_AREA_FK200": ctx_fk,
                "CTX_AREA_NK200": ctx_nk,
            }
            
            try:
                result = self._get(tr["path"], headers=headers, params=params)
            except Exception as exc:
                logger.warning(
                    "[US_BALANCE][EXCHANGE][PAGE_ERROR] exchange=%s page=%d error=%s",
                    exchange_code,
                    page,
                    exc,
                )
                # 첫 페이지 실패 시 빈 결과 반환
                if page == 1:
                    return {"rt_cd": "0", "output1": [], "output2": {}}
                else:
                    # 2페이지 이상 실패 시 지금까지 수집한 데이터 반환
                    break
            
            # output1
            page_output1 = result.get("output1", [])
            if isinstance(page_output1, dict):
                page_output1 = [page_output1]
            elif not isinstance(page_output1, list):
                page_output1 = []
            
            all_output1.extend(page_output1)
            
            # output2 (첫 페이지만)
            if output2 is None:
                output2 = result.get("output2")
            
            # pagination cursor 체크 — 반드시 strip() 처리 (공백 문자열은 다음 페이지 아님)
            next_fk_raw = result.get("ctx_area_fk200") or result.get("CTX_AREA_FK200") or ""
            next_nk_raw = result.get("ctx_area_nk200") or result.get("CTX_AREA_NK200") or ""

            next_fk = str(next_fk_raw).strip()
            next_nk = str(next_nk_raw).strip()

            if not next_fk and not next_nk:
                logger.info(
                    "[US_BALANCE][EXCHANGE][PAGE_END] exchange=%s page=%d reason=empty_cursor rows=%d",
                    exchange_code,
                    page,
                    len(page_output1),
                )
                break

            if next_fk == ctx_fk and next_nk == ctx_nk:
                logger.warning(
                    "[US_BALANCE][EXCHANGE][PAGE_END] exchange=%s page=%d reason=same_cursor rows=%d",
                    exchange_code,
                    page,
                    len(page_output1),
                )
                break

            ctx_fk = next_fk
            ctx_nk = next_nk

            logger.debug(
                "[US_BALANCE][EXCHANGE][PAGE] exchange=%s page=%d count=%d next_fk=%s next_nk=%s",
                exchange_code,
                page,
                len(page_output1),
                bool(next_fk),
                bool(next_nk),
            )
        
        return {
            "rt_cd": "0",
            "output1": all_output1,
            "output2": output2,
        }
    
    def _merge_duplicate_symbols(self, rows: list[dict]) -> list[dict]:
        """symbol 중복 처리.

        원칙:
        1. 완전히 동일한 row (row_key 기준)는 중복 페이지 row → skip.
        2. 같은 symbol + 같은 exchange는 중복 페이지 → skip (합산 금지).
        3. 같은 symbol이라도 다른 exchange이면 cross-exchange 실제 보유 → 제한적 합산 허용.

        Args:
            rows: output1 row list

        Returns:
            중복 제거된 row list
        """
        if not rows:
            return []

        seen_exact_rows: set[tuple] = set()
        symbol_map: dict[str, dict] = {}  # symbol → merged row

        for row in rows:
            if not isinstance(row, dict):
                continue

            # symbol 추출
            symbol = row.get("ovrs_pdno") or row.get("pdno") or row.get("PDNO") or ""
            symbol = str(symbol).strip().upper()
            if not symbol:
                continue

            # exchange 추출
            exchange = (
                row.get("ovrs_excg_cd")
                or row.get("tr_mket_name")
                or row.get("exchange")
                or ""
            )
            exchange = str(exchange).strip().upper()

            # qty / orderable_qty / avg_price / market_value / buy_amount
            qty_raw = (
                row.get("ovrs_cblc_qty")
                or row.get("cblc_qty")
                or row.get("hldg_qty")
                or "0"
            )
            orderable_raw = (
                row.get("ord_psbl_qty")
                or row.get("sll_psbl_qty")
                or qty_raw
            )
            avg_price_raw = (
                row.get("pchs_avg_pric")
                or row.get("pchs_avg_price")
                or "0"
            )
            mv_raw = (
                row.get("ovrs_stck_evlu_amt")
                or row.get("frcr_evlu_amt2")
                or row.get("evlu_amt")
                or "0"
            )
            ba_raw = row.get("frcr_pchs_amt1") or row.get("pchs_amt") or "0"

            row_key = (
                symbol,
                exchange,
                str(qty_raw).strip(),
                str(orderable_raw).strip(),
                str(avg_price_raw).strip(),
                str(mv_raw).strip(),
                str(ba_raw).strip(),
            )

            # 1단계: 완전히 동일한 row → 중복 페이지 skip
            if row_key in seen_exact_rows:
                logger.warning(
                    "[US_BALANCE][DUPLICATE_ROW_SKIP] symbol=%s exchange=%s"
                    " qty=%s orderable_qty=%s avg_price=%s market_value=%s",
                    symbol,
                    exchange,
                    qty_raw,
                    orderable_raw,
                    avg_price_raw,
                    mv_raw,
                )
                continue
            seen_exact_rows.add(row_key)

            # 2단계: 새 symbol → 그냥 추가
            if symbol not in symbol_map:
                symbol_map[symbol] = dict(row)
                continue

            # 3단계: 같은 symbol이 이미 있음
            existing = symbol_map[symbol]
            existing_exchange = (
                existing.get("ovrs_excg_cd")
                or existing.get("tr_mket_name")
                or existing.get("exchange")
                or ""
            )
            existing_exchange = str(existing_exchange).strip().upper()
            existing_qty_raw = (
                existing.get("ovrs_cblc_qty")
                or existing.get("cblc_qty")
                or existing.get("hldg_qty")
                or "0"
            )

            if existing_exchange == exchange:
                # 같은 exchange → 중복 페이지 row → skip (합산 금지)
                logger.warning(
                    "[US_BALANCE][DUPLICATE_SYMBOL_SAME_EXCHANGE_SKIP]"
                    " symbol=%s exchange=%s existing_qty=%s duplicate_qty=%s",
                    symbol,
                    exchange,
                    existing_qty_raw,
                    qty_raw,
                )
                continue

            # 다른 exchange → cross-exchange 실제 보유 → 합산 허용
            logger.warning(
                "[US_BALANCE][DUPLICATE_SYMBOL_CROSS_EXCHANGE_MERGE]"
                " symbol=%s existing_exchange=%s new_exchange=%s"
                " existing_qty=%s new_qty=%s",
                symbol,
                existing_exchange,
                exchange,
                existing_qty_raw,
                qty_raw,
            )

            # qty 합산
            qty_keys = ("ovrs_cblc_qty", "cblc_qty", "hldg_qty", "qty")
            for k in qty_keys:
                if k in existing and k in row:
                    existing[k] = str(
                        int(self._safe_numeric(existing[k], 0))
                        + int(self._safe_numeric(row[k], 0))
                    )
                    break

            # market_value 합산
            mv_keys = ("ovrs_stck_evlu_amt", "frcr_evlu_amt2", "evlu_amt")
            for k in mv_keys:
                if k in existing and k in row:
                    existing[k] = str(
                        float(self._safe_numeric(existing[k], 0.0))
                        + float(self._safe_numeric(row[k], 0.0))
                    )
                    break

            # buy_amount 합산
            ba_keys = ("frcr_pchs_amt1", "pchs_amt")
            for k in ba_keys:
                if k in existing and k in row:
                    existing[k] = str(
                        float(self._safe_numeric(existing[k], 0.0))
                        + float(self._safe_numeric(row[k], 0.0))
                    )
                    break

            # pnl 합산
            pnl_keys = ("frcr_evlu_pfls_amt", "evlu_pfls_amt", "ovrs_stck_evlu_pfls_amt")
            for k in pnl_keys:
                if k in existing and k in row:
                    existing[k] = str(
                        float(self._safe_numeric(existing[k], 0.0))
                        + float(self._safe_numeric(row[k], 0.0))
                    )
                    break

        return list(symbol_map.values())
    
    def _safe_numeric(self, val: Any, default: float = 0.0) -> float:
        """숫자 변환 (safe)."""
        if val is None or val == "" or val == "-":
            return default
        if isinstance(val, (int, float)):
            return float(val)
        if isinstance(val, str):
            cleaned = val.replace(",", "").strip()
            if not cleaned or cleaned == "-":
                return default
            try:
                return float(cleaned)
            except (ValueError, TypeError):
                return default
        return default

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
        # Safe log: CANO, token, appkey, appsecret 미포함
        order_side = "SELL" if "sell" in tr.get("tr_id", "").lower() or "S" in tr.get("order_side", "") else "BUY"
        logger.info(
            "[US_ORDER][REQUEST_SAFE] side=%s symbol=%s exchange_input=%s exchange_api=%s"
            " qty=%s price=%.2f ord_dvsn=%s tr_id=%s",
            order_side,
            symbol,
            exchange,
            excg_code,
            qty,
            price,
            body.get("ORD_DVSN"),
            tr.get("tr_id", ""),
        )
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
        
        # Schema fallback: ALL_DATES first, then ORD_DT, then ORD_RANGE
        schemas = ["ALL_DATES", "ORD_DT", "ORD_RANGE"]
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
                    missing_field = _extract_input_field_name(error_msg)
                    logger.warning(
                        "[US_FILLS][SCHEMA_RETRY] failed_schema=%s missing_field=%s msg=%s",
                        schema,
                        missing_field,
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

                missing_field = _extract_input_field_name(error_msg)
                logger.warning(
                    "[US_FILLS][SCHEMA_RETRY] failed_schema=%s missing_field=%s msg=%s",
                    schema,
                    missing_field,
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
            schema: "ALL_DATES", "ORD_DT" 또는 "ORD_RANGE"
            
        Returns:
            KIS fills inquiry params dict
        """
        base = {
            "CANO": self._cano,
            "ACNT_PRDT_CD": self._acnt_prdt_cd,
            "PDNO": "",
            "ORD_GNO_BRNO": "",
            "ODNO": "",
            "SLL_BUY_DVSN": "00",
            "CCLD_NCCS_DVSN": "00",
            "OVRS_EXCG_CD": "NASD",
            "SORT_SQN": "DS",
            "CTX_AREA_FK200": "",
            "CTX_AREA_NK200": "",
        }

        if schema == "ALL_DATES":
            base["ORD_DT"] = ord_dt
            base["ORD_STRT_DT"] = ord_dt
            base["ORD_END_DT"] = ord_dt
        elif schema == "ORD_DT":
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
        last_error: Exception | None = None
        
        for attempt in range(1, max_attempts + 1):
            try:
                self._apply_rate_limit(path)
                
                url = self._base_url + path
                resp = requests.get(url, headers=headers, params=params, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                self._check_rt_cd(data)
                
                # Success after retry
                if attempt > 1 and last_error:
                    self.stats["temp_recovered_count"] += 1
                    logger.info(
                        f"[US_KIS][TEMP_RECOVERED] attempt={attempt}/{max_attempts} "
                        f"path={path!r} last_error={last_error!r}"
                    )
                
                return data
            
            except Exception as err:
                last_error = err
                is_temp = self._is_temporary_error(err, None)
                
                if is_temp and attempt < max_attempts:
                    self.stats["get_retry_count"] += 1
                    self.stats["temp_error_count"] += 1
                    backoff_sec = backoff_schedule[min(attempt - 1, len(backoff_schedule) - 1)]
                    jitter = random.uniform(0, 0.3 * backoff_sec)
                    sleep_time = backoff_sec + jitter
                    
                    logger.warning(
                        f"[US_KIS][TEMP_ERROR] endpoint=GET_{path.split('/')[- 1]} "
                        f"attempt={attempt}/{max_attempts} error={err!r} backoff={sleep_time:.2f}s"
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
        last_error: Exception | None = None
        
        for attempt in range(1, max_attempts + 1):
            try:
                self._apply_rate_limit(path)
                
                url = self._base_url + path
                resp = requests.post(url, headers=headers, json=body, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                self._check_rt_cd(data)
                
                # Success after retry
                if attempt > 1 and last_error:
                    self.stats["temp_recovered_count"] += 1
                    logger.info(
                        f"[US_KIS][TEMP_RECOVERED] attempt={attempt}/{max_attempts} "
                        f"path={path!r} last_error={last_error!r}"
                    )
                
                return data
            
            except Exception as err:
                last_error = err
                is_temp = self._is_temporary_error(err, None)
                
                if is_temp and attempt < max_attempts:
                    self.stats["post_retry_count"] += 1
                    self.stats["temp_error_count"] += 1
                    backoff_sec = backoff_schedule[min(attempt - 1, len(backoff_schedule) - 1)]
                    jitter = random.uniform(0, 0.3 * backoff_sec)
                    sleep_time = backoff_sec + jitter
                    
                    logger.warning(
                        f"[US_KIS][TEMP_ERROR] endpoint=POST_{path.split('/')[-1]} "
                        f"attempt={attempt}/{max_attempts} error={err!r} backoff={sleep_time:.2f}s"
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
