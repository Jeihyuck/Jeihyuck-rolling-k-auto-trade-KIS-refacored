# -*- coding: utf-8 -*-
"""미국주식 Data Provider.

offline 모드에서는 fixture 데이터를 반환한다.
online 모드에서는 KisUSClient를 통해 실제 데이터를 가져온다.
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)


def _make_stub_daily(symbol: str, count: int = 60) -> list[dict]:
    """offline/test 용 stub daily price 데이터 (xymd 오름차순)."""
    base_price = 100.0
    prices = []
    d = date.today()
    for i in range(count):
        idx = count - i - 1
        price = base_price * (1 + 0.001 * idx)
        prices.append({
            "xymd": (d - timedelta(days=idx)).strftime("%Y%m%d"),
            "clos": f"{price:.2f}",
            "open": f"{price * 0.99:.2f}",
            "high": f"{price * 1.01:.2f}",
            "low": f"{price * 0.98:.2f}",
            "tvol": "1000000",
            "symbol": symbol,
        })
    # 반드시 오름차순 반환 (전략 코드가 closes[-1]을 최신가로 가정)
    return sorted(prices, key=lambda r: str(r.get("xymd", "")))


def _make_stub_price(symbol: str) -> dict:
    """offline/test 용 stub current price."""
    return {
        "last": "100.00",
        "open": "99.00",
        "high": "101.00",
        "low": "98.00",
        "tvol": "1000000",
        "symbol": symbol,
    }


class USDataProvider:
    """미국주식 데이터 제공자.

    Args:
        offline: True이면 stub 데이터만 반환 (HTTP 호출 없음)
    """

    def __init__(self, offline: bool = False) -> None:
        self._offline = offline
        self._client = None

    def _get_client(self):
        if self._client is None:
            from trader.us.execution.kis_us_client import KisUSClient
            self._client = KisUSClient(env="practice")
        return self._client

    def get_current_price(self, symbol: str, exchange: str) -> dict:
        """현재가 조회."""
        if self._offline:
            logger.debug("[US_DATA][OFFLINE] current_price symbol=%s", symbol)
            return _make_stub_price(symbol)
        result = self._get_client().get_us_price(symbol, exchange)
        output = result.get("output", {})
        return {
            "last": output.get("last", "0"),
            "open": output.get("open", "0"),
            "high": output.get("high", "0"),
            "low": output.get("low", "0"),
            "tvol": output.get("tvol", "0"),
            "symbol": symbol,
        }

    def get_daily_prices(self, symbol: str, exchange: str, count: int = 120) -> list[dict]:
        """일봉 데이터 조회 (xymd 기준 오름차순 정렬).

        전략 코드가 closes[-1]을 최신 가격으로 가정하므로 반드시 오름차순 반환.
        """
        if self._offline:
            logger.debug("[US_DATA][OFFLINE] daily_prices symbol=%s count=%d", symbol, count)
            return _make_stub_daily(symbol, count)
        rows = self._get_client().get_us_daily_price(symbol, exchange, count)
        return sorted(rows, key=lambda r: str(r.get("xymd", "")))

    def get_balance(self) -> dict:
        """잔고 조회."""
        if self._offline:
            return {
                "total_pvs": "10000.00",
                "frcr_pchs_amt1": "5000.00",
                "ovrs_tot_pfls": "500.00",
                "positions": [],
            }
        raw = self._get_client().get_us_balance()
        return raw

    def get_orderable_cash(
        self,
        symbol: str = "AAPL",
        exchange: str = "NASDAQ",
        price: float = 100.0,
    ) -> float:
        """주문 가능 현금 (USD).

        Args:
            symbol:   종목 코드 (KIS 모의투자 필수 파라미터).
            exchange: 거래소. KIS API OVRS_EXCG_CD 로 변환.
            price:    호가 (OVRS_ORD_UNPR). KIS API 필수 파라미터.

        후보 필드 (KIS 환경에 따라 다를 수 있음):
        frcr_ord_psbl_amt1, ord_psbl_cash, ovrs_ord_psbl_amt,
        orderable_cash, cash, psbl_amt
        """
        if self._offline:
            return 1000.0
        try:
            raw = self._get_client().get_us_orderable_cash(
                symbol=symbol, exchange=exchange, price=price
            )
        except Exception as exc:
            logger.warning("[US_DATA][WARN] get_orderable_cash API failed: %s", exc)
            return 0.0
        output = raw.get("output", raw)  # output 없으면 raw 자체 시도
        candidates = (
            "frcr_ord_psbl_amt1", "ord_psbl_cash", "ovrs_ord_psbl_amt",
            "orderable_cash", "cash", "psbl_amt",
        )
        for key in candidates:
            val = output.get(key)
            if val is not None:
                try:
                    return float(val)
                except (ValueError, TypeError):
                    logger.warning("[US_DATA][WARN] orderable_cash field %s not numeric: %s", key, val)
        logger.warning("[US_DATA][WARN] orderable_cash not found in response, returning 0.0")
        return 0.0
