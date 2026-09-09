# -*- coding: utf-8 -*-
"""KIS 해외주식 모의투자 API 레지스트리.

endpoint/tr_id를 코드 곳곳에 하드코딩하지 않고 이 파일에서만 관리한다.
KIS OpenAPI 해외주식 모의투자(vts) 문서 기준.
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Base URLs
# ---------------------------------------------------------------------------
KIS_VTS_BASE_URL = "https://openapivts.koreainvestment.com:29443"  # 모의투자
KIS_REAL_BASE_URL = "https://openapi.koreainvestment.com:9443"     # 실거래 (사용금지)

# Practice (모의투자) OAuth endpoint
TOKEN_PATH = "/oauth2/tokenP"

# ---------------------------------------------------------------------------
# TR_ID 레지스트리
# 해외주식 모의투자 TR_ID (VTS)
# ---------------------------------------------------------------------------
TR_REGISTRY: dict[str, dict] = {
    # 해외주식 현재가
    "us_price": {
        "tr_id": "HHDFS00000300",
        "path": "/uapi/overseas-price/v1/quotations/price",
        "method": "GET",
        "description": "해외주식 현재가",
    },
    # 해외주식 기간별 시세 (일봉)
    "us_daily_price": {
        "tr_id": "HHDFS76240000",
        "path": "/uapi/overseas-price/v1/quotations/dailyprice",
        "method": "GET",
        "description": "해외주식 기간별 시세",
    },
    # 해외주식 잔고 (모의투자)
    "us_balance": {
        "tr_id": "VTTS3012R",
        "path": "/uapi/overseas-stock/v1/trading/inquire-balance",
        "method": "GET",
        "description": "해외주식 잔고 조회 (모의)",
    },
    # 해외주식 주문 가능 현금
    "us_orderable_cash": {
        "tr_id": "VTTS3007R",
        "path": "/uapi/overseas-stock/v1/trading/inquire-psamount",
        "method": "GET",
        "description": "해외주식 주문 가능 금액 (모의)",
    },
    # 해외주식 매수 주문 (모의투자)
    "us_buy_order": {
        "tr_id": "VTTT1002U",
        "path": "/uapi/overseas-stock/v1/trading/order",
        "method": "POST",
        "description": "해외주식 매수 주문 (모의)",
        "order_side": "BUY",
    },
    # 해외주식 매도 주문 (모의투자)
    "us_sell_order": {
        "tr_id": "VTTT1001U",
        "path": "/uapi/overseas-stock/v1/trading/order",
        "method": "POST",
        "description": "해외주식 매도 주문 (모의)",
        "order_side": "SELL",
    },
    # 해외주식 미국 정정/취소 주문 (모의투자)
    "us_order_cancel": {
        "tr_id": "VTTT1004U",
        "path": "/uapi/overseas-stock/v1/trading/order-rvsecncl",
        "method": "POST",
        "description": "해외주식 미국 주문 취소 (모의)",
    },
    # 해외주식 당일 체결 내역 (모의투자)
    "us_fills_today": {
        "tr_id": "VTTS3035R",
        "path": "/uapi/overseas-stock/v1/trading/inquire-ccnl",
        "method": "GET",
        "description": "해외주식 당일 체결 (모의)",
    },
}

# ---------------------------------------------------------------------------
# Order type codes
# ---------------------------------------------------------------------------
ORDER_TYPE_CODES: dict[str, str] = {
    "MARKET": "00",  # 시장가
    "LIMIT": "00",   # 지정가 (해외는 별도 코드 확인 필요, 우선 '00' placeholder)
}

# ---------------------------------------------------------------------------
# 거래소 → KIS 주문용 거래소 코드 매핑 (kis_us_registry와 중복 없이 분리)
# 주문 API에서 사용하는 OVRS_EXCG_CD
# ---------------------------------------------------------------------------
ORDER_EXCHANGE_REGISTRY: dict[str, str] = {
    "NASD": "NASD",
    "NYSE": "NYSE",
    "AMEX": "AMEX",
    "NASDAQ": "NASD",  # 정규화 alias
}


def get_tr_info(action: str) -> dict:
    """action key로 TR 정보 반환."""
    info = TR_REGISTRY.get(action)
    if info is None:
        raise KeyError(f"get_tr_info: unknown action={action!r}")
    return dict(info)


def get_order_exchange_code_for_api(exchange: str) -> str:
    """주문 API용 거래소 코드 반환."""
    code = ORDER_EXCHANGE_REGISTRY.get(exchange.upper())
    if code is None:
        raise ValueError(f"get_order_exchange_code_for_api: unknown exchange={exchange!r}")
    return code
