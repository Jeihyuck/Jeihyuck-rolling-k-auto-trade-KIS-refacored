# -*- coding: utf-8 -*-
"""미국주식 심볼/거래소 레지스트리.

- ticker는 문자열 그대로 사용 (국내 6자리 코드 zfill 금지)
- NASDAQ/NYSE/AMEX 거래소 코드 변환
- KIS quote용 exchange code와 order용 exchange code 분리
"""
from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# 거래소 레지스트리
# KIS 해외주식 API에서 사용하는 코드 매핑
# ---------------------------------------------------------------------------
US_EXCHANGE_REGISTRY: dict[str, dict[str, str]] = {
    "NASDAQ": {
        "quote_excd": "NAS",
        "order_exchange_code": "NASD",
        "timezone": "America/New_York",
    },
    "NYSE": {
        "quote_excd": "NYS",
        "order_exchange_code": "NYSE",
        "timezone": "America/New_York",
    },
    "AMEX": {
        "quote_excd": "AMS",
        "order_exchange_code": "AMEX",
        "timezone": "America/New_York",
    },
}

# symbol → 기본 거래소 매핑 (known symbols)
# 신규 심볼은 us_universe.yaml 로드 후 보완
_SYMBOL_EXCHANGE_MAP: dict[str, str] = {
    # Core ETF
    "SPY": "NYSE",
    "QQQ": "NASDAQ",
    "QQQM": "NASDAQ",
    "SMH": "NASDAQ",
    "SOXX": "NASDAQ",
    # Mega AI
    "NVDA": "NASDAQ",
    "MSFT": "NASDAQ",
    "AAPL": "NASDAQ",
    "AMZN": "NASDAQ",
    "META": "NASDAQ",
    "GOOGL": "NASDAQ",
    "AVGO": "NASDAQ",
    "TSM": "NYSE",
    # AI Infra
    "VRT": "NYSE",
    "CRDO": "NASDAQ",
    "CIEN": "NYSE",
    "MRVL": "NASDAQ",
    "NVTS": "NASDAQ",
    "VIAVI": "NASDAQ",
    "LITE": "NASDAQ",
    "COHR": "NYSE",
    "AAOI": "NASDAQ",
    # Large-cap NYSE
    "IBM": "NYSE",
    "ACN": "NYSE",
    "MA": "NYSE",
    "V": "NYSE",
    "WMT": "NYSE",
    "HD": "NYSE",
    "CAT": "NYSE",
    "LLY": "NYSE",
    "NVO": "NYSE",
    "UBER": "NYSE",
    "BE": "NYSE",
    "GEV": "NYSE",
    "ETN": "NYSE",
    "HUBB": "NYSE",
    "PWR": "NYSE",
    "VST": "NYSE",
    "NRG": "NYSE",
    "DELL": "NYSE",
    "HPE": "NYSE",
    "GLW": "NYSE",
    "TEL": "NYSE",
    "APH": "NYSE",
    "JPM": "NYSE",
    # Large-cap NASDAQ
    "COST": "NASDAQ",
    "TSLA": "NASDAQ",
    "NFLX": "NASDAQ",
    "ABNB": "NASDAQ",
    "COIN": "NASDAQ",
    "HOOD": "NASDAQ",
    "MSTR": "NASDAQ",
    "CEG": "NASDAQ",
    "AMD": "NASDAQ", "AMAT": "NASDAQ", "ALAB": "NASDAQ", "CRWD": "NASDAQ",
    "DDOG": "NASDAQ", "INTC": "NASDAQ", "MDB": "NASDAQ", "MU": "NASDAQ",
    "PANW": "NASDAQ", "TXN": "NASDAQ",
    "PLTR": "NASDAQ",
    "DIA": "NYSE", "IWM": "NYSE", "RSP": "NYSE",
    "XLK": "NYSE", "XLV": "NYSE",
}

_VALID_SYMBOL_RE = re.compile(r"^[A-Z]{1,5}$")

# 미국장 거래소 정규화 매핑
# KIS API output에서 반환되는 다양한 변형을 표준 exchange로 변환
_US_EXCHANGE_NORMALIZE_MAP: dict[str, str] = {
    # NASDAQ variants
    "NASD": "NASDAQ",
    "NAS": "NASDAQ",
    "NASDAQ": "NASDAQ",
    # NYSE variants
    "NYS": "NYSE",
    "NYSE": "NYSE",
    # AMEX variants
    "AMS": "AMEX",
    "ASE": "AMEX",
    "AMEX": "AMEX",
}


def normalize_us_exchange(exchange: str) -> str:
    """미국장 거래소 코드를 표준화.
    
    KIS API에서 반환되는 다양한 exchange code를 표준값으로 정규화합니다:
    - NASD, NAS → NASDAQ
    - NYS → NYSE
    - AMS, ASE → AMEX
    
    Args:
        exchange: KIS API에서 반환된 거래소 코드
        
    Returns:
        표준화된 거래소 코드 (NASDAQ, NYSE, AMEX)
        
    Raises:
        ValueError: 인식되지 않는 거래소 코드
        
    Examples:
        >>> normalize_us_exchange("NASD")
        "NASDAQ"
        >>> normalize_us_exchange("NYS")
        "NYSE"
    """
    if not exchange or not isinstance(exchange, str):
        raise ValueError(f"normalize_us_exchange: invalid exchange={exchange!r}")
    
    normalized = exchange.strip().upper()
    
    result = _US_EXCHANGE_NORMALIZE_MAP.get(normalized)
    if result is None:
        raise ValueError(f"normalize_us_exchange: unknown exchange={normalized!r}")
    
    return result


def normalize_symbol(symbol: str) -> str:
    """ticker를 대문자로 정규화. 빈 값/비정상 입력은 ValueError."""
    if not symbol or not isinstance(symbol, str):
        raise ValueError(f"normalize_symbol: invalid symbol={symbol!r}")
    normalized = symbol.strip().upper()
    if not _VALID_SYMBOL_RE.match(normalized):
        raise ValueError(f"normalize_symbol: bad format symbol={normalized!r}")
    return normalized


def resolve_exchange(symbol: str) -> str:
    """심볼의 거래소를 반환. 미등록 심볼이면 ValueError."""
    sym = normalize_symbol(symbol)
    exchange = _SYMBOL_EXCHANGE_MAP.get(sym)
    if exchange is None:
        raise ValueError(f"resolve_exchange: unknown symbol={sym!r}")
    import logging as _logging
    _logging.getLogger(__name__).debug(
        "[US_SYMBOLS][EXCHANGE_RESOLVE] symbol=%s exchange=%s", sym, exchange
    )
    return exchange


def get_quote_exchange_code(exchange: str) -> str:
    """KIS quote API용 exchange code (예: NAS, NYS, AMS)."""
    normalized = normalize_us_exchange(exchange)
    info = US_EXCHANGE_REGISTRY.get(normalized)
    if info is None:
        raise ValueError(f"get_quote_exchange_code: unknown exchange={exchange!r}")
    return info["quote_excd"]


def get_order_exchange_code(exchange: str) -> str:
    """KIS order API용 exchange code (예: NASD, NYSE, AMEX)."""
    normalized = normalize_us_exchange(exchange)
    info = US_EXCHANGE_REGISTRY.get(normalized)
    if info is None:
        raise ValueError(f"get_order_exchange_code: unknown exchange={exchange!r}")
    return info["order_exchange_code"]


def reject_unknown_symbol(symbol: str) -> None:
    """심볼이 레지스트리에 없으면 ValueError 발생."""
    sym = normalize_symbol(symbol)
    if sym not in _SYMBOL_EXCHANGE_MAP:
        raise ValueError(f"reject_unknown_symbol: not in registry symbol={sym!r}")


def register_symbol(symbol: str, exchange: str) -> None:
    """런타임에 심볼을 레지스트리에 추가 (universe 로드 시 사용)."""
    sym = normalize_symbol(symbol)
    exc = normalize_us_exchange(exchange)
    if exc not in US_EXCHANGE_REGISTRY:
        raise ValueError(f"register_symbol: unknown exchange={exc!r}")
    _SYMBOL_EXCHANGE_MAP[sym] = exc


def list_known_symbols() -> list[str]:
    """등록된 모든 심볼 목록."""
    return sorted(_SYMBOL_EXCHANGE_MAP.keys())


def is_known_symbol(symbol: str) -> bool:
    """심볼이 레지스트리에 존재하는지 확인."""
    try:
        sym = normalize_symbol(symbol)
    except ValueError:
        return False
    return sym in _SYMBOL_EXCHANGE_MAP
