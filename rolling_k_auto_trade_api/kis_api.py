# rolling_k_auto_trade_api/kis_api.py
"""
KIS API 모듈 레벨 래퍼

- 외부에서 기대하는 모듈 레벨 심볼(get_price_data, send_order_wrapper, get_cash_balance_wrapper)을 보장.
- 내부 실제 구현은 rolling_k_auto_trade_api.kis_wrapper.KisAPI 로 일원화.
- 싱글턴 인스턴스로 토큰/세션 일관성 유지, 로깅 표준화, 이름 변경에 대비한 fallback 제공.
"""

from __future__ import annotations

import logging
from functools import lru_cache
from typing import Any, Dict, Optional, Tuple

# (옵션) 프로젝트 전역 설정이 있을 수 있으나, 라우팅/헤더는 KisAPI에서 처리.
try:
    from settings import APP_KEY, APP_SECRET, API_BASE_URL, CANO, ACNT_PRDT_CD, KIS_ENV  # noqa: F401
except Exception:
    pass

# ✅ 경로 정정: rolling_k_auto_trade_api.kis_wrapper 에서 가져옵니다.
from rolling_k_auto_trade_api.kis_wrapper import KisAPI

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


@lru_cache(maxsize=1)
def _api() -> KisAPI:
    api = KisAPI()
    logger.info(
        "[kis_api] KisAPI singleton ready (env=%s, base=%s)",
        getattr(api, "env", "unknown"),
        getattr(api, "api_base_url", "unknown"),
    )
    return api


def _call_with_fallback(obj: Any, primary: str, fallbacks: Tuple[str, ...], *args, **kwargs):
    """obj에 primary가 없으면 fallbacks를 순서대로 찾아 호출."""
    if hasattr(obj, primary):
        return getattr(obj, primary)(*args, **kwargs)
    for name in fallbacks:
        if hasattr(obj, name):
            logger.debug("[kis_api][fallback] using %s instead of %s", name, primary)
            return getattr(obj, name)(*args, **kwargs)
    raise AttributeError(f"{obj.__class__.__name__} has no method: {primary} (or fallbacks: {fallbacks})")


# ------------------------
# 공개 계약 심볼
# ------------------------
def get_price_data(
    code: str,
    interval: str = "1m",
    lookback: int = 120,
    market: Optional[str] = None,
) -> Any:
    """
    시세 데이터 조회(최소 보장: 현재가 기반 구조). 자세한 분봉/일봉은 KisAPI 구현에 위임.
    """
    logger.info("[kis_api.get_price_data] code=%s interval=%s lookback=%s market=%s",
                code, interval, lookback, market)
    try:
        # KisAPI에 구현되어 있으면 그대로 사용
        return _call_with_fallback(
            _api(),
            "get_price_data",
            ("fetch_price_data", "get_prices"),
            code=code,
            interval=interval,
            lookback=lookback,
            market=market,
        )
    except AttributeError:
        # 최소 보장: 현재가 1포인트를 리스트로 감싸서 반환
        price = get_current_price(code)
        from datetime import datetime, timezone
        return [{"ts": datetime.now(timezone.utc).isoformat(), "price": price}]
    except Exception as e:
        logger.exception("[kis_api.get_price_data][ERROR] code=%s: %s", code, e)
        raise


def get_current_price(code: str) -> float:
    """현재가 단건 조회(편의 함수)."""
    logger.debug("[kis_api.get_current_price] code=%s", code)
    try:
        return float(_call_with_fallback(_api(), "get_current_price", ("price",), code))
    except Exception as e:
        logger.exception("[kis_api.get_current_price][ERROR] code=%s: %s", code, e)
        raise


def send_order_wrapper(
    code: str,
    qty: int,
    side: str,
    price: float = 0.0,
    order_type: Optional[str] = None,
    tr_id: Optional[str] = None,
    rqest_id: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    현금 주문 래퍼 (BUY/SELL)
    - side: "BUY" | "SELL"
    - order_type: KIS 주문유형 코드 (예: "01" 시장가, "00" 지정가 등)
    """
    logger.info("[kis_api.send_order_wrapper] %s %s qty=%s price=%s order_type=%s tr_id=%s",
                side, code, qty, price, order_type, tr_id)
    try:
        return _call_with_fallback(
            _api(),
            "order_cash",
            ("send_order_cash", "send_order"),   # ← kis_wrapper의 실제 메서드명 대응
            code=code,
            qty=qty,
            side=side,
            price=price,
            order_type=order_type,
            tr_id=tr_id,
            rqest_id=rqest_id,
            extra=extra or {},
        )
    except Exception as e:
        logger.exception("[kis_api.send_order_wrapper][ERROR] %s %s qty=%s: %s",
                         side, code, qty, e)
        raise


def get_cash_balance_wrapper() -> Dict[str, Any]:
    """예수금/현금성 잔고 조회 래퍼."""
    logger.info("[kis_api.get_cash_balance_wrapper]")
    try:
        return _call_with_fallback(
            _api(),
            "get_cash_balance",
            ("inquire_cash_balance", "cash_balance", "balance_cash"),
        )
    except Exception as e:
        logger.exception("[kis_api.get_cash_balance_wrapper][ERROR] %s", e)
        raise


# ------------------------
# 선택 편의/호환 API
# ------------------------
def is_market_open() -> bool:
    try:
        return bool(_call_with_fallback(_api(), "is_market_open", ("market_open",)))
    except AttributeError:
        logger.debug("[kis_api.is_market_open] not implemented → False")
        return False
    except Exception as e:
        logger.exception("[kis_api.is_market_open][ERROR] %s", e)
        raise


# 하위호환 별칭
send_order = send_order_wrapper
get_cash_balance = get_cash_balance_wrapper

__all__ = [
    "get_price_data",
    "send_order_wrapper",
    "get_cash_balance_wrapper",
    "get_current_price",
    "is_market_open",
    "send_order",
    "get_cash_balance",
]
