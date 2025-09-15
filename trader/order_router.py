"""
주문 라우터 (IOC 지정가 우선 → 재시도 → 최종 시장가 백업)
- KIS OpenAPI 래퍼(kis_wrapper)와 분리된 '전략 계층' 유틸.
- 슬리피지 가드와 재시도(지수형 백오프 + 지터) 정책을 표준화.
- KIS 오류코드(일부)와 네트워크 예외를 '재시도 가능(RETTRYABLE)'로 처리.

사용 패턴 1) 함수 주입형 (가장 호환성 높음)
    from trader.order_router import place_with_retry
    res = place_with_retry(place_ioc_limit=kis.place_limit_ioc,
                           place_market=kis.place_market,
                           params={"code": code, "side": "BUY", "qty": qty, "price": px},
                           max_attempts=5, base_delay=0.5)

사용 패턴 2) 클래스형 + 슬리피지 가드
    from trader.slippage import SlippageGuard, Quote
    from trader.order_router import OrderRouter

    router = OrderRouter(
        place_limit_ioc=kis.place_limit_ioc,
        place_market=kis.place_market,
        get_quote=kis.get_quote,  # callable: code -> Quote(bid, ask, last)
        slippage=SlippageGuard(max_pct=0.0025, max_extra_ticks=2),
        max_attempts=5,
        backoff_base=0.5,
        market_backup=True,
    )
    res = router.buy(code="035720", qty=10, target_px=12340)

반환 표준(딕셔너리):
    {
      'status': 'ok'|'partial'|'skip'|'fail',
      'order_id': '...',            # 가능 시
      'filled_qty': 10,
      'remaining_qty': 0,
      'reason': 'OK|OVER_CAP|RETRY_EXHAUSTED|...'(skip/fail일 때),
      'last_error': { ... },        # 실패 시 마지막 오류 응답
      'meta': { 'side': 'BUY', 'code': '035720', ... }
    }
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, Any, Optional, Tuple
import random
import time
import logging

from .slippage import SlippageGuard, Quote, round_to_tick

log = logging.getLogger(__name__)

# 재시도 대상(예시): 네트워크/일시적 오류/쿼터 초과/처리중
RETRYABLE_CODES = {
    "TTTC0011U",  # 처리중
    "VTTC0011U",  # 주문 과다/제한
    "VTTC0012U",  # 호가 과다/제한
    "HTS_E999",   # 일반 에러
    "-1",         # 네트워크 등 임의 표준화
}


def _expo_backoff(attempt: int, base: float = 0.5, cap: float = 32.0) -> float:
    """지수형 백오프 + 지터."""
    delay = min(cap, base * (2 ** (attempt - 1)))
    jitter = delay * 0.2 * random.random()
    return delay + jitter


def _normalize_resp(resp: Dict[str, Any]) -> Dict[str, Any]:
    """kis_wrapper 응답을 라우터 표준 키로 최대한 정규화."""
    if resp is None:
        return {"status": "fail", "reason": "NO_RESPONSE"}
    out = dict(resp)
    # status 표준화
    s = out.get("status")
    if s is None:
        # KIS는 체결/접수 결과를 세부 필드로 줄 수 있음. 최소화해서 표준화 시도.
        if out.get("error_code"):
            s = "fail"
        else:
            s = "ok"
    out["status"] = s
    # 수량 표준화
    out.setdefault("filled_qty", out.get("filled") or 0)
    out.setdefault("remaining_qty", out.get("remaining") or 0)
    return out


def place_with_retry(
    *,
    place_ioc_limit: Callable[..., Dict[str, Any]],
    place_market: Callable[..., Dict[str, Any]],
    params: Dict[str, Any],
    max_attempts: int = 5,
    base_delay: float = 0.5,
    market_backup: bool = True,
) -> Dict[str, Any]:
    """IOC 지정가로 우선 시도, 재시도 후 실패 시(옵션) 시장가 백업.
    params: kis_wrapper.place_limit_ioc 서명에 맞는 딕트 (예: code, side, qty, price)
    """
    last_err: Optional[Dict[str, Any]] = None

    for i in range(1, max_attempts + 1):
        try:
            resp = place_ioc_limit(**params)
            norm = _normalize_resp(resp)
            s = str(norm.get("status")).lower()
            if s in {"ok", "partial"}:
                return {**norm, "reason": "OK", "meta": {"attempts": i, **params}}
            # 실패 처리
            err_code = str(norm.get("error_code")) if norm.get("error_code") is not None else None
            last_err = norm
            if err_code in RETRYABLE_CODES:
                d = _expo_backoff(i, base_delay)
                log.warning("[ORDER][RETRY %s/%s] code=%s sleep=%.2fs params=%s",
                            i, max_attempts, err_code, d, {k: v for k, v in params.items() if k != 'price'})
                time.sleep(d)
                continue
            break  # 비재시도 사유 -> 루프 탈출
        except Exception as e:
            last_err = {"error": str(e)}
            d = _expo_backoff(i, base_delay)
            log.exception("[ORDER][EXC][RETRY %s/%s] %s", i, max_attempts, e)
            time.sleep(d)

    # 최종 백업: 시장가
    if market_backup:
        try:
            resp = place_market(**{k: v for k, v in params.items() if k != "price"})
            norm = _normalize_resp(resp)
            s = str(norm.get("status")).lower()
            if s in {"ok", "partial"}:
                return {**norm, "reason": "OK_MARKET_BACKUP", "meta": {"attempts": max_attempts + 1, **params}}
            last_err = norm
        except Exception as e:
            last_err = {"error": str(e)}
    return {"status": "fail", "reason": "RETRY_EXHAUSTED", "last_error": last_err, "meta": params}


# --------------------------------------------------
# 클래스형 라우터: 슬리피지 가드 + 재시도 + 시장가 백업
# --------------------------------------------------
@dataclass
class RouterConfig:
    max_attempts: int = 5
    backoff_base: float = 0.5
    market_backup: bool = True


class OrderRouter:
    def __init__(
        self,
        *,
        place_limit_ioc: Callable[..., Dict[str, Any]],
        place_market: Callable[..., Dict[str, Any]],
        get_quote: Callable[[str], Quote],
        slippage: SlippageGuard,
        max_attempts: int = 5,
        backoff_base: float = 0.5,
        market_backup: bool = True,
    ) -> None:
        self.place_limit_ioc = place_limit_ioc
        self.place_market = place_market
        self.get_quote = get_quote
        self.slip = slippage
        self.cfg = RouterConfig(max_attempts=max_attempts, backoff_base=backoff_base, market_backup=market_backup)

    # -------------- 공용 내부 --------------
    def _ioc(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return place_with_retry(
            place_ioc_limit=self.place_limit_ioc,
            place_market=self.place_market,
            params=params,
            max_attempts=self.cfg.max_attempts,
            base_delay=self.cfg.backoff_base,
            market_backup=self.cfg.market_backup,
        )

    # -------------- 매수/매도 --------------
    def buy(self, code: str, qty: int, target_px: float) -> Dict[str, Any]:
        q = self.get_quote(code)
        limit_px, info = self.slip.decide_buy(q, target_px)
        if limit_px is None:
            return {"status": "skip", "reason": info.get("reason", "OVER_CAP"), "meta": info}
        params = {"code": code, "side": "BUY", "qty": int(qty), "price": float(limit_px)}
        res = self._ioc(params)
        res.setdefault("meta", {}).update({"side": "BUY", "code": code, "qty": qty, "limit_px": limit_px, "target_px": target_px})
        return res

    def sell(self, code: str, qty: int, target_px: float) -> Dict[str, Any]:
        q = self.get_quote(code)
        limit_px, info = self.slip.decide_sell(q, target_px)
        if limit_px is None:
            return {"status": "skip", "reason": info.get("reason", "UNDER_FLOOR"), "meta": info}
        params = {"code": code, "side": "SELL", "qty": int(qty), "price": float(limit_px)}
        res = self._ioc(params)
        res.setdefault("meta", {}).update({"side": "SELL", "code": code, "qty": qty, "limit_px": limit_px, "target_px": target_px})
        return res

    # -------------- 강제청산(시장가) --------------
    def force_sell_market(self, code: str, qty: int) -> Dict[str, Any]:
        try:
            resp = self.place_market(code=code, side="SELL", qty=int(qty))
            norm = _normalize_resp(resp)
            s = str(norm.get("status")).lower()
            if s in {"ok", "partial"}:
                return {**norm, "reason": "OK_FORCE_MARKET", "meta": {"side": "SELL", "code": code, "qty": qty}}
            return {"status": "fail", "reason": "FORCE_MARKET_FAIL", "last_error": norm, "meta": {"side": "SELL", "code": code, "qty": qty}}
        except Exception as e:
            return {"status": "fail", "reason": "EXC_FORCE_MARKET", "last_error": {"error": str(e)}, "meta": {"side": "SELL", "code": code, "qty": qty}}


# ---------------------------------------
# 로컬 간단 테스트
# ---------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # 가짜 KIS 함수
    def fake_place_limit_ioc(code: str, side: str, qty: int, price: float) -> Dict[str, Any]:
        # 첫 시도는 실패, 두번째 OK
        fake_place_limit_ioc.calls += 1
        if fake_place_limit_ioc.calls == 1:
            return {"status": "fail", "error_code": "TTTC0011U"}
        return {"status": "ok", "order_id": "A1", "filled_qty": qty, "remaining_qty": 0}

    def fake_place_market(code: str, side: str, qty: int) -> Dict[str, Any]:
        return {"status": "ok", "order_id": "M1", "filled_qty": qty, "remaining_qty": 0}

    def fake_get_quote(code: str) -> Quote:
        return Quote(code=code, bid=12350, ask=12360, last=12355)

    fake_place_limit_ioc.calls = 0

    slip = SlippageGuard(max_pct=0.0025, max_extra_ticks=2)
    router = OrderRouter(
        place_limit_ioc=fake_place_limit_ioc,
        place_market=fake_place_market,
        get_quote=fake_get_quote,
        slippage=slip,
        max_attempts=3,
        backoff_base=0.1,
        market_backup=True,
    )

    print("BUY TEST:", router.buy("TEST", 10, 12340))
    print("SELL TEST:", router.sell("TEST", 10, 12370))
