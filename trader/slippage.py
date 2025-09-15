"""
슬리피지 가드 (매수/매도 공용)
- 목표가 대비 허용 슬리피지(비율 + 틱수 상한)를 적용해 즉시 체결 가능한 지정가(IOC/FAK)에 사용할 가격을 산출.
- KOSPI/KOSDAQ 기본 호가단위 테이블 포함(로컬 계산). rkmax_utils.get_tick_size가 있으면 우선 사용.
- 모든 의사결정은 사유코드와 함께 반환/로깅하여 사후 분석이 가능하도록 구성.

사용 예:
    from trader.slippage import SlippageGuard, Quote
    guard = SlippageGuard(max_pct=0.0025, max_extra_ticks=2)
    px, info = guard.decide_buy(Quote(code="035720", bid=12350, ask=12360, last=12355), target_px=12340)
    if px is None:
        log.info("[SKIP][%s] %s", info["code"], info["reason"])  # 가격 과도 등으로 주문 보류
    else:
        place_limit_ioc(code, qty, price=px)  # px을 지정가로 사용
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple
import logging

log = logging.getLogger(__name__)

# ---------------------------------------
# 틱사이즈 유틸
# ---------------------------------------
try:
    # 있으면 우선 사용 (종목/시장 구분 정밀)
    from .rkmax_utils import get_tick_size as _rk_tick
except Exception:  # pragma: no cover
    _rk_tick = None  # type: ignore


def _fallback_tick_size(price: float) -> int:
    """KOSPI/KOSDAQ 공통 기본 호가단위 (원)
    - <1,000: 1원
    - 1,000~4,990: 5원
    - 5,000~9,990: 10원
    - 10,000~49,950: 50원
    - 50,000~99,950: 100원
    - ≥100,000: 500원
    """
    if price < 1000:
        return 1
    if price < 5000:
        return 5
    if price < 10000:
        return 10
    if price < 50000:
        return 50
    if price < 100000:
        return 100
    return 500


def tick_size(price: float) -> int:
    if _rk_tick is not None:
        try:
            return int(_rk_tick(price))
        except Exception:
            pass
    return _fallback_tick_size(price)


def round_to_tick(price: float, tick: Optional[int] = None, *, up: bool = False, down: bool = False) -> float:
    """호가단위에 맞춰 반올림. up/down 중 하나를 True로 주면 그 방향으로 올림/내림.
    아무 플래그도 없으면 가장 가까운 틱으로 반올림.
    """
    t = tick or tick_size(price)
    if t <= 0:
        return price
    q, r = divmod(price, t)
    if up and r:
        return (q + 1) * t
    if down:
        return q * t
    # 일반 반올림
    return (q + (1 if r >= t / 2 else 0)) * t


# ---------------------------------------
# 시세/의사결정 모델
# ---------------------------------------
@dataclass
class Quote:
    code: str
    bid: Optional[float]  # 최우선 매수호가
    ask: Optional[float]  # 최우선 매도호가
    last: Optional[float] = None


@dataclass
class Decision:
    ok: bool
    price: Optional[float]
    reason: str
    detail: Dict[str, Any]

    def as_tuple(self) -> Tuple[Optional[float], Dict[str, Any]]:
        return self.price, {"reason": self.reason, **self.detail}


class SlippageGuard:
    """슬리피지 제한(비율 + 틱 수) 기반으로 즉시 체결 가능한 지정가를 산출.

    매수(BUY): cap = min(target * (1+max_pct), target + tick*max_extra_ticks)
               ask <= cap 이면 지정가=cap (또는 ask에 맞춰 더 타이트하게 낼 수도 있으나, cap이 안전)
    매도(SELL): floor = max(target * (1-max_pct), target - tick*max_extra_ticks)
               bid >= floor 이면 지정가=floor (즉시 체결 시 실제 체결은 bid ≥ floor에서 발생)
    """

    def __init__(self, max_pct: float, max_extra_ticks: int) -> None:
        self.max_pct = float(max_pct)
        self.max_extra_ticks = int(max_extra_ticks)

    # ---------- 내부 계산 ----------
    def _cap_prices(self, side: str, target_px: float) -> Dict[str, float]:
        t = tick_size(target_px)
        cap_by_pct = target_px * (1 + self.max_pct)
        cap_by_tick = target_px + (t * self.max_extra_ticks)
        floor_by_pct = target_px * (1 - self.max_pct)
        floor_by_tick = target_px - (t * self.max_extra_ticks)
        return {
            "tick": float(t),
            "cap": float(min(cap_by_pct, cap_by_tick)),
            "floor": float(max(floor_by_pct, floor_by_tick)),
        }

    # ---------- 공개 API ----------
    def decide_buy(self, q: Quote, target_px: float) -> Tuple[Optional[float], Dict[str, Any]]:
        """매수 지정가 산출. 허용 슬리피지 초과면 (None, 사유) 반환."""
        if q.ask is None:
            return None, {"side": "BUY", "code": q.code, "reason": "NO_ASK"}
        caps = self._cap_prices("BUY", target_px)
        cap = caps["cap"]
        if q.ask <= cap:
            price = round_to_tick(cap, int(caps["tick"]))  # cap을 지정가로 사용
            info = {
                "side": "BUY", "code": q.code, "reason": "OK",
                "ask": q.ask, "target": target_px, **caps
            }
            log.debug("[SLIP][BUY][%s] ask=%.2f target=%.2f cap=%.2f -> limit=%.2f",
                      q.code, q.ask, target_px, cap, price)
            return price, info
        # 캡 초과로 미체결
        info = {"side": "BUY", "code": q.code, "reason": "OVER_CAP", "ask": q.ask, "target": target_px, **caps}
        log.info("[SLIP][BUY][SKIP][%s] ask=%.2f > cap=%.2f (target=%.2f)", q.code, q.ask, cap, target_px)
        return None, info

    def decide_sell(self, q: Quote, target_px: float) -> Tuple[Optional[float], Dict[str, Any]]:
        """매도 지정가 산출. 허용 슬리피지 초과면 (None, 사유) 반환."""
        if q.bid is None:
            return None, {"side": "SELL", "code": q.code, "reason": "NO_BID"}
        caps = self._cap_prices("SELL", target_px)
        floor = caps["floor"]
        if q.bid >= floor:
            price = round_to_tick(floor, int(caps["tick"]))  # floor를 지정가로 사용
            info = {
                "side": "SELL", "code": q.code, "reason": "OK",
                "bid": q.bid, "target": target_px, **caps
            }
            log.debug("[SLIP][SELL][%s] bid=%.2f target=%.2f floor=%.2f -> limit=%.2f",
                      q.code, q.bid, target_px, floor, price)
            return price, info
        info = {"side": "SELL", "code": q.code, "reason": "UNDER_FLOOR", "bid": q.bid, "target": target_px, **caps}
        log.info("[SLIP][SELL][SKIP][%s] bid=%.2f < floor=%.2f (target=%.2f)", q.code, q.bid, floor, target_px)
        return None, info

    def is_slippage_ok(self, side: str, q: Quote, target_px: float) -> Tuple[bool, Dict[str, Any]]:
        """현재 최우선 호가가 허용 범위 내인지 여부만 판정(가격 산출 X)."""
        if side.upper() == "BUY":
            if q.ask is None:
                return False, {"side": "BUY", "code": q.code, "reason": "NO_ASK"}
            cap = self._cap_prices("BUY", target_px)["cap"]
            return (q.ask <= cap), {"side": "BUY", "code": q.code, "reason": "OK" if q.ask <= cap else "OVER_CAP", "ask": q.ask, "cap": cap}
        else:
            if q.bid is None:
                return False, {"side": "SELL", "code": q.code, "reason": "NO_BID"}
            floor = self._cap_prices("SELL", target_px)["floor"]
            return (q.bid >= floor), {"side": "SELL", "code": q.code, "reason": "OK" if q.bid >= floor else "UNDER_FLOOR", "bid": q.bid, "floor": floor}


# ---------------------------------------
# 간단 테스트
# ---------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    g = SlippageGuard(max_pct=0.0025, max_extra_ticks=2)

    buy_px, info = g.decide_buy(Quote(code="TEST", bid=12350, ask=12360, last=12355), target_px=12340)
    print("BUY", buy_px, info)

    sell_px, info = g.decide_sell(Quote(code="TEST", bid=12350, ask=12360, last=12355), target_px=12370)
    print("SELL", sell_px, info)
