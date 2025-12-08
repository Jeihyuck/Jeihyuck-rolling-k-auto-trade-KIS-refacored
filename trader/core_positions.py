import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from .kis_wrapper import KisAPI

logger = logging.getLogger(__name__)


class CorePositionEngine:
    def __init__(
        self,
        box_range_pct: float,
        breakout_pct: float,
        min_vol_spike: float,
        max_fraction: float,
        weight_one: float,
        daily_candle_fetcher: Callable[[KisAPI, str, int], List[Dict[str, Any]]],
        qty_calculator: Callable[[KisAPI, str, int, Optional[float]], int],
        price_fetcher: Callable[[KisAPI, str], Optional[float]],
        buyer: Callable[[KisAPI, str, int, Optional[int]], Dict[str, Any]],
        state_initializer: Callable[[KisAPI, Dict[str, Any], str, float, int, Optional[Any], Optional[Any], Any], None],
        trade_logger: Callable[[dict], None],
        tzinfo,
    ) -> None:
        self.box_range_pct = box_range_pct
        self.breakout_pct = breakout_pct
        self.min_vol_spike = min_vol_spike
        self.max_fraction = max_fraction
        self.weight_one = weight_one
        self._daily_candle_fetcher = daily_candle_fetcher
        self._qty_calculator = qty_calculator
        self._price_fetcher = price_fetcher
        self._buyer = buyer
        self._state_initializer = state_initializer
        self._trade_logger = trade_logger
        self._tzinfo = tzinfo
        self.candidates: List[Dict[str, Any]] = []

    def _is_core_candidate(self, kis: KisAPI, code: str) -> Dict[str, Any]:
        ctx: Dict[str, Any] = {"ok": False}
        try:
            candles = self._daily_candle_fetcher(kis, code, 220)
        except Exception as e:
            return {"ok": False, "reason": f"fetch_fail:{e}"}
        if not candles or len(candles) < 200:
            return {"ok": False, "reason": "not_enough_candles"}
        today = datetime.now(self._tzinfo).strftime("%Y%m%d")
        completed = list(candles)
        if completed and str(completed[-1].get("date")) == today:
            completed = completed[:-1]
        if len(completed) < 200:
            return {"ok": False, "reason": "not_enough_completed"}
        closes = [float(c.get("close") or 0.0) for c in completed if c.get("close")]
        highs = [float(c.get("high") or 0.0) for c in completed if c.get("high")]
        opens = [float(c.get("open") or 0.0) for c in completed if c.get("open")]
        vols = [float(c.get("volume") or 0.0) for c in completed if c.get("volume")]
        if len(closes) < 200:
            return {"ok": False, "reason": "close_short"}
        ma200 = sum(closes[-200:]) / 200.0
        ctx["ma200"] = ma200
        last_close = closes[-1]
        last_open = opens[-1] if opens else last_close
        box_high = max(closes[-40:])
        box_low = min(closes[-40:])
        box_range_pct = (box_high - box_low) / box_low * 100.0 if box_low else 0.0
        ctx["box_range_pct"] = box_range_pct
        volume_ratio = 0.0
        if vols and len(vols) >= 21:
            volume_ratio = (vols[-1] / (sum(vols[-21:-1]) / 20.0)) if (sum(vols[-21:-1]) > 0) else 0.0
        ctx["volume_ratio"] = volume_ratio
        breakout = (
            box_range_pct <= self.box_range_pct
            and last_close >= ma200 * (1 + self.breakout_pct / 100.0)
            and last_close >= last_open * (1 + self.breakout_pct / 100.0)
            and volume_ratio >= self.min_vol_spike
        )
        near_ma200 = abs(last_close - ma200) / ma200 * 100.0 <= self.box_range_pct
        ctx["near_ma200"] = near_ma200
        ctx["breakout"] = breakout
        ctx["ok"] = breakout and near_ma200
        return ctx

    def scan(self, kis: KisAPI, universe: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        self.candidates = []
        for code, info in universe.items():
            ctx = self._is_core_candidate(kis, code)
            if ctx.get("ok"):
                self.candidates.append({"code": code, "name": info.get("name"), "ctx": ctx})
        if self.candidates:
            logger.info(
                "[CORE-SCAN] 코어 포지션 후보 %d종목 탐색 완료: %s",
                len(self.candidates),
                ",".join([c.get("code", "") for c in self.candidates]),
            )
        return self.candidates

    def enter(
        self, kis: KisAPI, capital_active: int, holding: Dict[str, Any], traded: Dict[str, Any], can_buy: bool
    ) -> None:
        if not self.candidates or capital_active <= 0 or not can_buy:
            if not can_buy:
                logger.info("[CORE-ENTRY] 예수금 부족 → 코어 포지션 신규 매수 스킵")
            return
        max_core_cap = int(capital_active * self.max_fraction)
        per_notional = int(max_core_cap * self.weight_one)
        for cand in self.candidates:
            code = cand.get("code")
            if not code or code in holding or code in traded:
                continue
            qty = self._qty_calculator(kis, code, per_notional)
            if qty <= 0:
                continue
            price = self._price_fetcher(kis, code)
            result = self._buyer(kis, code, qty, limit_price=int(price or 0))
            self._state_initializer(
                kis, holding, code, float(price or 0.0), int(qty), None, price, strategy="CORE"
            )
            traded[code] = {
                "buy_time": datetime.now(self._tzinfo).strftime("%Y-%m-%d %H:%M:%S"),
                "qty": int(qty),
                "price": float(price or 0.0),
                "strategy": "CORE",
            }
            logger.info(
                "[CORE-ENTRY] code=%s qty=%s price=%s ctx=%s",
                code,
                qty,
                price,
                cand.get("ctx"),
            )
            self._trade_logger(
                {
                    "datetime": datetime.now(self._tzinfo).strftime("%Y-%m-%d %H:%M:%S"),
                    "code": code,
                    "name": cand.get("name"),
                    "qty": int(qty),
                    "side": "BUY",
                    "price": price,
                    "amount": int(qty) * int(price or 0),
                    "strategy": "코어 포지션",
                    "result": result,
                }
            )
