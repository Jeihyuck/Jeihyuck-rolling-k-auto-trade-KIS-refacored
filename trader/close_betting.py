import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from .kis_wrapper import KisAPI

logger = logging.getLogger(__name__)


class CloseBettingEngine:
    def __init__(
        self,
        topn: int,
        cap_fraction: float,
        min_ret_pct: float,
        max_pullback_pct: float,
        min_vol_spike: float,
        price_fetcher: Callable[[KisAPI, str], Optional[float]],
        daily_ctx_fetcher: Callable[[KisAPI, str, float], Dict[str, Any]],
        intraday_ctx_fetcher: Callable[[KisAPI, str, Optional[float]], Dict[str, Any]],
        qty_calculator: Callable[[KisAPI, str, int, Optional[float]], int],
        buyer: Callable[[KisAPI, str, int, Optional[int]], Dict[str, Any]],
        state_initializer: Callable[[KisAPI, Dict[str, Any], str, float, int, Optional[Any], Optional[Any], Any], None],
        trade_logger: Callable[[dict], None],
    ) -> None:
        self.topn = topn
        self.cap_fraction = cap_fraction
        self.min_ret_pct = min_ret_pct
        self.max_pullback_pct = max_pullback_pct
        self.min_vol_spike = min_vol_spike
        self._price_fetcher = price_fetcher
        self._daily_ctx_fetcher = daily_ctx_fetcher
        self._intraday_ctx_fetcher = intraday_ctx_fetcher
        self._qty_calculator = qty_calculator
        self._buyer = buyer
        self._state_initializer = state_initializer
        self._trade_logger = trade_logger
        self.candidates: List[Dict[str, Any]] = []
        self.entered: Dict[str, Any] = {}

    def _score_candidate(self, daily_ctx: Dict[str, Any], intraday_ctx: Dict[str, Any]) -> float:
        ret = daily_ctx.get("ret_today_pct") or 0.0
        vol_ratio = daily_ctx.get("volume_ratio") or 0.0
        from_high = intraday_ctx.get("from_high_pct") or 0.0
        m5 = daily_ctx.get("ma5") or 0.0
        m20 = daily_ctx.get("ma20") or 0.0
        trend_bonus = 2.0 if (m5 and m20 and m5 > m20) else 0.0
        location_score = max(0.0, 5.0 - from_high)
        return ret * 0.6 + vol_ratio * 3.0 + location_score + trend_bonus

    def scan_candidates(
        self,
        kis: KisAPI,
        now_dt: datetime,
        universe: Dict[str, Dict[str, Any]],
        holding: Dict[str, Any],
        traded: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        self.candidates = []
        for code, info in universe.items():
            if code in holding or code in traded:
                continue
            try:
                price = self._price_fetcher(kis, code)
            except Exception:
                price = None
            if price is None or price <= 0:
                continue
            daily_ctx = self._daily_ctx_fetcher(kis, code, price)
            intraday_ctx = self._intraday_ctx_fetcher(kis, code, prev_high=info.get("prev_high"))

            ret_pct = daily_ctx.get("ret_today_pct")
            if ret_pct is None or ret_pct < self.min_ret_pct:
                continue
            vol_ratio = daily_ctx.get("volume_ratio") or 0.0
            if vol_ratio < self.min_vol_spike:
                continue
            from_high = intraday_ctx.get("from_high_pct")
            if from_high is not None and from_high > self.max_pullback_pct:
                continue
            if intraday_ctx.get("vwap") and intraday_ctx.get("last_close"):
                if intraday_ctx["last_close"] < intraday_ctx["vwap"]:
                    continue
            if daily_ctx.get("ma5") and daily_ctx.get("ma20"):
                if daily_ctx["ma5"] <= daily_ctx["ma20"]:
                    continue

            score = self._score_candidate(daily_ctx, intraday_ctx)
            self.candidates.append(
                {
                    "code": code,
                    "name": info.get("name"),
                    "price": price,
                    "score": score,
                    "daily_ctx": daily_ctx,
                    "intraday_ctx": intraday_ctx,
                }
            )
        picked = sorted(self.candidates, key=lambda x: x.get("score", 0), reverse=True)[: self.topn]
        if picked:
            msg = ", ".join([f"{c.get('code')}({c.get('score'):.2f})" for c in picked])
            logger.info("[CLOSE-BET-SCAN] %d개 후보 정렬 결과: %s", len(picked), msg)
        else:
            logger.info("[CLOSE-BET-SCAN] 종가 베팅 후보 없음")
        return picked

    def enter_close_bets(
        self,
        kis: KisAPI,
        now_dt: datetime,
        capital_active: int,
        holding: Dict[str, Any],
        traded: Dict[str, Any],
        can_buy: bool,
    ) -> None:
        if not can_buy:
            logger.info("[CLOSE-BET-ENTRY] 예수금 부족 → 종가 베팅 매수 스킵")
            return
        if not self.candidates:
            return
        per_notional = int(max(0, capital_active * self.cap_fraction) / max(1, len(self.candidates)))
        for cand in self.candidates[: self.topn]:
            code = cand.get("code")
            if not code or code in holding or code in traded:
                continue
            qty = self._qty_calculator(kis, code, per_notional, ref_price=cand.get("price"))
            if qty <= 0:
                continue
            result = self._buyer(kis, code, qty, limit_price=int(cand.get("price") or 0))
            self._state_initializer(
                kis,
                holding,
                code,
                float(cand.get("price") or 0.0),
                int(qty),
                None,
                cand.get("price"),
                strategy="CLOSE_BET",
            )
            traded[code] = {
                "buy_time": now_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "qty": int(qty),
                "price": float(cand.get("price") or 0.0),
                "strategy": "CLOSE_BET",
            }
            logger.info(
                "[CLOSE-BET-ENTRY] code=%s qty=%s price=%s score=%.2f",
                code,
                qty,
                cand.get("price"),
                cand.get("score", 0.0),
            )
            self._trade_logger(
                {
                    "datetime": now_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "code": code,
                    "name": cand.get("name"),
                    "qty": int(qty),
                    "side": "BUY",
                    "price": cand.get("price"),
                    "amount": int(qty) * int(cand.get("price") or 0),
                    "strategy": "종가 베팅",
                    "result": result,
                }
            )
