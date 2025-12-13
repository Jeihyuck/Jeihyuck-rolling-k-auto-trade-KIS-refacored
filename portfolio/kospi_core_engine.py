from __future__ import annotations

import logging
from datetime import datetime, time, timedelta, timezone
from typing import Any, Dict

from trader import state_manager
from .base_engine import BaseEngine
from strategy.kospi.rebalance import INDEX_CODE, build_target_allocations, evaluate_regime
from strategy.kospi.signals import execute_rebalance

logger = logging.getLogger(__name__)
KST = timezone(timedelta(hours=9))
INTRADAY_DROP_LIMIT = -2.0


class KospiCoreEngine(BaseEngine):
    def __init__(self, capital: float, top_n: int = 100, rebalance_days: int = 30) -> None:
        super().__init__("kospi_core", capital)
        self.top_n = top_n
        self.rebalance_days = rebalance_days
        self._last_rebalance: datetime | None = self._load_last_rebalance()

    def _load_last_rebalance(self) -> datetime | None:
        _, _, meta = state_manager.load_state(self.name, include_meta=True)
        ts = meta.get("last_rebalance") if isinstance(meta, dict) else None
        if not ts:
            return None
        try:
            return datetime.fromisoformat(ts)
        except Exception:
            logger.warning("[KOSPI_CORE] invalid last_rebalance in state: %s", ts)
            return None

    def _should_rebalance(self) -> bool:
        if self._last_rebalance is None:
            return True
        return datetime.now() - self._last_rebalance >= timedelta(days=self.rebalance_days)

    def _current_index_change_pct(self) -> float | None:
        try:
            from rolling_k_auto_trade_api.kis_api import get_price_quote

            quote = get_price_quote(INDEX_CODE)
            current = float(quote.get("stck_prpr") or quote.get("prpr") or 0)
            prev_close = float(quote.get("prdy_clpr") or 0)
            if current and prev_close:
                return (current / prev_close - 1) * 100
        except Exception:
            logger.exception("[KOSPI_CORE] failed to fetch live index quote")
        return None

    def _buys_permitted(self, regime: Dict[str, Any]) -> bool:
        if not regime.get("regime_on"):
            self._log("[KOSPI_CORE][BUYS] blocked (regime OFF)")
            return False

        now = datetime.now(tz=KST).time()
        if now < time(9, 30):
            self._log("[KOSPI_CORE][BUYS] blocked (pre-open window)")
            return False

        daily_change = float(regime.get("daily_change_pct") or 0)
        if daily_change <= INTRADAY_DROP_LIMIT:
            self._log(
                f"[KOSPI_CORE][BUYS] blocked (prev change {daily_change:.2f}% <= {INTRADAY_DROP_LIMIT}%)"
            )
            return False

        live_change = self._current_index_change_pct()
        if live_change is not None and live_change <= INTRADAY_DROP_LIMIT:
            self._log(
                f"[KOSPI_CORE][BUYS] blocked (live change {live_change:.2f}% <= {INTRADAY_DROP_LIMIT}%)"
            )
            return False

        return True

    def rebalance_if_needed(self) -> Dict[str, Any]:
        if not self._should_rebalance():
            return {"status": "skip"}
        regime = evaluate_regime()
        allow_buys = self._buys_permitted(regime)
        if not regime.get("regime_on"):
            targets: list[dict[str, float]] = []
            self._log("[KOSPI_CORE][REGIME] OFF → liquidate positions")
        else:
            targets, meta = build_target_allocations(self.capital, self.top_n)
            self._log(f"[KOSPI_CORE][SELECTION] selected={meta.get('selected')}")

        fills = execute_rebalance(targets, self.capital, self.tag, allow_buys=allow_buys)
        self._last_rebalance = datetime.now()
        holding, traded, _ = state_manager.load_state(self.name, include_meta=True)
        state_manager.save_state(
            self.name,
            holding,
            traded,
            meta={"last_rebalance": self._last_rebalance.isoformat()},
        )
        self._log(f"[KOSPI_CORE][PORTFOLIO] targets={len(targets)} fills={len(fills)}")
        return {"targets": targets, "fills": fills, "regime": regime}

    def trade_loop(self) -> Dict[str, Any]:
        return self.rebalance_if_needed()
