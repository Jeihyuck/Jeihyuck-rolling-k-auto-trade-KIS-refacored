from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List

from rolling_k_auto_trade_api.kis_api import get_price_quote, inquire_balance, inquire_cash_balance

logger = logging.getLogger(__name__)


@dataclass
class PositionSnapshot:
    code: str
    qty: int
    avg_price: float
    last_price: float
    market_value: float
    unrealized_pnl: float


class PerformanceTracker:
    """Aggregate realized/unrealized marks for portfolio-level observability.

    Engine-level PnL is an attribution estimate using allocation ratios because
    KIS positions are pooled at the account level.
    """

    def __init__(self) -> None:
        self._peak_value: float | None = None
        self._max_drawdown_pct: float = 0.0

    def _mark_positions(self) -> List[PositionSnapshot]:
        positions: List[PositionSnapshot] = []
        for row in inquire_balance():
            code = str(row.get("pdno") or row.get("code") or "").zfill(6)
            qty = int(float(row.get("hldg_qty") or row.get("qty") or 0))
            if qty <= 0:
                continue
            avg_price = float(row.get("pchs_avg_pric") or row.get("avg_price") or 0)
            last_price = float(row.get("prpr") or row.get("stck_prpr") or 0)
            if not last_price:
                try:
                    quote = get_price_quote(code)
                    last_price = float(quote.get("stck_prpr") or quote.get("askp1") or 0)
                except Exception:
                    logger.exception("[PERF] quote fail for %s", code)
                    last_price = 0.0
            market_value = max(last_price, 0.0) * qty
            unrealized_pnl = (last_price - avg_price) * qty if avg_price else 0.0
            positions.append(
                PositionSnapshot(
                    code=code,
                    qty=qty,
                    avg_price=avg_price,
                    last_price=last_price,
                    market_value=market_value,
                    unrealized_pnl=unrealized_pnl,
                )
            )
        return positions

    def snapshot(self, engine_capitals: Dict[str, float]) -> Dict[str, Any]:
        """Return per-engine and portfolio PnL snapshots.

        engine_capitals: engine name -> allocated capital (absolute, not ratio)
        """

        cash = float(inquire_cash_balance())
        positions = self._mark_positions()
        equity_value = sum(p.market_value for p in positions)
        unrealized = sum(p.unrealized_pnl for p in positions)
        total_allocated = sum(engine_capitals.values())
        total_value = cash + equity_value
        pnl = total_value - float(total_allocated)
        pnl_pct = (pnl / float(total_allocated) * 100) if total_allocated else 0.0

        if self._peak_value is None or total_value > self._peak_value:
            self._peak_value = total_value
        drawdown_pct = 0.0
        if self._peak_value:
            drawdown_pct = (total_value / self._peak_value - 1.0) * 100
            self._max_drawdown_pct = min(self._max_drawdown_pct, drawdown_pct)

        engines: Dict[str, Dict[str, Any]] = {}
        for name, cap in engine_capitals.items():
            ratio = cap / total_allocated if total_allocated else 0.0
            engine_cash = cash * ratio
            engine_equity = equity_value * ratio
            engine_value = engine_cash + engine_equity
            engine_pnl = engine_value - cap
            engine_pct = (engine_pnl / cap * 100) if cap else 0.0
            engines[name] = {
                "allocated_capital": cap,
                "cash": engine_cash,
                "equity_value": engine_equity,
                "total_value": engine_value,
                "pnl": engine_pnl,
                "pnl_pct": engine_pct,
            }
            logger.info(
                "[%s][PERF] alloc=%.0f value=%.0f pnl=%.0f (%.2f%%)",
                name.upper(),
                cap,
                engine_value,
                engine_pnl,
                engine_pct,
            )

        logger.info(
            "[PORTFOLIO][PERF] total=%.0f cash=%.0f equity=%.0f pnl=%.0f (%.2f%%)",
            total_value,
            cash,
            equity_value,
            pnl,
            pnl_pct,
        )

        logger.info(
            "[PORTFOLIO][DRAWDOWN] current=%.2f%% max=%.2f%%",
            drawdown_pct,
            self._max_drawdown_pct,
        )

        return {
            "portfolio": {
                "cash": cash,
                "equity_value": equity_value,
                "total_value": total_value,
                "unrealized": unrealized,
                "pnl": pnl,
                "pnl_pct": pnl_pct,
                "drawdown_pct": drawdown_pct,
                "max_drawdown_pct": self._max_drawdown_pct,
            },
            "engines": engines,
            "positions": [p.__dict__ for p in positions],
        }
