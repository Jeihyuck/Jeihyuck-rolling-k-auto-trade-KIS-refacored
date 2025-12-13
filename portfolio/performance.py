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
    """Aggregate realized/unrealized marks for portfolio-level observability."""

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

    def snapshot(self, total_capital: float, allocations: Dict[str, float]) -> Dict[str, Any]:
        """Return a portfolio-wide PnL snapshot.

        total_capital: configured base capital (used for pct math)
        allocations: engine name -> capital ratio (for reporting only)
        """

        cash = float(inquire_cash_balance())
        positions = self._mark_positions()
        equity_value = sum(p.market_value for p in positions)
        unrealized = sum(p.unrealized_pnl for p in positions)
        total_value = cash + equity_value
        pnl = total_value - float(total_capital)
        pnl_pct = (pnl / float(total_capital) * 100) if total_capital else 0.0

        logger.info(
            "[PORTFOLIO][PERF] total=%.0f cash=%.0f equity=%.0f pnl=%.0f (%.2f%%)",
            total_value,
            cash,
            equity_value,
            pnl,
            pnl_pct,
        )

        return {
            "cash": cash,
            "equity_value": equity_value,
            "total_value": total_value,
            "unrealized": unrealized,
            "pnl": pnl,
            "pnl_pct": pnl_pct,
            "positions": [p.__dict__ for p in positions],
            "allocations": allocations,
        }
