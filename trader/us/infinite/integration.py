from __future__ import annotations

import logging
import uuid
from dataclasses import replace
from datetime import date
from typing import Any, Callable

from .config import InfiniteConfig
from .models import Action, InfiniteState, PositionSnapshot, Status
from .repository import InfiniteRepository
from .risk_adapter import assess_market_risk
from .strategy import evaluate

logger = logging.getLogger(__name__)


def owns_symbol(symbol: str, config: InfiniteConfig | None = None) -> bool:
    config = config or InfiniteConfig.from_env()
    return bool(config.enabled and str(symbol or "").upper().strip() == config.symbol)


def exclude_owned(rows: list[dict], config: InfiniteConfig | None = None) -> list[dict]:
    config = config or InfiniteConfig.from_env()
    if not config.enabled:
        return rows
    return [row for row in rows if not owns_symbol(row.get("symbol") or row.get("code"), config)]


def _position(raw: dict | None, price: float) -> PositionSnapshot:
    raw = raw or {}
    def number(*keys: str) -> float:
        for key in keys:
            try:
                value = float(str(raw.get(key, "")).replace(",", ""))
                if value > 0: return value
            except (TypeError, ValueError): pass
        return 0.0
    return PositionSnapshot(
        qty=int(number("sellable_qty", "orderable_qty", "qty", "quantity")),
        average_price=number("avg_price_usd", "avg_price", "avg_cost", "pchs_avg_pric"),
        price=price or number("current_price", "last_price", "price", "ovrs_now_pric"),
        exchange=str(raw.get("exchange") or "NASDAQ"),
    )


def run_sleeve(*, positions: list[dict], price: float, trading_date: date, overlay: dict,
               repository: InfiniteRepository | None = None,
               route: Callable[[dict], dict] | None = None) -> dict[str, Any]:
    """Exception-isolated boundary. OFF returns before any DB access."""
    config = InfiniteConfig.from_env()
    if not config.enabled:
        return {"status": "OFF", "orders": []}
    try:
        repository = repository or InfiniteRepository()
        repository.ensure_schema()
        raw = next((p for p in positions if str(p.get("symbol") or p.get("code") or "").upper() == config.symbol), None)
        broker = _position(raw, price)
        state = repository.load_state(symbol=config.symbol)
        if state is None and broker.qty == 0:
            state = InfiniteState(symbol=config.symbol)
        if state is not None:
            state = repository.reconcile_metadata(state, trading_date=trading_date, broker_qty=broker.qty,
                                                  broker_average_price=broker.average_price,
                                                  core_cap=config.core_capital_usd)
            # A cycle becomes ACTIVE only from broker/fill evidence, never from
            # an order request or ACK (PostgreSQL and KIS are not one transaction).
            if broker.qty > 0 and state.core_filled_notional + state.reserve_filled_notional > 0 and not state.cycle_id:
                state = replace(state, cycle_id=str(uuid.uuid4()), cycle_start_date=state.last_buy_date or trading_date,
                                status=Status.ACTIVE)
            risk = assess_market_risk(overlay)
            if risk.market_crash:
                crash_date = state.metadata.get("last_market_crash_date")
                if crash_date != trading_date.isoformat():
                    state = replace(state, market_crash_streak=state.market_crash_streak + 1,
                                    material_market_crash=True,
                                    metadata={**state.metadata, "last_market_crash_date": trading_date.isoformat()})
            elif risk.verified_rebound:
                unlock = state.core_filled_notional >= config.core_capital_usd and state.material_market_crash
                state = replace(state, market_crash_streak=0, reserve_unlocked=state.reserve_unlocked or unlock,
                                status=Status.RESERVE if unlock else (Status.ACTIVE if broker.qty else state.status))
            repository.save_state(state)
        pending_buy, pending_sell = repository.pending_sides(trading_date, config.symbol)
        daily = 0.0
        if state is not None:
            _cycle, daily, _sells, _last, _anchor = repository.fill_accounting(state, trading_date)
        decision = evaluate(config=config, state=state, position=broker, trading_date=trading_date,
                            pending_buy=pending_buy, pending_sell=pending_sell,
                            daily_filled_buy_notional=daily, overlay=overlay)
        logger.info("[TQQQ_INF][STATE] cycle_id=%s status=%s price=%s broker_qty=%s broker_avg=%s anchor=%s drawdown=%s cycle_age=%s core_filled=%s reserve_filled=%s market_state=%s market_reason=%s crash_streak=%s reserve_unlocked=%s pending_buy=%s pending_sell=%s",
                    getattr(state, "cycle_id", None), getattr(getattr(state, "status", None), "value", None), broker.price, broker.qty, broker.average_price,
                    getattr(state, "anchor_price", None), (broker.price / state.anchor_price - 1 if state and state.anchor_price else None),
                    getattr(state, "cycle_age_trading_days", None), getattr(state, "core_filled_notional", None), getattr(state, "reserve_filled_notional", None),
                    overlay.get("market_state"), overlay.get("reason") or overlay.get("market_reason"), getattr(state, "market_crash_streak", None),
                    getattr(state, "reserve_unlocked", None), pending_buy, pending_sell)
        logger.info("[TQQQ_INF][DECISION] action=%s qty=%s notional=%.2f reason=%s shadow=%s",
                    decision.action.value, decision.qty, decision.notional, decision.reason, int(not config.real_order))
        if state and decision.next_status and state.status != decision.next_status:
            state = replace(state, status=decision.next_status)
            repository.save_state(state)
        if not config.real_order or decision.action not in {Action.BUY, Action.SELL}:
            return {"status": "SHADOW" if not config.real_order else decision.action.value,
                    "decision": decision, "orders": []}
        if route is None:
            return {"status": "BLOCK", "reason": "router_unavailable", "decision": decision, "orders": []}
        intent = {
            "symbol": config.symbol, "exchange": broker.exchange or "NASDAQ", "side": decision.action.value,
            "qty": decision.qty, "limit_price": broker.price, "notional_usd": decision.notional,
            "trade_date": trading_date.isoformat(),
            "client_order_key": f"TQQQ_INF_V3:{trading_date.isoformat()}:{decision.action.value}",
            "strategy": "TQQQ_INFINITE_V3", "position_action": "ADD_TO_EXISTING_BUY" if broker.qty else "NEW_POSITION_BUY",
            "reason": "TAKE_PROFIT_TQQQ_INFINITE" if decision.action == Action.SELL else decision.reason,
            "meta": {"strategy": "TQQQ_INFINITE_V3", "reason": decision.reason,
                     "position_action": "ADD_TO_EXISTING_BUY" if broker.qty else "NEW_POSITION_BUY",
                     "book": "TQQQ_INFINITE", "horizon": "INFINITE_CYCLE"},
        }
        result = route(intent)
        return {"status": result.get("status", "UNKNOWN"), "decision": decision, "orders": [result]}
    except Exception as exc:
        logger.exception("[TQQQ_INF][BLOCK] reason=isolated_exception error=%s", exc)
        return {"status": "BLOCK", "reason": "isolated_exception", "error": str(exc), "orders": []}
