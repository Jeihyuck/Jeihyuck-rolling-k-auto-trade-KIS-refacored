from __future__ import annotations

import logging
import math
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Dict, List, Tuple

import trader.state_store as state_store
from strategy.base import BaseStrategy
from strategy.strategies import (
    BreakoutStrategy,
    MeanReversionStrategy,
    MomentumStrategy,
    PullbackStrategy,
    VolatilityStrategy,
)
from strategy.types import OrderIntent
from trader.config import (
    ACTIVE_STRATEGIES,
    ALLOW_ADOPT_UNMANAGED,
    DAILY_CAPITAL,
    ENABLED_STRATEGIES_SET,
    KST,
    STRATEGY_MAX_POSITION_PCT,
    STRATEGY_WEIGHTS,
    UNMANAGED_STRATEGY_ID,
)
from trader import state_store as runtime_state_store

logger = logging.getLogger(__name__)


@dataclass
class StrategySlot:
    name: str
    sid: int
    weight: float
    strategy: BaseStrategy


class StrategyManager:
    def __init__(self, total_capital: float | None = None) -> None:
        self.total_capital = float(total_capital or DAILY_CAPITAL)
        self._seq = 0
        self._zero_weight_warned: set[str] = set()
        self.slots: list[StrategySlot] = self._register_strategies()

    def _register_strategies(self) -> list[StrategySlot]:
        return [
            StrategySlot("breakout", 1, float(STRATEGY_WEIGHTS.get("breakout", 0.0)), BreakoutStrategy()),
            StrategySlot("pullback", 2, float(STRATEGY_WEIGHTS.get("pullback", 0.0)), PullbackStrategy()),
            StrategySlot("momentum", 3, float(STRATEGY_WEIGHTS.get("momentum", 0.0)), MomentumStrategy()),
            StrategySlot(
                "mean_reversion",
                4,
                float(STRATEGY_WEIGHTS.get("mean_reversion", 0.0)),
                MeanReversionStrategy(),
            ),
            StrategySlot(
                "volatility",
                5,
                float(STRATEGY_WEIGHTS.get("volatility", 0.0)),
                VolatilityStrategy(),
            ),
        ]

    def enabled_slots(self) -> list[StrategySlot]:
        enabled: list[StrategySlot] = []
        for slot in self.slots:
            if slot.sid not in ACTIVE_STRATEGIES:
                logger.info(
                    "[STRATEGY_MANAGER] strategy %s sid=%s not in active_strategies=%s -> skipped",
                    slot.name,
                    slot.sid,
                    sorted(ACTIVE_STRATEGIES),
                )
                continue
            if slot.name not in ENABLED_STRATEGIES_SET:
                continue
            if float(slot.weight) <= 0:
                if slot.name not in self._zero_weight_warned:
                    logger.info(
                        "[STRATEGY_MANAGER] strategy %s enabled but weight=0 -> skipped", slot.name
                    )
                    self._zero_weight_warned.add(slot.name)
                continue
            enabled.append(slot)
        return enabled

    def _next_intent_id(self, strategy: str, side: str, symbol: str, ts: str) -> str:
        self._seq += 1
        return f"{ts}-{strategy}-{side}-{symbol}-{self._seq}"

    def _last_price(self, symbol: str, market_data: Dict[str, Any]) -> float | None:
        prices = market_data.get("prices") if isinstance(market_data, dict) else None
        if isinstance(prices, dict):
            data = prices.get(symbol) or prices.get(str(symbol).zfill(6)) or {}
            try:
                return float(data.get("last_price") or data.get("price") or 0.0)
            except Exception:
                return None
        return None

    def _size_position(self, weight: float, price: float) -> int:
        if price <= 0 or weight <= 0:
            return 0
        allocated_capital = self.total_capital * float(weight)
        max_cap = allocated_capital * STRATEGY_MAX_POSITION_PCT
        qty = math.floor(max_cap / price) if price else 0
        return max(qty, 0)

    def _position_matches_strategy(self, position: Dict[str, Any], slot: StrategySlot) -> bool:
        if not bool(position.get("managed")):
            return False
        strategy_id = position.get("strategy_id")
        if strategy_id is None:
            return False
        strategy_key = str(strategy_id).lower()
        return strategy_key in {slot.name, str(slot.sid)}

    def _build_intent(
        self,
        slot: StrategySlot,
        side: str,
        symbol: str,
        qty: int,
        order_type: str = "MARKET",
        limit_price: float | None = None,
        reason: str = "",
        meta: Dict[str, Any] | None = None,
    ) -> OrderIntent:
        ts = datetime.now(KST).isoformat()
        intent_id = self._next_intent_id(slot.name, side, symbol, ts)
        return OrderIntent(
            intent_id=intent_id,
            ts=ts,
            strategy=slot.name,
            sid=slot.sid,
            side=side.upper(),
            symbol=str(symbol).zfill(6),
            qty=int(qty),
            order_type=order_type.upper(),
            limit_price=limit_price,
            reason=reason or f"{slot.name}_{side.lower()}",
            meta=meta or {},
        )

    def _entry_intents_for_slot(
        self,
        slot: StrategySlot,
        market_data: Dict[str, Any],
        portfolio_state: Dict[str, Any],
    ) -> list[OrderIntent]:
        strategy = slot.strategy
        if not strategy.should_enter(market_data, portfolio_state):
            return []
        entry = strategy.compute_entry(market_data, portfolio_state) or {}
        if not isinstance(entry, dict):
            return []
        symbol = entry.get("symbol")
        if not symbol:
            return []
        price = entry.get("price") or self._last_price(symbol, market_data)
        qty = self._size_position(slot.weight, float(price or 0))
        if qty <= 0:
            return []
        pos = (portfolio_state.get("positions") or {}).get(str(symbol).zfill(6)) if isinstance(portfolio_state, dict) else None
        if slot.sid == 1 and isinstance(pos, dict) and not bool(pos.get("managed")):
            if not ALLOW_ADOPT_UNMANAGED:
                logger.info(
                    "[UNMANAGED-SKIP-ENTRY] code=%s reason=ALREADY_HELD_UNMANAGED strategy_id=%s",
                    str(symbol).zfill(6),
                    pos.get("strategy_id"),
                )
                return []
            pos["strategy_id"] = slot.sid
            pos["managed"] = True
            pos["position_key"] = f"{str(symbol).zfill(6)}:{slot.sid}"
            pos.setdefault("opened_at", pos.get("opened_at") or datetime.now(KST).isoformat())
            pos["updated_at"] = datetime.now(KST).isoformat()
            try:
                state = portfolio_state or runtime_state_store.load_state()
                state.setdefault("positions", {})[str(symbol).zfill(6)] = pos
                runtime_state_store.save_state(state)
                logger.info(
                    "[UNMANAGED-ADOPT] code=%s adopted_by_strategy=%s allow_adopt_unmanaged=%s",
                    str(symbol).zfill(6),
                    slot.sid,
                    ALLOW_ADOPT_UNMANAGED,
                )
            except Exception:
                logger.exception("[UNMANAGED-ADOPT][FAIL] code=%s strategy=%s", str(symbol).zfill(6), slot.sid)
        order_type = entry.get("order_type") or "MARKET"
        limit_price = entry.get("limit_price")
        reason = entry.get("reason") or f"{slot.name}_entry"
        meta = entry.get("meta")
        return [
            self._build_intent(
                slot,
                "BUY",
                symbol,
                qty,
                order_type=order_type,
                limit_price=limit_price,
                reason=reason,
                meta=meta,
            )
        ]

    def _exit_intents_for_slot(
        self,
        slot: StrategySlot,
        market_data: Dict[str, Any],
        portfolio_state: Dict[str, Any],
    ) -> list[OrderIntent]:
        intents: list[OrderIntent] = []
        positions = portfolio_state.get("positions", {}) if isinstance(portfolio_state, dict) else {}
        for symbol, position in positions.items():
            if not isinstance(position, dict):
                continue
            if not bool(position.get("managed")):
                logger.info(
                    "[UNMANAGED-SKIP-EXIT] code=%s qty=%s strategy_id=%s reason=UNMANAGED_HOLDING",
                    symbol,
                    position.get("qty"),
                    position.get("strategy_id"),
                )
                continue
            if not self._position_matches_strategy(position, slot):
                continue
            qty = int(position.get("qty") or 0)
            if qty <= 0:
                continue
            if not slot.strategy.should_exit(position, market_data, portfolio_state):
                continue
            exit_payload = slot.strategy.compute_exit(position, market_data, portfolio_state) or {}
            if not isinstance(exit_payload, dict):
                continue
            price = exit_payload.get("price") or self._last_price(symbol, market_data)
            if price and price > 0:
                qty = min(qty, int(exit_payload.get("qty") or qty))
            order_type = exit_payload.get("order_type") or "MARKET"
            limit_price = exit_payload.get("limit_price")
            reason = exit_payload.get("reason") or f"{slot.name}_exit"
            intents.append(
                self._build_intent(
                    slot,
                    "SELL",
                    symbol,
                    qty,
                    order_type=order_type,
                    limit_price=limit_price,
                    reason=reason,
                    meta=exit_payload.get("meta"),
                )
            )
        return intents

    def run_once(
        self,
        market_data: Dict[str, Any] | None = None,
        portfolio_state: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        state = portfolio_state or state_store.load_state()
        market_data = market_data or {}
        intents: list[OrderIntent] = []
        enabled: list[str] = []
        dedupe_keys: set[Tuple[str, str, str]] = set()

        for slot in self.enabled_slots():
            enabled.append(slot.name)
            try:
                slot.strategy.update_state(market_data)
            except Exception:
                logger.exception("[STRATEGY_MANAGER] failed to update state for %s", slot.name)
            for intent in self._entry_intents_for_slot(slot, market_data, state):
                key = (slot.name, intent.symbol, intent.side)
                if key in dedupe_keys:
                    continue
                dedupe_keys.add(key)
                intents.append(intent)
            for intent in self._exit_intents_for_slot(slot, market_data, state):
                key = (slot.name, intent.symbol, intent.side)
                if key in dedupe_keys:
                    continue
                dedupe_keys.add(key)
                intents.append(intent)

        return {"enabled": enabled, "intents": [asdict(intent) for intent in intents]}
