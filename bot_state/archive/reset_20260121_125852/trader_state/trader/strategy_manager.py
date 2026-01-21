from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from .code_utils import normalize_code
from .config import (
    DAILY_CAPITAL,
    STRATEGY_CONFIG,
    STRATEGY_WATCHLIST,
    STRATEGY_WEIGHTS,
)
from .ledger import record_trade_ledger
from .state_store import mark_fill, mark_order
from .strategy_registry import normalize_sid
from .time_utils import now_kst
from .kis_wrapper import KisAPI
from .strategies import (
    BaseStrategy,
    BreakoutStrategy,
    MeanReversionStrategy,
    MomentumStrategy,
    PullbackStrategy,
    VolatilityStrategy,
)

logger = logging.getLogger(__name__)


STRATEGY_CLASS_MAP = {
    "breakout": BreakoutStrategy,
    "pullback": PullbackStrategy,
    "momentum": MomentumStrategy,
    "mean_reversion": MeanReversionStrategy,
    "volatility": VolatilityStrategy,
}


class StrategyManager:
    """Instantiate and orchestrate enabled strategies."""

    def __init__(
        self,
        *,
        kis: KisAPI,
        strategy_configs: Dict[str, Dict[str, Any]] | None = None,
        strategy_weights: Dict[int, float] | None = None,
        watchlist: Iterable[str] | None = None,
    ) -> None:
        self.kis = kis
        self.strategy_configs = strategy_configs or STRATEGY_CONFIG
        self.strategy_weights = strategy_weights or STRATEGY_WEIGHTS
        self.watchlist = [normalize_code(code) for code in (watchlist or STRATEGY_WATCHLIST) if normalize_code(code)]
        self.strategies = self._init_strategies()

    def _init_strategies(self) -> Dict[str, BaseStrategy]:
        strategies: Dict[str, BaseStrategy] = {}
        for name, cfg in self.strategy_configs.items():
            cls = STRATEGY_CLASS_MAP.get(name)
            if not cls:
                continue
            try:
                strat: BaseStrategy = cls(cfg)
                sid = normalize_sid(cfg.get("strategy_id"))
            except Exception:
                continue
            strategies[sid] = strat
        return strategies

    def _strategy_for_id(self, strategy_id: Any) -> Optional[BaseStrategy]:
        sid = normalize_sid(strategy_id)
        return self.strategies.get(sid)

    def _capital_by_strategy(self, total_cash: float) -> Dict[str, float]:
        allocations: Dict[str, float] = {}
        for sid, weight in self.strategy_weights.items():
            allocations[normalize_sid(sid)] = float(total_cash) * float(weight)
        if not allocations and total_cash > 0:
            per = float(total_cash) / max(len(self.strategies), 1)
            for sid in self.strategies:
                allocations[sid] = per
        return allocations

    def _candidate_symbols(self, lots: List[Dict[str, Any]], extra: Iterable[str] | None) -> List[str]:
        universe = set(self.watchlist)
        for lot in lots:
            pdno = normalize_code(lot.get("pdno") or "")
            if pdno:
                universe.add(pdno)
        if extra:
            universe.update(normalize_code(code) for code in extra if normalize_code(code))
        return [code for code in universe if code]

    @staticmethod
    def _extract_order_id(resp: Any) -> str | None:
        try:
            out = resp.get("output") if isinstance(resp, dict) else None
            if isinstance(out, dict):
                return out.get("ODNO") or out.get("ord_no") or out.get("odno")
        except Exception:
            return None
        return None

    def fetch_market_data(self, symbols: Iterable[str]) -> Dict[str, Dict[str, Any]]:
        market_data: Dict[str, Dict[str, Any]] = {}
        for code in symbols:
            code_key = normalize_code(code)
            if not code_key:
                continue
            snap: Dict[str, Any] = {}
            try:
                snap = self.kis.get_quote_snapshot(code_key)
            except Exception as e:
                logger.warning("[STRAT] snapshot fail %s: %s", code_key, e)
            price = float(snap.get("tp") or 0.0)
            prev_close = None
            reversal_price = price
            try:
                prev_close = float(self.kis.get_prev_close(code_key) or 0.0)
            except Exception:
                prev_close = None
            try:
                candles = self.kis.get_daily_candles(code_key, count=30)
            except Exception:
                candles = []
            recent_high, recent_low = 0.0, 0.0
            ma_fast, ma_slow = 0.0, 0.0
            volatility = 0.0
            closes: List[float] = []
            highs: List[float] = []
            lows: List[float] = []
            for candle in candles or []:
                try:
                    closes.append(float(candle.get("close") or 0.0))
                    highs.append(float(candle.get("high") or 0.0))
                    lows.append(float(candle.get("low") or 0.0))
                except Exception:
                    continue
            if highs:
                recent_high = max(highs[-10:]) if len(highs) >= 10 else max(highs)
            if lows:
                recent_low = min(lows[-10:]) if len(lows) >= 10 else min(lows)
            if closes:
                window = min(len(closes), 20)
                ma_fast = sum(closes[-5:]) / min(len(closes), 5)
                ma_slow = sum(closes[-window:]) / window
                if recent_high and recent_low and prev_close:
                    volatility = (recent_high - recent_low) / prev_close * 100
                if len(closes) >= 3:
                    reversal_price = max(closes[-3:])
                else:
                    reversal_price = price
            prev_close_val = float(prev_close or 0.0)
            market_data[code_key] = {
                "price": price,
                "ask": snap.get("ap"),
                "bid": snap.get("bp"),
                "prev_close": prev_close_val,
                "recent_high": recent_high,
                "recent_low": recent_low,
                "reversal_price": reversal_price,
                "ma_fast": ma_fast,
                "ma_slow": ma_slow,
                "mean_price": ma_slow,
                "volatility": volatility,
                "vwap": float(snap.get("tp") or price),  # fallback: 실시간 VWAP 불가 시 현재가 사용
            }
        return market_data

    def run_cycle(
        self,
        state: Dict[str, Any],
        balance: Dict[str, Any],
        candidates: Iterable[str] | None = None,
    ) -> Dict[str, Any]:
        lots = [lot for lot in state.get("lots", []) if int(lot.get("remaining_qty") or lot.get("qty") or 0) > 0]
        symbols = self._candidate_symbols(lots, candidates)
        if not symbols:
            logger.info("[STRAT] no symbols to evaluate")
            return {"entries": 0, "exits": 0}
        market_data = self.fetch_market_data(symbols)
        exits = self._evaluate_exits(state, lots, market_data)
        entries = self._evaluate_entries(state, lots, market_data, balance)
        return {"entries": entries, "exits": exits}

    def _evaluate_entries(
        self,
        state: Dict[str, Any],
        lots: List[Dict[str, Any]],
        market_data: Dict[str, Dict[str, Any]],
        balance: Dict[str, Any],
    ) -> int:
        total_cash = float(balance.get("cash") or 0.0) if isinstance(balance, dict) else 0.0
        if total_cash <= 0:
            total_cash = float(DAILY_CAPITAL)
        allocations = self._capital_by_strategy(total_cash)
        entries = 0
        for sid, strategy in self.strategies.items():
            alloc = allocations.get(sid, total_cash / max(len(self.strategies), 1))
            cfg = self.strategy_configs.get(strategy.name, {})
            entry_pct = cfg.get("entry_allocation_pct") or 0.2
            if entry_pct > 1:
                entry_pct = entry_pct / 100.0
            budget = alloc * float(entry_pct)
            existing_codes = {
                normalize_code(lot.get("pdno") or "")
                for lot in lots
                if normalize_sid(lot.get("strategy_id")) == sid and int(lot.get("remaining_qty") or lot.get("qty") or 0) > 0
            }
            for code, data in market_data.items():
                # skip if any strategy already holds the symbol
                if normalize_code(code) in existing_codes:
                    continue
                if not strategy.should_enter(code, data):
                    continue
                entry_price = strategy.compute_entry_price(code, data)
                qty = self._qty_for_budget(budget, entry_price)
                if qty <= 0:
                    logger.debug(
                        "[STRAT][ENTRY_SKIP] %s sid=%s budget=%.0f px=%.2f qty<=0",
                        code,
                        sid,
                        budget,
                        entry_price,
                    )
                    continue
                ts = now_kst().isoformat()
                oid = mark_order(state, code, "BUY", sid, qty, entry_price, ts, reason="strategy_entry")
                try:
                    resp = (
                        self.kis.buy_stock_limit_guarded(code, qty, int(entry_price), sid=sid)
                        if entry_price > 0
                        else self.kis.buy_stock_market_guarded(code, qty, sid=sid)
                    )
                except Exception as e:
                    logger.error("[STRAT][BUY_FAIL] %s sid=%s ex=%s", code, sid, e)
                    continue
                order_id = self._extract_order_id(resp)
                if order_id:
                    mark_order(
                        state,
                        code,
                        "BUY",
                        sid,
                        qty,
                        entry_price,
                        ts,
                        order_id=order_id,
                        status="ack",
                        reason="order_ack",
                    )
                if self.kis.check_filled(resp):
                    mark_fill(
                        state,
                        code,
                        "BUY",
                        sid,
                        qty,
                        entry_price,
                        ts,
                        status="filled",
                        order_id=order_id or oid,
                        source="strategy_manager",
                    )
                    record_trade_ledger(
                        timestamp=ts,
                        code=code,
                        strategy_id=sid,
                        side="BUY",
                        qty=qty,
                        price=entry_price,
                        meta={"engine": "strategy_manager", "resp": resp},
                    )
                    entries += 1
                    logger.info(
                        "[STRAT][ENTRY] code=%s sid=%s qty=%s price=%.2f budget=%.0f",
                        code,
                        sid,
                        qty,
                        entry_price,
                        budget,
                    )
        return entries

    def _evaluate_exits(
        self, state: Dict[str, Any], lots: List[Dict[str, Any]], market_data: Dict[str, Dict[str, Any]]
    ) -> int:
        exits = 0
        for lot in list(lots):
            remaining = int(lot.get("remaining_qty") or lot.get("qty") or 0)
            if remaining <= 0:
                continue
            code_key = normalize_code(lot.get("pdno") or "")
            sid = normalize_sid(lot.get("strategy_id"))
            strategy = self._strategy_for_id(sid)
            if not strategy:
                continue
            data = market_data.get(code_key) or {}
            pos_state = {"qty": remaining, "avg_price": float(lot.get("entry_price") or 0.0)}
            if not strategy.should_exit(pos_state, data):
                continue
            ts = now_kst().isoformat()
            oid = mark_order(state, code_key, "SELL", sid, remaining, data.get("price") or 0.0, ts, reason="strategy_exit")
            try:
                resp = self.kis.sell_stock_market_guarded(code_key, remaining, sid=sid)
            except Exception as e:
                logger.error("[STRAT][SELL_FAIL] %s sid=%s ex=%s", code_key, sid, e)
                continue
            order_id = self._extract_order_id(resp)
            if order_id:
                mark_order(
                    state,
                    code_key,
                    "SELL",
                    sid,
                    remaining,
                    data.get("price") or 0.0,
                    ts,
                    order_id=order_id,
                    status="ack",
                    reason="order_ack",
                )
            if self.kis.check_filled(resp):
                price = float(data.get("price") or 0.0)
                mark_fill(state, code_key, "SELL", sid, remaining, price, ts, status="filled", order_id=order_id or oid)
                record_trade_ledger(
                    timestamp=ts,
                    code=code_key,
                    strategy_id=sid,
                    side="SELL",
                    qty=remaining,
                    price=price,
                    meta={"engine": "strategy_manager", "resp": resp},
                )
                exits += 1
                logger.info(
                    "[STRAT][EXIT] code=%s sid=%s qty=%s price=%.2f reason=signal",
                    code_key,
                    sid,
                    remaining,
                    price,
                )
        return exits

    @staticmethod
    def _qty_for_budget(budget: float, price: float) -> int:
        try:
            if budget <= 0 or price <= 0:
                return 0
            return int(budget // price)
        except Exception:
            return 0
