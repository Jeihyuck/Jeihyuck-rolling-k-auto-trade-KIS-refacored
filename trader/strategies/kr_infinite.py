from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from typing import Literal

Action = Literal["HOLD", "BUY", "SELL"]


@dataclass(frozen=True)
class KrInfiniteConfig:
    """Risk-bounded accumulation / harvest policy for KR leveraged ETFs.

    The strategy intentionally avoids martingale sizing. Every campaign has a fixed
    capital budget split into equal tranches; unused tranches remain cash reserves.
    """

    tranches: int = 24
    initial_tranches: int = 2
    max_deployed_tranches: int = 18
    add_step_pct: float = 0.025
    take_profit_pct: float = 0.045
    take_profit_reduced_pct: float = 0.030
    max_drawdown_pct: float = 0.18
    max_campaign_days: int = 90
    max_buys_per_day: int = 1
    min_regime_score: float = 0.0
    defense_regime_score: float = -1.0
    max_market_premium_pct: float = 0.015

    def __post_init__(self) -> None:
        if self.tranches <= 0:
            raise ValueError("tranches_must_be_positive")
        if not (1 <= self.initial_tranches <= self.tranches):
            raise ValueError("invalid_initial_tranches")
        if not (self.initial_tranches <= self.max_deployed_tranches <= self.tranches):
            raise ValueError("invalid_max_deployed_tranches")
        if self.add_step_pct <= 0:
            raise ValueError("add_step_pct_must_be_positive")
        if self.take_profit_pct <= 0 or self.take_profit_reduced_pct <= 0:
            raise ValueError("take_profit_pct_must_be_positive")
        if not (0 < self.max_drawdown_pct < 1):
            raise ValueError("invalid_max_drawdown_pct")


@dataclass(frozen=True)
class CampaignState:
    code: str
    cycle_id: str
    started_on: date
    qty: int = 0
    avg_price: float = 0.0
    invested_krw: float = 0.0
    deployed_tranches: int = 0
    last_fill_price: float = 0.0
    buys_today: int = 0


@dataclass(frozen=True)
class MarketState:
    price: float
    regime_score: float
    nav_premium_pct: float = 0.0
    tradable: bool = True
    stale_price: bool = False


@dataclass(frozen=True)
class TradePlan:
    action: Action
    qty: int = 0
    tranche_count: int = 0
    reason: str = "hold"
    target_price: float | None = None


def tranche_budget(campaign_budget_krw: float, config: KrInfiniteConfig) -> float:
    if campaign_budget_krw <= 0:
        return 0.0
    return float(campaign_budget_krw) / float(config.tranches)


def qty_for_tranches(*, price: float, campaign_budget_krw: float, tranches: int, config: KrInfiniteConfig) -> int:
    if price <= 0 or tranches <= 0:
        return 0
    budget = tranche_budget(campaign_budget_krw, config) * tranches
    return max(0, int(math.floor(budget / price)))


def next_add_trigger(state: CampaignState, config: KrInfiniteConfig) -> float | None:
    anchor = float(state.last_fill_price or state.avg_price or 0.0)
    if anchor <= 0:
        return None
    return anchor * (1.0 - config.add_step_pct)


def take_profit_trigger(state: CampaignState, market: MarketState, config: KrInfiniteConfig) -> float | None:
    if state.qty <= 0 or state.avg_price <= 0:
        return None
    target_pct = (
        config.take_profit_reduced_pct
        if market.regime_score < config.min_regime_score
        else config.take_profit_pct
    )
    return state.avg_price * (1.0 + target_pct)


def campaign_drawdown(state: CampaignState, market: MarketState) -> float:
    if state.avg_price <= 0 or market.price <= 0:
        return 0.0
    return (market.price / state.avg_price) - 1.0


def evaluate_trade(
    *,
    state: CampaignState,
    market: MarketState,
    campaign_budget_krw: float,
    available_cash_krw: float,
    today: date,
    config: KrInfiniteConfig,
) -> TradePlan:
    """Return the next *intent* only; fills must update state via broker reconciliation."""
    if not market.tradable:
        return TradePlan("HOLD", reason="market_not_tradable")
    if market.stale_price or market.price <= 0:
        return TradePlan("HOLD", reason="price_unavailable")
    if abs(market.nav_premium_pct) > config.max_market_premium_pct:
        return TradePlan("HOLD", reason="nav_premium_guard")

    if state.qty > 0:
        tp = take_profit_trigger(state, market, config)
        if tp is not None and market.price >= tp:
            return TradePlan("SELL", qty=state.qty, reason="cycle_take_profit", target_price=tp)

        age_days = max(0, (today - state.started_on).days)
        dd = campaign_drawdown(state, market)
        if dd <= -config.max_drawdown_pct:
            return TradePlan("SELL", qty=state.qty, reason="campaign_drawdown_stop")
        if age_days >= config.max_campaign_days and market.price >= state.avg_price:
            return TradePlan("SELL", qty=state.qty, reason="campaign_time_exit")

    if market.regime_score <= config.defense_regime_score:
        return TradePlan("HOLD", reason="defense_regime")
    if state.buys_today >= config.max_buys_per_day:
        return TradePlan("HOLD", reason="daily_buy_limit")
    if state.deployed_tranches >= config.max_deployed_tranches:
        return TradePlan("HOLD", reason="deployment_cap")

    if state.qty <= 0:
        if market.regime_score < config.min_regime_score:
            return TradePlan("HOLD", reason="entry_regime_block")
        requested = min(config.initial_tranches, config.max_deployed_tranches)
        reason = "cycle_start"
        trigger = market.price
    else:
        trigger = next_add_trigger(state, config)
        if trigger is None or market.price > trigger:
            return TradePlan("HOLD", reason="wait_next_ladder", target_price=trigger)
        requested = 1
        reason = "ladder_add"

    requested = min(requested, config.max_deployed_tranches - state.deployed_tranches)
    qty = qty_for_tranches(
        price=market.price,
        campaign_budget_krw=campaign_budget_krw,
        tranches=requested,
        config=config,
    )
    max_cash_qty = int(max(0.0, available_cash_krw) // market.price)
    qty = min(qty, max_cash_qty)
    if qty <= 0:
        return TradePlan("HOLD", reason="insufficient_cash_or_tranche")
    return TradePlan("BUY", qty=qty, tranche_count=requested, reason=reason, target_price=trigger)
