from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, time, timezone
from decimal import Decimal
import json, math
from pathlib import Path
from typing import Iterable, Mapping

from .config import InfiniteConfig
from .models import CycleStatus, MarketInput, SleeveState
from .policy import policy_checksum
from .strategy import decide


def validate_policy_identity(runtime: InfiniteConfig, replay: InfiniteConfig) -> None:
    if policy_checksum(runtime) != policy_checksum(replay): raise ValueError("runtime/replay policy checksum mismatch")


def replay_decision(config: InfiniteConfig, state: SleeveState, signal: dict, next_open: Decimal):
    signal_date, execution_date = signal["as_of"], signal["execution_date"]
    if signal_date >= execution_date: raise ValueError("point-in-time signal must precede execution")
    stamp = datetime.combine(execution_date, time(), tzinfo=timezone.utc)
    market = MarketInput(execution_date, next_open, stamp, signal["state"], execution_date, stamp,
                         signal.get("data_quality", "OK"), orderable_cash=config.capital_krw,
                         recovery_confirmed=signal.get("recovery_confirmed", False),
                         long_trend_broken=signal.get("long_trend_broken", False))
    return decide(config, state, market, stamp)


@dataclass(frozen=True)
class ReplayResult:
    total_return: float; cagr: float; mdd: float; calmar: float
    completed_cycles: int; unfinished_cycles: int; average_cycle_days: float; max_cycle_days: int
    maximum_used_units: float; maximum_capital: float; early_capital_exhaustion: bool
    equity_curve: tuple[dict, ...]; cycle_history: tuple[dict, ...]


def run_replay(rows: Iterable[Mapping], config: InfiniteConfig | None = None) -> ReplayResult:
    """Replay canonical, precomputed point-in-time regime snapshots.

    Each row's ``regime_state`` and quality must have been computed at ``signal_date``
    by the production historical adapter.  Execution occurs at the strictly later
    ``trade_date`` open; this engine never substitutes an MA proxy.
    """
    cfg = config or InfiniteConfig(); data = list(rows)
    cash = cfg.capital_krw; state = SleeveState("replay-1"); cycles=[]; curve=[]; start_idx=0
    max_units=Decimal(0); max_capital=Decimal(0); peak=cfg.capital_krw; mdd=Decimal(0)
    for idx, row in enumerate(data):
        td = _date(row["trade_date"]); sd = _date(row["signal_date"])
        if sd >= td: raise ValueError("lookahead detected: signal_date must precede trade_date")
        price=Decimal(str(row["open"])); stamp=datetime.combine(td,time(),tzinfo=timezone.utc)
        market=MarketInput(td,price,stamp,str(row["regime_state"]),td,stamp,str(row.get("data_quality","OK")),
            orderable_cash=cash,recovery_confirmed=bool(row.get("recovery_confirmed")),
            long_trend_broken=bool(row.get("long_trend_broken")))
        action=decide(cfg,state,market,stamp)
        if action.action=="BUY":
            cost=price*action.quantity*(Decimal(1)+cfg.buy_fee_rate+cfg.slippage_rate)
            if cost <= cash:
                old_qty=state.filled_quantity; qty=old_qty+action.quantity
                state=replace(state,status=CycleStatus.ACTIVE,filled_quantity=qty,buy_notional=state.buy_notional+cost,
                    average_price=(state.average_price*old_qty+price*action.quantity)/qty,last_buy_date=td)
                cash-=cost
        elif action.action=="SELL":
            qty=min(action.quantity,state.filled_quantity)
            proceeds=price*qty*(Decimal(1)-cfg.sell_fee_rate-cfg.sell_tax_rate-cfg.slippage_rate)
            remaining=state.filled_quantity-qty; cash+=proceeds
            state=replace(state,filled_quantity=remaining,sell_notional=state.sell_notional+proceeds,last_sell_date=td,
                          status=CycleStatus.COMPLETE if remaining==0 else CycleStatus.ACTIVE)
            if remaining==0:
                cycles.append({"start_index":start_idx,"end_index":idx,"days":idx-start_idx+1,
                               "buy_notional":float(state.buy_notional),"sell_notional":float(state.sell_notional)})
        equity=cash+price*state.filled_quantity*(Decimal(1)-cfg.sell_fee_rate-cfg.sell_tax_rate-cfg.slippage_rate)
        peak=max(peak,equity); mdd=min(mdd,equity/peak-1)
        units=state.buy_notional/cfg.unit_krw; max_units=max(max_units,units); max_capital=max(max_capital,state.buy_notional)
        curve.append({"date":td.isoformat(),"equity":float(equity),"drawdown":float(equity/peak-1)})
        if state.status==CycleStatus.COMPLETE and idx+1<len(data):
            next_date=_date(data[idx+1]["trade_date"])
            if next_date>td:
                state=SleeveState(f"replay-{len(cycles)+1}"); start_idx=idx+1
    final=Decimal(str(curve[-1]["equity"])) if curve else cfg.capital_krw
    total=final/cfg.capital_krw-1; years=max(len(data)/252,1/252); cagr=(float(final/cfg.capital_krw)**(1/years)-1)
    durations=[c["days"] for c in cycles]
    return ReplayResult(float(total),cagr,float(mdd),cagr/abs(float(mdd)) if mdd else 0.0,len(cycles),
        int(state.filled_quantity>0),sum(durations)/len(durations) if durations else 0,max(durations,default=0),
        float(max_units),float(max_capital),max_units>=cfg.units,tuple(curve),tuple(cycles))


def run_candidate_matrix(rows: Iterable[Mapping], output: str | Path) -> list[dict]:
    """Run the executable target/cost core; unsupported dimensions are explicit, never fabricated."""
    data=list(rows); results=[]
    for target in (6,8,10,12):
        for cost_scale in (.8,1.0,1.2):
            base=InfiniteConfig(); cfg=replace(base,target_net_return=Decimal(target)/100,
                buy_fee_rate=base.buy_fee_rate*Decimal(str(cost_scale)),sell_fee_rate=base.sell_fee_rate*Decimal(str(cost_scale)),
                sell_tax_rate=base.sell_tax_rate*Decimal(str(cost_scale)),slippage_rate=base.slippage_rate*Decimal(str(cost_scale)))
            result=run_replay(data,cfg)
            results.append({"target_pct":target,"sell_mode":"full","cost_scale":cost_scale,**result.__dict__,
                "unsupported_dimensions":["staged","trailing","variable_regime_caps","defense_threshold","cooldown"]})
    Path(output).write_text(json.dumps(results,ensure_ascii=False,indent=2,default=str),encoding="utf-8")
    return results


def _date(value) -> date:
    return value if isinstance(value,date) else date.fromisoformat(str(value))
