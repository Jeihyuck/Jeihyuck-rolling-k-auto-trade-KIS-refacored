"""Shared side-effect-free ADAPTIVE_RUNWAY_V2 persistent state transition."""
from __future__ import annotations

from dataclasses import dataclass, replace
import uuid
from datetime import date

from .config import InfiniteConfig
from .models import InfiniteState, Status
from .risk_adapter import assess_market_risk
from .strategy import classify_long_trend


@dataclass(frozen=True)
class ActualFillEvidence:
    rebound_probe_fill_date: date | None = None


def reserve_new_cycle(state: InfiniteState, trading_date: date,
                      *, cycle_id: str | None = None) -> InfiniteState:
    """Apply the one canonical production/replay new-cycle reservation reset."""
    return replace(
        state, cycle_id=cycle_id or str(uuid.uuid4()), cycle_start_date=trading_date,
        cycle_complete_date=None, anchor_price=None, core_filled_notional=0,
        reserve_filled_notional=0, last_buy_date=None,
        market_crash_streak=0, material_market_crash=False,
        reserve_unlocked=False, cycle_age_trading_days=0, status=Status.READY,
        metadata={},
    )


def update_adaptive_policy_state(*, state: InfiniteState, trading_date: date,
                                 overlay: dict, config: InfiniteConfig,
                                 actual_fill_evidence: ActualFillEvidence | None = None) -> InfiniteState:
    """Apply the production/replay daily transition without I/O or order effects."""
    metadata = dict(state.metadata or {})
    today = trading_date.isoformat()
    previous = str(metadata.get("long_trend") or "TRANSITION")
    long_trend = classify_long_trend(overlay, bool(metadata.get("structural_bear_seen")))
    market_state = str(overlay.get("market_state") or "")
    if metadata.get("last_policy_eval_date") != today:
        streak = int(metadata.get("recovery_streak") or 0)
        if market_state in {"DEFENSE_CRASH_PENDING", "DEFENSE_CRASH_CONFIRMED"}:
            streak = 0
        elif long_trend == "RECOVERY":
            streak += 1
        else:
            streak = 0
        metadata.update(previous_long_trend=previous, recovery_streak=streak,
                        last_policy_eval_date=today)
    structural = bool(metadata.get("structural_bear_seen")) or long_trend == "BEAR"
    try:
        dd = float(overlay.get("qqq_drawdown_252"))
        rv = float(overlay.get("qqq_realized_vol_20d"))
        efficiency = float(overlay.get("qqq_trend_efficiency_20d"))
    except (TypeError, ValueError):
        dd, rv, efficiency = 0.0, 0.0, 1.0
    chop = rv >= config.chop_rv20_min and efficiency <= config.chop_efficiency_max
    cp = long_trend == "BEAR" and (dd <= config.capital_preservation_drawdown
         or state.cycle_age_trading_days >= config.max_cycle_age_trading_days
         or state.core_filled_notional >= config.capital_preservation_core_used)
    remaining = max(0, int((config.max_total_capital_usd - state.total_filled_notional) // config.unit_usd))
    gap = config.capital_preservation_gap if cp else (config.chop_gap if chop else config.bear_gap if long_trend == "BEAR" else 2)
    metadata.update(policy_version=config.policy_version, long_trend=long_trend,
                    structural_bear_seen=structural,
                    deep_bear_unlocked=bool(metadata.get("deep_bear_unlocked")) or
                    (long_trend == "BEAR" and dd <= config.deep_bear_unlock_drawdown),
                    chop_high_vol=chop, capital_preservation=cp,
                    policy_mode="CAPITAL_PRESERVATION" if cp else "CHOP_HIGH_VOL" if chop else long_trend,
                    remaining_units=remaining, estimated_runway_days=remaining * gap)
    evidence = actual_fill_evidence or ActualFillEvidence()
    if evidence.rebound_probe_fill_date:
        metadata["rebound_probe_date"] = evidence.rebound_probe_fill_date.isoformat()
    risk = assess_market_risk(overlay)
    crash_streak = state.market_crash_streak
    material_crash = state.material_market_crash
    status = state.status
    if risk.market_crash:
        if metadata.get("last_market_crash_date") != today:
            crash_streak += 1
            metadata["last_market_crash_date"] = today
        material_crash = True
    elif risk.verified_rebound:
        crash_streak = 0
        if status not in {Status.READY, Status.COMPLETE}:
            status = Status.ACTIVE
    unlock = (state.core_filled_notional >= config.core_capital_usd and
              (material_crash or structural) and
              int(metadata.get("recovery_streak") or 0) >= config.recovery_confirmation_days and
              market_state not in {"DEFENSE_CRASH_PENDING", "DEFENSE_CRASH_CONFIRMED"})
    return replace(state, metadata=metadata, market_crash_streak=crash_streak,
                   material_market_crash=material_crash,
                   reserve_unlocked=state.reserve_unlocked or unlock, status=status)
