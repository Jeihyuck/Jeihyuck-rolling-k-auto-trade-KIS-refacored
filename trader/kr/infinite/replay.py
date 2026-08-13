from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from .config import InfiniteConfig
from .models import MarketInput, SleeveState
from .policy import policy_checksum
from .strategy import decide


def validate_policy_identity(runtime: InfiniteConfig, replay: InfiniteConfig) -> None:
    if policy_checksum(runtime) != policy_checksum(replay): raise ValueError("runtime/replay policy checksum mismatch")


def replay_decision(config: InfiniteConfig, state: SleeveState, signal: dict, next_open: Decimal):
    """Use a prior-day canonical snapshot and execute only at the next session open."""
    signal_date, execution_date = signal["as_of"], signal["execution_date"]
    if signal_date >= execution_date: raise ValueError("point-in-time signal must precede execution")
    stamp = datetime.combine(execution_date, datetime.min.time(), tzinfo=timezone.utc)
    market = MarketInput(execution_date, next_open, stamp, signal["state"], execution_date, stamp,
                         signal.get("data_quality", "OK"), orderable_cash=config.capital_krw,
                         recovery_confirmed=signal.get("recovery_confirmed", False))
    return decide(config, state, market, stamp)
