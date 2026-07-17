"""Immutable-per-tick broker snapshots and order fencing state."""
from __future__ import annotations

from dataclasses import dataclass, field
from threading import Event
from typing import Any


@dataclass
class TickExecutionContext:
    trade_date: str
    session: str
    session_run_id: str
    session_generation: int
    tick_id: str
    prep_run_id: str = ""
    balance_snapshot: dict = field(default_factory=dict)
    positions_by_symbol: dict[str, dict] = field(default_factory=dict)
    exchange_by_symbol: dict[str, str] = field(default_factory=dict)
    fills_snapshot: list[dict] = field(default_factory=list)
    pending_ack_orders: list[dict] = field(default_factory=list)
    today_order_keys: set[str] = field(default_factory=set)
    cancellation_token: Any = field(default_factory=Event)
    session_state: str = "ACTIVE"
    active_tick_id: str = ""
    active_session_run_id: str = ""
    active_session_generation: int | None = None
    counters: dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.active_tick_id = self.active_tick_id or self.tick_id
        self.active_session_run_id = self.active_session_run_id or self.session_run_id
        if self.active_session_generation is None:
            self.active_session_generation = self.session_generation

    def is_cancelled(self) -> bool:
        token = self.cancellation_token
        return bool(token.is_set() if hasattr(token, "is_set") else token)

    def broker_submit_allowed(self) -> bool:
        return (
            not self.is_cancelled()
            and self.session_state == "ACTIVE"
            and self.session_run_id == self.active_session_run_id
            and self.session_generation == self.active_session_generation
            and self.tick_id == self.active_tick_id
        )
