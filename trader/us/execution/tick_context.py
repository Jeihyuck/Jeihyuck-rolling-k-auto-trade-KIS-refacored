"""Immutable-per-tick broker snapshots and order fencing state."""
from __future__ import annotations

from dataclasses import dataclass, field
from threading import Event
from typing import Any
import json
from pathlib import Path


@dataclass
class TickExecutionContext:
    trade_date: str
    session: str
    session_run_id: str
    session_generation: int
    tick_id: str
    prep_run_id: str = ""
    run_source: str = ""
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
    active_session_state_path: str | None = None
    blocked_symbol_sides: set[tuple[str, str]] = field(default_factory=set)

    def __post_init__(self) -> None:
        self.active_tick_id = self.active_tick_id or self.tick_id
        self.active_session_run_id = self.active_session_run_id or self.session_run_id
        if self.active_session_generation is None:
            self.active_session_generation = self.session_generation

    def is_cancelled(self) -> bool:
        token = self.cancellation_token
        return bool(token.is_set() if hasattr(token, "is_set") else token)

    def broker_submit_allowed(self) -> bool:
        durable = self._durable_state()
        return (
            not self.is_cancelled()
            and self.session_state == "ACTIVE"
            and self.session_run_id == self.active_session_run_id
            and self.session_generation == self.active_session_generation
            and self.tick_id == self.active_tick_id
            and durable.get("state", "ACTIVE") == "ACTIVE"
            and durable.get("session_run_id", self.session_run_id) == self.session_run_id
            and int(durable.get("session_generation", self.session_generation)) == self.session_generation
            and durable.get("active_tick_id", self.tick_id) == self.tick_id
        )

    def _durable_state(self) -> dict:
        if not self.active_session_state_path:
            return {}
        try:
            return json.loads(Path(self.active_session_state_path).read_text(encoding="utf-8"))
        except Exception:
            # Missing/corrupt durable fencing state must fail closed.
            return {"state": "INVALID"}
