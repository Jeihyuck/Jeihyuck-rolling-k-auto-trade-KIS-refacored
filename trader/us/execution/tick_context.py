"""Immutable-per-tick broker snapshots and order fencing state."""
from __future__ import annotations

from dataclasses import dataclass, field
from threading import Event
from typing import Any
import json
from pathlib import Path
import time
from contextlib import contextmanager


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
    sell_balance_snapshot: dict | None = None
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
    balance_snapshot_at: float | None = None
    fills_snapshot_at: float | None = None
    price_cache: dict[tuple[str, str], Any] = field(default_factory=dict)
    psamount_cache: dict[tuple[str, str, float, str], Any] = field(default_factory=dict)
    prep_status: dict | None = None
    locked_watchlist: list[dict] | None = None
    invalidated_symbols: set[str] = field(default_factory=set)
    deadline: float | None = None
    metrics: dict[str, float | int] = field(default_factory=dict)
    post_order_balance_refreshes: int = 0

    def __post_init__(self) -> None:
        self.active_tick_id = self.active_tick_id or self.tick_id
        self.active_session_run_id = self.active_session_run_id or self.session_run_id
        if self.active_session_generation is None:
            self.active_session_generation = self.session_generation
        if self.balance_snapshot and self.balance_snapshot_at is None:
            self.balance_snapshot_at = time.monotonic()
        if self.fills_snapshot and self.fills_snapshot_at is None:
            self.fills_snapshot_at = time.monotonic()

    def remaining_sec(self) -> float:
        return float("inf") if self.deadline is None else max(0.0, self.deadline - time.monotonic())

    def has_budget(self, minimum_safe_sec: float) -> bool:
        return self.remaining_sec() >= max(0.0, minimum_safe_sec)

    def count(self, name: str, amount: int = 1) -> None:
        self.counters[name] = int(self.counters.get(name, 0)) + int(amount)

    @contextmanager
    def measure(self, name: str):
        started = time.monotonic()
        try:
            yield
        finally:
            self.metrics[name] = float(self.metrics.get(name, 0.0)) + (time.monotonic() - started) * 1000.0

    def invalidate_after_order(self, symbol: str) -> None:
        """Invalidate only order-sensitive state; unrelated quotes remain reusable."""
        symbol = str(symbol or "").upper().strip()
        if symbol:
            self.invalidated_symbols.add(symbol)
            for key in list(self.price_cache):
                if key[0] == symbol:
                    self.price_cache.pop(key, None)
        self.fills_snapshot_at = None
        self.balance_snapshot_at = None

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
