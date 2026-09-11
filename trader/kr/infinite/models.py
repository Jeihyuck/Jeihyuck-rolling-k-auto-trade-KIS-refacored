from __future__ import annotations
from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Any

class Status(str, Enum):
    READY="READY"; ACTIVE="ACTIVE"; EXIT_PENDING="EXIT_PENDING"; COMPLETE="COMPLETE"; FROZEN="FROZEN"
class Action(str, Enum):
    BUY="BUY"; RECOVERY="RECOVERY"; SELL_PARTIAL="SELL_PARTIAL"; SELL_ALL="SELL_ALL"; WAIT="WAIT"; BLOCK="BLOCK"

@dataclass(frozen=True)
class State:
    strategy_id: str="KR_INFINITE_V1"; symbol: str="122630"; cycle_id: str|None=None
    cycle_start_date: date|None=None; cycle_complete_date: date|None=None
    allocated_capital_krw: float=0; unit_krw: float=0; core_filled_notional: float=0; reserve_filled_notional: float=0
    units_used: int=0; core_units_used: int=0; reserve_units_used: int=0
    last_buy_date: date|None=None; last_buy_price: float|None=None; last_exit_date: date|None=None
    reserve_unlocked: bool=False; crash_seen: bool=False; recovery_probe_done: bool=False
    cycle_age_trading_days: int=0; capital_preservation: bool=False; status: Status=Status.READY
    metadata: dict[str,Any]=field(default_factory=dict); version: int=1
    @property
    def filled_notional(self): return self.core_filled_notional+self.reserve_filled_notional

@dataclass(frozen=True)
class BrokerPosition:
    qty: int; orderable_qty: int; average_price: float; current_price: float

@dataclass(frozen=True)
class Decision:
    action: Action; reason: str; qty: int=0; notional: float=0; idempotency_key: str|None=None; next_status: Status|None=None
    metadata: dict[str, Any]=field(default_factory=dict)

@dataclass(frozen=True)
class OrderIntent:
    id: int
    cycle_id: str
    trade_date: date
    side: str
    idempotency_key: str
    requested_qty: int
    broker_order_id: str|None = None
    status: str = "INTENT_CREATED"
    filled_qty: int = 0
    filled_notional_krw: float = 0
    unit_sequence: int|None = None
    metadata: dict[str, Any]=field(default_factory=dict)

@dataclass(frozen=True)
class BrokerOrderState:
    status: str
    filled_qty: int = 0
    filled_notional_krw: float = 0
    filled_avg_price: float|None = None
