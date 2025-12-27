from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class OrderIntent:
    intent_id: str
    ts: str
    strategy: str
    sid: int
    side: str  # BUY | SELL
    symbol: str
    qty: int
    order_type: str  # MARKET | LIMIT
    limit_price: Optional[float]
    reason: str
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExecutionAck:
    intent_id: str
    ok: bool
    message: str
    order_id: Optional[str] = None


@dataclass
class FillEvent:
    order_id: str
    symbol: str
    qty: int
    price: float
    ts: str
    side: str  # BUY | SELL
