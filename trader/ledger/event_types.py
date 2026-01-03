from __future__ import annotations

"""Ledger event models used for append-only JSONL persistence.

This module intentionally keeps the schema explicit and small so it can be
shared between the trading runtime and the bot-state worktree copier. Each
event is a dataclass with JSONL-friendly serialization helpers.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")


def _ts(dt: datetime | None = None) -> datetime:
    return (dt or datetime.now(tz=KST)).astimezone(tz=KST)


@dataclass
class LedgerEvent:
    event_type: str
    code: str
    market: str
    sid: int
    mode: int
    env: str
    run_id: str
    side: Optional[str] = None
    qty: Optional[int] = None
    price: Optional[float] = None
    odno: Optional[str] = None
    client_order_key: Optional[str] = None
    ok: bool = True
    reasons: List[str] = field(default_factory=list)
    payload: Optional[Dict[str, Any]] = None
    stage: Optional[str] = None
    event_id: str = field(default_factory=lambda: str(uuid4()))
    ts: datetime = field(default_factory=_ts)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "ts": self.ts.isoformat(),
            "run_id": self.run_id,
            "env": self.env,
            "code": str(self.code).zfill(6),
            "market": self.market,
            "sid": self.sid,
            "mode": self.mode,
            "side": self.side,
            "qty": self.qty,
            "price": self.price,
            "odno": self.odno,
            "client_order_key": self.client_order_key,
            "ok": bool(self.ok),
            "reasons": list(self.reasons or []),
            "payload": self.payload or {},
            "stage": self.stage,
        }

    def to_jsonl(self) -> str:
        import json

        return json.dumps(self.to_dict(), ensure_ascii=False)


def new_order_intent(**kwargs: Any) -> LedgerEvent:
    return LedgerEvent(event_type="ORDER_INTENT", **kwargs)


def new_order_ack(**kwargs: Any) -> LedgerEvent:
    return LedgerEvent(event_type="ORDER_ACK", **kwargs)


def new_fill(**kwargs: Any) -> LedgerEvent:
    return LedgerEvent(event_type="FILL", **kwargs)


def new_exit_intent(**kwargs: Any) -> LedgerEvent:
    return LedgerEvent(event_type="EXIT_INTENT", **kwargs)


def new_error(**kwargs: Any) -> LedgerEvent:
    return LedgerEvent(event_type="ERROR", ok=False, **kwargs)


def new_unfilled(**kwargs: Any) -> LedgerEvent:
    return LedgerEvent(event_type="UNFILLED", ok=False, **kwargs)


def new_shadow_check(**kwargs: Any) -> LedgerEvent:
    return LedgerEvent(event_type="SHADOW_CHECK", **kwargs)
