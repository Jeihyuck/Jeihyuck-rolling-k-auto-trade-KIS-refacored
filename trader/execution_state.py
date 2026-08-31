"""Durable, tick-scoped KR execution-state primitives.

This module contains pure objects so runners and production-style tests use the
same invariants rather than rebuilding balance/order semantics independently.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Iterable, Mapping
from uuid import uuid4


class OrderState(str, Enum):
    INTENT = "INTENT"
    SUBMITTED = "SUBMITTED"
    ACKED = "ACKED"
    PARTIAL_FILLED = "PARTIAL_FILLED"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"
    UNRESOLVED_ACK = "UNRESOLVED_ACK"
    RECONCILE_ERROR = "RECONCILE_ERROR"


SELL_GUARD_STATES = frozenset({"SUBMITTED", "ACKED", "ACCEPTED", "PARTIAL_FILLED", "FILLED",
                               "ACKED_IDEMPOTENT_RECOVERED", "NO_SELLABLE_QTY"})


@dataclass(frozen=True)
class BrokerPosition:
    qty: int = 0
    orderable_qty: int = 0
    avg_price: float = 0.0


@dataclass(frozen=True)
class BrokerBalanceSnapshot:
    snapshot_id: str
    fetched_at: datetime
    cash: int
    holdings_by_code: Mapping[str, BrokerPosition]
    source: str = "api"

    @classmethod
    def from_kis(cls, payload: Mapping[str, Any], *, source: str = "api",
                 fetched_at: datetime | None = None) -> "BrokerBalanceSnapshot":
        holdings: dict[str, BrokerPosition] = {}
        for row in payload.get("output1", []) or []:
            code = str(row.get("pdno") or row.get("code") or "").strip().zfill(6)
            if not code:
                continue
            holdings[code] = BrokerPosition(
                qty=int(float(row.get("hldg_qty") or row.get("qty") or 0)),
                orderable_qty=int(float(row.get("ord_psbl_qty") or row.get("orderable_qty") or 0)),
                avg_price=float(row.get("pchs_avg_pric") or row.get("avg_price") or 0),
            )
        summary = payload.get("output2") or [{}]
        summary = summary[0] if isinstance(summary, list) and summary else summary
        cash = int(float((summary or {}).get("ord_psbl_cash") or (summary or {}).get("dnca_tot_amt") or 0))
        return cls(str(uuid4()), fetched_at or datetime.now(timezone.utc), cash, holdings, source)

    def position(self, code: str) -> BrokerPosition:
        return self.holdings_by_code.get(str(code).zfill(6), BrokerPosition())

    def holding_qty(self, code: str) -> int:
        return self.position(code).qty


@dataclass(frozen=True)
class OrderBaseline:
    balance_snapshot_id: str
    pre_order_holding_qty: int
    pre_order_orderable_qty: int
    pre_order_avg_price: float
    requested_qty: int
    submitted_qty: int
    order_intent_ts: str

    @classmethod
    def capture(cls, snapshot: BrokerBalanceSnapshot, code: str, qty: int,
                *, intent_ts: datetime | None = None) -> "OrderBaseline":
        pos = snapshot.position(code)
        return cls(snapshot.snapshot_id, pos.qty, pos.orderable_qty, pos.avg_price,
                   int(qty), int(qty), (intent_ts or datetime.now(timezone.utc)).isoformat())


@dataclass(frozen=True)
class ReconcileDecision:
    state: OrderState
    confirmed_fill_qty: int
    source: str
    reason: str = ""


def reconcile_balance_delta(*, side: str, pre_qty: int, post_qty: int,
                            submitted_qty: int) -> ReconcileDecision:
    delta = post_qty - pre_qty if side.upper() == "BUY" else pre_qty - post_qty
    if delta < 0 or delta > submitted_qty:
        return ReconcileDecision(OrderState.RECONCILE_ERROR, 0, "balance_delta", "DELTA_OUT_OF_RANGE")
    if delta == 0:
        return ReconcileDecision(OrderState.UNRESOLVED_ACK, 0, "balance_delta", "NO_PROVEN_DELTA")
    state = OrderState.FILLED if delta == submitted_qty else OrderState.PARTIAL_FILLED
    return ReconcileDecision(state, delta, "balance_delta")


def durable_order_metrics(orders: Iterable[Mapping[str, Any]], fills: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    rows, fill_rows = list(orders), list(fills)
    states = [str(row.get("status") or "").upper() for row in rows]
    ack_states = {"ACKED", "ACCEPTED", "PARTIAL_FILLED", "FILLED", "ACKED_IDEMPOTENT_RECOVERED"}
    result: dict[str, Any] = {
        "order_intents_created": len(rows),
        "broker_submitted": sum(s in SELL_GUARD_STATES for s in states),
        "broker_acked": sum(s in ack_states for s in states),
        "fills_confirmed": len(fill_rows),
        "partial_fills": sum(s == "PARTIAL_FILLED" for s in states),
        "broker_rejected": sum(s in {"REJECTED", "ERROR", "FAILED"} for s in states),
        "cancelled": sum(s == "CANCELLED" for s in states),
        "unresolved_acks": sum(s in {"ACKED", "ACCEPTED", "UNRESOLVED_ACK"} for s in states),
    }
    result["by_side"] = {
        side: {"broker_acked": sum(str(r.get("side") or "").upper() == side and
                                    str(r.get("status") or "").upper() in ack_states for r in rows)}
        for side in ("BUY", "SELL")
    }
    return result


@dataclass
class BalanceRecoveryState:
    retry_interval_seconds: int = 60
    state: str = "NORMAL"
    retry_count: int = 0
    next_retry_at: datetime | None = None

    def failed(self, at: datetime) -> None:
        from datetime import timedelta
        self.state, self.retry_count = "BALANCE_RECOVERY_ONLY", self.retry_count + 1
        self.next_retry_at = at + timedelta(seconds=self.retry_interval_seconds)

    def recovered(self) -> None:
        self.state, self.retry_count, self.next_retry_at = "NORMAL", 0, None

    @property
    def new_order_allowed(self) -> bool:
        return self.state == "NORMAL"
