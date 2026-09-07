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


class BalanceFreshness(str, Enum):
    FRESH = "FRESH"
    CACHED = "CACHED"
    STALE = "STALE"
    INVALID = "INVALID"


def balance_freshness_for_source(source: str | None) -> BalanceFreshness:
    value = str(source or "").strip().lower()
    if value in {"api", "forced_refresh_output2_none", "forced_refresh_sanitized", "forced_refresh_cash"}:
        return BalanceFreshness.FRESH
    if value in {"persisted_cache", "stale_cache", "expired_cache", "stale"}:
        return BalanceFreshness.STALE
    if value in {"engine_fail_soft_empty", "invalid", "none"}:
        return BalanceFreshness.INVALID
    return BalanceFreshness.CACHED


SELL_GUARD_STATES = frozenset({"SUBMITTED", "ACKED", "ACCEPTED", "PARTIAL_FILLED", "FILLED",
                               "FILLED_QTY_CONFIRMED_PRICE_UNRESOLVED",
                               "ACKED_IDEMPOTENT_RECOVERED", "NO_SELLABLE_QTY"})
PENDING_SELL_STATES = frozenset({"SUBMITTED", "ACKED", "ACCEPTED", "UNRESOLVED_ACK",
                                 "PARTIAL_FILLED", "ACKED_IDEMPOTENT_RECOVERED"})


def exit_stage_for_reason(reason: str | None, *, requested_sell_qty: int | None = None,
                          broker_qty_before: int | None = None,
                          sell_pct: float | None = None) -> str:
    """Map decision reasons to policy stages; reasons never define dedupe identity."""
    value = str(reason or "").upper()
    if requested_sell_qty is not None and broker_qty_before is not None:
        if int(requested_sell_qty) >= int(broker_qty_before) > 0:
            return "FULL_EXIT"
    partial = ((sell_pct is not None and 0 < float(sell_pct) < 1)
               or (requested_sell_qty is not None and broker_qty_before is not None
                   and 0 < int(requested_sell_qty) < int(broker_qty_before)))
    if value in {
        "TP1", "TAKE_PROFIT_1", "ABS_TP1", "ABS_TP1_10PCT",
        "SWING_TP1_R", "SWING_TP1_PCT", "SWING_PCT_TP1",
        "EXIT_SWING_TP1", "CORE_TP1_R", "EXIT_CORE_TP1",
    }:
        return "TP1"
    if value in {
        "TP2", "TAKE_PROFIT_2", "ABS_TP2", "SWING_TP2_R",
        "SWING_TP2_PCT", "SWING_PCT_TP2", "EXIT_SWING_TP2",
        "CORE_TP2_R", "EXIT_CORE_TP2",
    }:
        return "TP2"
    if value in {"PROFIT_PROTECT_8PCT", "SWING_PROFIT_PROTECT_GIVEBACK",
                 "MOMENTUM_PROFIT_PROTECT_GIVEBACK"} or value.startswith("PROFIT_PROTECT_PARTIAL_1"):
        return "PROFIT_PROTECT_PARTIAL_1" if partial or requested_sell_qty is None else "FULL_EXIT"
    if value in {"DEFENSE_RISK_OFF_TRIM", "CLUSTER_EXPOSURE_TRIM"} or value.startswith("DEFENSE_TRIM_1"):
        return "DEFENSE_TRIM_1" if partial or requested_sell_qty is None else "FULL_EXIT"
    return "FULL_EXIT"


def legal_next_exit_stage(previous: str, current: str) -> bool:
    return (str(previous).upper(), str(current).upper()) in {
        ("TP1", "TP2"),
        ("TP1", "FULL_EXIT"),
        ("TP2", "FULL_EXIT"),
        ("PROFIT_PROTECT_PARTIAL_1", "FULL_EXIT"),
        ("DEFENSE_TRIM_1", "FULL_EXIT"),
    }


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
    freshness: BalanceFreshness = BalanceFreshness.FRESH

    @classmethod
    def from_kis(cls, payload: Mapping[str, Any], *, source: str = "api",
                 freshness: BalanceFreshness | None = None,
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
        return cls(str(uuid4()), fetched_at or datetime.now(timezone.utc), cash, holdings, source,
                   freshness or balance_freshness_for_source(source))

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
    ack_states = {"ACKED", "ACCEPTED", "PARTIAL_FILLED", "FILLED",
                  "FILLED_QTY_CONFIRMED_PRICE_UNRESOLVED", "ACKED_IDEMPOTENT_RECOVERED"}
    submitted_states = ack_states | {"SUBMITTED", "REJECTED", "FAILED", "UNRESOLVED_ACK",
                                     "RECONCILE_ERROR", "FILLED_QTY_CONFIRMED_PRICE_UNRESOLVED"}
    filled_order_ids = {str(row.get("order_id")) for row in fill_rows if row.get("order_id")}
    ack_without_fill_ids = {
        str(row.get("order_id")) for row in rows
        if row.get("order_id") and str(row.get("status") or "").upper() in {"ACKED", "ACCEPTED"}
    } - filled_order_ids
    order_status_by_id = {str(row.get("order_id")): str(row.get("status") or "").upper()
                          for row in rows if row.get("order_id")}
    consistency_warnings = [
        f"FILLED_WITHOUT_LEDGER_FILL:{order_id}"
        for order_id, status in order_status_by_id.items()
        if status in {"FILLED", "PARTIAL_FILLED"} and order_id not in filled_order_ids
    ]
    consistency_warnings.extend(
        f"FILL_WITH_NONEXECUTED_ORDER:{order_id}"
        for order_id in filled_order_ids
        if order_status_by_id.get(order_id) in {"CREATED", "INTENT"}
    )
    result: dict[str, Any] = {
        "order_intents_created": len(rows),
        "broker_submitted": sum(s in submitted_states for s in states),
        "broker_acked": sum(s in ack_states for s in states),
        "fills_confirmed": len(fill_rows),
        "partial_fills": sum(s == "PARTIAL_FILLED" for s in states),
        "broker_rejected": sum(s in {"REJECTED", "ERROR", "FAILED"} for s in states),
        "cancelled": sum(s == "CANCELLED" for s in states),
        "unresolved_acks": sum(s == "UNRESOLVED_ACK" for s in states),
        "ack_without_confirmed_fill": len(ack_without_fill_ids),
        "consistency_warnings": consistency_warnings,
    }
    result["by_side"] = {}
    for side in ("BUY", "SELL"):
        side_rows = [r for r in rows if str(r.get("side") or "").upper() == side]
        side_states = [str(r.get("status") or "").upper() for r in side_rows]
        side_fills = [f for f in fill_rows if str(f.get("side") or "").upper() == side]
        side_filled_order_ids = {str(f.get("order_id")) for f in side_fills if f.get("order_id")}
        side_ack_without_fill = {
            str(row.get("order_id")) for row in side_rows
            if row.get("order_id") and str(row.get("status") or "").upper() in {"ACKED", "ACCEPTED"}
        } - side_filled_order_ids
        result["by_side"][side] = {
            "order_intents_created": len(side_rows),
            "broker_submitted": sum(s in submitted_states for s in side_states),
            "broker_acked": sum(s in ack_states for s in side_states),
            "fills_confirmed": len(side_fills),
            "partial_fills": sum(s == "PARTIAL_FILLED" for s in side_states),
            "broker_rejected": sum(s in {"REJECTED", "ERROR", "FAILED"} for s in side_states),
            "cancelled": sum(s == "CANCELLED" for s in side_states),
            "unresolved_acks": sum(s == "UNRESOLVED_ACK" for s in side_states),
            "ack_without_confirmed_fill": len(side_ack_without_fill),
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
