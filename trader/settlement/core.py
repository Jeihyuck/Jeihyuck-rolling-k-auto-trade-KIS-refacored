"""Transactional, idempotent KR/US economic settlement primitive.

This does not talk to KIS, select strategy policy, or issue orders. Market-
specific adapters validate provenance and project economic deltas via the
provided SQLAlchemy Connection; any failure rolls the whole operation back.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
import hashlib
from typing import Callable

import sqlalchemy as sa
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .schema import applications, evidence

_SOURCES = frozenset({
    "KIS_EXECUTION_ACTUAL",
    "KIS_ORDER_CUMULATIVE_ACTUAL",
    "HOLDINGS_DELTA_EXCLUSIVE",
})


class SettlementConflict(RuntimeError):
    """Proof is missing, contradictory or would double-apply a broker fill."""


@dataclass(frozen=True)
class SettlementObservation:
    env: str
    market: str
    account_scope: str
    trading_epoch_id: str
    strategy_owner: str
    position_cycle_id: str
    client_order_key: str
    broker_trade_date: date
    exchange: str
    broker_order_no: str
    side: str
    requested_qty: int
    cumulative_qty: int
    evidence_type: str
    evidence_digest: str
    currency: str
    execution_price: Decimal | None = None
    execution_qty: int | None = None
    pre_holding_qty: int | None = None
    post_holding_qty: int | None = None
    exclusive_order_proof: bool = False

    def __post_init__(self) -> None:
        if self.market not in {"KR", "US"} or self.side not in {"BUY", "SELL"}:
            raise SettlementConflict("INVALID_MARKET_OR_SIDE")
        if self.evidence_type not in _SOURCES:
            raise SettlementConflict("UNAUTHORIZED_EVIDENCE_TYPE")
        if self.currency != ("KRW" if self.market == "KR" else "USD"):
            raise SettlementConflict("CURRENCY_MISMATCH")
        required = (self.env, self.account_scope, self.trading_epoch_id,
                    self.strategy_owner, self.position_cycle_id,
                    self.client_order_key, self.exchange, self.evidence_digest)
        if not all(str(value).strip() for value in required):
            raise SettlementConflict("ORDER_PROVENANCE_MISSING")
        if not isinstance(self.broker_trade_date, date):
            raise SettlementConflict("ORIGINAL_BROKER_TRADE_DATE_REQUIRED")
        if not (0 <= self.cumulative_qty <= self.requested_qty and self.requested_qty > 0):
            raise SettlementConflict("CUMULATIVE_QUANTITY_OUT_OF_RANGE")
        if self.execution_price is not None and (
            not self.execution_price.is_finite() or self.execution_price <= 0
        ):
            raise SettlementConflict("EXECUTION_PRICE_INVALID")
        if self.execution_price is None and self.execution_qty is not None:
            raise SettlementConflict("EXECUTION_QTY_WITHOUT_PRICE")
        if self.evidence_type == "KIS_EXECUTION_ACTUAL" and self.execution_price is not None:
            if self.execution_qty is None or not (0 < self.execution_qty <= self.cumulative_qty):
                raise SettlementConflict("INDIVIDUAL_EXECUTION_QTY_REQUIRED")
        if self.evidence_type == "KIS_ORDER_CUMULATIVE_ACTUAL" and self.execution_qty is not None:
            raise SettlementConflict("CUMULATIVE_PRICE_MUST_NOT_HAVE_EXECUTION_QTY")
        if self.evidence_type == "HOLDINGS_DELTA_EXCLUSIVE":
            if not self.exclusive_order_proof or (
                self.pre_holding_qty is None or self.post_holding_qty is None
            ):
                raise SettlementConflict("HOLDINGS_ORDER_ATTRIBUTION_UNPROVEN")
            delta = (self.post_holding_qty - self.pre_holding_qty
                     if self.side == "BUY"
                     else self.pre_holding_qty - self.post_holding_qty)
            if delta != self.cumulative_qty or delta < 0:
                raise SettlementConflict("HOLDINGS_DELTA_MISMATCH")
            if self.execution_price is not None:
                raise SettlementConflict("HOLDINGS_NOT_EXECUTION_PRICE_EVIDENCE")


def canonical_settlement_key(obs: SettlementObservation) -> str:
    # Local client order identity survives an ambiguous ACK and later order
    # number discovery; broker order number alone is never globally unique.
    fields = (obs.env, obs.market, obs.account_scope, obs.trading_epoch_id,
              obs.strategy_owner, obs.position_cycle_id,
              obs.client_order_key, obs.side, obs.broker_trade_date.isoformat(),
              obs.exchange)
    return hashlib.sha256("|".join(fields).encode("utf-8")).hexdigest()


def _evidence_id(obs: SettlementObservation) -> str:
    fields = (canonical_settlement_key(obs), obs.evidence_type,
              obs.evidence_digest, str(obs.cumulative_qty),
              str(obs.execution_price) if obs.execution_price is not None else "",
              str(obs.execution_qty) if obs.execution_qty is not None else "")
    return hashlib.sha256("|".join(fields).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class SettlementDecision:
    settlement_key: str
    evidence_id: str
    qty_delta: int
    notional_delta: Decimal
    cumulative_qty: int
    priced_qty_delta: int
    priced_qty: int
    price_status: str
    settlement_status: str


def preview_decision(obs: SettlementObservation, *, applied_qty: int = 0,
                     applied_notional: Decimal = Decimal("0"),
                     priced_qty: int = 0,
                     price_status: str = "PRICE_PENDING") -> SettlementDecision:
    """Pure replayable transition; neither I/O nor position mutation.

    Individual KIS executions contribute price only for their execution_qty.
    Order-cumulative evidence may replace the cumulative notional because its
    execution_price is explicitly the broker cumulative average.
    """
    if obs.cumulative_qty < applied_qty:
        raise SettlementConflict("CUMULATIVE_QUANTITY_REGRESSION")
    if obs.cumulative_qty > obs.requested_qty:
        raise SettlementConflict("FILL_EXCEEDS_REQUEST")
    if priced_qty < 0 or priced_qty > applied_qty:
        raise SettlementConflict("PRICED_QUANTITY_WATERMARK_INVALID")

    previous_notional = Decimal(str(applied_notional))
    previous_priced_qty = int(priced_qty)
    new_notional = previous_notional
    new_priced_qty = previous_priced_qty

    if obs.execution_price is not None:
        if obs.evidence_type == "KIS_EXECUTION_ACTUAL":
            execution_qty = int(obs.execution_qty or 0)
            if previous_priced_qty + execution_qty > obs.cumulative_qty:
                raise SettlementConflict("PRICE_COVERAGE_EXCEEDS_CUMULATIVE")
            new_notional = previous_notional + obs.execution_price * Decimal(execution_qty)
            new_priced_qty = previous_priced_qty + execution_qty
        elif obs.evidence_type == "KIS_ORDER_CUMULATIVE_ACTUAL":
            target_notional = obs.execution_price * Decimal(obs.cumulative_qty)
            if obs.cumulative_qty < previous_priced_qty:
                raise SettlementConflict("PRICED_QUANTITY_REGRESSION")
            if (
                obs.cumulative_qty == previous_priced_qty
                and price_status == "CONFIRMED"
                and abs(target_notional - previous_notional) > Decimal("0.00000001")
            ):
                raise SettlementConflict("CONFLICTING_CONFIRMED_EXECUTION_PRICE")
            new_notional = target_notional
            new_priced_qty = obs.cumulative_qty

    qty_delta = obs.cumulative_qty - applied_qty
    priced_qty_delta = new_priced_qty - previous_priced_qty
    next_price_status = (
        "CONFIRMED"
        if obs.cumulative_qty > 0 and new_priced_qty == obs.cumulative_qty
        else "PRICE_PENDING"
    )
    return SettlementDecision(
        settlement_key=canonical_settlement_key(obs),
        evidence_id=_evidence_id(obs),
        qty_delta=qty_delta,
        notional_delta=new_notional - previous_notional,
        cumulative_qty=obs.cumulative_qty,
        priced_qty_delta=priced_qty_delta,
        priced_qty=new_priced_qty,
        price_status=next_price_status,
        settlement_status=(
            "QUANTITY_PENDING" if obs.cumulative_qty == 0
            else "PRICE_PENDING" if next_price_status != "CONFIRMED"
            else "PARTIALLY_SETTLED" if obs.cumulative_qty < obs.requested_qty
            else "SETTLED"
        ),
    )

def _insert_if_absent(conn: Connection, table: sa.Table, values: dict) -> None:
    dialect = conn.dialect.name
    if dialect == "postgresql":
        stmt = pg_insert(table).values(**values).on_conflict_do_nothing()
    elif dialect == "sqlite":
        stmt = sqlite_insert(table).values(**values).on_conflict_do_nothing()
    else:
        raise SettlementConflict("UNSUPPORTED_ATOMIC_DB_DIALECT")
    conn.execute(stmt)


def settle_atomic(
    engine: Engine,
    obs: SettlementObservation,
    apply_economic_delta: Callable[[Connection, SettlementObservation, SettlementDecision], None],
) -> SettlementDecision:
    """Persist broker evidence, position accounting and watermark atomically.

    The callback MUST use the given `conn` (not a repository method that opens
    another transaction). It must apply `qty_delta` and `notional_delta` once;
    owner/cycle and price-pending semantics belong to the market adapter.
    """
    if apply_economic_delta is None:
        raise SettlementConflict("ECONOMIC_PROJECTION_CALLBACK_REQUIRED")
    key = canonical_settlement_key(obs)
    evidence_id = _evidence_id(obs)
    with engine.begin() as conn:
        _insert_if_absent(conn, applications, {
            "settlement_key": key, "env": obs.env, "market": obs.market,
            "account_scope": obs.account_scope,
            "trading_epoch_id": obs.trading_epoch_id,
            "strategy_owner": obs.strategy_owner,
            "position_cycle_id": obs.position_cycle_id,
            "client_order_key": obs.client_order_key,
            "requested_qty": obs.requested_qty,
            "broker_trade_date": obs.broker_trade_date,
            "exchange": obs.exchange, "side": obs.side, "currency": obs.currency,
            "applied_qty": 0, "applied_notional": Decimal("0"),
            "priced_qty": 0, "price_status": "PRICE_PENDING", "settlement_status": "QUANTITY_PENDING",
        })
        row = conn.execute(
            sa.select(applications).where(applications.c.settlement_key == key)
            .with_for_update()
        ).mappings().one()
        scope_fields = ("env", "market", "account_scope", "trading_epoch_id",
                        "strategy_owner", "position_cycle_id", "client_order_key",
                        "broker_trade_date", "exchange", "side", "currency", "requested_qty")
        if any(str(row[field]) != str(getattr(obs, field)) for field in scope_fields):
            raise SettlementConflict("SETTLEMENT_SCOPE_CONFLICT")
        existing_evidence = conn.execute(
            sa.select(evidence.c.evidence_id).where(evidence.c.evidence_id == evidence_id)
        ).scalar_one_or_none()
        if existing_evidence is not None:
            return SettlementDecision(
                settlement_key=key, evidence_id=evidence_id,
                qty_delta=0, notional_delta=Decimal("0"),
                cumulative_qty=obs.cumulative_qty,
                priced_qty_delta=0, priced_qty=int(row["priced_qty"]),
                price_status=str(row["price_status"]),
                settlement_status=str(row["settlement_status"]),
            )
        decision = preview_decision(
            obs,
            applied_qty=int(row["applied_qty"]),
            applied_notional=Decimal(str(row["applied_notional"])),
            priced_qty=int(row["priced_qty"]),
            price_status=str(row["price_status"]),
        )
        _insert_if_absent(conn, evidence, {
            "evidence_id": evidence_id, "settlement_key": key,
            "env": obs.env, "market": obs.market, "account_scope": obs.account_scope,
            "trading_epoch_id": obs.trading_epoch_id,
            "strategy_owner": obs.strategy_owner,
            "position_cycle_id": obs.position_cycle_id,
            "client_order_key": obs.client_order_key,
            "broker_trade_date": obs.broker_trade_date,
            "exchange": obs.exchange, "broker_order_no": obs.broker_order_no or None,
            "side": obs.side, "evidence_type": obs.evidence_type,
            "evidence_digest": obs.evidence_digest,
            "confirmed_cumulative_qty": obs.cumulative_qty,
            "execution_price": obs.execution_price,
            "execution_qty": obs.execution_qty,
            "currency": obs.currency,
        })
        # Application callback failure rolls evidence + new application row back.
        apply_economic_delta(conn, obs, decision)
        conn.execute(sa.update(applications).where(
            applications.c.settlement_key == key
        ).values(
            applied_qty=obs.cumulative_qty,
            applied_notional=(
                Decimal(str(row["applied_notional"])) + decision.notional_delta
            ),
            priced_qty=decision.priced_qty,
            price_status=decision.price_status,
            settlement_status=decision.settlement_status,
            last_evidence_id=evidence_id,
            updated_at=sa.func.now(),
        ))
    return decision
