from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from trader.execution_state import SemanticActionIdentity


def build_execution_claim_tables(
    metadata: sa.MetaData, action_name: str, attempt_name: str,
) -> tuple[sa.Table, sa.Table]:
    actions = sa.Table(
        action_name,
        metadata,
        sa.Column("action_key", sa.String(64), primary_key=True),
        sa.Column("env", sa.String, nullable=False),
        sa.Column("account_id", sa.String, nullable=False),
        sa.Column("market", sa.String, nullable=False),
        sa.Column("trading_epoch_id", sa.String, nullable=False),
        sa.Column("strategy_owner", sa.String, nullable=False),
        sa.Column("lifecycle_id", sa.String, nullable=False),
        sa.Column("action", sa.String, nullable=False),
        sa.Column("target_qty", sa.Integer, nullable=False),
        sa.Column("cumulative_filled_qty", sa.Integer),
        sa.Column("remaining_target_qty", sa.Integer),
        sa.Column("filled_qty_before_attempt", sa.Integer, nullable=False, server_default="0"),
        sa.Column("active_attempt_id", sa.String),
        sa.Column("action_state", sa.String, nullable=False),
        sa.Column("claim_conflicts", sa.Integer, nullable=False, server_default="0"),
        sa.Column("trade_date", sa.Date),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    attempts = sa.Table(
        attempt_name,
        metadata,
        sa.Column("attempt_id", sa.String, primary_key=True),
        sa.Column("action_key", sa.String(64), sa.ForeignKey(f"{action_name}.action_key"), nullable=False),
        sa.Column("attempt_no", sa.Integer, nullable=False),
        sa.Column("requested_qty", sa.Integer, nullable=False),
        sa.Column("client_order_key", sa.String),
        sa.Column("cumulative_filled_qty", sa.Integer),
        sa.Column("attempt_state", sa.String, nullable=False),
        sa.Column("authoritative", sa.Boolean, nullable=False, server_default=sa.text("false")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("action_key", "attempt_no", name=f"uq_{attempt_name}_action_attempt"),
        sa.Index(f"ix_{attempt_name}_client_key", "client_order_key"),
    )
    return actions, attempts


@dataclass(frozen=True)
class ExecutionClaim:
    acquired: bool
    action_key: str
    attempt_id: str | None = None
    reason: str = ""


@dataclass(frozen=True)
class ExecutionClaimSnapshot:
    action_key: str
    action_state: str
    target_qty: int
    cumulative_filled_qty: int | None
    remaining_target_qty: int | None
    active_attempt_id: str | None
    trade_date: date | None


class DurableExecutionClaimRepo:
    def __init__(self, engine: Any, action_table: sa.Table, attempt_table: sa.Table | None = None):
        self.engine = engine
        self.actions = action_table
        if attempt_table is None:
            attempt_name = (
                "us_execution_attempts"
                if action_table.name == "us_execution_claims"
                else "execution_attempts"
            )
            attempt_table = action_table.metadata.tables[attempt_name]
        self.attempts = attempt_table

    def _insert(self, conn: Any, table: sa.Table, values: dict) -> Any:
        dialect = conn.dialect.name
        if dialect == "postgresql":
            return pg_insert(table).values(**values)
        if dialect == "sqlite":
            return sqlite_insert(table).values(**values)
        raise RuntimeError(f"durable execution claims unsupported dialect: {dialect}")

    @staticmethod
    def _date(identity: SemanticActionIdentity) -> date | None:
        value = identity.trade_date
        if isinstance(value, datetime):
            return value.date()
        return value

    def acquire(
        self,
        identity: SemanticActionIdentity,
        *,
        attempt_id: str,
        requested_qty: int,
        fresh_validation: bool = False,
        client_order_key: str | None = None,
        retry_action_prefix: str | None = None,
    ) -> ExecutionClaim:
        qty = int(requested_qty)
        if qty <= 0 or not str(attempt_id or "").strip():
            raise ValueError("submit claim requires positive quantity and attempt id")
        key = identity.action_key
        now = datetime.now(timezone.utc)
        action = self.actions
        attempt = self.attempts
        base = {
            "action_key": key,
            "env": identity.env.strip().lower(),
            "account_id": identity.account_id.strip(),
            "market": identity.market.strip().upper(),
            "trading_epoch_id": identity.trading_epoch_id.strip(),
            "strategy_owner": identity.strategy_owner.strip().upper(),
            "lifecycle_id": identity.lifecycle_id.strip(),
            "action": identity.action.strip().upper(),
            "target_qty": qty,
            "cumulative_filled_qty": 0,
            "remaining_target_qty": qty,
            "filled_qty_before_attempt": 0,
            "active_attempt_id": attempt_id,
            "action_state": "IN_FLIGHT",
            "trade_date": self._date(identity),
            "updated_at": now,
            "claim_conflicts": 0,
        }
        with self.engine.begin() as conn:
            if conn.dialect.name == "postgresql":
                scope = json.dumps(
                    (
                        identity.env.strip().lower(),
                        identity.account_id.strip(),
                        identity.market.strip().upper(),
                        identity.trading_epoch_id.strip(),
                        identity.strategy_owner.strip().upper(),
                        identity.lifecycle_id.strip(),
                    ),
                    separators=(",", ":"),
                )
                lock_id = int.from_bytes(
                    hashlib.sha256(scope.encode("utf-8")).digest()[:8],
                    byteorder="big",
                    signed=True,
                )
                conn.execute(sa.select(sa.func.pg_advisory_xact_lock(lock_id)))
            if retry_action_prefix:
                prefix = str(retry_action_prefix).strip().upper()
                retryable_sibling = conn.execute(
                    sa.select(action.c.action_key)
                    .where(
                        action.c.action_key != key,
                        action.c.env == identity.env.strip().lower(),
                        action.c.account_id == identity.account_id.strip(),
                        action.c.market == identity.market.strip().upper(),
                        action.c.trading_epoch_id == identity.trading_epoch_id.strip(),
                        action.c.strategy_owner == identity.strategy_owner.strip().upper(),
                        action.c.lifecycle_id == identity.lifecycle_id.strip(),
                        sa.or_(
                            action.c.action == prefix,
                            sa.func.substr(action.c.action, 1, len(prefix) + 1)
                            == f"{prefix}:",
                        ),
                        action.c.action_state.in_(
                            ("OPEN", "RETRYABLE", "PARTIALLY_SATISFIED")
                        ),
                        action.c.active_attempt_id.is_(None),
                    )
                    .limit(1)
                ).scalar_one_or_none()
                if retryable_sibling is not None:
                    self._record_conflict(conn, str(retryable_sibling))
                    return ExecutionClaim(
                        False, key, reason="retryable_lifecycle_action_exists",
                    )
            active_sibling = conn.execute(
                sa.select(action.c.action_key)
                .where(
                    action.c.action_key != key,
                    action.c.env == identity.env.strip().lower(),
                    action.c.account_id == identity.account_id.strip(),
                    action.c.market == identity.market.strip().upper(),
                    action.c.trading_epoch_id == identity.trading_epoch_id.strip(),
                    action.c.strategy_owner == identity.strategy_owner.strip().upper(),
                    action.c.lifecycle_id == identity.lifecycle_id.strip(),
                    sa.or_(
                        action.c.action_state.in_(("IN_FLIGHT", "UNCERTAIN")),
                        action.c.active_attempt_id.is_not(None),
                    ),
                )
                .limit(1)
            ).scalar_one_or_none()
            if active_sibling is not None:
                self._record_conflict(conn, str(active_sibling))
                return ExecutionClaim(
                    False, key, reason="unresolved_lifecycle_action",
                )
            insert_action = self._insert(conn, action, base)
            created = conn.execute(
                insert_action.on_conflict_do_nothing(index_elements=[action.c.action_key])
                .returning(action.c.action_key)
            ).scalar_one_or_none()
            if created is not None:
                self._insert_attempt(
                    conn, attempt, key, attempt_id, 1, qty, client_order_key,
                )
                return ExecutionClaim(True, key, attempt_id)

            row = conn.execute(
                sa.select(action).where(action.c.action_key == key).with_for_update()
            ).mappings().one_or_none()
            if row is None:
                raise RuntimeError("execution claim vanished during acquisition")
            if row["active_attempt_id"]:
                self._record_conflict(conn, key)
                return ExecutionClaim(False, key, reason="unresolved_attempt_active")
            if row["action_state"] not in {"OPEN", "RETRYABLE", "PARTIALLY_SATISFIED"}:
                self._record_conflict(conn, key)
                return ExecutionClaim(False, key, reason="action_not_retryable")
            if not fresh_validation:
                self._record_conflict(conn, key)
                return ExecutionClaim(False, key, reason="fresh_validation_required")
            remaining = row["remaining_target_qty"]
            if remaining is None or qty != int(remaining):
                self._record_conflict(conn, key)
                return ExecutionClaim(False, key, reason="remaining_target_qty_mismatch")
            attempt_no = int(conn.execute(
                sa.select(sa.func.coalesce(sa.func.max(attempt.c.attempt_no), 0))
                .where(attempt.c.action_key == key)
            ).scalar_one()) + 1
            claimed = conn.execute(
                sa.update(action)
                .where(
                    action.c.action_key == key,
                    action.c.active_attempt_id.is_(None),
                    action.c.action_state.in_(("OPEN", "RETRYABLE", "PARTIALLY_SATISFIED")),
                    action.c.remaining_target_qty == qty,
                )
                .values(
                    active_attempt_id=attempt_id,
                    action_state="IN_FLIGHT",
                    filled_qty_before_attempt=row["cumulative_filled_qty"],
                    cumulative_filled_qty=None,
                    remaining_target_qty=None,
                    trade_date=self._date(identity),
                    updated_at=now,
                )
                .returning(action.c.action_key)
            ).scalar_one_or_none()
            if claimed is None:
                self._record_conflict(conn, key)
                return ExecutionClaim(False, key, reason="claim_race_lost")
            self._insert_attempt(
                conn, attempt, key, attempt_id, attempt_no, qty, client_order_key,
            )
            return ExecutionClaim(True, key, attempt_id)

    @staticmethod
    def _insert_attempt(
        conn: Any, attempts: sa.Table, key: str, attempt_id: str, attempt_no: int,
        qty: int, client_order_key: str | None,
    ) -> None:
        conn.execute(sa.insert(attempts).values(
            attempt_id=attempt_id,
            action_key=key,
            attempt_no=attempt_no,
            requested_qty=qty,
            client_order_key=str(client_order_key) if client_order_key else None,
            cumulative_filled_qty=None,
            attempt_state="CREATED",
            authoritative=False,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        ))

    def _record_conflict(self, conn: Any, key: str) -> None:
        conn.execute(
            sa.update(self.actions)
            .where(self.actions.c.action_key == key)
            .values(
                claim_conflicts=self.actions.c.claim_conflicts + 1,
                updated_at=datetime.now(timezone.utc),
            )
        )

    def record_observation(
        self,
        identity: SemanticActionIdentity | str,
        *,
        attempt_id: str,
        state: str,
        cumulative_filled_qty: int | None,
        authoritative: bool,
    ) -> ExecutionClaimSnapshot:
        state = str(state or "").strip().upper()
        if state not in {
            "CREATED", "SUBMITTED", "ACKED", "UNRESOLVED", "PARTIALLY_FILLED",
            "FILLED", "REJECTED_EXPLICIT", "CANCELLED", "CANCELLED_ZERO_FILL",
            "CANCELLED_PARTIAL_FILL", "RECONCILE_ERROR",
        }:
            raise ValueError(f"unsupported submit attempt state: {state}")
        observed_qty = None if cumulative_filled_qty is None else int(cumulative_filled_qty)
        if observed_qty is not None and observed_qty < 0:
            raise ValueError("cumulative fill quantity cannot be negative")
        key = self._action_key(identity)
        actions, attempts = self.actions, self.attempts
        with self.engine.begin() as conn:
            action_row = conn.execute(
                sa.select(actions).where(actions.c.action_key == key).with_for_update()
            ).mappings().one_or_none()
            attempt_row = conn.execute(
                sa.select(attempts).where(
                    attempts.c.action_key == key,
                    attempts.c.attempt_id == str(attempt_id),
                ).with_for_update()
            ).mappings().one_or_none()
            if action_row is None or attempt_row is None:
                raise RuntimeError("execution action or submit attempt not found")
            latest_id = conn.execute(
                sa.select(attempts.c.attempt_id)
                .where(attempts.c.action_key == key)
                .order_by(attempts.c.attempt_no.desc())
                .limit(1)
            ).scalar_one()
            if str(latest_id) != str(attempt_id):
                raise RuntimeError("observation for superseded submit attempt")
            requested = int(attempt_row["requested_qty"])
            previous = attempt_row["cumulative_filled_qty"]
            if observed_qty is not None and observed_qty > requested:
                raise ValueError("cumulative fill quantity exceeds submitted quantity")
            if state == "REJECTED_EXPLICIT" and authoritative and observed_qty is None:
                observed_qty = 0
            if observed_qty is not None and previous is not None:
                observed_qty = max(int(previous), observed_qty)
            elif observed_qty is None:
                observed_qty = previous
            if state in {"CANCELLED_ZERO_FILL", "CANCELLED_PARTIAL_FILL"}:
                state = "CANCELLED"

            before = int(action_row["filled_qty_before_attempt"] or 0)
            total = None if observed_qty is None else before + observed_qty
            target = int(action_row["target_qty"])
            remaining = None if total is None else max(0, target - total)
            action_state = "IN_FLIGHT"
            active_attempt_id: str | None = str(attempt_id)
            attempt_state = state
            if state in {"CREATED", "SUBMITTED", "ACKED", "UNRESOLVED", "RECONCILE_ERROR"}:
                action_state = "UNCERTAIN" if state in {"UNRESOLVED", "RECONCILE_ERROR"} else "IN_FLIGHT"
            elif state == "PARTIALLY_FILLED":
                action_state = "PARTIALLY_SATISFIED"
            elif state == "FILLED":
                action_state = "SATISFIED" if remaining == 0 else "PARTIALLY_SATISFIED"
                active_attempt_id = None
            elif state == "REJECTED_EXPLICIT":
                if authoritative and observed_qty == 0:
                    action_state = "RETRYABLE" if total == 0 else "PARTIALLY_SATISFIED"
                    active_attempt_id = None
                else:
                    action_state = "UNCERTAIN"
            elif state == "CANCELLED":
                if authoritative and observed_qty is not None:
                    action_state = "RETRYABLE" if total == 0 else "PARTIALLY_SATISFIED"
                    active_attempt_id = None
                    attempt_state = "CANCELLED_ZERO_FILL" if observed_qty == 0 else "CANCELLED_PARTIAL_FILL"
                else:
                    action_state = "UNCERTAIN"

            conn.execute(
                sa.update(attempts)
                .where(attempts.c.attempt_id == str(attempt_id))
                .values(
                    cumulative_filled_qty=observed_qty,
                    attempt_state=attempt_state,
                    authoritative=bool(authoritative),
                    updated_at=datetime.now(timezone.utc),
                )
            )
            conn.execute(
                sa.update(actions)
                .where(actions.c.action_key == key)
                .values(
                    cumulative_filled_qty=total,
                    remaining_target_qty=remaining,
                    active_attempt_id=active_attempt_id,
                    action_state=action_state,
                    updated_at=datetime.now(timezone.utc),
                )
            )
        return self.get(identity)

    def release_before_submit(
        self, identity: SemanticActionIdentity, *, attempt_id: str,
    ) -> None:
        key = identity.action_key
        with self.engine.begin() as conn:
            result = conn.execute(
                sa.update(self.actions)
                .where(
                    self.actions.c.action_key == key,
                    self.actions.c.active_attempt_id == str(attempt_id),
                    self.actions.c.action_state == "IN_FLIGHT",
                )
                .values(
                    active_attempt_id=None,
                    action_state="OPEN",
                    cumulative_filled_qty=self.actions.c.filled_qty_before_attempt,
                    remaining_target_qty=(
                        self.actions.c.target_qty - self.actions.c.filled_qty_before_attempt
                    ),
                    updated_at=datetime.now(timezone.utc),
                )
            )
            if result.rowcount != 1:
                raise RuntimeError("execution claim could not be released before submit")
            conn.execute(
                sa.update(self.attempts)
                .where(self.attempts.c.attempt_id == str(attempt_id))
                .values(
                    attempt_state="ABANDONED_PRE_SUBMIT",
                    authoritative=True,
                    updated_at=datetime.now(timezone.utc),
                )
            )

    @staticmethod
    def _action_key(identity: SemanticActionIdentity | str) -> str:
        return identity.action_key if isinstance(identity, SemanticActionIdentity) else str(identity)

    def find_retryable_action(
        self,
        identity: SemanticActionIdentity,
        *,
        action_prefix: str,
    ) -> str | None:
        prefix = str(action_prefix or "").strip().upper()
        if not prefix:
            raise ValueError("retryable action lookup requires an action prefix")
        with self.engine.connect() as conn:
            row = conn.execute(
                sa.select(self.actions.c.action)
                .where(
                    self.actions.c.env == identity.env.strip().lower(),
                    self.actions.c.account_id == identity.account_id.strip(),
                    self.actions.c.market == identity.market.strip().upper(),
                    self.actions.c.trading_epoch_id == identity.trading_epoch_id.strip(),
                    self.actions.c.strategy_owner == identity.strategy_owner.strip().upper(),
                    self.actions.c.lifecycle_id == identity.lifecycle_id.strip(),
                    sa.or_(
                        self.actions.c.action == prefix,
                        sa.func.substr(self.actions.c.action, 1, len(prefix) + 1)
                        == f"{prefix}:",
                    ),
                    self.actions.c.action_state.in_(
                        ("OPEN", "RETRYABLE", "PARTIALLY_SATISFIED")
                    ),
                    self.actions.c.active_attempt_id.is_(None),
                )
                .order_by(self.actions.c.updated_at.desc())
                .limit(1)
            ).scalar_one_or_none()
        return str(row) if row is not None else None

    def get(self, identity: SemanticActionIdentity | str) -> ExecutionClaimSnapshot:
        key = self._action_key(identity)
        with self.engine.connect() as conn:
            row = conn.execute(
                sa.select(self.actions).where(self.actions.c.action_key == key)
            ).mappings().one_or_none()
        if row is None:
            raise RuntimeError("semantic action not found")
        return ExecutionClaimSnapshot(
            action_key=str(row["action_key"]),
            action_state=str(row["action_state"]),
            target_qty=int(row["target_qty"]),
            cumulative_filled_qty=(
                None if row["cumulative_filled_qty"] is None else int(row["cumulative_filled_qty"])
            ),
            remaining_target_qty=(
                None if row["remaining_target_qty"] is None else int(row["remaining_target_qty"])
            ),
            active_attempt_id=row["active_attempt_id"],
            trade_date=row["trade_date"],
        )

    def find_attempt_for_client_order_key(self, client_order_key: str) -> tuple[str, str] | None:
        with self.engine.connect() as conn:
            row = conn.execute(
                sa.select(self.attempts.c.action_key, self.attempts.c.attempt_id)
                .where(self.attempts.c.client_order_key == str(client_order_key))
                .order_by(self.attempts.c.attempt_no.desc())
                .limit(1)
            ).first()
        return (str(row[0]), str(row[1])) if row else None

    def health(self) -> dict[str, int]:
        with self.engine.connect() as conn:
            values = conn.execute(
                sa.select(
                    sa.func.sum(sa.case(
                        (
                            sa.or_(
                                self.actions.c.action_state.in_(("IN_FLIGHT", "UNCERTAIN")),
                                self.actions.c.active_attempt_id.is_not(None),
                            ),
                            1,
                        ),
                        else_=0,
                    )),
                    sa.func.sum(self.actions.c.claim_conflicts),
                )
            ).one()
        return {
            "unresolved_execution_actions": int(values[0] or 0),
            "execution_claim_conflicts": int(values[1] or 0),
        }
