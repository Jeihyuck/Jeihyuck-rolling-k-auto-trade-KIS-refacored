from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict
from datetime import date
from uuid import uuid4

from sqlalchemy import text
from trader.db.schema import schema_for_engine
from trader.db.engine import get_engine
from trader.account_state import get_account_key, resolve_env_name
from trader.db.trading_epoch import active_trading_epoch_id, trading_epoch_enforced
from trader.execution_claims import DurableExecutionClaimRepo
from trader.execution_state import SemanticActionIdentity
from trader.kr_price_utils import normalize_kr_order_price

from .models import BrokerOrderState, Decision, OrderIntent, State, Status

PENDING = frozenset({"INTENT_CREATED", "SUBMITTED", "ACK", "PENDING", "PARTIALLY_FILLED", "RECONCILE_PENDING"})


def execution_action_identity(
    *,
    env: str,
    account_key: str,
    market: str,
    trading_epoch_id: str,
    symbol: str,
    cycle_id: str,
    side: str,
    reason: str,
    stage: str | None = None,
    unit_sequence: int | None = None,
    trade_date: date | None = None,
) -> SemanticActionIdentity:
    side_value = str(side or "").upper()
    if side_value in {"SELL_PARTIAL", "SELL_ALL"}:
        semantic_action = f"SELL:{str(stage or reason).upper()}"
    else:
        semantic_action = f"{side_value}:{str(reason).upper()}:UNIT:{int(unit_sequence or 0)}"
    return SemanticActionIdentity(
        env=env,
        account_id=hashlib.sha256(account_key.encode("utf-8")).hexdigest(),
        market=market,
        trading_epoch_id=trading_epoch_id,
        strategy_owner="KR_INFINITE",
        lifecycle_id=f"{symbol}:{cycle_id}",
        action=semantic_action,
        trade_date=trade_date,
    )


class InfiniteRepository:
    def __init__(self, engine=None):
        self.engine = engine or get_engine()

    def _epoch_id(self, bind, *, required: bool | None = None) -> str | None:
        if required is None:
            required = trading_epoch_enforced()
        env = resolve_env_name()
        return active_trading_epoch_id(
            bind, env=env, account_id=get_account_key(env=env), required=bool(required)
        )

    def claim_submit(
        self,
        *,
        state: State,
        decision: Decision,
        trade_date: date,
        symbol: str,
        env: str,
    ):
        account_key = get_account_key(env=env)
        epoch_id = active_trading_epoch_id(
            self.engine, env=env, account_id=account_key, required=True,
        )
        stage = str(
            decision.metadata.get("desired_profit_stage")
            or decision.metadata.get("profit_stage")
            or ""
        )
        identity = execution_action_identity(
            env=env,
            account_key=account_key,
            market="KR",
            trading_epoch_id=str(epoch_id),
            symbol=symbol,
            cycle_id=str(state.cycle_id or ""),
            side=decision.action.value,
            reason=decision.reason,
            stage=stage or None,
            unit_sequence=state.units_used + 1 if decision.action.value in {"BUY", "RECOVERY"} else None,
            trade_date=trade_date,
        )
        schema = schema_for_engine(self.engine)
        repo = DurableExecutionClaimRepo(
            self.engine, schema.execution_claims, schema.execution_attempts,
        )
        attempt_id = str(uuid4())
        claim = repo.acquire(
            identity,
            attempt_id=attempt_id,
            requested_qty=int(decision.qty or 0),
            client_order_key=str(decision.idempotency_key or ""),
            fresh_validation=True,
        )
        if claim.acquired:
            with self.engine.begin() as conn:
                metadata = json.dumps({
                    "execution_claim_action_key": claim.action_key,
                    "execution_submit_attempt_id": attempt_id,
                })
                if conn.dialect.name == "sqlite":
                    metadata_update = """
                        metadata=json_patch(COALESCE(metadata,'{}'), :metadata),
                        updated_at=CURRENT_TIMESTAMP
                    """
                else:
                    metadata_update = """
                        metadata=COALESCE(metadata,'{}'::jsonb) || CAST(:metadata AS jsonb),
                        updated_at=NOW()
                    """
                conn.execute(text(f"""UPDATE kr_infinite_order_intents
                    SET {metadata_update}
                    WHERE trading_epoch_id=:epoch_id AND idempotency_key=:key"""), {
                        "metadata": metadata,
                        "epoch_id": epoch_id,
                        "key": decision.idempotency_key,
                    })
        return identity, claim

    def record_execution_claim_observation(
        self,
        identity: SemanticActionIdentity,
        *,
        attempt_id: str,
        state: str,
        cumulative_filled_qty: int | None,
        authoritative: bool,
    ):
        schema = schema_for_engine(self.engine)
        repo = DurableExecutionClaimRepo(
            self.engine, schema.execution_claims, schema.execution_attempts,
        )
        return repo.record_observation(
            identity,
            attempt_id=attempt_id,
            state=state,
            cumulative_filled_qty=cumulative_filled_qty,
            authoritative=authoritative,
        )

    def record_execution_claim_for_order(
        self,
        client_order_key: str,
        *,
        state: str,
        cumulative_filled_qty: int | None,
        authoritative: bool,
    ):
        schema = schema_for_engine(self.engine)
        repo = DurableExecutionClaimRepo(
            self.engine, schema.execution_claims, schema.execution_attempts,
        )
        attempt = repo.find_attempt_for_client_order_key(client_order_key)
        if attempt is None:
            raise LookupError(
                f"execution claim attempt not found for client order key {client_order_key}"
            )
        action_key, attempt_id = attempt
        return repo.record_observation(
            action_key,
            attempt_id=attempt_id,
            state=state,
            cumulative_filled_qty=cumulative_filled_qty,
            authoritative=authoritative,
        )

    def ensure_schema(self) -> None:
        with self.engine.connect() as conn:
            state = conn.execute(text("SELECT to_regclass('public.kr_infinite_state')")).scalar()
            intents = conn.execute(text("SELECT to_regclass('public.kr_infinite_order_intents')")).scalar()
        if not state or not intents:
            raise RuntimeError("KR_INF_DB_UNAVAILABLE")

    def load_state(self) -> State | None:
        with self.engine.connect() as conn:
            epoch_id = self._epoch_id(conn)
            if epoch_id:
                row = conn.execute(text("""SELECT * FROM kr_infinite_state
                    WHERE trading_epoch_id=:epoch_id
                      AND strategy_id='KR_INFINITE_V1' AND symbol='122630'"""),
                    {"epoch_id": epoch_id}).mappings().first()
            else:
                row = conn.execute(text("SELECT * FROM kr_infinite_state WHERE strategy_id='KR_INFINITE_V1' AND symbol='122630'")).mappings().first()
        if row is None:
            return None
        values = {key: row[key] for key in State.__dataclass_fields__ if key in row}
        values["status"] = Status(values["status"])
        values["metadata"] = values.get("metadata") or {}
        return State(**values)

    def intent_keys(self) -> frozenset[str]:
        with self.engine.connect() as conn:
            epoch_id = self._epoch_id(conn)
            if epoch_id:
                rows = conn.execute(text("""SELECT idempotency_key FROM kr_infinite_order_intents
                    WHERE trading_epoch_id=:epoch_id"""), {"epoch_id": epoch_id}).all()
            else:
                rows = conn.execute(text("SELECT idempotency_key FROM kr_infinite_order_intents")).all()
        return frozenset(row[0] for row in rows)

    def pending_intents(self) -> list[OrderIntent]:
        with self.engine.connect() as conn:
            epoch_id = self._epoch_id(conn)
            if epoch_id:
                rows = conn.execute(text("""SELECT id,cycle_id,trade_date,side,idempotency_key,requested_qty,limit_price,unit_sequence,
                    broker_order_id,status,filled_qty,filled_notional_krw,metadata FROM kr_infinite_order_intents
                    WHERE trading_epoch_id=:epoch_id AND status=ANY(:statuses) ORDER BY id"""),
                    {"epoch_id": epoch_id, "statuses": list(PENDING)}).mappings().all()
            else:
                rows = conn.execute(text("""SELECT id,cycle_id,trade_date,side,idempotency_key,requested_qty,limit_price,unit_sequence,
                    broker_order_id,status,filled_qty,filled_notional_krw,metadata FROM kr_infinite_order_intents
                    WHERE status=ANY(:statuses) ORDER BY id"""), {"statuses": list(PENDING)}).mappings().all()
        result: list[OrderIntent] = []
        for row in rows:
            payload = dict(row)
            metadata = payload.get("metadata") or {}
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except Exception:
                    metadata = {}
            payload["metadata"] = metadata if isinstance(metadata, dict) else {}
            result.append(OrderIntent(**payload))
        return result

    @staticmethod
    def _intent_metadata(state: State, decision: Decision) -> dict:
        """Persist immutable broker baseline/provenance required for balance-delta reconciliation.

        Existing JSONB is used deliberately: no schema migration is required.  For
        TP decisions the strategy already supplies remaining_qty, so pre-order qty
        is provable.  A new-cycle BUY is known to start from flat.  Broker-adopted
        positions also carry their authoritative broker quantity/average in state.
        """
        metadata = dict(decision.metadata or {})
        if metadata.get("pre_order_holding_qty") is None:
            remaining = metadata.get("remaining_qty")
            if decision.action.value in {"SELL_PARTIAL", "SELL_ALL"} and remaining is not None:
                metadata["pre_order_holding_qty"] = max(0, int(remaining) + int(decision.qty or 0))
            elif decision.reason == "NEW_CYCLE_BUY":
                metadata["pre_order_holding_qty"] = 0
            elif (state.metadata or {}).get("broker_qty") is not None:
                metadata["pre_order_holding_qty"] = max(0, int((state.metadata or {}).get("broker_qty") or 0))
        if metadata.get("pre_order_avg_price") is None:
            if int(metadata.get("pre_order_holding_qty") or 0) == 0:
                metadata["pre_order_avg_price"] = 0.0
            elif (state.metadata or {}).get("broker_average_price") is not None:
                metadata["pre_order_avg_price"] = float((state.metadata or {}).get("broker_average_price") or 0.0)
        metadata["reconcile_contract_version"] = "KR_INF_BALANCE_DELTA_V1"
        metadata["strategy_owner"] = "KR_INFINITE"
        return metadata

    def create_intent(self, state: State, decision: Decision, trade_date: date, market_state: str) -> bool:
        """Persist first. A uniqueness loss means the caller must not submit."""
        intent_metadata = self._intent_metadata(state, decision)
        if decision.action.value in {"BUY", "RECOVERY"}:
            if int(decision.qty or 0) <= 0:
                raise ValueError("KR_INF_BUY_QTY_INVALID")
            decision_limit = float(decision.notional) / int(decision.qty)
            native_price, price_tick = normalize_kr_order_price(
                int(decision_limit), side="BUY",
            )
            if native_price <= 0:
                raise ValueError("KR_INF_SUBMIT_PRICE_INVALID")
            # The executor sends precisely this normalized BUY LIMIT to the
            # KIS wrapper, which normalizes idempotently. These are price
            # provenance fields, NOT broker execution/fill evidence.
            intent_metadata.update({
                "owner_decision_price_krw": decision_limit,
                "kis_submit_limit_price_krw": native_price,
                "kis_submit_tick_size_krw": price_tick,
                "kis_submit_price_basis": "OWNER_NOTIONAL_KRX_TICK_PREPARED",
                "kis_fill_price_verified": False,
            })
        with self.engine.begin() as conn:
            epoch_id = self._epoch_id(conn)
            if not epoch_id:
                raise RuntimeError("KR_INFINITE_ACTIVE_TRADING_EPOCH_REQUIRED")
            intent_metadata["trading_epoch_id"] = epoch_id
            row = conn.execute(text("""INSERT INTO kr_infinite_order_intents(
                trading_epoch_id,strategy_id,symbol,cycle_id,trade_date,side,reason,unit_sequence,requested_notional_krw,
                requested_qty,limit_price,idempotency_key,market_state,metadata)
                VALUES(:epoch_id,'KR_INFINITE_V1','122630',:cycle,:day,:side,:reason,:seq,:notional,:qty,:price,:key,:market,CAST(:metadata AS jsonb))
                ON CONFLICT(trading_epoch_id,idempotency_key) DO NOTHING RETURNING id"""), {
                "epoch_id": epoch_id,
                "cycle": state.cycle_id, "day": trade_date, "side": decision.action.value,
                "reason": decision.reason, "seq": state.units_used + 1 if decision.action.value in {"BUY", "RECOVERY"} else None,
                "notional": decision.notional, "qty": decision.qty,
                "price": decision.notional / decision.qty if decision.qty else None,
                "key": decision.idempotency_key, "market": market_state,
                "metadata": json.dumps(intent_metadata, default=str),
            }).first()
        return row is not None

    def mark_submitted(self, key: str, broker_order_id: str | None) -> None:
        with self.engine.begin() as conn:
            epoch_id = self._epoch_id(conn, required=True)
            conn.execute(text("""UPDATE kr_infinite_order_intents SET broker_order_id=:order_id,
                status='SUBMITTED',updated_at=NOW()
                WHERE trading_epoch_id=:epoch_id AND idempotency_key=:key"""),
                {"epoch_id": epoch_id, "order_id": broker_order_id, "key": key})

    def mark_rejected(self, key: str, reason: str) -> None:
        with self.engine.begin() as conn:
            epoch_id = self._epoch_id(conn, required=True)
            conn.execute(text("""UPDATE kr_infinite_order_intents SET status='REJECTED',
                metadata=metadata || CAST(:metadata AS jsonb),updated_at=NOW()
                WHERE trading_epoch_id=:epoch_id AND idempotency_key=:key"""),
                {"epoch_id": epoch_id, "key": key, "metadata": json.dumps({"rejection": reason})})

    def mark_reconcile_pending(self, key: str, reason: str) -> None:
        """Persist an ambiguous broker submit as a durable duplicate fence."""
        with self.engine.begin() as conn:
            epoch_id = self._epoch_id(conn, required=True)
            conn.execute(text("""UPDATE kr_infinite_order_intents
                SET status='RECONCILE_PENDING',
                    metadata=metadata || CAST(:metadata AS jsonb),
                    updated_at=NOW()
                WHERE trading_epoch_id=:epoch_id AND idempotency_key=:key"""),
                {
                    "epoch_id": epoch_id,
                    "key": key,
                    "metadata": json.dumps({
                        "submit_outcome": "UNKNOWN",
                        "reconcile_required": True,
                        "submit_exception": reason,
                    }),
                })

    def persist_reconciliation(self, state: State, updates: list[tuple[OrderIntent, BrokerOrderState]]) -> None:
        """Commit the complete broker-fill journal and sleeve state atomically.

        PostgreSQL writers lock the one active-epoch sleeve row *before* the
        intent rows; the economic state must never regress and each original
        intent's cumulative snapshot must match the locked DB journal. A
        concurrent tick, older KIS response or mismatched owner/cycle aborts
        the entire transaction. No retry
        submits a new order: the next tick must reload KIS and the database.

        Legacy SQLite claim-only tests have a reduced table schema; their
        state writer is supplied by the test fixture and cannot provide the
        PostgreSQL locking guarantee.
        """
        from decimal import Decimal, InvalidOperation

        def money(value):
            try:
                amount = Decimal(str(0 if value is None else value))
                if not amount.is_finite() or amount < 0:
                    raise ValueError("KR_INF_FILL_NOTIONAL_INVALID")
                return amount
            except (InvalidOperation, TypeError):
                raise ValueError("KR_INF_FILL_NOTIONAL_INVALID")

        with self.engine.begin() as conn:
            epoch_id = self._epoch_id(conn, required=True)
            is_postgres = conn.dialect.name == "postgresql"
            if is_postgres:
                # Lock the economic state first, consistently across workers.
                locked_state = conn.execute(text("""
                    SELECT version,cycle_id,
                           units_used,core_filled_notional,reserve_filled_notional,
                           status
                    FROM kr_infinite_state
                    WHERE trading_epoch_id=:epoch_id
                      AND strategy_id='KR_INFINITE_V1' AND symbol='122630'
                    FOR UPDATE
                """), {"epoch_id": epoch_id}).mappings().first()
                if (not locked_state
                        or str(locked_state["cycle_id"] or "") != str(state.cycle_id or "")):
                    raise RuntimeError("KR_INF_RECONCILE_STATE_RACE_RELOAD")
                # Earlier market/submit lifecycle save_state() operations in
                # THIS tick legitimately advanced the DB version without
                # modifying the immutable caller's State.version. Therefore a
                # strict version == state.version check would block every
                # post-submit fill. Guard non-regressing economics here; the
                # locked intent's original cumulative snapshot below provides
                # the exact compare-and-swap of economic fill application.
                if (int(state.units_used) < int(locked_state["units_used"])
                        or money(state.core_filled_notional) < money(locked_state["core_filled_notional"])
                        or money(state.reserve_filled_notional) < money(locked_state["reserve_filled_notional"])
                        or (str(locked_state["status"]).upper() == "FROZEN"
                            and state.status != Status.FROZEN)):
                    raise RuntimeError("KR_INF_RECONCILE_ECONOMIC_STATE_REGRESSION")

            seen_ids = set()
            for intent, broker in updates:
                if intent.id in seen_ids:
                    raise ValueError("KR_INF_RECONCILE_DUPLICATE_INTENT")
                seen_ids.add(intent.id)
                previous_qty = int(intent.filled_qty or 0)
                previous_notional = money(intent.filled_notional_krw)
                if is_postgres:
                    persisted = conn.execute(text("""
                        SELECT trading_epoch_id,strategy_id,symbol,cycle_id,side,
                               idempotency_key,broker_order_id,trade_date,
                               requested_qty,filled_qty,filled_notional_krw,status
                        FROM kr_infinite_order_intents
                        WHERE trading_epoch_id=:epoch_id AND id=:id
                        FOR UPDATE
                    """), {"epoch_id": epoch_id, "id": intent.id}).mappings().first()
                    if (not persisted
                            or str(persisted["trading_epoch_id"]) != str(epoch_id)
                            or persisted["strategy_id"] != "KR_INFINITE_V1"
                            or persisted["symbol"] != "122630"
                            or persisted["cycle_id"] != intent.cycle_id
                            or persisted["cycle_id"] != state.cycle_id
                            or persisted["side"] != intent.side
                            or persisted["idempotency_key"] != intent.idempotency_key
                            or persisted["trade_date"] != intent.trade_date
                            or str(persisted["broker_order_id"] or "") != str(intent.broker_order_id or "")
                            or int(persisted["filled_qty"] or 0) != previous_qty
                            or money(persisted["filled_notional_krw"]) != previous_notional):
                        raise RuntimeError("KR_INF_RECONCILE_INTENT_RACE_RELOAD")
                    if str(persisted["status"]).upper() == "FILLED" and str(broker.status).upper() != "FILLED":
                        raise RuntimeError("KR_INF_RECONCILE_TERMINAL_REGRESSION")
                    requested_qty = int(persisted["requested_qty"] or 0)
                    if requested_qty <= 0:
                        raise RuntimeError("KR_INF_RECONCILE_REQUEST_QTY_UNVERIFIED")
                else:
                    requested_qty = int(intent.requested_qty or 0)

                if is_postgres and (
                    str(broker.status or "").upper() in {"FILLED", "PARTIALLY_FILLED"}
                    and broker.filled_qty is None
                ):
                    raise RuntimeError("KR_INF_RECONCILE_TERMINAL_QTY_UNPROVEN")
                cumulative_qty = (
                    int(broker.filled_qty) if broker.filled_qty is not None
                    else previous_qty
                )
                if cumulative_qty < previous_qty or (
                    requested_qty > 0 and cumulative_qty > requested_qty
                ):
                    raise RuntimeError("KR_INF_RECONCILE_FILL_QTY_REGRESSION")
                cumulative_notional = (
                    money(broker.filled_notional_krw)
                    if broker.filled_qty is not None and cumulative_qty > 0
                    else previous_notional
                )
                if cumulative_notional < previous_notional:
                    raise RuntimeError("KR_INF_RECONCILE_FILL_NOTIONAL_REGRESSION")
                if is_postgres and (
                    (cumulative_qty > previous_qty and cumulative_notional <= previous_notional)
                    or (cumulative_qty == previous_qty and cumulative_notional != previous_notional)
                ):
                    # Quantity without original broker cash evidence may not
                    # consume the intent; else later genuine fill is lost.
                    raise RuntimeError("KR_INF_RECONCILE_PRICE_UNVERIFIED")

                # PostgreSQL numeric and all Python arithmetic stay Decimal.
                # A missing broker fill count NEVER clears the stored 0.
                persisted_qty = cumulative_qty if is_postgres else (
                    broker.filled_qty if broker.filled_qty is not None
                    else previous_qty if previous_qty > 0 else None
                )
                persisted_notional = (
                    cumulative_notional if is_postgres or broker.filled_qty is None
                    else broker.filled_notional_krw
                )
                conn.execute(text("""UPDATE kr_infinite_order_intents
                    SET status=:status,filled_qty=:qty,
                        filled_notional_krw=:notional,filled_avg_price=:average,
                        updated_at=NOW()
                    WHERE id=:id"""),
                    {"status": broker.status, "qty": persisted_qty,
                     "notional": persisted_notional,
                     "average": broker.filled_avg_price, "id": intent.id})
            self._save_state(conn, state, epoch_id=epoch_id)

        # The intent+state commit above is the recoverable economic journal.
        # A claim observation is derived from that journal; if this separate
        # materialized view fails, the next AM/Close replay must repair it.
        terminal_fill_states = {"FILLED", "PARTIALLY_FILLED", "REJECTED", "CANCELLED", "EXPIRED"}
        for intent, broker in updates:
            metadata = dict(intent.metadata or {})
            if not metadata.get("execution_claim_action_key"):
                continue
            status = str(broker.status or "").upper()
            if status == "FILLED":
                claim_state = "FILLED"
            elif status == "PARTIALLY_FILLED":
                claim_state = "PARTIALLY_FILLED"
            elif status == "REJECTED":
                claim_state = "REJECTED_EXPLICIT"
            elif status in {"CANCELLED", "EXPIRED"}:
                claim_state = "CANCELLED"
            elif status == "RECONCILE_PENDING":
                claim_state = "UNRESOLVED"
            else:
                claim_state = "ACKED"
            qty = broker.filled_qty if status in terminal_fill_states else None
            authoritative = status == "REJECTED" or (
                status in {"FILLED", "PARTIALLY_FILLED"} and qty is not None
            )
            if status in {"CANCELLED", "EXPIRED"}:
                authoritative = broker.filled_qty is not None
            if status in {"FILLED", "PARTIALLY_FILLED"} and qty is None:
                claim_state = "UNRESOLVED"
            self.record_execution_claim_for_order(
                intent.idempotency_key,
                state=claim_state,
                cumulative_filled_qty=qty,
                authoritative=authoritative,
            )

    def save_state(self, state: State) -> None:
        with self.engine.begin() as conn:
            epoch_id = self._epoch_id(conn, required=True)
            self._save_state(conn, state, epoch_id=epoch_id)

    @staticmethod
    def _save_state(conn, state: State, *, epoch_id: str) -> None:
        payload = asdict(state)
        payload["status"] = state.status.value
        payload["metadata"] = json.dumps({**state.metadata, "trading_epoch_id": epoch_id}, default=str)
        payload["trading_epoch_id"] = epoch_id
        columns = ",".join(payload)
        values = ",".join(f":{key}" if key != "metadata" else "CAST(:metadata AS jsonb)" for key in payload)
        updates = ",".join(f"{key}=EXCLUDED.{key}" for key in payload if key not in {"trading_epoch_id", "strategy_id", "symbol", "version"})
        conn.execute(text(f"""INSERT INTO kr_infinite_state({columns}) VALUES({values})
            ON CONFLICT(trading_epoch_id,strategy_id,symbol) DO UPDATE SET {updates},
            version=kr_infinite_state.version+1,updated_at=NOW()"""), payload)
