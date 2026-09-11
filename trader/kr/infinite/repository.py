from __future__ import annotations

import json
from dataclasses import asdict
from datetime import date

from sqlalchemy import text
from trader.db.engine import get_engine

from .models import BrokerOrderState, Decision, OrderIntent, State, Status

PENDING = frozenset({"INTENT_CREATED", "SUBMITTED", "ACK", "PENDING", "PARTIALLY_FILLED", "RECONCILE_PENDING"})


class InfiniteRepository:
    def __init__(self, engine=None):
        self.engine = engine or get_engine()

    def ensure_schema(self) -> None:
        with self.engine.connect() as conn:
            state = conn.execute(text("SELECT to_regclass('public.kr_infinite_state')")).scalar()
            intents = conn.execute(text("SELECT to_regclass('public.kr_infinite_order_intents')")).scalar()
        if not state or not intents:
            raise RuntimeError("KR_INF_DB_UNAVAILABLE")

    def load_state(self) -> State | None:
        with self.engine.connect() as conn:
            row = conn.execute(text("SELECT * FROM kr_infinite_state WHERE strategy_id='KR_INFINITE_V1' AND symbol='122630'")).mappings().first()
        if row is None:
            return None
        values = {key: row[key] for key in State.__dataclass_fields__ if key in row}
        values["status"] = Status(values["status"])
        values["metadata"] = values.get("metadata") or {}
        return State(**values)

    def intent_keys(self) -> frozenset[str]:
        with self.engine.connect() as conn:
            rows = conn.execute(text("SELECT idempotency_key FROM kr_infinite_order_intents")).all()
        return frozenset(row[0] for row in rows)

    def pending_intents(self) -> list[OrderIntent]:
        with self.engine.connect() as conn:
            rows = conn.execute(text("""SELECT id,cycle_id,trade_date,side,idempotency_key,requested_qty,unit_sequence,
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
        with self.engine.begin() as conn:
            row = conn.execute(text("""INSERT INTO kr_infinite_order_intents(
                strategy_id,symbol,cycle_id,trade_date,side,reason,unit_sequence,requested_notional_krw,
                requested_qty,limit_price,idempotency_key,market_state,metadata)
                VALUES('KR_INFINITE_V1','122630',:cycle,:day,:side,:reason,:seq,:notional,:qty,:price,:key,:market,CAST(:metadata AS jsonb))
                ON CONFLICT(idempotency_key) DO NOTHING RETURNING id"""), {
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
            conn.execute(text("""UPDATE kr_infinite_order_intents SET broker_order_id=:order_id,
                status='SUBMITTED',updated_at=NOW() WHERE idempotency_key=:key"""), {"order_id": broker_order_id, "key": key})

    def mark_rejected(self, key: str, reason: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(text("""UPDATE kr_infinite_order_intents SET status='REJECTED',
                metadata=metadata || CAST(:metadata AS jsonb),updated_at=NOW() WHERE idempotency_key=:key"""),
                {"key": key, "metadata": json.dumps({"rejection": reason})})

    def persist_reconciliation(self, state: State, updates: list[tuple[OrderIntent, BrokerOrderState]]) -> None:
        with self.engine.begin() as conn:
            for intent, broker in updates:
                conn.execute(text("""UPDATE kr_infinite_order_intents SET status=:status,filled_qty=:qty,
                    filled_notional_krw=:notional,filled_avg_price=:average,updated_at=NOW() WHERE id=:id"""),
                    {"status": broker.status, "qty": broker.filled_qty, "notional": broker.filled_notional_krw,
                     "average": broker.filled_avg_price, "id": intent.id})
            self._save_state(conn, state)

    def save_state(self, state: State) -> None:
        with self.engine.begin() as conn:
            self._save_state(conn, state)

    @staticmethod
    def _save_state(conn, state: State) -> None:
        payload = asdict(state)
        payload["status"] = state.status.value
        payload["metadata"] = json.dumps(state.metadata, default=str)
        columns = ",".join(payload)
        values = ",".join(f":{key}" if key != "metadata" else "CAST(:metadata AS jsonb)" for key in payload)
        updates = ",".join(f"{key}=EXCLUDED.{key}" for key in payload if key not in {"strategy_id", "symbol", "version"})
        conn.execute(text(f"""INSERT INTO kr_infinite_state({columns}) VALUES({values})
            ON CONFLICT(strategy_id,symbol) DO UPDATE SET {updates},
            version=kr_infinite_state.version+1,updated_at=NOW()"""), payload)
