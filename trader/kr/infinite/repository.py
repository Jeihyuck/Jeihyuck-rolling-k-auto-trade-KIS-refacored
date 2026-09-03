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

    def pending_intents(self, *, cycle_id: str | None = None, side: str | None = None) -> list[OrderIntent]:
        with self.engine.connect() as conn:
            rows = conn.execute(text("""SELECT id,cycle_id,trade_date,side,idempotency_key,requested_qty,unit_sequence,
                broker_order_id,status,filled_qty,filled_notional_krw FROM kr_infinite_order_intents
                WHERE status=ANY(:statuses) ORDER BY id"""), {"statuses": list(PENDING)}).mappings().all()
        result = [OrderIntent(**dict(row)) for row in rows]
        return [item for item in result
                if (cycle_id is None or item.cycle_id == cycle_id)
                and (side is None or item.side == side)]

    def create_intent(self, state: State, decision: Decision, trade_date: date, market_state: str) -> bool:
        """Persist first. A uniqueness loss means the caller must not submit."""
        with self.engine.begin() as conn:
            row = conn.execute(text("""INSERT INTO kr_infinite_order_intents(
                strategy_id,symbol,cycle_id,trade_date,side,reason,unit_sequence,requested_notional_krw,
                requested_qty,limit_price,idempotency_key,market_state)
                VALUES('KR_INFINITE_V1','122630',:cycle,:day,:side,:reason,:seq,:notional,:qty,:price,:key,:market)
                ON CONFLICT(idempotency_key) DO NOTHING RETURNING id"""), {
                "cycle": state.cycle_id, "day": trade_date, "side": decision.action.value,
                "reason": decision.reason, "seq": state.units_used + 1 if decision.action.value in {"BUY", "RECOVERY"} else None,
                "notional": decision.notional, "qty": decision.qty,
                "price": decision.notional / decision.qty if decision.qty else None,
                "key": decision.idempotency_key, "market": market_state,
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
