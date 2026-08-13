from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from decimal import Decimal
import json, uuid
from sqlalchemy import text

from .models import CycleStatus, SleeveState

LOCK_KEY = 12263002
TERMINAL_ORDER_STATES = frozenset({"FILLED", "CANCELLED", "ERROR", "REJECTED"})


@contextmanager
def sleeve_lock(engine):
    """Session lock with explicit transactions and guaranteed same-session unlock.

    Individual durable phases may commit on ``conn`` before a broker call while the
    session lock remains held.  This avoids both lost intents and duplicate runners.
    """
    with engine.connect() as conn:
        if conn.dialect.name != "postgresql":
            raise RuntimeError("KR infinite advisory lock requires PostgreSQL")
        acquired = bool(conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": LOCK_KEY}).scalar())
        conn.commit()
        try:
            yield conn if acquired else None
            if acquired and conn.in_transaction(): conn.commit()
        except Exception:
            if conn.in_transaction(): conn.rollback()
            raise
        finally:
            if acquired:
                if conn.in_transaction(): conn.rollback()
                conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": LOCK_KEY})
                conn.commit()


class SleeveRepository:
    def __init__(self, conn): self.conn = conn

    def ensure_schema(self) -> None:
        exists = self.conn.execute(text("SELECT to_regclass('public.kr_infinite_state')")).scalar()
        if not exists: raise RuntimeError("migration 0049_kr_infinite_v2.sql is not applied")

    def load_state(self, strategy_id="kr_kodex_infinite_v2", symbol="122630") -> dict | None:
        row = self.conn.execute(text("SELECT * FROM kr_infinite_state WHERE strategy_id=:s AND symbol=:p"), {"s":strategy_id,"p":symbol}).mappings().first()
        return dict(row) if row else None

    def save_state(self, values: dict, *, commit: bool = False) -> None:
        payload = dict(values); payload["metadata"] = json.dumps(payload.get("metadata") or {}, default=str)
        self.conn.execute(text("""INSERT INTO kr_infinite_state
          (strategy_id,symbol,book,cycle_id,cycle_status,policy_version,filled_quantity,
           authoritative_buy_notional,authoritative_sell_notional,authoritative_average_price,
           used_unit_fraction,last_buy_trade_date,pending_order_key,current_regime_state,
           current_regime_score,data_quality,metadata)
          VALUES (:strategy_id,:symbol,:book,:cycle_id,:cycle_status,:policy_version,:filled_quantity,
           :authoritative_buy_notional,:authoritative_sell_notional,:authoritative_average_price,
           :used_unit_fraction,:last_buy_trade_date,:pending_order_key,:current_regime_state,
           :current_regime_score,:data_quality,CAST(:metadata AS JSONB))
          ON CONFLICT (strategy_id,symbol) DO UPDATE SET cycle_id=EXCLUDED.cycle_id,
           cycle_status=EXCLUDED.cycle_status,filled_quantity=EXCLUDED.filled_quantity,
           authoritative_buy_notional=EXCLUDED.authoritative_buy_notional,
           authoritative_sell_notional=EXCLUDED.authoritative_sell_notional,
           authoritative_average_price=EXCLUDED.authoritative_average_price,
           used_unit_fraction=EXCLUDED.used_unit_fraction,last_buy_trade_date=EXCLUDED.last_buy_trade_date,
           pending_order_key=EXCLUDED.pending_order_key,current_regime_state=EXCLUDED.current_regime_state,
           current_regime_score=EXCLUDED.current_regime_score,data_quality=EXCLUDED.data_quality,
           metadata=EXCLUDED.metadata,updated_at=NOW()"""), payload)
        if commit: self.conn.commit()

    def load_attributed_orders(self, cycle_id: str) -> list[dict]:
        rows = self.conn.execute(text("""SELECT * FROM orders WHERE code='122630' AND strategy='kr_kodex_infinite_v2'
          AND request_json->>'book'='KR_INFINITE' AND request_json->>'cycle_id'=:c"""), {"c":cycle_id}).mappings()
        return [dict(row) for row in rows]

    def load_attributed_fills(self, cycle_id: str) -> list[dict]:
        rows = self.conn.execute(text("""SELECT f.* FROM fills f JOIN orders o ON o.order_id=f.order_id
          WHERE f.code='122630' AND o.strategy='kr_kodex_infinite_v2' AND o.request_json->>'book'='KR_INFINITE'
          AND o.request_json->>'cycle_id'=:c"""), {"c":cycle_id}).mappings()
        return [dict(row) for row in rows]

    @staticmethod
    def pending_is_terminal(order: dict | None) -> bool:
        return bool(order and str(order.get("status") or "").upper() in TERMINAL_ORDER_STATES)

    @staticmethod
    def ownership_conflict(broker_quantity: int, attributed_quantity: int) -> bool:
        return broker_quantity != attributed_quantity

    def archive_cycle(self, state: dict, trade_date: date, reason: str, checksum: str) -> None:
        if int(state["filled_quantity"]) != 0 or state["cycle_status"] != "COMPLETE":
            raise ValueError("cycle requires zero broker balance and COMPLETE status")
        self.conn.execute(text("""INSERT INTO kr_infinite_cycle_history
          (cycle_id,strategy_id,symbol,book,cycle_status,policy_version,policy_checksum,cycle_start_date,
           cycle_end_date,used_unit_fraction,authoritative_average_price,last_buy_trade_date,complete_reason,
           filled_quantity,authoritative_buy_notional,authoritative_sell_notional,metadata)
          VALUES (:cycle_id,:strategy_id,:symbol,:book,:cycle_status,:policy_version,:checksum,
           CAST(:cycle_start_date AS DATE),:ended,:used_unit_fraction,:authoritative_average_price,
           :last_buy_trade_date,:reason,:filled_quantity,:authoritative_buy_notional,
           :authoritative_sell_notional,CAST(:metadata AS JSONB)) ON CONFLICT(cycle_id) DO NOTHING"""),
          {**state,"checksum":checksum,"ended":trade_date,"reason":reason,
           "cycle_start_date":(state.get("metadata") or {}).get("cycle_start_date"),
           "metadata":json.dumps(state.get("metadata") or {}, default=str)})

    def create_next_cycle(self, previous: dict, trade_date: date) -> dict:
        completed = previous.get("cycle_complete_trade_date") or (previous.get("metadata") or {}).get("cycle_complete_trade_date")
        if completed and str(completed) >= trade_date.isoformat(): raise ValueError("same-day re-entry forbidden")
        return {**previous,"cycle_id":str(uuid.uuid4()),"cycle_status":"READY","filled_quantity":0,
                "authoritative_buy_notional":Decimal(0),"authoritative_sell_notional":Decimal(0),
                "authoritative_average_price":Decimal(0),"used_unit_fraction":Decimal(0),
                "last_buy_trade_date":None,"pending_order_key":None,
                "metadata":{"cycle_start_date":trade_date.isoformat()}}
