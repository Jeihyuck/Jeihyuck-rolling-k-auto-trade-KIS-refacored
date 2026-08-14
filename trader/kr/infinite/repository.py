from __future__ import annotations
import json
from dataclasses import asdict
from datetime import date
from sqlalchemy import text
from trader.db.engine import get_engine
from .models import Decision, State, Status

PENDING=frozenset({"INTENT_CREATED","SUBMITTED","ACK","PENDING","PARTIALLY_FILLED","RECONCILE_PENDING"})

class InfiniteRepository:
    def __init__(self,engine=None): self.engine=engine or get_engine()
    def ensure_schema(self):
        with self.engine.connect() as c:
            if not c.execute(text("SELECT to_regclass('public.kr_infinite_state')")).scalar(): raise RuntimeError("KR_INF_DB_UNAVAILABLE")
            if not c.execute(text("SELECT to_regclass('public.kr_infinite_order_intents')")).scalar(): raise RuntimeError("KR_INF_DB_UNAVAILABLE")
    def load_state(self)->State|None:
        with self.engine.connect() as c: row=c.execute(text("SELECT * FROM kr_infinite_state WHERE strategy_id='KR_INFINITE_V1' AND symbol='122630'")).mappings().first()
        if not row:return None
        values={k:row[k] for k in State.__dataclass_fields__ if k in row}; values["status"]=Status(values["status"]); values["metadata"]=values.get("metadata") or {}
        return State(**values)
    def intent_keys(self)->frozenset[str]:
        with self.engine.connect() as c: rows=c.execute(text("SELECT idempotency_key FROM kr_infinite_order_intents")).all()
        return frozenset(r[0] for r in rows)
    def pending_sides(self)->set[str]:
        with self.engine.connect() as c: rows=c.execute(text("SELECT side,status FROM kr_infinite_order_intents WHERE status=ANY(:statuses)"),{"statuses":list(PENDING)}).all()
        return {r[0] for r in rows}
    def create_intent(self,state:State,decision:Decision,trade_date:date,market_state:str)->bool:
        """Atomic unique journal write. False means broker submission must not occur."""
        with self.engine.begin() as c:
            row=c.execute(text("""INSERT INTO kr_infinite_order_intents(strategy_id,symbol,cycle_id,trade_date,side,reason,unit_sequence,requested_notional_krw,requested_qty,limit_price,idempotency_key,market_state)
              VALUES('KR_INFINITE_V1','122630',:cycle,:day,:side,:reason,:seq,:notional,:qty,:price,:key,:market) ON CONFLICT(idempotency_key) DO NOTHING RETURNING id"""),
              {"cycle":state.cycle_id or "NEW","day":trade_date,"side":decision.action.value,"reason":decision.reason,"seq":state.units_used+1 if decision.action.value in {"BUY","RECOVERY"} else None,"notional":decision.notional,"qty":decision.qty,"price":decision.notional/decision.qty if decision.qty else None,"key":decision.idempotency_key,"market":market_state}).first()
        return row is not None
    def mark_submitted(self,key:str,broker_order_id:str):
        with self.engine.begin() as c:c.execute(text("UPDATE kr_infinite_order_intents SET broker_order_id=:oid,status='SUBMITTED',updated_at=NOW() WHERE idempotency_key=:key"),{"oid":broker_order_id,"key":key})
    def save_state(self,s:State):
        p=asdict(s);p["status"]=s.status.value;p["metadata"]=json.dumps(s.metadata,default=str)
        cols=",".join(p); vals=",".join(f":{x}" if x!="metadata" else "CAST(:metadata AS jsonb)" for x in p)
        updates=",".join(f"{x}=EXCLUDED.{x}" for x in p if x not in {"strategy_id","symbol","version"})
        with self.engine.begin() as c:c.execute(text(f"INSERT INTO kr_infinite_state({cols}) VALUES({vals}) ON CONFLICT(strategy_id,symbol) DO UPDATE SET {updates},version=kr_infinite_state.version+1,updated_at=NOW()"),p)
