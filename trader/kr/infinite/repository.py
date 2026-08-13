from __future__ import annotations

import json
from dataclasses import asdict
from datetime import date

from sqlalchemy import text

from trader.db.engine import get_engine
from .models import InfiniteState


class InfiniteRepository:
    def __init__(self, engine=None):
        self.engine = engine or get_engine()

    def ensure_schema(self) -> None:
        with self.engine.connect() as conn:
            if not conn.execute(text("SELECT to_regclass('public.kr_infinite_state')")).scalar():
                raise RuntimeError("kr_infinite_state migration is not installed")

    def load_state(self, symbol: str = "122630") -> InfiniteState | None:
        with self.engine.connect() as conn:
            row = conn.execute(text("SELECT * FROM kr_infinite_state WHERE strategy_id=:s AND symbol=:x"),
                               {"s": "KR_KODEX_LEVERAGE_INFINITE_V1", "x": symbol}).mappings().first()
        if not row:
            return None
        data = dict(row); data.pop("created_at", None); data.pop("updated_at", None)
        if isinstance(data.get("metadata"), str): data["metadata"] = json.loads(data["metadata"])
        return InfiniteState(**data)

    def save_state(self, state: InfiniteState) -> None:
        p = asdict(state); p["metadata"] = json.dumps(p["metadata"], default=str)
        with self.engine.begin() as conn:
            conn.execute(text("""INSERT INTO kr_infinite_state
              (strategy_id,symbol,book,cycle_id,cycle_status,policy_version,filled_quantity,
               authoritative_buy_notional,authoritative_sell_notional,authoritative_average_price,
               used_unit_fraction,last_buy_trade_date,pending_order_key,current_regime_state,
               current_regime_score,data_quality,metadata)
              VALUES (:strategy_id,:symbol,:book,:cycle_id,:cycle_status,:policy_version,:filled_quantity,
               :authoritative_buy_notional,:authoritative_sell_notional,:authoritative_average_price,
               :used_unit_fraction,:last_buy_trade_date,:pending_order_key,:current_regime_state,
               :current_regime_score,:data_quality,CAST(:metadata AS jsonb))
              ON CONFLICT(strategy_id,symbol) DO UPDATE SET
               book=EXCLUDED.book,cycle_id=EXCLUDED.cycle_id,cycle_status=EXCLUDED.cycle_status,
               policy_version=EXCLUDED.policy_version,filled_quantity=EXCLUDED.filled_quantity,
               authoritative_buy_notional=EXCLUDED.authoritative_buy_notional,
               authoritative_sell_notional=EXCLUDED.authoritative_sell_notional,
               authoritative_average_price=EXCLUDED.authoritative_average_price,
               used_unit_fraction=EXCLUDED.used_unit_fraction,last_buy_trade_date=EXCLUDED.last_buy_trade_date,
               pending_order_key=EXCLUDED.pending_order_key,current_regime_state=EXCLUDED.current_regime_state,
               current_regime_score=EXCLUDED.current_regime_score,data_quality=EXCLUDED.data_quality,
               metadata=EXCLUDED.metadata,updated_at=NOW()"""), p)

    def try_lock(self) -> bool:
        with self.engine.connect() as conn:
            return bool(conn.execute(text("SELECT pg_try_advisory_lock(hashtext('KR_INF:122630'))")).scalar())

    def unlock(self) -> None:
        with self.engine.connect() as conn:
            conn.execute(text("SELECT pg_advisory_unlock(hashtext('KR_INF:122630'))"))
