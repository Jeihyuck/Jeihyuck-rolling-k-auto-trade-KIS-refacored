from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict
from sqlalchemy import text

LOCK_KEY = 12263002


@contextmanager
def sleeve_lock(engine):
    """Keep the same physical PostgreSQL session for the complete sleeve run."""
    with engine.connect() as conn:
        if conn.dialect.name != "postgresql":
            raise RuntimeError("KR infinite advisory lock requires PostgreSQL")
        acquired = bool(conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": LOCK_KEY}).scalar())
        try:
            yield conn if acquired else None
        finally:
            if acquired:
                conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": LOCK_KEY})


class SleeveRepository:
    def __init__(self, conn): self.conn = conn

    def save_state(self, values: dict) -> None:
        self.conn.execute(text("""INSERT INTO kr_infinite_state
          (strategy_id,symbol,book,cycle_id,cycle_status,policy_version,filled_quantity,
           authoritative_buy_notional,authoritative_sell_notional,authoritative_average_price,
           used_unit_fraction,last_buy_trade_date,pending_order_key,current_regime_state,
           current_regime_score,data_quality,metadata)
          VALUES (:strategy_id,:symbol,:book,:cycle_id,:cycle_status,:policy_version,:filled_quantity,
           :authoritative_buy_notional,:authoritative_sell_notional,:authoritative_average_price,
           :used_unit_fraction,:last_buy_trade_date,:pending_order_key,:current_regime_state,
           :current_regime_score,:data_quality,CAST(:metadata AS JSONB))
          ON CONFLICT (strategy_id,symbol) DO UPDATE SET
           cycle_id=EXCLUDED.cycle_id,cycle_status=EXCLUDED.cycle_status,
           filled_quantity=EXCLUDED.filled_quantity,
           authoritative_buy_notional=EXCLUDED.authoritative_buy_notional,
           authoritative_sell_notional=EXCLUDED.authoritative_sell_notional,
           authoritative_average_price=EXCLUDED.authoritative_average_price,
           used_unit_fraction=EXCLUDED.used_unit_fraction,last_buy_trade_date=EXCLUDED.last_buy_trade_date,
           pending_order_key=EXCLUDED.pending_order_key,current_regime_state=EXCLUDED.current_regime_state,
           current_regime_score=EXCLUDED.current_regime_score,data_quality=EXCLUDED.data_quality,
           metadata=EXCLUDED.metadata,updated_at=NOW()"""), values)
