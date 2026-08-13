from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import asdict
from dataclasses import replace
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

    def archive_cycle(self, state: InfiniteState) -> None:
        if not state.cycle_id:
            return
        p = asdict(state); p["metadata"] = json.dumps(p["metadata"], default=str)
        with self.engine.begin() as conn:
            conn.execute(text("""INSERT INTO kr_infinite_cycle_history
              (strategy_id,symbol,book,cycle_id,cycle_status,policy_version,filled_quantity,
               authoritative_buy_notional,authoritative_sell_notional,authoritative_average_price,
               used_unit_fraction,last_buy_trade_date,metadata,completed_at)
              VALUES (:strategy_id,:symbol,:book,:cycle_id,:cycle_status,:policy_version,:filled_quantity,
               :authoritative_buy_notional,:authoritative_sell_notional,:authoritative_average_price,
               :used_unit_fraction,:last_buy_trade_date,CAST(:metadata AS jsonb),NOW())
              ON CONFLICT(strategy_id,cycle_id) DO UPDATE SET
               cycle_status=EXCLUDED.cycle_status,filled_quantity=EXCLUDED.filled_quantity,
               authoritative_buy_notional=EXCLUDED.authoritative_buy_notional,
               authoritative_sell_notional=EXCLUDED.authoritative_sell_notional,
               authoritative_average_price=EXCLUDED.authoritative_average_price,
               used_unit_fraction=EXCLUDED.used_unit_fraction,metadata=EXCLUDED.metadata,
               completed_at=EXCLUDED.completed_at"""), p)

    def load_evidence(self, state: InfiniteState, trading_date: date) -> tuple[list[dict], list[dict]]:
        """Read only rows whose persisted intent metadata proves sleeve ownership."""
        params = {"symbol": state.symbol, "strategy": state.strategy_id,
                  "book": state.book, "cycle": state.cycle_id or ""}
        with self.engine.connect() as conn:
            orders = conn.execute(text("""SELECT side,status,client_order_key,request_json,kis_odno
              FROM orders WHERE code=:symbol AND strategy=:strategy"""), params).mappings().all()
            fills = conn.execute(text("""SELECT f.fill_id,f.trade_id,f.side,f.qty,f.price,f.fee,f.tax,f.filled_at,f.raw_json,
                     o.request_json,o.client_order_key
              FROM fills f JOIN orders o ON o.order_id=f.order_id
              WHERE f.code=:symbol AND o.strategy=:strategy"""), params).mappings().all()
        def metadata(row, key):
            raw = row.get(key) or {}
            try: return json.loads(raw) if isinstance(raw, str) else dict(raw)
            except (ValueError, TypeError): return {}
        owned_orders = []
        for row in orders:
            md = metadata(row, "request_json")
            if md.get("book") == state.book and str(md.get("cycle_id") or "") == params["cycle"]:
                owned_orders.append({**dict(row), **md, "strategy_id": state.strategy_id})
        owned_fills = []
        for row in fills:
            md = metadata(row, "request_json"); raw = metadata(row, "raw_json")
            if md.get("book") == state.book and str(md.get("cycle_id") or "") == params["cycle"]:
                owned_fills.append({**dict(row), **md, **raw, "strategy_id": state.strategy_id,
                    "trade_date": str(row.get("filled_at") or "")[:10]})
        return owned_fills, owned_orders

    @contextmanager
    def critical_section(self):
        """Hold one PostgreSQL session for lock, work and unlock.

        Session advisory locks are connection-owned; acquiring and releasing on
        separate pooled connections can leak a lock indefinitely.
        """
        conn = self.engine.connect()
        acquired = False
        try:
            acquired = bool(conn.execute(text("SELECT pg_try_advisory_lock(hashtext('KR_INF:122630'))")).scalar())
            yield acquired
        finally:
            if acquired:
                conn.execute(text("SELECT pg_advisory_unlock(hashtext('KR_INF:122630'))"))
            conn.close()

    @staticmethod
    def reconcile_evidence(state: InfiniteState, *, broker_quantity: int,
                           broker_average_price: float, fills: list[dict],
                           orders: list[dict], unit_krw: float = 375_000,
                           trading_date: date | None = None) -> InfiniteState:
        """Rebuild accounting from attributed authoritative fills and orders."""
        def owned(row):
            return (str(row.get("strategy_id") or row.get("strategy") or "") == state.strategy_id
                    and str(row.get("book") or "") == state.book
                    and str(row.get("cycle_id") or "") == str(state.cycle_id or ""))
        buys = sells = 0.0; buy_qty = sell_qty = 0; last_buy = None; seen = set()
        for row in fills:
            if not owned(row): continue
            identity = row.get("fill_id") or row.get("trade_id") or (
                row.get("client_order_key"), row.get("side"), row.get("qty"),
                row.get("price"), row.get("filled_at"))
            if identity in seen: continue
            seen.add(identity)
            qty = max(0, int(row.get("filled_quantity") or row.get("qty") or 0))
            price = max(0.0, float(row.get("fill_price") or row.get("price") or 0))
            fee = max(0.0, float(row.get("fee") or 0))
            tax = max(0.0, float(row.get("tax") or 0))
            if str(row.get("side") or "").upper() == "BUY":
                buys += qty * price + fee; buy_qty += qty
                td = row.get("trade_date")
                if td: last_buy = max(filter(None, (last_buy, td)))
            elif str(row.get("side") or "").upper() == "SELL":
                sells += qty * price - fee - tax; sell_qty += qty
        pending_states = {"INTENT", "SUBMITTED", "ACK", "OPEN", "PENDING", "PARTIALLY_FILLED", "RECONCILE_PENDING"}
        pending = next((str(o.get("client_order_key")) for o in reversed(orders)
                        if owned(o) and str(o.get("status") or "").upper() in pending_states), None)
        if broker_quantity != 0 and broker_average_price <= 0:
            raise ValueError("broker average price missing for open position")
        if broker_quantity > 0 and buys <= 0:
            raise ValueError("broker position has no attributed cycle fills")
        if buy_qty - sell_qty != broker_quantity:
            raise ValueError("broker and attributed fill quantity mismatch")
        status = "ACTIVE" if broker_quantity > 0 else ("EXIT_PENDING" if pending else "COMPLETE" if sells else "READY")
        metadata = dict(state.metadata)
        if status == "COMPLETE" and state.cycle_status != "COMPLETE" and trading_date:
            metadata["cycle_completed_trade_date"] = str(trading_date)
        return replace(state, filled_quantity=broker_quantity,
            authoritative_buy_notional=buys, authoritative_sell_notional=sells,
            authoritative_average_price=broker_average_price if broker_quantity else 0.0,
            used_unit_fraction=buys / unit_krw, last_buy_trade_date=last_buy,
            pending_order_key=pending, cycle_status=status, metadata=metadata)
