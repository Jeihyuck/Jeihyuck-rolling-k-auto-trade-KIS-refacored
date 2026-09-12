from __future__ import annotations

import json
import logging
from dataclasses import asdict, replace
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text

from trader.db.engine import get_engine

from .models import InfiniteState, Status

logger = logging.getLogger(__name__)

_PENDING = {"INTENT", "SUBMITTED", "ACK", "PENDING", "PARTIALLY_FILLED", "RECONCILE_PENDING", "ACK_DB_FAILED"}


class StateCorruptionError(RuntimeError):
    pass


class InfiniteRepository:
    """TQQQ metadata access using the shared PostgreSQL engine only."""

    def __init__(self, engine=None):
        self.engine = engine or get_engine()

    def ensure_schema(self) -> None:
        """Lightweight runtime readiness probe; migrations never run in a tick."""
        with self.engine.connect() as conn:
            exists = conn.execute(text("SELECT to_regclass('public.us_tqqq_infinite_state')")).scalar()
        if not exists:
            raise RuntimeError("TQQQ Infinite state table unavailable")
        logger.info("[TQQQ_INF][DB] status=READY table=us_tqqq_infinite_state")

    def load_state(self, strategy_id: str = "TQQQ_INFINITE_V3", symbol: str = "TQQQ") -> InfiniteState | None:
        try:
            with self.engine.connect() as conn:
                row = conn.execute(text("""
                    SELECT * FROM us_tqqq_infinite_state
                    WHERE strategy_id=:strategy_id AND symbol=:symbol
                """), {"strategy_id": strategy_id, "symbol": symbol}).mappings().first()
            if row is None:
                return None
            metadata = row.get("metadata") or {}
            if isinstance(metadata, str):
                metadata = json.loads(metadata)
            state = InfiniteState(
                strategy_id=row["strategy_id"], symbol=row["symbol"], cycle_id=row.get("cycle_id"),
                cycle_start_date=row.get("cycle_start_date"), cycle_complete_date=row.get("cycle_complete_date"),
                anchor_price=float(row["anchor_price"]) if row.get("anchor_price") is not None else None,
                core_filled_notional=float(row.get("core_filled_notional") or 0),
                reserve_filled_notional=float(row.get("reserve_filled_notional") or 0),
                last_buy_date=row.get("last_buy_date"), last_exit_date=row.get("last_exit_date"),
                market_crash_streak=int(row.get("market_crash_streak") or 0),
                material_market_crash=bool(row.get("material_market_crash")),
                reserve_unlocked=bool(row.get("reserve_unlocked")),
                cycle_age_trading_days=int(row.get("cycle_age_trading_days") or 0),
                status=Status(str(row.get("status"))), metadata=metadata, version=int(row.get("version") or 1),
            )
            return state.validate()
        except Exception as exc:
            raise StateCorruptionError(str(exc)) from exc

    def save_state(self, state: InfiniteState) -> None:
        state.validate()
        payload = asdict(state)
        payload["status"] = state.status.value
        payload["metadata"] = json.dumps(state.metadata, default=str)
        with self.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO us_tqqq_infinite_state (
                    strategy_id,symbol,cycle_id,cycle_start_date,cycle_complete_date,anchor_price,
                    core_filled_notional,reserve_filled_notional,last_buy_date,last_exit_date,
                    market_crash_streak,material_market_crash,reserve_unlocked,cycle_age_trading_days,
                    status,metadata,version,updated_at
                ) VALUES (
                    :strategy_id,:symbol,:cycle_id,:cycle_start_date,:cycle_complete_date,:anchor_price,
                    :core_filled_notional,:reserve_filled_notional,:last_buy_date,:last_exit_date,
                    :market_crash_streak,:material_market_crash,:reserve_unlocked,:cycle_age_trading_days,
                    :status,CAST(:metadata AS jsonb),:version,NOW()
                ) ON CONFLICT (strategy_id,symbol) DO UPDATE SET
                    cycle_id=EXCLUDED.cycle_id,cycle_start_date=EXCLUDED.cycle_start_date,
                    cycle_complete_date=EXCLUDED.cycle_complete_date,anchor_price=EXCLUDED.anchor_price,
                    core_filled_notional=EXCLUDED.core_filled_notional,
                    reserve_filled_notional=EXCLUDED.reserve_filled_notional,last_buy_date=EXCLUDED.last_buy_date,
                    last_exit_date=EXCLUDED.last_exit_date,market_crash_streak=EXCLUDED.market_crash_streak,
                    material_market_crash=EXCLUDED.material_market_crash,reserve_unlocked=EXCLUDED.reserve_unlocked,
                    cycle_age_trading_days=EXCLUDED.cycle_age_trading_days,status=EXCLUDED.status,
                    metadata=EXCLUDED.metadata,version=us_tqqq_infinite_state.version+1,updated_at=NOW()
            """), payload)

    def pending_sides(self, trade_date: date, symbol: str = "TQQQ") -> tuple[bool, bool]:
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT side,status FROM us_orders WHERE trade_date=:trade_date AND symbol=:symbol
            """), {"trade_date": trade_date, "symbol": symbol}).mappings()
            sides = {str(r["side"]).upper() for r in rows if str(r["status"]).upper() in _PENDING}
        return "BUY" in sides, "SELL" in sides

    def load_open_orders(self, *, symbol: str, cycle_id: str, side: str | None = None) -> list[dict]:
        """Load unresolved orders only for this Infinite cycle.

        PostgreSQL cannot infer a type from a nullable optional filter reliably,
        so the unfiltered and side-filtered queries deliberately have separate
        bind sets.
        """
        params = {
            "symbol": symbol,
            "cycle_prefix": f"TQQQ_INF_V3:{cycle_id}:%",
        }
        sql = """
            SELECT * FROM us_orders
            WHERE symbol=:symbol AND client_order_key LIKE :cycle_prefix
              AND status IN ('INTENT','SUBMITTED','ACK','OPEN','PENDING',
                             'PARTIALLY_FILLED','RECONCILE_PENDING','ACK_DB_FAILED')
        """
        if side is not None:
            sql += " AND side=:side"
            params["side"] = str(side).upper()
        with self.engine.connect() as conn:
            return [dict(row) for row in conn.execute(text(sql), params).mappings().all()]

    def load_expired_open_buy_orders(self, *, now: datetime, ttl_seconds: int,
                                     symbol: str = "TQQQ") -> list[dict]:
        """Return only this sleeve's unresolved BUYs whose broker TTL elapsed."""
        cutoff = now - timedelta(seconds=max(0, int(ttl_seconds)))
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT * FROM us_orders
                WHERE symbol=:symbol AND side='BUY'
                  AND client_order_key LIKE 'TQQQ_INF_V3:%'
                  AND status IN ('INTENT','SUBMITTED','ACK','OPEN','PENDING',
                                 'PARTIALLY_FILLED','RECONCILE_PENDING','ACK_DB_FAILED')
                  AND created_at <= :cutoff
                ORDER BY created_at ASC
            """), {"symbol": symbol, "cutoff": cutoff}).mappings().all()
        return [dict(row) for row in rows]

    def mark_ttl_cancel_requested(self, order: dict, *, requested_at: datetime,
                                   cancel_result: dict | None = None) -> None:
        """Journal a cancel request without treating its acknowledgement as terminal."""
        key = str(order.get("client_order_key") or "")
        trade_date = str(order.get("trade_date") or "")
        if not key or not trade_date:
            raise ValueError("TTL cancel request requires original order identity")
        patch = json.dumps({
            "tqqq_ttl_cancel_requested_at": requested_at.isoformat(),
            "tqqq_ttl_cancel_order_no": str(order.get("order_no") or ""),
            "tqqq_ttl_cancel_result": cancel_result or {},
        }, default=str)
        with self.engine.begin() as conn:
            conn.execute(text("""
                UPDATE us_orders
                SET meta=COALESCE(meta, '{}'::jsonb) || CAST(:patch AS jsonb), updated_at=NOW()
                WHERE trade_date=:trade_date AND client_order_key=:key
            """), {"patch": patch, "trade_date": trade_date, "key": key})

    def apply_ttl_terminal_observation(self, order: dict, observation: dict) -> dict:
        """Persist only broker-confirmed terminal truth for the original order."""
        from trader.us.db.repos import apply_broker_order_observation
        from trader.us.utils.order_no import normalize_us_order_no

        status = str(observation.get("status") or "").upper().replace("CANCELED", "CANCELLED")
        if status not in {"CANCELLED", "REJECTED", "EXPIRED", "FILLED"}:
            return {"status": "PENDING"}
        requested = int(order.get("qty_requested") or order.get("qty") or 0)
        filled = int(observation.get("filled_qty") or observation.get("cumulative_filled_qty") or 0)
        return apply_broker_order_observation(
            trade_date=str(order["trade_date"]),
            client_order_key=str(order["client_order_key"]),
            raw_order_no=str(order["order_no"]),
            canonical_order_no=normalize_us_order_no(str(order["order_no"])),
            symbol=str(order["symbol"]),
            side="BUY",
            requested_qty=requested,
            filled_qty=filled,
            remaining_qty=max(0, requested - filled),
            broker_status=status,
            evidence_type=str(observation.get("evidence_type") or "TQQQ_TTL_BROKER_REQUERY"),
            observed_at=observation.get("observed_at"),
            raw_row=observation,
        )

    def pending_buy_notional(self, trade_date: date, symbol: str = "TQQQ") -> float:
        """Capital reserved by unresolved BUY ACK/pending quantities."""
        with self.engine.connect() as conn:
            value = conn.execute(text("""
                SELECT COALESCE(SUM(
                    CASE WHEN qty_requested > 0 THEN
                        committed_notional_usd * GREATEST(qty_requested-qty_filled,0) / qty_requested
                    ELSE committed_notional_usd END
                ),0)
                FROM us_orders WHERE trade_date=:trade_date AND symbol=:symbol AND side='BUY'
                  AND status IN ('INTENT','SUBMITTED','ACK','OPEN','PENDING','PARTIALLY_FILLED',
                                 'RECONCILE_PENDING','ACK_DB_FAILED')
            """), {"trade_date": trade_date, "symbol": symbol}).scalar()
        return max(0.0, float(value or 0))

    def next_full_exit_sequence(self, trade_date: date, cycle_id: str) -> int:
        """Return a deterministic retry sequence after terminal full-exit orders."""
        prefix = f"TQQQ_INF_V3:{cycle_id}:{trade_date.isoformat()}:SELL%"
        with self.engine.connect() as conn:
            value = conn.execute(text("""
                SELECT COUNT(*) FROM us_orders
                WHERE symbol='TQQQ' AND side='SELL' AND client_order_key LIKE :prefix
                  AND status IN ('CANCELLED','REJECTED','EXPIRED')
            """), {"prefix": prefix}).scalar()
        return int(value or 0) + 1

    def has_pending_infinite_order(self, symbol: str = "TQQQ") -> bool:
        with self.engine.connect() as conn:
            return bool(conn.execute(text("""
                SELECT 1 FROM us_orders
                WHERE symbol=:symbol
                  AND status IN ('INTENT','SUBMITTED','ACK','PENDING','PARTIALLY_FILLED','RECONCILE_PENDING','ACK_DB_FAILED')
                  AND client_order_key LIKE 'TQQQ_INF_V3:%'
                LIMIT 1
            """), {"symbol": symbol}).first())

    def find_recovery_cycle_id(self, symbol: str = "TQQQ") -> str | None:
        """Reuse the newest durable Infinite key without rewriting its identity."""
        with self.engine.connect() as conn:
            row = conn.execute(text("""
                SELECT client_order_key FROM (
                    SELECT client_order_key, created_at FROM us_orders WHERE symbol=:symbol
                    UNION ALL
                    SELECT client_order_key, created_at FROM us_order_intents WHERE symbol=:symbol
                ) history
                WHERE client_order_key LIKE 'TQQQ_INF_V3:%'
                ORDER BY created_at DESC NULLS LAST LIMIT 1
            """), {"symbol": symbol}).first()
        key = str(row[0] if row else "")
        parts = key.split(":")
        return parts[1] if len(parts) >= 3 and parts[1] else None

    def backfill_tqqq_attribution(self, strategy_version: str) -> None:
        """Add ownership JSON only; quantities, prices, fills and keys are untouched."""
        attribution = json.dumps({
            "strategy_owner": "TQQQ_INFINITE", "strategy_name": "TQQQ_INFINITE",
            "strategy_version": strategy_version, "sleeve_id": "TQQQ_INFINITE",
            "ownership_source": "SYMBOL_INVARIANT_RECOVERY",
        })
        with self.engine.begin() as conn:
            for table in ("us_order_intents", "us_orders", "us_fills"):
                conn.execute(text(f"""
                    UPDATE {table} SET meta=COALESCE(meta, '{{}}'::jsonb) || CAST(:attribution AS jsonb)
                    WHERE symbol='TQQQ' AND (
                        COALESCE(meta->>'strategy_owner','') <> 'TQQQ_INFINITE'
                        OR COALESCE(meta->>'sleeve_id','') <> 'TQQQ_INFINITE'
                    )
                """), {"attribution": attribution})

    def fill_accounting(self, state: InfiniteState, trading_date: date) -> tuple[float, float, float, date | None, float | None]:
        """Return cycle BUY total, today's BUY total, cycle SELL total, last BUY date and first fill price."""
        start = state.cycle_start_date or trading_date
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT f.trade_date,f.side,f.qty,f.price_usd,f.meta,
                       f.client_order_key,f.order_no,
                       i.strategy AS intent_strategy,i.meta AS intent_meta,
                       o.meta AS order_meta
                FROM us_fills f
                LEFT JOIN us_order_intents i
                  ON i.client_order_key=f.client_order_key
                LEFT JOIN us_orders o
                  ON o.client_order_key=f.client_order_key
                WHERE f.symbol=:symbol AND f.trade_date>=:start
                ORDER BY f.trade_date,f.filled_at,f.created_at
            """), {"symbol": state.symbol, "start": start}).mappings().all()
        return self._summarize_fill_rows(rows, state, trading_date)

    def cycle_fill_stats(self, state: InfiniteState, trading_date: date) -> dict[str, Any]:
        """Extended fill diagnostics without changing ``fill_accounting``'s tuple contract."""
        start = state.cycle_start_date or trading_date
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT f.trade_date,f.side,f.qty,f.price_usd,f.meta,f.client_order_key,
                       i.strategy AS intent_strategy,i.meta AS intent_meta,o.meta AS order_meta
                FROM us_fills f LEFT JOIN us_order_intents i ON i.client_order_key=f.client_order_key
                LEFT JOIN us_orders o ON o.client_order_key=f.client_order_key
                WHERE f.symbol=:symbol AND f.trade_date>=:start
                ORDER BY f.trade_date,f.filled_at,f.created_at
            """), {"symbol": state.symbol, "start": start}).mappings().all()
        summary = self._summarize_fill_rows(rows, state, trading_date)
        last_price = None
        last_profit_stage = None
        for row in rows:
            if self._belongs_to_cycle(row, state) and str(row.get("side") or "").upper() == "BUY":
                meta = self._json_object(row.get("meta"))
                if meta.get("accounting_active") is not False:
                    value = float(row.get("price_usd") or 0)
                    if value > 0:
                        last_price = value
            if self._belongs_to_cycle(row, state) and str(row.get("side") or "").upper() == "SELL":
                metas = (self._json_object(row.get("meta")), self._json_object(row.get("intent_meta")),
                         self._json_object(row.get("order_meta")))
                stage = next((str(meta.get("desired_profit_stage") or meta.get("profit_stage") or "").upper()
                              for meta in metas if meta.get("desired_profit_stage") or meta.get("profit_stage")), "")
                if stage:
                    last_profit_stage = stage.removesuffix("_SUBMITTED").removesuffix("_FILLED") + "_FILLED"
        return {"total_buy_notional": summary[0], "daily_buy_notional": summary[1],
                "total_sell_notional": summary[2], "last_buy_date": summary[3],
                "first_fill_price": summary[4], "last_buy_fill_price": last_price,
                "last_rebound_probe_fill_date": self._last_rebound_probe_fill_date(rows, state),
                "last_profit_stage": last_profit_stage}

    @classmethod
    def _last_rebound_probe_fill_date(cls, rows: list[Any], state: InfiniteState) -> date | None:
        """Return only an attributed, actual BUY fill tagged as a rebound probe."""
        last_date = None
        for row in rows:
            if not cls._belongs_to_cycle(row, state) or str(row.get("side") or "").upper() != "BUY":
                continue
            fill_meta = cls._json_object(row.get("meta"))
            intent_meta = cls._json_object(row.get("intent_meta"))
            order_meta = cls._json_object(row.get("order_meta"))
            if fill_meta.get("accounting_active") is False:
                continue
            if any(str(meta.get("policy_action") or "").upper() == "REBOUND_PROBE"
                   for meta in (fill_meta, intent_meta, order_meta)):
                last_date = row.get("trade_date")
        return last_date

    @classmethod
    def _summarize_fill_rows(cls, rows: list[Any], state: InfiniteState,
                             trading_date: date) -> tuple[float, float, float, date | None, float | None]:
        buys = daily = sells = 0.0
        last_buy = None
        anchor = None
        for row in rows:
            if not cls._belongs_to_cycle(row, state):
                continue
            meta = row.get("meta") or {}
            if isinstance(meta, str):
                try: meta = json.loads(meta)
                except ValueError: meta = {}
            if meta.get("accounting_active") is False:
                continue
            notional = float(row.get("qty") or 0) * float(row.get("price_usd") or 0)
            side = str(row.get("side") or "").upper()
            if side == "BUY":
                buys += notional
                last_buy = row["trade_date"]
                anchor = anchor or float(row.get("price_usd") or 0) or None
                if row["trade_date"] == trading_date:
                    daily += notional
            elif side == "SELL":
                sells += notional
        return buys, daily, sells, last_buy, anchor

    @staticmethod
    def _json_object(value: Any) -> dict:
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                return parsed if isinstance(parsed, dict) else {}
            except ValueError:
                return {}
        return {}

    @classmethod
    def _belongs_to_cycle(cls, row: Any, state: InfiniteState) -> bool:
        """Strict attribution: identity plus persisted strategy/book/cycle evidence."""
        if not state.cycle_id:
            return False
        key = str(row.get("client_order_key") or "")
        expected_prefix = f"TQQQ_INF_V3:{state.cycle_id}:"
        if not key.startswith(expected_prefix):
            return False
        fill_meta = cls._json_object(row.get("meta"))
        intent_meta = cls._json_object(row.get("intent_meta"))
        order_meta = cls._json_object(row.get("order_meta"))
        metadata = (fill_meta, intent_meta, order_meta)
        strategy_ok = str(row.get("intent_strategy") or "") == "TQQQ_INFINITE_V3" or any(
            str(meta.get("strategy") or "") == "TQQQ_INFINITE_V3" for meta in metadata
        )
        book_ok = any(str(meta.get("book") or "") == "TQQQ_INFINITE" for meta in metadata)
        cycle_ok = any(str(meta.get("cycle_id") or "") == state.cycle_id for meta in metadata)
        return strategy_ok and book_ok and cycle_ok

    def reconcile_metadata(self, state: InfiniteState, *, trading_date: date, broker_qty: int,
                           broker_average_price: float, core_cap: float,
                           rebound_cooldown: int = 3) -> InfiniteState:
        stats = self.cycle_fill_stats(state, trading_date)
        buys, last_buy, first_price = stats["total_buy_notional"], stats["last_buy_date"], stats["first_fill_price"]
        if broker_qty > 0 and buys <= 0:
            # Missing historical attribution must not erase a real KIS holding.
            # The broker balance is the recovery notional authority; no qty,
            # average price, fill or client key is synthesized or rewritten.
            buys = broker_qty * broker_average_price
        core = min(buys, core_cap)
        reserve = max(0.0, buys - core)
        anchor = state.anchor_price or first_price or (broker_average_price if broker_qty > 0 else None)
        age = state.cycle_age_trading_days
        if state.cycle_start_date:
            from datetime import timedelta
            from trader.us.market_calendar import is_us_trading_day
            cursor = state.cycle_start_date
            age = 0
            while cursor < trading_date:
                cursor += timedelta(days=1)
                age += int(is_us_trading_day(cursor))
        if state.status == Status.EXIT_PENDING and broker_qty == 0:
            return replace(state, status=Status.COMPLETE, cycle_complete_date=trading_date,
                           last_exit_date=trading_date, anchor_price=None, core_filled_notional=0,
                           reserve_filled_notional=0, reserve_unlocked=False, market_crash_streak=0,
                           cycle_age_trading_days=age)
        actual_last_buy_price = (stats["last_buy_fill_price"]
                                 or state.metadata.get("last_buy_fill_price"))
        metadata = {**state.metadata, "last_buy_fill_price": actual_last_buy_price,
                    "broker_qty": broker_qty, "broker_average_price": broker_average_price}
        pending_stage = str(state.metadata.get("pending_profit_stage") or "").upper()
        filled_stage = str(stats.get("last_profit_stage") or "").upper()
        terminal_pending_profit = (
            pending_stage.endswith("_SUBMITTED")
            and filled_stage.endswith("_FILLED")
            and pending_stage.removesuffix("_SUBMITTED") == filled_stage.removesuffix("_FILLED")
        )
        if terminal_pending_profit:
            metadata.update(profit_stage=stats["last_profit_stage"], pending_profit_stage=None)
        if actual_last_buy_price:
            metadata.update(buy_reference_price=actual_last_buy_price,
                            buy_reference_source="ATTRIBUTED_BUY_FILL",
                            recovery_accounting_uncertain=False)
        elif broker_qty > 0 and broker_average_price > 0:
            # This is explicitly a conservative decision reference, not a
            # fabricated fill.  Keep last_buy_fill_price empty so accounting
            # and rebound confirmation cannot mistake the fallback for a fill.
            metadata.update(buy_reference_price=broker_average_price,
                            buy_reference_source="KIS_BROKER_AVG_FALLBACK")
        probe_fill_date = stats.get("last_rebound_probe_fill_date")
        if probe_fill_date:
            from datetime import timedelta
            from trader.us.market_calendar import is_us_trading_day
            cooldown_until = probe_fill_date
            remaining = rebound_cooldown
            while remaining:
                cooldown_until += timedelta(days=1)
                remaining -= int(is_us_trading_day(cooldown_until))
            metadata.update(rebound_probe_date=probe_fill_date.isoformat(),
                            rebound_cooldown_until=cooldown_until.isoformat())
        status = (
            Status.ACTIVE if state.status == Status.EXIT_PENDING and broker_qty > 0
            and terminal_pending_profit else state.status
        )
        return replace(state, core_filled_notional=core, reserve_filled_notional=reserve,
                       last_buy_date=last_buy or state.last_buy_date, anchor_price=anchor,
                       cycle_age_trading_days=age, status=status, metadata=metadata)
