from __future__ import annotations

from datetime import datetime, timedelta
import logging
import os
from typing import Any, Dict, Iterable, List, Optional
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy import Engine, and_, func, select

from .schema import (
    FILLS,
    LEDGER_EVENTS,
    ORDERS,
    POSITIONS,
    RUNS,
    UNIVERSE,
    UNIVERSE_MEMBERS,
    schema_for_engine,
    uuid_value_for_url,
)

logger = logging.getLogger(__name__)
ALLOW_UNIVERSE_DB_FAIL = os.getenv("ALLOW_UNIVERSE_DB_FAIL", "1") not in {"0", "false", "FALSE"}


def _coerce_uuid(value: Any, *, uses_native_uuid: bool, database_url: str) -> Any:
    return uuid_value_for_url(database_url, value if isinstance(value, UUID) else value)


class RunsRepo:
    def __init__(self, engine: Engine):
        self.engine = engine
        self._schema = schema_for_engine(engine)

    def start_run(
        self,
        env: str,
        strategy: str,
        run_window: str | None,
        phase: str,
        event_name: str,
        dry_run: bool,
        git_sha: str | None,
        workflow: str | None,
        workflow_run_id: str | None,
        workflow_attempt: int | None,
        config_json: dict | None,
    ) -> str:
        values = {
            "run_id": _coerce_uuid(None, uses_native_uuid=self._schema.uses_native_uuid, database_url=str(self.engine.url)),
            "env": env,
            "strategy": strategy,
            "run_window": run_window,
            "phase": phase,
            "event_name": event_name,
            "dry_run": dry_run,
            "git_sha": git_sha,
            "workflow": workflow,
            "workflow_run_id": workflow_run_id,
            "workflow_attempt": workflow_attempt,
            "config_json": config_json or {},
        }
        stmt = sa.insert(self._schema.runs).values(**values)
        run_id = values["run_id"]
        with self.engine.begin() as conn:
            try:
                res = conn.execute(stmt.returning(self._schema.runs.c.run_id))
                run_id = res.scalar() or run_id
            except Exception:
                conn.execute(stmt)
        return str(run_id)

    def finish_run(self, run_id: str, status: str, notes: str | None = None) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                sa.update(self._schema.runs)
                .where(self._schema.runs.c.run_id == run_id)
                .values(status=status, finished_at=func.now(), notes=notes),
            )


class UniverseRepo:
    def __init__(self, engine: Engine):
        self.engine = engine
        self._schema = schema_for_engine(engine)

    def _fetch_members_for_universe(self, universe_id: str) -> list[dict]:
        stmt = (
            select(
                self._schema.universe_members.c.code,
                self._schema.universe_members.c.market,
                self._schema.universe_members.c.weight,
                self._schema.universe_members.c.rank,
                self._schema.universe_members.c.meta_json,
                self._schema.universe.c.as_of_date,
                self._schema.universe.c.strategy,
                self._schema.universe.c.env,
            )
            .select_from(
                self._schema.universe_members.join(
                    self._schema.universe, self._schema.universe_members.c.universe_id == self._schema.universe.c.universe_id
                )
            )
            .where(self._schema.universe_members.c.universe_id == universe_id)
            .order_by(self._schema.universe_members.c.rank.nullsfirst(), self._schema.universe_members.c.universe_member_id)
        )
        with self.engine.begin() as conn:
            rows = conn.execute(stmt).mappings().all()
            return [dict(row) for row in rows]

    def get_universe_members(self, env: str, strategy: str, as_of_date: str) -> list[dict]:
        stmt = (
            select(self._schema.universe.c.universe_id)
            .where(
                and_(
                    self._schema.universe.c.env == env,
                    self._schema.universe.c.strategy == strategy,
                    self._schema.universe.c.as_of_date == as_of_date,
                )
            )
            .order_by(self._schema.universe.c.created_at.desc())
            .limit(1)
        )
        with self.engine.begin() as conn:
            universe_id = conn.execute(stmt).scalar()
        if not universe_id:
            return []
        return self._fetch_members_for_universe(str(universe_id))

    def get_latest_universe_members(self, env: str, strategy: str) -> list[dict]:
        stmt = (
            select(self._schema.universe.c.universe_id)
            .where(and_(self._schema.universe.c.env == env, self._schema.universe.c.strategy == strategy))
            .order_by(self._schema.universe.c.as_of_date.desc(), self._schema.universe.c.created_at.desc())
            .limit(1)
        )
        with self.engine.begin() as conn:
            universe_id = conn.execute(stmt).scalar()
        if not universe_id:
            return []
        return self._fetch_members_for_universe(str(universe_id))

    def get_latest_universe(
        self, env: str, strategy: str, *, max_age_days: int = 10
    ) -> tuple[dict, list[dict]] | None:
        cutoff = datetime.utcnow() - timedelta(days=max_age_days)
        stmt = (
            select(self._schema.universe)
            .where(
                and_(
                    self._schema.universe.c.env == env,
                    self._schema.universe.c.strategy == strategy,
                    self._schema.universe.c.created_at >= cutoff,
                )
            )
            .order_by(self._schema.universe.c.created_at.desc())
            .limit(1)
        )
        with self.engine.begin() as conn:
            row = conn.execute(stmt).mappings().first()
        if not row:
            return None
        universe_id = str(row.get("universe_id"))
        return dict(row), self._fetch_members_for_universe(universe_id)

    def store_universe(
        self,
        env: str,
        strategy: str,
        as_of_date: str,
        source: str,
        params_json: dict,
        payload_json: dict,
        members: Iterable[dict],
    ) -> str | None:
        members_list = list(members)
        db_url = str(self.engine.url)
        values = {
            "universe_id": _coerce_uuid(None, uses_native_uuid=self._schema.uses_native_uuid, database_url=db_url),
            "env": env,
            "strategy": strategy,
            "as_of_date": as_of_date,
            "source": source,
            "params_json": params_json or {},
            "payload_json": payload_json or {},
        }
        try:
            with self.engine.begin() as conn:
                existing = conn.execute(
                    select(self._schema.universe.c.universe_id).where(
                        and_(
                            self._schema.universe.c.env == env,
                            self._schema.universe.c.strategy == strategy,
                            self._schema.universe.c.as_of_date == as_of_date,
                        )
                    )
                ).scalar()
                universe_id = existing or values["universe_id"]
                if existing:
                    conn.execute(
                        sa.update(self._schema.universe)
                        .where(self._schema.universe.c.universe_id == existing)
                        .values(source=source, params_json=params_json or {}, payload_json=payload_json or {}, created_at=func.now()),
                    )
                    conn.execute(
                        sa.delete(self._schema.universe_members).where(self._schema.universe_members.c.universe_id == existing)
                    )
                else:
                    res = conn.execute(
                        sa.insert(self._schema.universe)
                        .values(**values)
                        .returning(self._schema.universe.c.universe_id)
                    )
                    universe_id = res.scalar() or universe_id
                fk_universe_id = uuid_value_for_url(db_url, universe_id)
                for rank, m in enumerate(members_list, start=1):
                    conn.execute(
                        sa.insert(self._schema.universe_members).values(
                            universe_member_id=_coerce_uuid(None, uses_native_uuid=self._schema.uses_native_uuid, database_url=db_url),
                            universe_id=fk_universe_id,
                            code=str(m.get("code") or "").zfill(6),
                            market=m.get("market"),
                            weight=m.get("weight"),
                            rank=m.get("rank") if m.get("rank") is not None else rank,
                            meta_json=m.get("meta_json") or {},
                        )
                    )
            return str(universe_id)
        except Exception:
            logger.exception("[UNIVERSE][STORE][FAIL] env=%s strategy=%s as_of=%s", env, strategy, as_of_date)
            if ALLOW_UNIVERSE_DB_FAIL:
                return None
            raise


class OrdersRepo:
    def __init__(self, engine: Engine):
        self.engine = engine
        self._schema = schema_for_engine(engine)

    def create_intent_idempotent(
        self,
        env: str,
        run_id: str,
        strategy: str,
        sid: int,
        mode: int,
        code: str,
        market: str | None,
        side: str,
        ord_type: str,
        qty: int,
        limit_price: float | None,
        stage: str,
        client_order_key: str,
        request_json: dict | None,
    ) -> str:
        db_url = str(self.engine.url)
        payload = {
            "order_id": _coerce_uuid(None, uses_native_uuid=self._schema.uses_native_uuid, database_url=db_url),
            "env": env,
            "run_id": uuid_value_for_url(db_url, run_id) if run_id is not None else None,
            "strategy": strategy,
            "sid": sid,
            "mode": mode,
            "code": code,
            "market": market,
            "side": side,
            "ord_type": ord_type,
            "qty": qty,
            "limit_price": limit_price,
            "stage": stage,
            "client_order_key": client_order_key,
            "request_json": request_json or {},
        }
        with self.engine.begin() as conn:
            existing = conn.execute(
                select(self._schema.orders.c.order_id).where(
                    and_(self._schema.orders.c.env == env, self._schema.orders.c.client_order_key == client_order_key)
                )
            ).scalar()
            if existing:
                return str(existing)
            stmt = sa.insert(self._schema.orders).values(**payload).returning(self._schema.orders.c.order_id)
            try:
                res = conn.execute(stmt)
                return str(res.scalar())
            except Exception:
                conn.execute(sa.insert(self._schema.orders).values(**payload))
                return str(payload["order_id"])

    def mark_submitted(self, env: str, client_order_key: str, kis_odno: str | None, response_json: dict | None) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                sa.update(self._schema.orders)
                .where(and_(self._schema.orders.c.env == env, self._schema.orders.c.client_order_key == client_order_key))
                .values(
                    status="SUBMITTED",
                    kis_odno=kis_odno,
                    response_json=response_json,
                    submitted_at=func.now(),
                    updated_at=func.now(),
                )
            )

    def mark_acked(self, env: str, kis_odno: str | None, response_json: dict | None) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                sa.update(self._schema.orders)
                .where(and_(self._schema.orders.c.env == env, self._schema.orders.c.kis_odno == kis_odno))
                .values(
                    status="ACKED",
                    response_json=response_json,
                    acked_at=func.now(),
                    updated_at=func.now(),
                )
            )

    def mark_error(self, env: str, client_order_key: str, error_payload: dict | None) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                sa.update(self._schema.orders)
                .where(and_(self._schema.orders.c.env == env, self._schema.orders.c.client_order_key == client_order_key))
                .values(status="ERROR", response_json=error_payload or {}, updated_at=func.now()),
            )

    def get_open_orders(self, env: str) -> list[dict]:
        stmt = select(self._schema.orders).where(
            and_(self._schema.orders.c.env == env, self._schema.orders.c.status.in_(["INTENT", "SUBMITTED"]))
        )
        with self.engine.begin() as conn:
            rows = conn.execute(stmt).mappings().all()
            return [dict(r) for r in rows]

    def has_client_order_key(self, env: str, client_order_key: str) -> bool:
        stmt = select(self._schema.orders.c.order_id).where(
            and_(self._schema.orders.c.env == env, self._schema.orders.c.client_order_key == client_order_key)
        )
        with self.engine.begin() as conn:
            return conn.execute(stmt).scalar() is not None


class FillsRepo:
    def __init__(self, engine: Engine):
        self.engine = engine
        self._schema = schema_for_engine(engine)

    def upsert_fill(
        self,
        *,
        env: str,
        run_id: str | None,
        order_id: str | None,
        kis_odno: str | None,
        trade_id: str | None,
        code: str,
        market: str | None,
        side: str,
        qty: int,
        price: float,
        fee: float,
        tax: float,
        filled_at: datetime,
        raw_json: dict | None,
    ) -> str:
        db_url = str(self.engine.url)
        with self.engine.begin() as conn:
            fill_id = None
            if trade_id:
                fill_id = conn.execute(
                    select(self._schema.fills.c.fill_id).where(
                        and_(self._schema.fills.c.env == env, self._schema.fills.c.trade_id == trade_id)
                    )
                ).scalar()
            if not fill_id and kis_odno:
                fill_id = conn.execute(
                    select(self._schema.fills.c.fill_id).where(
                        and_(
                            self._schema.fills.c.env == env,
                            self._schema.fills.c.kis_odno == kis_odno,
                            self._schema.fills.c.code == code,
                            self._schema.fills.c.side == side,
                            self._schema.fills.c.qty == qty,
                            self._schema.fills.c.price == price,
                            self._schema.fills.c.filled_at == filled_at,
                        )
                    )
                ).scalar()
            if fill_id:
                conn.execute(
                    sa.update(self._schema.fills)
                    .where(self._schema.fills.c.fill_id == fill_id)
                    .values(raw_json=raw_json or {}),
                )
                return str(fill_id)

            payload = {
                "fill_id": _coerce_uuid(None, uses_native_uuid=self._schema.uses_native_uuid, database_url=db_url),
                "env": env,
                "run_id": uuid_value_for_url(db_url, run_id) if run_id is not None else None,
                "order_id": uuid_value_for_url(db_url, order_id) if order_id is not None else None,
                "kis_odno": kis_odno,
                "trade_id": trade_id,
                "code": code,
                "market": market,
                "side": side,
                "qty": qty,
                "price": price,
                "fee": fee,
                "tax": tax,
                "filled_at": filled_at,
                "raw_json": raw_json or {},
            }
            stmt = sa.insert(self._schema.fills).values(**payload).returning(self._schema.fills.c.fill_id)
            try:
                res = conn.execute(stmt)
                return str(res.scalar())
            except Exception:
                conn.execute(sa.insert(self._schema.fills).values(**payload))
                return str(payload["fill_id"])


class PositionsRepo:
    def __init__(self, engine: Engine):
        self.engine = engine
        self._schema = schema_for_engine(engine)

    def _get_position_row(self, env: str, strategy: str, sid: int, mode: int, code: str) -> Optional[dict]:
        stmt = select(self._schema.positions).where(
            and_(
                self._schema.positions.c.env == env,
                self._schema.positions.c.strategy == strategy,
                self._schema.positions.c.sid == sid,
                self._schema.positions.c.mode == mode,
                self._schema.positions.c.code == code,
            )
        )
        with self.engine.begin() as conn:
            row = conn.execute(stmt).mappings().first()
            return dict(row) if row else None

    def list_positions(self, env: str, strategy: str) -> list[dict]:
        stmt = select(self._schema.positions).where(
            and_(self._schema.positions.c.env == env, self._schema.positions.c.strategy == strategy)
        )
        with self.engine.begin() as conn:
            rows = conn.execute(stmt).mappings().all()
            return [dict(r) for r in rows]

    def apply_fill(
        self,
        *,
        env: str,
        strategy: str,
        sid: int,
        mode: int,
        code: str,
        market: str | None,
        side: str,
        qty: int,
        price: float,
        fee: float,
        tax: float,
        filled_at: datetime,
    ) -> None:
        with self.engine.begin() as conn:
            stmt = select(self._schema.positions).where(
                and_(
                    self._schema.positions.c.env == env,
                    self._schema.positions.c.strategy == strategy,
                    self._schema.positions.c.sid == sid,
                    self._schema.positions.c.mode == mode,
                    self._schema.positions.c.code == code,
                )
            )
            row = conn.execute(stmt).mappings().first()
            if row:
                row = dict(row)
            qty = int(qty)
            cost_delta = (qty * float(price)) + float(fee) + float(tax)
            if row:
                current_qty = int(row.get("qty") or 0)
                avg_buy_price = float(row.get("avg_buy_price") or 0.0)
                total_cost = float(row.get("total_cost") or 0.0)
                realized_pnl = float(row.get("realized_pnl") or 0.0)
            else:
                current_qty = 0
                avg_buy_price = 0.0
                total_cost = 0.0
                realized_pnl = 0.0

            if side.upper() == "BUY":
                new_qty = current_qty + qty
                new_total_cost = total_cost + cost_delta
                new_avg = new_total_cost / new_qty if new_qty > 0 else None
                values = {
                    "qty": new_qty,
                    "avg_buy_price": new_avg,
                    "total_cost": new_total_cost,
                    "realized_pnl": realized_pnl,
                    "market": market,
                    "last_trade_at": filled_at,
                }
            else:
                qty_to_close = min(qty, current_qty)
                remaining_qty = max(current_qty - qty_to_close, 0)
                proceeds = (float(price) * qty_to_close) - float(fee) - float(tax)
                cost_basis = avg_buy_price * qty_to_close
                new_realized = realized_pnl + (proceeds - cost_basis)
                new_total_cost = max(total_cost - cost_basis, 0.0)
                new_avg = avg_buy_price if remaining_qty > 0 else None
                values = {
                    "qty": remaining_qty,
                    "avg_buy_price": new_avg,
                    "total_cost": new_total_cost,
                    "realized_pnl": new_realized,
                    "market": market,
                    "last_trade_at": filled_at,
                }

            if row:
                conn.execute(
                    sa.update(self._schema.positions)
                    .where(self._schema.positions.c.position_id == row["position_id"])
                    .values(**values, updated_at=func.now()),
                )
            else:
                conn.execute(
                    sa.insert(self._schema.positions).values(
                        position_id=_coerce_uuid(None, uses_native_uuid=self._schema.uses_native_uuid, database_url=str(self.engine.url)),
                        env=env,
                        strategy=strategy,
                        sid=sid,
                        mode=mode,
                        code=code,
                        market=market,
                        qty=values["qty"],
                        avg_buy_price=values["avg_buy_price"],
                        total_cost=values["total_cost"],
                        realized_pnl=values["realized_pnl"],
                        last_trade_at=filled_at,
                    )
                )

    def bootstrap_from_kis_holdings(
        self, env: str, strategy: str, sid: int, mode: int, holdings: Iterable[dict]
    ) -> int:
        count = 0
        with self.engine.begin() as conn:
            for row in holdings or []:
                try:
                    code = str(row.get("pdno") or row.get("code") or "").zfill(6)
                    qty = int(float(row.get("hldg_qty") or row.get("qty") or 0))
                    if qty <= 0 or not code:
                        continue
                    avg_price = float(row.get("pchs_avg_pric") or row.get("pchs_avg_price") or row.get("avg_price") or 0.0)
                    total_cost = float(row.get("pchs_amt") or row.get("total_cost") or avg_price * qty)
                    market = row.get("prdt_type_cd") or row.get("market") or row.get("mket_gb")
                except Exception:
                    continue
                existing = conn.execute(
                    select(self._schema.positions.c.position_id).where(
                        and_(
                            self._schema.positions.c.env == env,
                            self._schema.positions.c.strategy == strategy,
                            self._schema.positions.c.sid == sid,
                            self._schema.positions.c.mode == mode,
                            self._schema.positions.c.code == code,
                        )
                    )
                ).scalar()
                if existing:
                    continue
                conn.execute(
                    sa.insert(self._schema.positions).values(
                        position_id=_coerce_uuid(None, uses_native_uuid=self._schema.uses_native_uuid, database_url=str(self.engine.url)),
                        env=env,
                        strategy=strategy,
                        sid=sid,
                        mode=mode,
                        code=code,
                        market=market,
                        qty=qty,
                        avg_buy_price=avg_price or None,
                        total_cost=total_cost or 0.0,
                        realized_pnl=0.0,
                        last_trade_at=None,
                    )
                )
                count += 1
        return count
