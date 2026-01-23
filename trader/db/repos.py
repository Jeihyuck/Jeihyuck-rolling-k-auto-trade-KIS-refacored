from __future__ import annotations

from datetime import datetime, timedelta
import logging
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.exc import OperationalError
from sqlalchemy import Engine, and_, func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .schema import (
    FILLS,
    LEDGER_EVENTS,
    ORDERS,
    POSITIONS,
    RUNS,
    UNIVERSE_MEMBERS,
    UNIVERSE_CURRENT,
    UNIVERSE_RUNS,
    schema_for_engine,
    uuid_value_for_url,
)
from .migrate import ensure_sqlite_writable, run_migrations
from . import config
from trader.time_utils import now_kst
from trader.utils.json_sanitize import json_safe

logger = logging.getLogger(__name__)
ALLOW_UNIVERSE_DB_FAIL = os.getenv("ALLOW_UNIVERSE_DB_FAIL", "1") not in {"0", "false", "FALSE"}


def _coerce_uuid(value: Any, *, uses_native_uuid: bool, database_url: str) -> Any:
    return uuid_value_for_url(database_url, value if isinstance(value, UUID) else value)


def _is_sqlite_readonly(exc: Exception, engine: Engine) -> bool:
    message = str(exc).lower()
    if "readonly" not in message:
        return False
    return config.is_sqlite_url(str(engine.url))


def _recover_sqlite_readonly(engine: Engine) -> bool:
    url = str(engine.url)
    if not config.is_sqlite_url(url):
        return False
    db_path = Path(engine.url.database or "")
    if not db_path:
        return False
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = db_path.with_name(f"{db_path.name}.readonly.bak.{ts}")
    try:
        if db_path.exists():
            db_path.rename(backup_path)
            logger.warning("[DB][READONLY][BACKUP] backup=%s", backup_path)
    except Exception:
        logger.warning("[DB][READONLY][BACKUP_FAIL] path=%s", db_path, exc_info=True)

    for suffix in ("-wal", "-shm"):
        sidecar = Path(f"{db_path}{suffix}")
        if sidecar.exists():
            try:
                sidecar.unlink()
            except Exception:
                logger.warning("[DB][READONLY][SIDECAR_REMOVE_FAIL] path=%s", sidecar, exc_info=True)
    try:
        engine.dispose()
    except Exception:
        logger.warning("[DB][READONLY][DISPOSE_FAIL] url=%s", url, exc_info=True)
    ensure_sqlite_writable(db_path)
    run_migrations(engine)
    return True


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
        for attempt in range(2):
            with self.engine.begin() as conn:
                try:
                    res = conn.execute(stmt.returning(self._schema.runs.c.run_id))
                    run_id = res.scalar() or run_id
                    return str(run_id)
                except OperationalError as exc:
                    if attempt == 0 and _is_sqlite_readonly(exc, self.engine):
                        _recover_sqlite_readonly(self.engine)
                        continue
                    raise
                except Exception:
                    conn.execute(stmt)
                    return str(run_id)
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

    def _strategy_key(self, env: str, strategy: str) -> str:
        return f"{env}:{strategy}"

    def _fetch_members_for_run(self, run_id: str, *, env: str, strategy: str) -> list[dict]:
        stmt = (
            select(
                self._schema.universe_members.c.stock_code,
                self._schema.universe_members.c.name,
                self._schema.universe_members.c.market,
                self._schema.universe_members.c.rank,
                self._schema.universe_members.c.market_cap,
                self._schema.universe_members.c.reason,
                self._schema.universe_runs.c.as_of,
                self._schema.universe_runs.c.provider,
            )
            .select_from(
                self._schema.universe_members.join(
                    self._schema.universe_runs,
                    self._schema.universe_members.c.run_id == self._schema.universe_runs.c.run_id,
                )
            )
            .where(self._schema.universe_members.c.run_id == run_id)
            .order_by(self._schema.universe_members.c.rank.nullsfirst())
        )
        with self.engine.begin() as conn:
            rows = conn.execute(stmt).mappings().all()
            results: list[dict] = []
            for row in rows:
                payload = dict(row)
                payload["code"] = payload.pop("stock_code")
                payload["as_of_date"] = payload.pop("as_of")
                payload["env"] = env
                payload["strategy"] = strategy
                results.append(payload)
            return results

    def get_current_universe_members(self, env: str, strategy: str) -> list[dict]:
        strategy_key = self._strategy_key(env, strategy)
        stmt = select(self._schema.universe_current.c.run_id).where(self._schema.universe_current.c.strategy == strategy_key)
        with self.engine.begin() as conn:
            run_id = conn.execute(stmt).scalar()
        if not run_id:
            return []
        return self._fetch_members_for_run(str(run_id), env=env, strategy=strategy)

    def store_universe_snapshot(
        self,
        *,
        env: str,
        strategy: str,
        as_of_date: str,
        provider: str,
        members: Iterable[dict],
        reason: str | None = None,
    ) -> str | None:
        members_list = list(members)
        db_url = str(self.engine.url)
        run_id = _coerce_uuid(None, uses_native_uuid=self._schema.uses_native_uuid, database_url=db_url)
        strategy_key = self._strategy_key(env, strategy)
        try:
            with self.engine.begin() as conn:
                existing = conn.execute(
                    select(self._schema.universe_runs.c.run_id).where(
                        and_(
                            self._schema.universe_runs.c.strategy == strategy_key,
                            self._schema.universe_runs.c.provider == provider,
                            self._schema.universe_runs.c.as_of == as_of_date,
                        )
                    )
                ).scalar()
                if existing:
                    conn.execute(sa.delete(self._schema.universe_runs).where(self._schema.universe_runs.c.run_id == existing))
                conn.execute(
                    sa.insert(self._schema.universe_runs).values(
                        run_id=run_id,
                        strategy=strategy_key,
                        provider=provider,
                        as_of=as_of_date,
                        created_ts=now_kst().isoformat(),
                    )
                )
                for rank, member in enumerate(members_list, start=1):
                    meta = member.get("meta_json") or {}
                    conn.execute(
                        sa.insert(self._schema.universe_members).values(
                            run_id=uuid_value_for_url(db_url, run_id),
                            stock_code=str(member.get("code") or "").zfill(6),
                            name=member.get("name") or meta.get("name"),
                            market=member.get("market"),
                            rank=member.get("rank") if member.get("rank") is not None else rank,
                            market_cap=member.get("market_cap") or meta.get("market_cap") or meta.get("mktcap"),
                            reason=member.get("reason") or reason,
                        )
                    )
                if self.engine.dialect.name == "postgresql":
                    insert_stmt = pg_insert(self._schema.universe_current).values(
                        strategy=strategy_key,
                        run_id=uuid_value_for_url(db_url, run_id),
                        updated_ts=now_kst().isoformat(),
                    )
                else:
                    insert_stmt = sqlite_insert(self._schema.universe_current).values(
                        strategy=strategy_key,
                        run_id=uuid_value_for_url(db_url, run_id),
                        updated_ts=now_kst().isoformat(),
                    )
                conn.execute(
                    insert_stmt.on_conflict_do_update(
                        index_elements=[self._schema.universe_current.c.strategy],
                        set_={"run_id": uuid_value_for_url(db_url, run_id), "updated_ts": now_kst().isoformat()},
                    )
                )
            return str(run_id)
        except Exception:
            logger.exception("[UNIVERSE][STORE][FAIL] env=%s strategy=%s as_of=%s", env, strategy, as_of_date)
            if ALLOW_UNIVERSE_DB_FAIL:
                return None
            raise

    def cleanup_old_runs(self, *, retain_days: int = 30) -> None:
        cutoff = (now_kst() - timedelta(days=retain_days)).isoformat()
        with self.engine.begin() as conn:
            current_runs = select(self._schema.universe_current.c.run_id)
            conn.execute(
                sa.delete(self._schema.universe_runs).where(
                    and_(
                        self._schema.universe_runs.c.created_ts < cutoff,
                        self._schema.universe_runs.c.run_id.not_in(current_runs),
                    )
                )
            )


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
        *,
        status: str = "CREATED",
    ) -> tuple[str, bool]:
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
            "status": status,
            "request_json": request_json or {},
        }
        with self.engine.begin() as conn:
            existing = conn.execute(
                select(self._schema.orders.c.order_id).where(
                    and_(self._schema.orders.c.env == env, self._schema.orders.c.client_order_key == client_order_key)
                )
            ).scalar()
            if existing:
                return str(existing), False
            stmt = sa.insert(self._schema.orders).values(**payload).returning(self._schema.orders.c.order_id)
            try:
                res = conn.execute(stmt)
                return str(res.scalar()), True
            except Exception:
                conn.execute(sa.insert(self._schema.orders).values(**payload))
                return str(payload["order_id"]), True

    def mark_submitted(self, env: str, client_order_key: str, kis_odno: str | None, response_json: dict | None) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                sa.update(self._schema.orders)
                .where(and_(self._schema.orders.c.env == env, self._schema.orders.c.client_order_key == client_order_key))
                .values(
                    status="SUBMITTED",
                    kis_odno=kis_odno,
                    broker_order_id=kis_odno or client_order_key,
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
                    broker_order_id=kis_odno,
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

    def mark_cancelled(self, env: str, client_order_key: str, response_json: dict | None = None) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                sa.update(self._schema.orders)
                .where(and_(self._schema.orders.c.env == env, self._schema.orders.c.client_order_key == client_order_key))
                .values(status="CANCELLED", response_json=response_json or {}, updated_at=func.now()),
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

    def list_today_orders(
        self,
        env: str,
        *,
        side: str | None = None,
        code: str | None = None,
        status_exclude: Iterable[str] | None = ("ERROR",),
    ) -> list[dict]:
        now = now_kst()
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        conditions = [self._schema.orders.c.env == env, self._schema.orders.c.created_at >= start, self._schema.orders.c.created_at < end]
        if side:
            conditions.append(self._schema.orders.c.side == side)
        if code:
            conditions.append(self._schema.orders.c.code == code)
        if status_exclude:
            conditions.append(self._schema.orders.c.status.not_in(list(status_exclude)))
        stmt = select(self._schema.orders).where(and_(*conditions))
        with self.engine.begin() as conn:
            rows = conn.execute(stmt).mappings().all()
        return [dict(r) for r in rows]

    def upsert_reconciled_order(
        self,
        *,
        env: str,
        run_id: str | None,
        strategy: str,
        sid: int,
        mode: int,
        code: str,
        market: str | None,
        side: str,
        ord_type: str,
        qty: int,
        limit_price: float | None,
        stage: str | None,
        client_order_key: str,
        kis_odno: str | None,
        status: str,
        request_json: dict | None,
        response_json: dict | None,
        submitted_at: datetime | None,
        acked_at: datetime | None,
    ) -> str:
        db_url = str(self.engine.url)
        broker_order_id = kis_odno or client_order_key
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
            "status": status,
            "kis_odno": kis_odno,
            "broker_order_id": broker_order_id,
            "request_json": request_json or {},
            "response_json": response_json or {},
            "submitted_at": submitted_at,
            "acked_at": acked_at,
        }
        conflict_cols = ["env", "broker_order_id"] if broker_order_id else ["env", "client_order_key"]
        update_cols = {
            "status": status,
            "kis_odno": kis_odno,
            "broker_order_id": broker_order_id,
            "response_json": response_json or {},
            "request_json": request_json or {},
            "submitted_at": submitted_at,
            "acked_at": acked_at,
            "updated_at": func.now(),
        }
        insert_stmt: sa.Insert
        if self.engine.dialect.name == "postgresql":
            insert_stmt = pg_insert(self._schema.orders).values(**payload)
        else:
            insert_stmt = sqlite_insert(self._schema.orders).values(**payload)
        stmt = insert_stmt.on_conflict_do_update(
            index_elements=conflict_cols,
            set_=update_cols,
        ).returning(self._schema.orders.c.order_id)
        with self.engine.begin() as conn:
            try:
                res = conn.execute(stmt)
                order_id = res.scalar()
                if order_id:
                    return str(order_id)
            except Exception:
                pass
            existing = conn.execute(
                select(self._schema.orders.c.order_id).where(
                    and_(
                        self._schema.orders.c.env == env,
                        self._schema.orders.c.client_order_key == client_order_key,
                    )
                )
            ).scalar()
            return str(existing or payload["order_id"])


class FillsRepo:
    def __init__(self, engine: Engine):
        self.engine = engine
        self._schema = schema_for_engine(engine)

    def list_today_fills(
        self,
        env: str,
        *,
        side: str | None = None,
        code: str | None = None,
    ) -> list[dict]:
        now = now_kst()
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        conditions = [self._schema.fills.c.env == env, self._schema.fills.c.filled_at >= start, self._schema.fills.c.filled_at < end]
        if side:
            conditions.append(self._schema.fills.c.side == side)
        if code:
            conditions.append(self._schema.fills.c.code == code)
        stmt = select(self._schema.fills).where(and_(*conditions))
        with self.engine.begin() as conn:
            rows = conn.execute(stmt).mappings().all()
        return [dict(r) for r in rows]

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
        broker_fill_id = trade_id or None
        payload = {
            "fill_id": _coerce_uuid(None, uses_native_uuid=self._schema.uses_native_uuid, database_url=db_url),
            "env": env,
            "run_id": uuid_value_for_url(db_url, run_id) if run_id is not None else None,
            "order_id": uuid_value_for_url(db_url, order_id) if order_id is not None else None,
            "kis_odno": kis_odno,
            "trade_id": trade_id,
            "broker_fill_id": broker_fill_id,
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
        conflict_cols = ["env", "broker_fill_id"] if broker_fill_id else [
            "env",
            "kis_odno",
            "code",
            "side",
            "qty",
            "price",
            "filled_at",
        ]
        update_cols = {
            "raw_json": raw_json or {},
            "broker_fill_id": broker_fill_id,
        }
        insert_stmt: sa.Insert
        if self.engine.dialect.name == "postgresql":
            insert_stmt = pg_insert(self._schema.fills).values(**payload)
        else:
            insert_stmt = sqlite_insert(self._schema.fills).values(**payload)
        stmt = insert_stmt.on_conflict_do_update(
            index_elements=conflict_cols,
            set_=update_cols,
        ).returning(self._schema.fills.c.fill_id)
        with self.engine.begin() as conn:
            try:
                res = conn.execute(stmt)
                fill_id = res.scalar()
                if fill_id:
                    return str(fill_id)
            except Exception:
                pass
            existing = None
            if trade_id:
                existing = conn.execute(
                    select(self._schema.fills.c.fill_id).where(
                        and_(self._schema.fills.c.env == env, self._schema.fills.c.trade_id == trade_id)
                    )
                ).scalar()
            return str(existing or payload["fill_id"])


class LedgerEventsRepo:
    def __init__(self, engine: Engine):
        self.engine = engine
        self._schema = schema_for_engine(engine)

    def append_event(
        self,
        *,
        env: str,
        run_id: str | None,
        event_type: str,
        ts: datetime,
        code: str | None = None,
        market: str | None = None,
        sid: int | None = None,
        mode: int | None = None,
        side: str | None = None,
        qty: int | None = None,
        price: float | None = None,
        kis_odno: str | None = None,
        client_order_key: str | None = None,
        ok: bool = True,
        reasons: list[str] | None = None,
        stage: str | None = None,
        payload_json: dict | None = None,
    ) -> str:
        db_url = str(self.engine.url)
        safe_payload_json = json_safe(payload_json) if payload_json is not None else {}
        payload = {
            "ledger_event_id": _coerce_uuid(None, uses_native_uuid=self._schema.uses_native_uuid, database_url=db_url),
            "env": env,
            "run_id": uuid_value_for_url(db_url, run_id) if run_id is not None else None,
            "event_type": event_type,
            "ts": ts,
            "code": code,
            "market": market,
            "sid": sid,
            "mode": mode,
            "side": side,
            "qty": qty,
            "price": price,
            "kis_odno": kis_odno,
            "client_order_key": client_order_key,
            "ok": ok,
            "reasons": reasons or [],
            "stage": stage,
            "payload_json": safe_payload_json,
        }
        stmt = sa.insert(self._schema.ledger_events).values(**payload).returning(self._schema.ledger_events.c.ledger_event_id)
        with self.engine.begin() as conn:
            try:
                res = conn.execute(stmt)
                return str(res.scalar())
            except Exception:
                conn.execute(sa.insert(self._schema.ledger_events).values(**payload))
                return str(payload["ledger_event_id"])


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

    def get_position(self, *, env: str, strategy: str, sid: int, mode: int, code: str) -> dict | None:
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

    def list_positions_by_codes(self, *, env: str, strategy: str, codes: list[str]) -> list[dict]:
        if not codes:
            return []
        stmt = select(self._schema.positions).where(
            and_(
                self._schema.positions.c.env == env,
                self._schema.positions.c.strategy == strategy,
                self._schema.positions.c.code.in_(codes),
            )
        )
        with self.engine.begin() as conn:
            rows = conn.execute(stmt).mappings().all()
            return [dict(r) for r in rows]

    def close_positions(self, *, env: str, strategy: str, codes: list[str]) -> int:
        if not codes:
            return 0
        stmt = (
            sa.update(self._schema.positions)
            .where(
                and_(
                    self._schema.positions.c.env == env,
                    self._schema.positions.c.strategy == strategy,
                    self._schema.positions.c.code.in_(codes),
                    self._schema.positions.c.qty > 0,
                )
            )
            .values(qty=0, avg_buy_price=None, total_cost=0.0, updated_at=func.now())
        )
        with self.engine.begin() as conn:
            result = conn.execute(stmt)
            return int(result.rowcount or 0)

    def update_position_fields(
        self,
        *,
        env: str,
        strategy: str,
        sid: int,
        mode: int,
        code: str,
        fields: dict,
    ) -> None:
        if not fields:
            return
        stmt = (
            sa.update(self._schema.positions)
            .where(
                and_(
                    self._schema.positions.c.env == env,
                    self._schema.positions.c.strategy == strategy,
                    self._schema.positions.c.sid == sid,
                    self._schema.positions.c.mode == mode,
                    self._schema.positions.c.code == code,
                )
            )
            .values(**fields, updated_at=func.now())
        )
        with self.engine.begin() as conn:
            conn.execute(stmt)

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
                        status="OPEN",
                        last_reconciled_at=func.now(),
                    )
                )
                count += 1
        return count

    def restore_missing_from_holdings(
        self,
        *,
        env: str,
        strategy: str,
        sid: int,
        mode: int,
        holdings: Iterable[dict],
    ) -> int:
        restored = 0
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
                        status="OPEN",
                        last_reconciled_at=func.now(),
                    )
                )
                restored += 1
        return restored


class ReconcileLogRepo:
    def __init__(self, engine: Engine):
        self.engine = engine
        self._schema = schema_for_engine(engine)

    def append_log(
        self,
        *,
        env: str,
        strategy: str,
        tick_ts: datetime,
        action: str,
        details_json: dict | None,
    ) -> None:
        payload = {
            "env": env,
            "strategy": strategy,
            "tick_ts": tick_ts,
            "action": action,
            "details_json": details_json or {},
        }
        stmt = sa.insert(self._schema.reconcile_log).values(**payload)
        with self.engine.begin() as conn:
            conn.execute(stmt)
