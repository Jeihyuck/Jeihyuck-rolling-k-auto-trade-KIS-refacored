from __future__ import annotations

from datetime import date, datetime, timedelta
import logging, os
import time
from typing import Any, Dict, Iterable, List, Optional
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.exc import OperationalError, StatementError
from sqlalchemy import Engine, and_, func, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .schema import (
    FILLS,
    LEDGER_EVENTS,
    ORDERS,
    POSITIONS,
    PRICE_DAILY,
    RUNS,
    UNIVERSE_MEMBERS,
    UNIVERSE_CURRENT,
    UNIVERSE_RUNS,
    SchemaTables,
    schema_for_engine,
    uuid_value_for_url,
)
from trader.time_utils import now_kst
from trader.db.json_safe import json_sanitize

logger = logging.getLogger(__name__)


def _coerce_uuid(value: Any, *, uses_native_uuid: bool, database_url: str) -> Any:
    return uuid_value_for_url(database_url, value if isinstance(value, UUID) else value)


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def ensure_run(
    conn: sa.Connection,
    schema: SchemaTables,
    *,
    run_id: str,
    env: str,
    run_window: str | None,
    strategy: str,
    ts: datetime,
    database_url: str,
) -> None:
    if not run_id or not strategy:
        return
    values = {
        "run_id": uuid_value_for_url(database_url, run_id),
        "env": env,
        "strategy": strategy,
        "run_window": run_window,
        "phase": None,
        "event_name": "ledger_event",
        "dry_run": False,
        "config_json": {},
        "started_at": ts,
    }
    if conn.dialect.name == "postgresql":
        insert_stmt = pg_insert(schema.runs).values(**values).on_conflict_do_nothing(index_elements=["run_id"])
    else:
        insert_stmt = sa.insert(schema.runs).values(**values)
    try:
        conn.execute(insert_stmt)
    except Exception:
        return


def execute_with_retry(conn, stmt, payload=None, retries: int = 5, base_sleep: float = 0.2):
    last_exc: OperationalError | None = None
    for attempt in range(retries):
        try:
            if payload is None:
                return conn.execute(stmt)
            return conn.execute(stmt, payload)
        except OperationalError as exc:
            last_exc = exc
            msg = str(exc).lower()
            if "database is locked" in msg or "busy" in msg:
                time.sleep(base_sleep * (2 ** attempt))
                continue
            raise
    if last_exc:
        raise last_exc
    raise OperationalError("database is locked", params=None, orig=None)


def _collect_json_type_paths(value: Any, *, path: str = "request_json", limit: int = 50) -> list[str]:
    results: list[str] = []

    def _walk(node: Any, prefix: str) -> None:
        if len(results) >= limit:
            return
        if isinstance(node, dict):
            for key, val in node.items():
                _walk(val, f"{prefix}.{key}")
            return
        if isinstance(node, (list, tuple, set)):
            for idx, val in enumerate(node):
                _walk(val, f"{prefix}[{idx}]")
            return
        results.append(f"{prefix}:{type(node).__name__}")

    _walk(value, path)
    return results


def _is_in_failed_transaction_error(exc: Exception) -> bool:
    message = f"{exc.__class__.__name__}:{exc}"
    if "InFailedSqlTransaction" in message:
        return True
    orig = getattr(exc, "orig", None)
    if orig and "InFailedSqlTransaction" in f"{orig.__class__.__name__}:{orig}":
        return True
    try:
        from psycopg.errors import InFailedSqlTransaction as PsycopgInFailedSqlTransaction
    except Exception:
        PsycopgInFailedSqlTransaction = None
    if PsycopgInFailedSqlTransaction and isinstance(orig or exc, PsycopgInFailedSqlTransaction):
        return True
    return False


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
        # Handle dry_run type compatibility: try bool first, fallback to int if column is INTEGER
        dry_run_value = dry_run
        values = {
            "run_id": _coerce_uuid(None, uses_native_uuid=self._schema.uses_native_uuid, database_url=str(self.engine.url)),
            "env": env,
            "strategy": strategy,
            "run_window": run_window,
            "phase": phase,
            "event_name": event_name,
            "dry_run": dry_run_value,
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
                except OperationalError:
                    raise
                except Exception as e:
                    # If type mismatch, try with int conversion
                    if "dry_run" in str(e) and isinstance(dry_run_value, bool):
                        dry_run_value = int(dry_run)
                        values["dry_run"] = dry_run_value
                        stmt = sa.insert(self._schema.runs).values(**values)
                        conn.rollback()  # Rollback failed transaction
                        res = conn.execute(stmt.returning(self._schema.runs.c.run_id))
                        run_id = res.scalar() or run_id
                        return str(run_id)
                    else:
                        raise
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

    def get_current_universe_snapshot(self, env: str, strategy: str) -> dict | None:
        strategy_key = self._strategy_key(env, strategy)
        members_count_sq = (
            select(
                self._schema.universe_members.c.run_id,
                func.count().label("members_count"),
            )
            .group_by(self._schema.universe_members.c.run_id)
            .subquery()
        )
        stmt = (
            select(
                self._schema.universe_current.c.run_id,
                self._schema.universe_runs.c.as_of,
                self._schema.universe_runs.c.provider,
                self._schema.universe_runs.c.status,
                members_count_sq.c.members_count,
            )
            .select_from(
                self._schema.universe_current.join(
                    self._schema.universe_runs,
                    self._schema.universe_current.c.run_id == self._schema.universe_runs.c.run_id,
                ).outerjoin(
                    members_count_sq,
                    members_count_sq.c.run_id == self._schema.universe_runs.c.run_id,
                )
            )
            .where(self._schema.universe_current.c.strategy == strategy_key)
        )
        with self.engine.begin() as conn:
            row = conn.execute(stmt).mappings().first()
        if not row:
            return None
        run_id = str(row["run_id"])
        members = self._fetch_members_for_run(run_id, env=env, strategy=strategy)
        return {
            "run_id": run_id,
            "as_of": row.get("as_of"),
            "provider": row.get("provider"),
            "status": row.get("status"),
            "members_count": len(members),
            "members": members,
            "sample_codes": [m.get("code") for m in members[:5]],
        }

    def get_latest_universe_members(
        self,
        *,
        env: str,
        strategy: str,
        as_of_date: str,
    ) -> list[dict]:
        """Get the most recent universe members for as_of <= as_of_date."""
        strategy_key = self._strategy_key(env, strategy)
        as_of_d = _as_date(as_of_date)
        stmt = (
            select(self._schema.universe_runs.c.run_id)
            .where(
                and_(
                    self._schema.universe_runs.c.strategy == strategy_key,
                    self._schema.universe_runs.c.as_of <= as_of_d,
                )
            )
            .order_by(self._schema.universe_runs.c.as_of.desc())
            .limit(1)
        )
        with self.engine.begin() as conn:
            run_id = conn.execute(stmt).scalar()
        if not run_id:
            return []
        return self._fetch_members_for_run(str(run_id), env=env, strategy=strategy)
        strategy_key = self._strategy_key(env, strategy)
        as_of_d = _as_date(as_of_date)
        stmt = (
            select(
                self._schema.universe_runs.c.run_id,
                self._schema.universe_runs.c.as_of,
                self._schema.universe_runs.c.provider,
                self._schema.universe_runs.c.status,
            )
            .where(
                and_(
                    self._schema.universe_runs.c.strategy == strategy_key,
                    self._schema.universe_runs.c.as_of <= as_of_d,
                    or_(
                        self._schema.universe_runs.c.status == "SUCCESS",
                        self._schema.universe_runs.c.status.is_(None),
                    ),
                )
            )
            .order_by(self._schema.universe_runs.c.as_of.desc(), self._schema.universe_runs.c.created_ts.desc())
            .limit(limit)
        )
        with self.engine.begin() as conn:
            rows = conn.execute(stmt).mappings().all()
        for row in rows:
            run_id = str(row["run_id"])
            members = self._fetch_members_for_run(run_id, env=env, strategy=strategy)
            if members:
                return {
                    "run_id": run_id,
                    "as_of": row.get("as_of"),
                    "provider": row.get("provider"),
                    "status": row.get("status"),
                    "members_count": len(members),
                    "members": members,
                    "sample_codes": [m.get("code") for m in members[:5]],
                }
        return None

    def get_universe_members(self, *, env: str, strategy: str, as_of_date: str) -> list[dict]:
        strategy_key = self._strategy_key(env, strategy)
        as_of_d = _as_date(as_of_date)
        stmt = (
            select(self._schema.universe_runs.c.run_id)
            .where(
                and_(
                    self._schema.universe_runs.c.strategy == strategy_key,
                    self._schema.universe_runs.c.as_of == as_of_d,
                )
            )
            .order_by(self._schema.universe_runs.c.created_ts.desc())
        )
        with self.engine.begin() as conn:
            run_id = conn.execute(stmt).scalar()
        if not run_id:
            logger.info("[UNIVERSE][DB][LOAD] as_of=%s members=0 (no run)", as_of_date)
            # [PATCH] Fallback to latest non-empty as_of
            fallback_stmt = (
                select(self._schema.universe_runs.c.as_of, self._schema.universe_runs.c.run_id)
                .where(self._schema.universe_runs.c.strategy == strategy_key)
                .order_by(self._schema.universe_runs.c.as_of.desc())
            )
            with self.engine.begin() as conn:
                result = conn.execute(fallback_stmt)
                candidates = result.mappings().all()
            for row in candidates:
                fallback_run_id = str(row["run_id"])
                fallback_members = self._fetch_members_for_run(fallback_run_id, env=env, strategy=strategy)
                if fallback_members:
                    logger.info("[UNIVERSE][DB][LOAD][FALLBACK] as_of=%s -> %s members=%s", as_of_date, row["as_of"], len(fallback_members))
                    return fallback_members
            logger.info("[UNIVERSE][DB][LOAD] as_of=%s members=0 (no fallback)", as_of_date)
            return []
        members = self._fetch_members_for_run(str(run_id), env=env, strategy=strategy)
        if not members:
            logger.info("[UNIVERSE][DB][LOAD] as_of=%s members=0", as_of_date)
            # [PATCH] Fallback to latest non-empty as_of
            fallback_stmt = (
                select(self._schema.universe_runs.c.as_of, self._schema.universe_runs.c.run_id)
                .where(
                    and_(
                        self._schema.universe_runs.c.strategy == strategy_key,
                        self._schema.universe_runs.c.as_of < as_of_d,
                    )
                )
                .order_by(self._schema.universe_runs.c.as_of.desc())
            )
            with self.engine.begin() as conn:
                result = conn.execute(fallback_stmt)
                candidates = result.mappings().all()
            for row in candidates:
                fallback_run_id = str(row["run_id"])
                fallback_members = self._fetch_members_for_run(fallback_run_id, env=env, strategy=strategy)
                if fallback_members:
                    logger.info("[UNIVERSE][DB][LOAD][FALLBACK] as_of=%s -> %s members=%s", as_of_date, row["as_of"], len(fallback_members))
                    return fallback_members
            logger.info("[UNIVERSE][DB][LOAD] as_of=%s members=0 (no fallback)", as_of_date)
        else:
            logger.info("[UNIVERSE][DB][LOAD] as_of=%s members=%s", as_of_date, len(members))
        return members

    def save_universe_run_and_members(
        self,
        *,
        env: str,
        strategy: str,
        as_of: str,
        members: list[dict],
    ) -> None:
        """Upsert universe run and members in a transaction."""
        self.store_universe_snapshot(
            env=env,
            strategy=strategy,
            as_of_date=as_of,
            provider="auto_build",
            members=members,
            reason="auto_build_from_ensure",
        )
        members_list = list(members)
        as_of_d = _as_date(as_of_date)
        db_url = str(self.engine.url)
        run_id = _coerce_uuid(None, uses_native_uuid=self._schema.uses_native_uuid, database_url=db_url)
        strategy_key = self._strategy_key(env, strategy)
        members_count = len(members_list)
        try:
            with self.engine.begin() as conn:
                existing = conn.execute(
                    select(self._schema.universe_runs.c.run_id).where(
                        and_(
                            self._schema.universe_runs.c.strategy == strategy_key,
                            self._schema.universe_runs.c.provider == provider,
                            self._schema.universe_runs.c.as_of == as_of_d,
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
                        as_of=as_of_d,
                        created_ts=now_kst().isoformat(),
                        status="OK",
                        members_count=members_count,
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
                if members_count > 0:
                    if self.engine.dialect.name == "postgresql":
                        insert_stmt = pg_insert(self._schema.universe_current).values(
                            strategy=strategy_key,
                            run_id=uuid_value_for_url(db_url, run_id),
                            updated_ts=now_kst().isoformat(),
                        )
                    else:
                        insert_stmt = sa.insert(self._schema.universe_current).values(
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
                conn.execute(
                    sa.update(self._schema.universe_runs)
                    .where(self._schema.universe_runs.c.run_id == run_id)
                    .values(status="SUCCESS", error_reason=None, members_count=members_count)
                )
            return str(run_id)
        except Exception:
            logger.exception("[UNIVERSE][STORE][FAIL] env=%s strategy=%s as_of=%s", env, strategy, as_of_date)
            raise

    def start_universe_run(
        self,
        *,
        env: str,
        strategy: str,
        as_of_date: str,
        provider: str,
    ) -> str:
        run_id = _coerce_uuid(None, uses_native_uuid=self._schema.uses_native_uuid, database_url=str(self.engine.url))
        strategy_key = self._strategy_key(env, strategy)
        as_of_d = _as_date(as_of_date)
        try:
            with self.engine.begin() as conn:
                existing = conn.execute(
                    select(self._schema.universe_runs.c.run_id).where(
                        and_(
                            self._schema.universe_runs.c.strategy == strategy_key,
                            self._schema.universe_runs.c.provider == provider,
                            self._schema.universe_runs.c.as_of == as_of_d,
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
                        as_of=as_of_d,
                        created_ts=now_kst().isoformat(),
                        status="RUNNING",
                        members_count=0,
                    )
                )
            return str(run_id)
        except Exception:
            logger.exception("[UNIVERSE][RUN][START_FAIL] env=%s strategy=%s as_of=%s", env, strategy, as_of_date)
            raise
        run_id = _coerce_uuid(None, uses_native_uuid=self._schema.uses_native_uuid, database_url=str(self.engine.url))
        strategy_key = self._strategy_key(env, strategy)
        as_of_d = _as_date(as_of_date)
        try:
            with self.engine.begin() as conn:
                existing = conn.execute(
                    select(self._schema.universe_runs.c.run_id).where(
                        and_(
                            self._schema.universe_runs.c.strategy == strategy_key,
                            self._schema.universe_runs.c.provider == provider,
                            self._schema.universe_runs.c.as_of == as_of_d,
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
                        as_of=as_of_d,
                        created_ts=now_kst().isoformat(),
                        status="FAIL",
                        error_reason=error_reason,
                        members_count=members_count,
                    )
                )
            return str(run_id)
        except Exception:
            logger.exception("[UNIVERSE][RUN][FAIL_LOG] env=%s strategy=%s as_of=%s", env, strategy, as_of_date)
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
        original_request_json = request_json
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
        payload = dict(payload)
        if "request_json" in payload:
            payload["request_json"] = json_sanitize(payload["request_json"])
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
                res = execute_with_retry(conn, stmt)
                return str(res.scalar()), True
            except StatementError as exc:
                if original_request_json is not None:
                    type_paths = _collect_json_type_paths(original_request_json)
                    logger.warning(
                        "[DB][ORDER_INTENT][REQUEST_JSON_TYPES] env=%s key=%s types=%s",
                        env,
                        client_order_key,
                        type_paths,
                    )
                raise exc
            except Exception:
                execute_with_retry(conn, sa.insert(self._schema.orders).values(**payload))
                return str(payload["order_id"]), True

    def mark_submitted(self, env: str, client_order_key: str, kis_odno: str | None, response_json: dict | None) -> None:
        safe_response_json = json_sanitize(response_json or {})
        with self.engine.begin() as conn:
            conn.execute(
                sa.update(self._schema.orders)
                .where(and_(self._schema.orders.c.env == env, self._schema.orders.c.client_order_key == client_order_key))
                .values(
                    status="SUBMITTED",
                    kis_odno=kis_odno,
                    broker_order_id=kis_odno or client_order_key,
                    response_json=safe_response_json,
                    submitted_at=func.now(),
                    updated_at=func.now(),
                )
            )

    def mark_acked(self, env: str, kis_odno: str | None, response_json: dict | None) -> None:
        safe_response_json = json_sanitize(response_json or {})
        with self.engine.begin() as conn:
            conn.execute(
                sa.update(self._schema.orders)
                .where(and_(self._schema.orders.c.env == env, self._schema.orders.c.kis_odno == kis_odno))
                .values(
                    status="ACKED",
                    response_json=safe_response_json,
                    broker_order_id=kis_odno,
                    acked_at=func.now(),
                    updated_at=func.now(),
                )
            )

    def mark_error(self, env: str, client_order_key: str, error_payload: dict | None) -> None:
        safe_error_payload = json_sanitize(error_payload or {})
        with self.engine.begin() as conn:
            conn.execute(
                sa.update(self._schema.orders)
                .where(and_(self._schema.orders.c.env == env, self._schema.orders.c.client_order_key == client_order_key))
                .values(status="ERROR", response_json=safe_error_payload, updated_at=func.now()),
            )

    def mark_cancelled(self, env: str, client_order_key: str, response_json: dict | None = None) -> None:
        safe_response_json = json_sanitize(response_json or {})
        with self.engine.begin() as conn:
            conn.execute(
                sa.update(self._schema.orders)
                .where(and_(self._schema.orders.c.env == env, self._schema.orders.c.client_order_key == client_order_key))
                .values(status="CANCELLED", response_json=safe_response_json, updated_at=func.now()),
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
        start_bp = sa.bindparam("created_at_1", start, type_=sa.DateTime(timezone=True))
        end_bp = sa.bindparam("created_at_2", end, type_=sa.DateTime(timezone=True))
        conditions = [self._schema.orders.c.env == sa.bindparam("env_1", env), self._schema.orders.c.created_at >= start_bp, self._schema.orders.c.created_at < end_bp]
        if side:
            conditions.append(self._schema.orders.c.side == sa.bindparam("side_1", side))
        if code:
            conditions.append(self._schema.orders.c.code == sa.bindparam("code_1", code))
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
        safe_request_json = json_sanitize(request_json or {})
        safe_response_json = json_sanitize(response_json or {})
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
            "request_json": safe_request_json,
            "response_json": safe_response_json,
            "submitted_at": submitted_at,
            "acked_at": acked_at,
        }
        conflict_cols = ["env", "broker_order_id"] if broker_order_id else ["env", "client_order_key"]
        update_cols = {
            "status": status,
            "kis_odno": kis_odno,
            "broker_order_id": broker_order_id,
            "response_json": safe_response_json,
            "request_json": safe_request_json,
            "submitted_at": submitted_at,
            "acked_at": acked_at,
            "updated_at": func.now(),
        }
        insert_stmt: sa.Insert
        if self.engine.dialect.name == "postgresql":
            insert_stmt = pg_insert(self._schema.orders).values(**payload)
        else:
            insert_stmt = sa.insert(self._schema.orders).values(**payload)
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
        safe_raw_json = json_sanitize(raw_json or {})
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
            "raw_json": safe_raw_json,
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
            "raw_json": safe_raw_json,
            "broker_fill_id": broker_fill_id,
        }
        insert_stmt: sa.Insert
        if self.engine.dialect.name == "postgresql":
            insert_stmt = pg_insert(self._schema.fills).values(**payload)
        else:
            insert_stmt = sa.insert(self._schema.fills).values(**payload)
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
        strategy: str | None = None,
        run_window: str | None = None,
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
    ) -> str | None:
        db_url = str(self.engine.url)
        db_store_required = os.getenv("DB_STORE_REQUIRED", "0") not in {"0", "false", "FALSE"}
        max_attempts = int(os.getenv("DB_WRITE_MAX_ATTEMPTS", "2"))
        base_backoff = float(os.getenv("DB_WRITE_BACKOFF_SEC", "0.2"))
        safe_payload_json = json_sanitize(payload_json) if payload_json is not None else {}
        safe_reasons = json_sanitize(reasons or [])
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
            "reasons": safe_reasons,
            "stage": stage,
            "payload_json": safe_payload_json,
        }
        stmt = sa.insert(self._schema.ledger_events).values(**payload).returning(self._schema.ledger_events.c.ledger_event_id)
        last_exc: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                with self.engine.begin() as conn:
                    logger.info(
                        "[DB][LEDGER_EVENT][APPEND] attempt=%s env=%s run_id=%s strategy=%s event_type=%s",
                        attempt,
                        env,
                        run_id,
                        strategy,
                        event_type,
                    )
                    if run_id is not None and strategy:
                        ensure_run(
                            conn,
                            self._schema,
                            run_id=run_id,
                            env=env,
                            run_window=run_window,
                            strategy=strategy,
                            ts=ts,
                            database_url=db_url,
                        )
                    res = conn.execute(stmt)
                    return str(res.scalar())
            except Exception as exc:
                last_exc = exc
                if attempt == 1:
                    logger.exception(
                        "[DB][LEDGER_EVENT][FAIL-FIRST] attempt=%s env=%s run_id=%s strategy=%s event_type=%s sql=%s payload=%s",
                        attempt,
                        env,
                        run_id,
                        strategy,
                        event_type,
                        stmt,
                        payload,
                    )
                if _is_in_failed_transaction_error(exc):
                    if attempt > 1:
                        logger.warning(
                            "[DB][LEDGER_EVENT][RETRY-IFT] attempt=%s err_type=%s err=%s",
                            attempt,
                            type(exc).__name__,
                            exc,
                        )
                        logger.info(
                            "[DB][LEDGER_EVENT][RETRY] reason=in_failed_transaction attempt=%s env=%s run_id=%s strategy=%s event_type=%s",
                            attempt,
                            env,
                            run_id,
                            strategy,
                            event_type,
                        )
                    try:
                        self.engine.dispose()
                    except Exception as dispose_exc:
                        logger.warning(
                            "[DB][LEDGER_EVENT][DISPOSE-FAIL] err_type=%s err=%s",
                            type(dispose_exc).__name__,
                            dispose_exc,
                        )
                elif attempt > 1:
                    logger.exception(
                        "[DB][LEDGER_EVENT][FAIL] attempt=%s env=%s run_id=%s strategy=%s event_type=%s err=%s sql=%s payload=%s",
                        attempt,
                        env,
                        run_id,
                        strategy,
                        event_type,
                        exc,
                        stmt,
                        payload,
                    )
                if attempt < max_attempts:
                    time.sleep(base_backoff * (2 ** (attempt - 1)))
        if db_store_required and last_exc is not None:
            raise last_exc
        if last_exc is not None:
            logger.warning(
                "[DB][LEDGER_EVENT][LAST_ERROR] err_type=%s err=%s",
                type(last_exc).__name__,
                last_exc,
            )
        logger.warning(
            "[DB][LEDGER_EVENT][SKIP] env=%s run_id=%s strategy=%s event_type=%s required=%s attempts=%s",
            env,
            run_id,
            strategy,
            event_type,
            int(db_store_required),
            max_attempts,
        )
        return None


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
        stmt = sa.insert(self._schema.reconcile_log).values(**payload)
        with self.engine.begin() as conn:
            conn.execute(stmt)


def load_price_daily(engine: Engine, code: str, start_date: date, end_date: date) -> List[Dict[str, Any]]:
    schema = schema_for_engine(engine)
    stmt = sa.select(schema.price_daily).where(
        and_(
            schema.price_daily.c.code == code,
            schema.price_daily.c.date >= start_date,
            schema.price_daily.c.date <= end_date,
        )
    ).order_by(schema.price_daily.c.date)
    with engine.connect() as conn:
        result = conn.execute(stmt)
        rows = result.fetchall()
        return [
            {
                "date": row.date.strftime("%Y%m%d"),
                "open": float(row.open) if row.open else None,
                "high": float(row.high) if row.high else None,
                "low": float(row.low) if row.low else None,
                "close": float(row.close) if row.close else None,
                "volume": float(row.volume) if row.volume else None,
                "value": float(row.value) if row.value else None,
            }
            for row in rows
        ]
