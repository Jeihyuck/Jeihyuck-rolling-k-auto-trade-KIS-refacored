from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Any
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy import Engine

from . import config


def _uuid_type_for_url(database_url: str) -> sa.types.TypeEngine:
    # Always use a string/text backing type for UUIDs to avoid casting issues across engines.
    return sa.String()


def uuid_value_for_url(database_url: str, value: Any | None = None) -> Any:
    try:
        if value is None:
            return str(uuid4())
        if isinstance(value, UUID):
            return str(value)
        return str(UUID(str(value)))
    except Exception:
        return str(uuid4())


@dataclass(frozen=True)
class SchemaTables:
    database_url: str
    metadata: sa.MetaData
    runs: sa.Table
    universe: sa.Table
    universe_members: sa.Table
    orders: sa.Table
    fills: sa.Table
    positions: sa.Table
    ledger_events: sa.Table
    reconcile_log: sa.Table
    uses_native_uuid: bool


def _build_schema(database_url: str) -> SchemaTables:
    metadata = sa.MetaData()
    uuid_type = _uuid_type_for_url(database_url)
    uses_native_uuid = False

    def uuid_col(name: str, **kwargs: Any) -> sa.Column:
        return sa.Column(name, uuid_type, default=lambda: str(uuid4()), **kwargs)

    runs = sa.Table(
        "runs",
        metadata,
        uuid_col("run_id", primary_key=True),
        sa.Column("env", sa.String, nullable=False),
        sa.Column("strategy", sa.String, nullable=False),
        sa.Column("run_window", sa.String),
        sa.Column("phase", sa.String),
        sa.Column("event_name", sa.String),
        sa.Column("dry_run", sa.Boolean, nullable=False, default=False),
        sa.Column("git_sha", sa.String),
        sa.Column("workflow", sa.String),
        sa.Column("workflow_run_id", sa.String),
        sa.Column("workflow_attempt", sa.Integer),
        sa.Column("config_json", sa.JSON, nullable=False, default=dict),
        sa.Column("status", sa.String, nullable=False, default="STARTED"),
        sa.Column("started_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("notes", sa.Text),
    )

    universe = sa.Table(
        "universe",
        metadata,
        uuid_col("universe_id", primary_key=True),
        sa.Column("env", sa.String, nullable=False),
        sa.Column("strategy", sa.String, nullable=False),
        sa.Column("as_of_date", sa.String, nullable=False),
        sa.Column("source", sa.String, nullable=False),
        sa.Column("params_json", sa.JSON, nullable=False, default=dict),
        sa.Column("payload_json", sa.JSON, nullable=False, default=dict),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    universe_members = sa.Table(
        "universe_members",
        metadata,
        uuid_col("universe_member_id", primary_key=True),
        sa.Column("universe_id", uuid_type, sa.ForeignKey("universe.universe_id"), nullable=False),
        sa.Column("code", sa.String, nullable=False),
        sa.Column("market", sa.String),
        sa.Column("weight", sa.Float),
        sa.Column("rank", sa.Integer),
        sa.Column("meta_json", sa.JSON, nullable=False, default=dict),
    )

    orders = sa.Table(
        "orders",
        metadata,
        uuid_col("order_id", primary_key=True),
        sa.Column("env", sa.String, nullable=False),
        sa.Column("run_id", uuid_type, sa.ForeignKey("runs.run_id")),
        sa.Column("strategy", sa.String, nullable=False),
        sa.Column("sid", sa.Integer, nullable=False),
        sa.Column("mode", sa.Integer, nullable=False),
        sa.Column("code", sa.String, nullable=False),
        sa.Column("market", sa.String),
        sa.Column("side", sa.String, nullable=False),
        sa.Column("ord_type", sa.String, nullable=False),
        sa.Column("qty", sa.Integer, nullable=False),
        sa.Column("limit_price", sa.Float),
        sa.Column("stage", sa.String),
        sa.Column("client_order_key", sa.String, nullable=False),
        sa.Column("status", sa.String, nullable=False, default="INTENT"),
        sa.Column("kis_odno", sa.String),
        sa.Column("broker_order_id", sa.String),
        sa.Column("request_json", sa.JSON, nullable=False, default=dict),
        sa.Column("response_json", sa.JSON),
        sa.Column("submitted_at", sa.DateTime(timezone=True)),
        sa.Column("acked_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now()),
        sa.UniqueConstraint("env", "client_order_key", name="uq_orders_env_client_order_key"),
        sa.UniqueConstraint("env", "broker_order_id", name="uq_orders_env_broker_order_id"),
    )

    fills = sa.Table(
        "fills",
        metadata,
        uuid_col("fill_id", primary_key=True),
        sa.Column("env", sa.String, nullable=False),
        sa.Column("run_id", uuid_type, sa.ForeignKey("runs.run_id")),
        sa.Column("order_id", uuid_type, sa.ForeignKey("orders.order_id")),
        sa.Column("kis_odno", sa.String),
        sa.Column("trade_id", sa.String),
        sa.Column("broker_fill_id", sa.String),
        sa.Column("code", sa.String, nullable=False),
        sa.Column("market", sa.String),
        sa.Column("side", sa.String, nullable=False),
        sa.Column("qty", sa.Integer, nullable=False),
        sa.Column("price", sa.Float, nullable=False),
        sa.Column("fee", sa.Float, nullable=False, default=0.0),
        sa.Column("tax", sa.Float, nullable=False, default=0.0),
        sa.Column("filled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw_json", sa.JSON, nullable=False, default=dict),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("env", "trade_id", name="uq_fills_env_trade_id"),
        sa.UniqueConstraint("env", "broker_fill_id", name="uq_fills_env_broker_fill_id"),
        sa.UniqueConstraint(
            "env",
            "kis_odno",
            "code",
            "side",
            "qty",
            "price",
            "filled_at",
            name="uq_fills_fallback",
        ),
    )

    positions = sa.Table(
        "positions",
        metadata,
        uuid_col("position_id", primary_key=True),
        sa.Column("env", sa.String, nullable=False),
        sa.Column("strategy", sa.String, nullable=False),
        sa.Column("sid", sa.Integer, nullable=False),
        sa.Column("mode", sa.Integer, nullable=False),
        sa.Column("code", sa.String, nullable=False),
        sa.Column("market", sa.String),
        sa.Column("qty", sa.Integer, nullable=False, default=0),
        sa.Column("avg_buy_price", sa.Float),
        sa.Column("total_cost", sa.Float, nullable=False, default=0.0),
        sa.Column("realized_pnl", sa.Float, nullable=False, default=0.0),
        sa.Column("entry_ts", sa.Text, nullable=True),
        sa.Column("initial_stop", sa.Float, nullable=True),
        sa.Column("stop_price", sa.Float, nullable=True),
        sa.Column("max_price", sa.Float, nullable=True),
        sa.Column("pyramid_level", sa.Integer, nullable=False, server_default="0"),
        sa.Column("pivot", sa.Float, nullable=True),
        sa.Column("last_add_price", sa.Float, nullable=True),
        sa.Column("last_stop_update_ts", sa.Text, nullable=True),
        sa.Column("partial_exit_level", sa.Integer, nullable=False, server_default="0"),
        sa.Column("base_id", sa.Text, nullable=True),
        sa.Column("setup_id", sa.Text, nullable=True),
        sa.Column("tight_low", sa.Float, nullable=True),
        sa.Column("base_high", sa.Float, nullable=True),
        sa.Column("entry_price", sa.Float, nullable=True),
        sa.Column("r_value", sa.Float, nullable=True),
        sa.Column("tp1_done", sa.Integer, nullable=False, server_default="0"),
        sa.Column("tp2_done", sa.Integer, nullable=False, server_default="0"),
        sa.Column("trail_mode", sa.Text, nullable=True),
        sa.Column("last_trail_stop", sa.Float, nullable=True),
        sa.Column("cooldown_until", sa.Text, nullable=True),
        sa.Column("regime_at_entry", sa.Text, nullable=True),
        sa.Column("risk_mult_at_entry", sa.Float, nullable=True),
        sa.Column("last_trade_at", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String, nullable=True),
        sa.Column("closed_reason", sa.String, nullable=True),
        sa.Column("closed_ts", sa.DateTime(timezone=True)),
        sa.Column("last_reconciled_at", sa.DateTime(timezone=True)),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now()),
        sa.UniqueConstraint("env", "strategy", "sid", "mode", "code", name="uq_positions_identity"),
    )

    ledger_events = sa.Table(
        "ledger_events",
        metadata,
        uuid_col("ledger_event_id", primary_key=True),
        sa.Column("env", sa.String, nullable=False),
        sa.Column("run_id", uuid_type, sa.ForeignKey("runs.run_id")),
        sa.Column("event_type", sa.String, nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("code", sa.String),
        sa.Column("market", sa.String),
        sa.Column("sid", sa.Integer),
        sa.Column("mode", sa.Integer),
        sa.Column("side", sa.String),
        sa.Column("qty", sa.Integer),
        sa.Column("price", sa.Float),
        sa.Column("kis_odno", sa.String),
        sa.Column("client_order_key", sa.String),
        sa.Column("ok", sa.Boolean, nullable=False, default=True),
        sa.Column("reasons", sa.JSON, nullable=False, default=list),
        sa.Column("stage", sa.String),
        sa.Column("payload_json", sa.JSON, nullable=False, default=dict),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    reconcile_log = sa.Table(
        "reconcile_log",
        metadata,
        sa.Column("env", sa.String, nullable=False),
        sa.Column("strategy", sa.String, nullable=False),
        sa.Column("tick_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("action", sa.String, nullable=False),
        sa.Column("details_json", sa.JSON, nullable=False, default=dict),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    return SchemaTables(
        database_url=database_url,
        metadata=metadata,
        runs=runs,
        universe=universe,
        universe_members=universe_members,
        orders=orders,
        fills=fills,
        positions=positions,
        ledger_events=ledger_events,
        reconcile_log=reconcile_log,
        uses_native_uuid=uses_native_uuid,
    )


_SCHEMA_CACHE: dict[str, SchemaTables] = {}


@lru_cache(maxsize=None)
def schema_for_url(database_url: str) -> SchemaTables:
    normalized = str(database_url)
    if normalized not in _SCHEMA_CACHE:
        _SCHEMA_CACHE[normalized] = _build_schema(normalized)
    return _SCHEMA_CACHE[normalized]


def schema_for_engine(engine: Engine) -> SchemaTables:
    return schema_for_url(str(engine.url))


def metadata_for_url(database_url: str) -> sa.MetaData:
    return schema_for_url(database_url).metadata


DEFAULT_DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./dev.db")
DEFAULT_SCHEMA = schema_for_url(DEFAULT_DATABASE_URL)

METADATA = DEFAULT_SCHEMA.metadata
RUNS = DEFAULT_SCHEMA.runs
UNIVERSE = DEFAULT_SCHEMA.universe
UNIVERSE_MEMBERS = DEFAULT_SCHEMA.universe_members
ORDERS = DEFAULT_SCHEMA.orders
FILLS = DEFAULT_SCHEMA.fills
POSITIONS = DEFAULT_SCHEMA.positions
LEDGER_EVENTS = DEFAULT_SCHEMA.ledger_events
RECONCILE_LOG = DEFAULT_SCHEMA.reconcile_log
