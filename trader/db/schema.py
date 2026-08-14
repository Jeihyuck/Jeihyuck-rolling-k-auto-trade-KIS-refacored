from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Any
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import Engine



def _uuid_type_for_url(database_url: str) -> sa.types.TypeEngine:
    # Always use a text backing type for IDs to match the migrated Postgres schema.
    return sa.Text()


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
    universe_runs: sa.Table
    universe_members: sa.Table
    universe_current: sa.Table
    orders: sa.Table
    fills: sa.Table
    positions: sa.Table
    portfolio_epochs: sa.Table
    ledger_events: sa.Table
    reconcile_log: sa.Table
    price_daily: sa.Table
    pb1_watchlist: sa.Table
    job_checkpoints: sa.Table
    derived_minervini: sa.Table
    derived_flow: sa.Table
    uses_native_uuid: bool


def _build_schema(database_url: str) -> SchemaTables:
    metadata = sa.MetaData()
    uuid_type = _uuid_type_for_url(database_url)
    uses_native_uuid = False
    jsonb_type = JSONB if database_url.startswith("postgres") else sa.JSON

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
        sa.Column("dry_run", sa.Boolean, nullable=False, server_default=sa.text("false")),
        sa.Column("git_sha", sa.String),
        sa.Column("workflow", sa.String),
        sa.Column("workflow_run_id", sa.String),
        sa.Column("workflow_attempt", sa.Integer),
        sa.Column("config_json", sa.JSON, nullable=False, default=dict),
        sa.Column("status", sa.String, nullable=False, default="STARTED"),
        sa.Column("started_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now()),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("aborted_reason", sa.Text),
        sa.Column("takeover_from_run_id", sa.Text, sa.ForeignKey("runs.run_id")),
        sa.Column("notes", sa.Text),
    )

    universe_runs = sa.Table(
        "universe_runs",
        metadata,
        sa.Column("strategy", sa.String, nullable=False),
        sa.Column("provider", sa.String, nullable=False),
        sa.Column("requested_as_of", sa.Date),
        sa.Column("actual_as_of", sa.Date),
        sa.Column("build_reason", sa.String),
        sa.Column("universe_name", sa.String),
        sa.Column("as_of", sa.Date, nullable=False),
        sa.Column("created_ts", sa.Text, nullable=False),
        sa.Column("status", sa.String, nullable=False, default="SUCCESS"),
        sa.Column("error_reason", sa.Text),
        sa.Column("members_count", sa.Integer),
        uuid_col("run_id", primary_key=True),
        sa.UniqueConstraint("strategy", "provider", "as_of", name="ux_universe_runs_key"),
    )

    universe_members = sa.Table(
        "universe_members",
        metadata,
        sa.Column("run_id", uuid_type, sa.ForeignKey("universe_runs.run_id"), nullable=False),
        sa.Column("stock_code", sa.String, nullable=False),
        sa.Column("name", sa.String),
        sa.Column("market", sa.String),
        sa.Column("rank", sa.Integer),
        sa.Column("market_cap", sa.Float),
        sa.Column("reason", sa.String),
        sa.PrimaryKeyConstraint("run_id", "stock_code"),
    )

    universe_current = sa.Table(
        "universe_current",
        metadata,
        sa.Column("strategy", sa.String, primary_key=True),
        sa.Column("run_id", uuid_type, sa.ForeignKey("universe_runs.run_id"), nullable=False),
        sa.Column("updated_ts", sa.Text, nullable=False),
    )

    orders = sa.Table(
        "orders",
        metadata,
        uuid_col("order_id", primary_key=True),
        uuid_col("position_cycle_id", nullable=True),
        uuid_col("portfolio_epoch_id", nullable=True),
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
        sa.Column("entry_reason", sa.String),
        sa.Column("entry_style_selected", sa.String),
        sa.Column("entry_decision_family", sa.String),
        sa.Column("entry_meta_json", jsonb_type, nullable=False, default=dict),
        sa.Column("stop_price_at_entry", sa.Float),
        sa.Column("pivot_price_at_entry", sa.Float),
        sa.Column("entry_rule_version", sa.String),
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
        uuid_col("position_cycle_id", nullable=True),
        uuid_col("portfolio_epoch_id", nullable=True),
        sa.Column("env", sa.String, nullable=False),
        sa.Column("run_id", uuid_type, sa.ForeignKey("runs.run_id")),
        sa.Column("order_id", uuid_type, sa.ForeignKey("orders.order_id")),
        sa.Column("kis_odno", sa.String),
        sa.Column("trade_id", sa.String),
        sa.Column("broker_fill_id", sa.String),
        sa.Column("entry_reason", sa.String),
        sa.Column("entry_style_selected", sa.String),
        sa.Column("entry_decision_family", sa.String),
        sa.Column("fill_meta_json", jsonb_type, nullable=False, default=dict),
        sa.Column("stop_price_at_entry", sa.Float),
        sa.Column("pivot_price_at_entry", sa.Float),
        sa.Column("entry_rule_version", sa.String),
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

    portfolio_epochs = sa.Table(
        "portfolio_epochs",
        metadata,
        uuid_col("portfolio_epoch_id", primary_key=True),
        sa.Column("env", sa.String, nullable=False),
        sa.Column("account_id", sa.String, nullable=False),
        sa.Column("sid", sa.Integer, nullable=False),
        sa.Column("mode", sa.Integer, nullable=False),
        sa.Column("strategy", sa.String, nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("ended_at", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String, nullable=False, server_default="ACTIVE"),
        sa.Column("reason", sa.String),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    sa.Index(
        "uq_portfolio_epochs_active", portfolio_epochs.c.env, portfolio_epochs.c.account_id,
        portfolio_epochs.c.sid, portfolio_epochs.c.mode, portfolio_epochs.c.strategy,
        unique=True, postgresql_where=portfolio_epochs.c.status == "ACTIVE",
        sqlite_where=portfolio_epochs.c.status == "ACTIVE",
    )

    positions = sa.Table(
        "positions",
        metadata,
        uuid_col("position_id", primary_key=True),
        uuid_col("position_cycle_id", nullable=False),
        uuid_col("portfolio_epoch_id", nullable=False),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("position_origin", sa.String, nullable=False, server_default="SYSTEM"),
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
        sa.Column("entry_reason", sa.String, nullable=True),
        sa.Column("entry_style_selected", sa.String, nullable=True),
        sa.Column("entry_thesis", sa.String, nullable=True),
        sa.Column("trade_horizon", sa.String, nullable=True),
        sa.Column("eod_action", sa.String, nullable=True),
        sa.Column("force_eod_close", sa.Boolean, nullable=False, server_default=sa.text("false")),
        sa.Column("max_trading_days", sa.Integer, nullable=True),
        sa.Column("initial_stop_price", sa.Float, nullable=True),
        sa.Column("initial_risk_r", sa.Float, nullable=True),
        sa.Column("entry_decision_family", sa.String, nullable=True),
        sa.Column("entry_rule_version", sa.String, nullable=True),
        sa.Column("entry_meta_json", jsonb_type, nullable=False, default=dict),
        sa.Column("stop_price_at_entry", sa.Float, nullable=True),
        sa.Column("pivot_price_at_entry", sa.Float, nullable=True),
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
        sa.Column("tp1_done", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("tp2_done", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("trail_mode", sa.Text, nullable=True),
        sa.Column("last_trail_stop", sa.Float, nullable=True),
        sa.Column("cooldown_until", sa.Text, nullable=True),
        sa.Column("regime_at_entry", sa.Text, nullable=True),
        sa.Column("risk_mult_at_entry", sa.Float, nullable=True),
        sa.Column("last_trade_at", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String, nullable=False, server_default="OPEN"),
        sa.Column("closed_reason", sa.String, nullable=True),
        sa.Column("closed_ts", sa.DateTime(timezone=True)),
        sa.Column("exit_policy_family", sa.String, nullable=True),
        sa.Column("entry_exit_plan_json", jsonb_type, nullable=False, default=dict),
        sa.Column("last_exit_plan_eval_json", jsonb_type, nullable=False, default=dict),
        sa.Column("policy_source", sa.String, nullable=True),
        sa.Column("policy_version", sa.String, nullable=True),
        sa.Column("last_exit_eval_json", jsonb_type, nullable=False, default=dict),
        sa.Column("position_meta", jsonb_type, nullable=False, default=dict),
        sa.Column("last_reconciled_at", sa.DateTime(timezone=True)),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now()),
        sa.UniqueConstraint("position_cycle_id", name="uq_positions_cycle_id"),
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
        sa.Column("ok", sa.Boolean, nullable=False, server_default=sa.text("true")),
        sa.Column("reasons", sa.JSON, nullable=False, default=list),
        sa.Column("stage", sa.String),
        sa.Column("payload_json", jsonb_type, nullable=False, default=dict),
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

    price_daily = sa.Table(
        "price_daily",
        metadata,
        sa.Column("market", sa.String, nullable=False),
        sa.Column("code", sa.String, nullable=False),
        sa.Column("date", sa.Date, nullable=False),
        sa.Column("open", sa.Numeric, nullable=True),
        sa.Column("high", sa.Numeric, nullable=True),
        sa.Column("low", sa.Numeric, nullable=True),
        sa.Column("close", sa.Numeric, nullable=True),
        sa.Column("volume", sa.Numeric, nullable=True),
        sa.Column("value", sa.Numeric, nullable=True),
        sa.Column("source", sa.String, nullable=False, default="KIS"),
        sa.PrimaryKeyConstraint("market", "code", "date"),
    )

    pb1_watchlist = sa.Table(
        "pb1_watchlist",
        metadata,
        sa.Column("env", sa.String, nullable=False),
        sa.Column("strategy", sa.String, nullable=False),
        sa.Column("as_of", sa.Date, nullable=False),
        sa.Column("code", sa.String, nullable=False),
        sa.Column("rank", sa.Integer, nullable=False),
        sa.Column("score", sa.Float, nullable=True),
        sa.Column("meta", sa.JSON, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("env", "strategy", "as_of", "code"),
        sa.Index("ix_pb1_watchlist_lookup", "env", "strategy", "as_of"),
    )

    derived_minervini = sa.Table(
        "derived_minervini",
        metadata,
        sa.Column("env", sa.String, nullable=False, server_default=sa.text("'practice'")),
        sa.Column("symbol", sa.String, nullable=False),
        sa.Column("as_of", sa.Date, nullable=False),
        sa.Column("close", sa.Float),
        sa.Column("ma50", sa.Float),
        sa.Column("ma150", sa.Float),
        sa.Column("ma200", sa.Float),
        sa.Column("ma200_slope", sa.Float),
        sa.Column("dollar_vol_50", sa.Float),
        sa.Column("atr", sa.Float),
        sa.Column("atr_pct", sa.Float),
        sa.Column("rs_percentile", sa.Float),
        sa.Column("rs_score", sa.Float),
        sa.Column("vcp_score", sa.Float),
        sa.Column("trend_score", sa.Float),
        sa.Column("breakout_score", sa.Float),
        sa.Column("pullback_score", sa.Float),
        sa.Column("momentum_score", sa.Float),
        sa.Column("vcp_ok", sa.Boolean),
        sa.Column("pivot", sa.Float),
        sa.Column("minervini_score", sa.Float),
        sa.Column("minervini_pass", sa.Boolean),
        sa.Column("features_json", sa.JSON, nullable=False, default=dict),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now()),
        sa.PrimaryKeyConstraint("env", "symbol", "as_of"),
        sa.Index("ix_derived_minervini_env_as_of", "env", "as_of"),
        sa.Index("ix_derived_minervini_as_of", "as_of"),
        sa.Index("ix_derived_minervini_symbol", "symbol"),
    )

    derived_flow = sa.Table(
        "derived_flow",
        metadata,
        sa.Column("env", sa.String, nullable=False),
        sa.Column("as_of", sa.Date, nullable=False),
        sa.Column("symbol", sa.String, nullable=False),
        sa.Column("flow_score", sa.Float),
        sa.Column("foreign_20_ratio", sa.Float),
        sa.Column("inst_20_ratio", sa.Float),
        sa.Column("flow_missing", sa.Boolean, nullable=False, server_default=sa.text("false")),
        sa.Column("source", sa.String),
        sa.Column("features_json", sa.JSON, nullable=False, default=dict),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now()),
        sa.PrimaryKeyConstraint("env", "as_of", "symbol"),
        sa.Index("ix_derived_flow_env_as_of", "env", "as_of"),
        sa.Index("ix_derived_flow_symbol_as_of", "symbol", "as_of"),
    )

    job_checkpoints = sa.Table(
        "job_checkpoints",
        metadata,
        sa.Column("job_key", sa.String(512), primary_key=True),
        sa.Column("updated_ts", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("payload", sa.JSON, nullable=False, default=dict),
        sa.Index("ix_job_checkpoints_updated", "updated_ts"),
    )

    return SchemaTables(
        database_url=database_url,
        metadata=metadata,
        runs=runs,
        universe_runs=universe_runs,
        universe_members=universe_members,
        universe_current=universe_current,
        orders=orders,
        fills=fills,
        positions=positions,
        portfolio_epochs=portfolio_epochs,
        ledger_events=ledger_events,
        reconcile_log=reconcile_log,
        price_daily=price_daily,
        pb1_watchlist=pb1_watchlist,
        job_checkpoints=job_checkpoints,
        derived_minervini=derived_minervini,
        derived_flow=derived_flow,
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


DEFAULT_DATABASE_URL = os.getenv("PBCORE_DB_URL", "postgresql+psycopg://")
DEFAULT_SCHEMA = schema_for_url(DEFAULT_DATABASE_URL)

METADATA = DEFAULT_SCHEMA.metadata
RUNS = DEFAULT_SCHEMA.runs
UNIVERSE_RUNS = DEFAULT_SCHEMA.universe_runs
UNIVERSE_MEMBERS = DEFAULT_SCHEMA.universe_members
UNIVERSE_CURRENT = DEFAULT_SCHEMA.universe_current
ORDERS = DEFAULT_SCHEMA.orders
FILLS = DEFAULT_SCHEMA.fills
POSITIONS = DEFAULT_SCHEMA.positions
LEDGER_EVENTS = DEFAULT_SCHEMA.ledger_events
RECONCILE_LOG = DEFAULT_SCHEMA.reconcile_log
PRICE_DAILY = DEFAULT_SCHEMA.price_daily
PB1_WATCHLIST = DEFAULT_SCHEMA.pb1_watchlist
DERIVED_MINERVINI = DEFAULT_SCHEMA.derived_minervini
DERIVED_FLOW = DEFAULT_SCHEMA.derived_flow
JOB_CHECKPOINTS = DEFAULT_SCHEMA.job_checkpoints
