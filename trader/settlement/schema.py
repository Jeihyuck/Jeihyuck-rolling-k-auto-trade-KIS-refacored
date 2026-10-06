"""SQLAlchemy representation of additive migration 0055."""
from __future__ import annotations

import sqlalchemy as sa

metadata = sa.MetaData()
evidence = sa.Table(
    "broker_settlement_evidence", metadata,
    sa.Column("evidence_id", sa.Text, primary_key=True),
    sa.Column("settlement_key", sa.Text, nullable=False),
    sa.Column("env", sa.Text, nullable=False),
    sa.Column("market", sa.Text, nullable=False),
    sa.Column("account_scope", sa.Text, nullable=False),
    sa.Column("trading_epoch_id", sa.Text, nullable=False),
    sa.Column("strategy_owner", sa.Text, nullable=False),
    sa.Column("position_cycle_id", sa.Text, nullable=False),
    sa.Column("client_order_key", sa.Text, nullable=False),
    sa.Column("broker_trade_date", sa.Date, nullable=False),
    sa.Column("exchange", sa.Text, nullable=False),
    sa.Column("broker_order_no", sa.Text),
    sa.Column("side", sa.Text, nullable=False),
    sa.Column("evidence_type", sa.Text, nullable=False),
    sa.Column("evidence_digest", sa.Text, nullable=False),
    sa.Column("confirmed_cumulative_qty", sa.Integer, nullable=False),
    sa.Column("execution_price", sa.Numeric(24, 8)),
    sa.Column("currency", sa.Text, nullable=False),
    sa.Column("observed_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
)
applications = sa.Table(
    "settlement_applications", metadata,
    sa.Column("settlement_key", sa.Text, primary_key=True),
    sa.Column("env", sa.Text, nullable=False),
    sa.Column("market", sa.Text, nullable=False),
    sa.Column("account_scope", sa.Text, nullable=False),
    sa.Column("trading_epoch_id", sa.Text, nullable=False),
    sa.Column("strategy_owner", sa.Text, nullable=False),
    sa.Column("position_cycle_id", sa.Text, nullable=False),
    sa.Column("client_order_key", sa.Text, nullable=False),
    sa.Column("broker_trade_date", sa.Date, nullable=False),
    sa.Column("exchange", sa.Text, nullable=False),
    sa.Column("side", sa.Text, nullable=False),
    sa.Column("currency", sa.Text, nullable=False),
    sa.Column("applied_qty", sa.Integer, nullable=False, server_default="0"),
    sa.Column("applied_notional", sa.Numeric(24, 8), nullable=False, server_default="0"),
    sa.Column("price_status", sa.Text, nullable=False, server_default="PRICE_PENDING"),
    sa.Column("settlement_status", sa.Text, nullable=False, server_default="QUANTITY_PENDING"),
    sa.Column("last_evidence_id", sa.Text),
    sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
)
