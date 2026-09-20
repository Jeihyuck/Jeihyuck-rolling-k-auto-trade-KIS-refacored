from __future__ import annotations

from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy import and_, func, select

from trader.account_state import get_account_key
from trader.db.schema import schema_for_engine

LEGACY_TRADING_EPOCH_ID = "LEGACY_PRE_EPOCH"


class TradingEpochError(RuntimeError):
    pass


def resolve_account_id(*, env: str, account_id: str | None = None) -> str:
    return str(account_id or get_account_key(env=env))


def get_active_trading_epoch_id(
    engine,
    *,
    env: str = "practice",
    account_id: str | None = None,
    create_if_missing: bool = True,
    reason: str = "AUTO_INITIAL_TRADING_EPOCH",
) -> str:
    env_name = str(env or "practice").strip().lower()
    account = resolve_account_id(env=env_name, account_id=account_id)
    schema = schema_for_engine(engine)
    with engine.begin() as conn:
        active = conn.execute(
            select(schema.trading_epochs.c.trading_epoch_id).where(and_(
                schema.trading_epochs.c.env == env_name,
                schema.trading_epochs.c.account_id == account,
                schema.trading_epochs.c.status == "ACTIVE",
            ))
        ).scalar()
        if active:
            return str(active)
        if not create_if_missing:
            raise TradingEpochError(
                f"ACTIVE_TRADING_EPOCH_MISSING env={env_name} account_id={account}"
            )
        epoch_id = str(uuid4())
        conn.execute(sa.insert(schema.trading_epochs).values(
            trading_epoch_id=epoch_id,
            env=env_name,
            account_id=account,
            status="ACTIVE",
            reason=reason,
        ))
        return epoch_id


def start_new_trading_epoch(
    engine,
    *,
    env: str = "practice",
    account_id: str | None = None,
    reason: str,
) -> str:
    """End the previous logical run and create one shared KR/US epoch.

    This is an explicit boundary operation. It does not delete historical rows.
    The caller is responsible for verifying the broker practice account is flat
    before invoking it.
    """
    env_name = str(env or "practice").strip().lower()
    if env_name != "practice":
        raise TradingEpochError(
            f"TRADING_EPOCH_RESET_ONLY_ALLOWED_FOR_PRACTICE env={env_name}"
        )
    account = resolve_account_id(env=env_name, account_id=account_id)
    schema = schema_for_engine(engine)
    epoch_id = str(uuid4())
    with engine.begin() as conn:
        conn.execute(
            sa.update(schema.trading_epochs)
            .where(and_(
                schema.trading_epochs.c.env == env_name,
                schema.trading_epochs.c.account_id == account,
                schema.trading_epochs.c.status == "ACTIVE",
            ))
            .values(status="ENDED", ended_at=func.now(), reason=reason)
        )

        active_portfolio_ids = list(conn.execute(
            select(schema.portfolio_epochs.c.portfolio_epoch_id).where(and_(
                schema.portfolio_epochs.c.env == env_name,
                schema.portfolio_epochs.c.account_id == account,
                schema.portfolio_epochs.c.status == "ACTIVE",
            ))
        ).scalars())
        if active_portfolio_ids:
            conn.execute(
                sa.update(schema.positions)
                .where(and_(
                    schema.positions.c.portfolio_epoch_id.in_(active_portfolio_ids),
                    schema.positions.c.status == "OPEN",
                ))
                .values(
                    status="CLOSED",
                    closed_ts=func.now(),
                    closed_reason="TRADING_EPOCH_ENDED",
                )
            )
        conn.execute(
            sa.update(schema.portfolio_epochs)
            .where(and_(
                schema.portfolio_epochs.c.env == env_name,
                schema.portfolio_epochs.c.account_id == account,
                schema.portfolio_epochs.c.status == "ACTIVE",
            ))
            .values(status="ENDED", ended_at=func.now(), reason=reason)
        )
        conn.execute(sa.insert(schema.trading_epochs).values(
            trading_epoch_id=epoch_id,
            env=env_name,
            account_id=account,
            status="ACTIVE",
            reason=reason,
        ))
    return epoch_id


def active_epoch_snapshot(engine, *, env: str = "practice", account_id: str | None = None) -> dict:
    env_name = str(env or "practice").strip().lower()
    account = resolve_account_id(env=env_name, account_id=account_id)
    schema = schema_for_engine(engine)
    with engine.connect() as conn:
        row = conn.execute(
            select(schema.trading_epochs).where(and_(
                schema.trading_epochs.c.env == env_name,
                schema.trading_epochs.c.account_id == account,
                schema.trading_epochs.c.status == "ACTIVE",
            ))
        ).mappings().first()
    return dict(row) if row else {}
