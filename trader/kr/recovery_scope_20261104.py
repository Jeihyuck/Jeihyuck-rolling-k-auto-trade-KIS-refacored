"""Strict account/epoch boundary shared by KR historical fill-recovery lanes.

Only canonical WSL trading runs require this boundary; legacy local migrations
can still inspect historical ledgers without authorizing trading recovery.
A portfolio epoch being ACTIVE is not enough: older *accounts* can be ACTIVE.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import sqlalchemy as sa

from trader.account_state import get_account_key
from trader.db.trading_epoch import active_trading_epoch_id, trading_epoch_enforced


@dataclass(frozen=True)
class KRRecoveryScope:
    env: str
    account_key: str
    trading_epoch_id: str


def resolve_kr_recovery_scope(
    *, engine: Any, env: str, kis: Any | None = None,
) -> KRRecoveryScope | None:
    """Resolve the exact live account. Missing production proof is fatal.

    A None scope is permitted only for genuinely non-enforced legacy fixtures;
    callers must not turn any *failed* enforced resolution into an unscoped query.
    """
    if not trading_epoch_enforced():
        return None
    key = get_account_key(env=env)
    parts = key.split(":")
    if len(parts) != 3 or any(part in ("", "unknown") for part in parts):
        raise RuntimeError("KR_RECOVERY_ACCOUNT_UNPROVEN")
    if kis is not None and get_account_key(env=env, kis=kis) != key:
        raise RuntimeError("KR_RECOVERY_BROKER_ACCOUNT_MISMATCH")
    epoch = active_trading_epoch_id(
        engine, env=env, account_id=key, required=True,
    )
    if not epoch:
        raise RuntimeError("KR_RECOVERY_TRADING_EPOCH_UNPROVEN")
    return KRRecoveryScope(env=env, account_key=key, trading_epoch_id=str(epoch))


def scoped_portfolio_predicates(schema: Any, scope: KRRecoveryScope) -> tuple[Any, ...]:
    return (
        schema.portfolio_epochs.c.env == scope.env,
        schema.portfolio_epochs.c.account_id == scope.account_key,
        schema.portfolio_epochs.c.trading_epoch_id == scope.trading_epoch_id,
        schema.portfolio_epochs.c.status == "ACTIVE",
    )


def order_matches_recovery_scope(
    *, conn: Any, schema: Any, order: dict[str, Any],
    strategy: str, scope: KRRecoveryScope | None,
) -> bool:
    """Check mutable order/portfolio ownership before every economic write."""
    if not order.get("position_cycle_id") or not order.get("portfolio_epoch_id"):
        return False
    if not order.get("order_id") or str(order.get("strategy") or "") != strategy:
        return False
    if scope is None:
        return True  # Legacy non-trading tests; WSL_RUN_MARKET requires a scope.
    if str(order.get("env") or "") != scope.env:
        return False
    if str(order.get("trading_epoch_id") or "") != scope.trading_epoch_id:
        return False
    portfolio = conn.execute(
        sa.select(schema.portfolio_epochs).where(
            schema.portfolio_epochs.c.portfolio_epoch_id == order["portfolio_epoch_id"],
            schema.portfolio_epochs.c.env == scope.env,
            schema.portfolio_epochs.c.account_id == scope.account_key,
            schema.portfolio_epochs.c.trading_epoch_id == scope.trading_epoch_id,
            schema.portfolio_epochs.c.strategy == strategy,
            schema.portfolio_epochs.c.status == "ACTIVE",
        ).limit(1)
    ).mappings().first()
    return portfolio is not None


def unowned_fill_matches_recovery_scope(
    *, fill: dict[str, Any], order: dict[str, Any],
    scope: KRRecoveryScope | None,
) -> bool:
    if scope is None:
        return True
    if (str(fill.get("env") or "") != scope.env
            or str(fill.get("trading_epoch_id") or "") != scope.trading_epoch_id):
        return False
    for field in ("portfolio_epoch_id", "position_cycle_id"):
        value = fill.get(field)
        if value and str(value) != str(order.get(field) or ""):
            return False
    return True
