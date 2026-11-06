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
from trader.db.schema import schema_for_engine


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


def relevant_open_orders_for_scope(
    *, orders: list[dict[str, Any]], portfolios: dict[str, dict[str, Any]],
    scope: KRRecoveryScope,
) -> list[dict[str, Any]]:
    """Ignore only *proven* other-account/epoch orders; unknown still blocks.

    The orders table carries no account_id. Never interpret an unjoined legacy
    order as belonging to another account, and never hide the current account's
    unresolved order. This is a proof filter, not a broker/order mutation.
    """
    relevant = []
    for order in orders:
        portfolio_id = str(order.get("portfolio_epoch_id") or "")
        portfolio = portfolios.get(portfolio_id)
        proven = bool(
            portfolio_id and order.get("order_id")
            and order.get("position_cycle_id")
            and order.get("trading_epoch_id")
            and portfolio
            and str(order.get("env") or "") == scope.env
            and str(portfolio.get("env") or "") == scope.env
            and str(portfolio.get("portfolio_epoch_id") or "") == portfolio_id
            and str(portfolio.get("strategy") or "") == str(order.get("strategy") or "")
            and str(portfolio.get("trading_epoch_id") or "") == str(order.get("trading_epoch_id") or "")
            and str(portfolio.get("account_id") or "").strip()
        )
        if not proven:
            relevant.append(order)  # Unknown ownership is NOT safe to ignore.
            continue
        if (
            str(portfolio["account_id"]) != scope.account_key
            or str(portfolio["trading_epoch_id"]) != scope.trading_epoch_id
        ):
            continue  # Foreign account or superseded epoch, with positive proof.
        relevant.append(order)
    return relevant


def current_scope_open_order_blockers(
    *, engine: Any, env: str, kis: Any, orders_repo: Any,
) -> list[dict[str, Any]]:
    """One fail-closed open-order read + one portfolio-owner batch lookup.

    All order states and all strategy owners remain in the blockers set for
    the current account/epoch, including KR_INFINITE; no cross-owner bypass.
    """
    scope = resolve_kr_recovery_scope(engine=engine, env=env, kis=kis)
    opened = orders_repo.get_open_orders(env, include_stale=True, fail_open=False)
    if not isinstance(opened, list):
        raise RuntimeError("KR_OPEN_ORDER_EVIDENCE_INVALID_FAIL_CLOSED")
    if not opened or scope is None:
        return opened
    schema = schema_for_engine(engine)
    ids = {str(row.get("portfolio_epoch_id") or "") for row in opened}
    ids.discard("")
    with engine.connect() as conn:
        portfolio_rows = conn.execute(sa.select(schema.portfolio_epochs).where(
            schema.portfolio_epochs.c.portfolio_epoch_id.in_(ids)
        )).mappings().all() if ids else []
    portfolios = {str(item["portfolio_epoch_id"]): dict(item) for item in portfolio_rows}
    return relevant_open_orders_for_scope(
        orders=opened, portfolios=portfolios, scope=scope,
    )
