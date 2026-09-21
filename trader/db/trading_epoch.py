from __future__ import annotations

import os
from uuid import uuid4
from typing import Any

from sqlalchemy import bindparam, inspect, text

from trader.account_state import get_account_key, resolve_env_name
from trader.db.engine import get_engine


LEGACY_TRADING_EPOCH_ID = "legacy-unscoped-0052"


class TradingEpochError(RuntimeError):
    pass


def trading_epoch_enforced() -> bool:
    """Fail closed in canonical KR/US WSL runtime even if .env overrides a flag."""
    explicit = str(os.getenv("TRADING_EPOCH_ENFORCE") or "").strip().lower()
    if explicit in {"1", "true", "yes", "on"}:
        return True
    market = str(os.getenv("WSL_RUN_MARKET") or "").strip().upper()
    if market in {"KR", "US"}:
        return True
    return False


def _identity(*, env: str | None = None, account_id: str | None = None) -> tuple[str, str]:
    env_name = resolve_env_name(env)
    account_key = str(account_id or get_account_key(env=env_name))
    return env_name, account_key


def active_trading_epoch_id(
    bind: Any | None = None,
    *,
    env: str | None = None,
    account_id: str | None = None,
    required: bool = True,
) -> str | None:
    """Return the single ACTIVE account generation.

    Trading never auto-creates this boundary. Starting a new epoch is an explicit
    operator action so practice resets and real go-live cannot silently inherit
    historical state.
    """
    env_name, account_key = _identity(env=env, account_id=account_id)
    owns_connection = bind is None or hasattr(bind, "connect")
    engine = bind or get_engine()
    conn = engine.connect() if owns_connection else bind
    try:
        # Legacy/test fixtures and the migration bootstrap may legitimately run
        # before migration 0052 exists.  Never probe a missing table with a
        # SELECT on PostgreSQL: even when Python catches the exception it would
        # abort the caller's transaction.
        if not inspect(conn).has_table("trading_epochs"):
            if required:
                raise TradingEpochError(
                    f"ACTIVE_TRADING_EPOCH_TABLE_MISSING env={env_name} account={account_key}"
                )
            return None
        rows = conn.execute(
            text("""
                SELECT trading_epoch_id
                FROM trading_epochs
                WHERE env=:env AND account_id=:account_id AND status='ACTIVE'
                ORDER BY started_at DESC, created_at DESC
                LIMIT 2
            """),
            {"env": env_name, "account_id": account_key},
        ).all()
    except Exception as exc:
        if required:
            raise TradingEpochError(
                f"ACTIVE_TRADING_EPOCH_UNAVAILABLE env={env_name} account={account_key}"
            ) from exc
        return None
    finally:
        if owns_connection:
            conn.close()

    if len(rows) == 1:
        return str(rows[0][0])
    if required:
        reason = "MISSING" if not rows else "MULTIPLE_ACTIVE"
        raise TradingEpochError(
            f"ACTIVE_TRADING_EPOCH_{reason} env={env_name} account={account_key}"
        )
    return None


def start_new_trading_epoch(
    engine: Any | None = None,
    *,
    env: str | None = None,
    account_id: str | None = None,
    reason: str,
) -> str:
    """End the old generation and create a new one without deleting history."""
    reason_n = str(reason or "").strip()
    if not reason_n:
        raise TradingEpochError("TRADING_EPOCH_REASON_REQUIRED")
    env_name, account_key = _identity(env=env, account_id=account_id)
    db = engine or get_engine()
    new_id = str(uuid4())

    with db.begin() as conn:
        # Serialize concurrent operator attempts for this account.
        if conn.dialect.name == "postgresql":
            conn.execute(
                text("SELECT pg_advisory_xact_lock(hashtext(:identity))"),
                {"identity": f"trading-epoch:{env_name}:{account_key}"},
            )

        conn.execute(
            text("""
                UPDATE trading_epochs
                SET status='ENDED', ended_at=CURRENT_TIMESTAMP
                WHERE env=:env AND account_id=:account_id AND status='ACTIVE'
            """),
            {"env": env_name, "account_id": account_key},
        )

        # KR portfolio epochs are children of the account generation. Close only
        # old ACTIVE children; historical rows remain queryable.
        old_portfolio_ids = [
            str(row[0])
            for row in conn.execute(
                text("""
                    SELECT portfolio_epoch_id
                    FROM portfolio_epochs
                    WHERE env=:env AND account_id=:account_id AND status='ACTIVE'
                """),
                {"env": env_name, "account_id": account_key},
            ).all()
        ]
        if old_portfolio_ids:
            close_stmt = text("""
                UPDATE positions
                SET status='CLOSED', closed_ts=CURRENT_TIMESTAMP,
                    closed_reason='TRADING_EPOCH_ENDED'
                WHERE status='OPEN'
                  AND portfolio_epoch_id IN :portfolio_ids
            """).bindparams(bindparam("portfolio_ids", expanding=True))
            conn.execute(close_stmt, {"portfolio_ids": old_portfolio_ids})
            conn.execute(
                text("""
                    UPDATE portfolio_epochs
                    SET status='ENDED', ended_at=CURRENT_TIMESTAMP, reason=:reason
                    WHERE env=:env AND account_id=:account_id AND status='ACTIVE'
                """),
                {"env": env_name, "account_id": account_key, "reason": reason_n},
            )

        conn.execute(
            text("""
                INSERT INTO trading_epochs(
                    trading_epoch_id, env, account_id, status, reason
                ) VALUES (:id, :env, :account_id, 'ACTIVE', :reason)
            """),
            {
                "id": new_id,
                "env": env_name,
                "account_id": account_key,
                "reason": reason_n,
            },
        )
    return new_id


def epoch_runtime_payload(
    bind: Any | None = None,
    *,
    env: str | None = None,
    account_id: str | None = None,
) -> dict[str, str]:
    env_name, account_key = _identity(env=env, account_id=account_id)
    epoch_id = active_trading_epoch_id(
        bind, env=env_name, account_id=account_key, required=True
    )
    return {
        "trading_epoch_id": str(epoch_id),
        "env": env_name,
        "account_id": account_key,
    }
