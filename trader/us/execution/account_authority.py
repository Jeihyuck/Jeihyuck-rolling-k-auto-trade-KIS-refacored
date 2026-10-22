"""Fenced, account-wide US order reservations (PostgreSQL only).

Infrastructure only: there is deliberately NO implicit trading-mode switch.
Only a verified broker snapshot may supply cash/available shares. No timeout
ever releases an unresolved KIS order; callers need terminal broker evidence.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import text


class AuthorityUnavailable(RuntimeError):
    """No current write lease, or an unavailable/ambiguous account scope."""


class ReservationConflict(RuntimeError):
    """Requested exposure exceeds fresh broker evidence or existing holds."""


def _scope(env: str, account_fingerprint: str, trading_epoch_id: str) -> dict:
    if env != "practice":
        raise AuthorityUnavailable("US_ACCOUNT_AUTHORITY_PRACTICE_ONLY")
    if (
        not account_fingerprint or len(account_fingerprint) != 64
        or any(x not in "0123456789abcdef" for x in account_fingerprint.lower())
        or not trading_epoch_id
    ):
        raise AuthorityUnavailable("US_ACCOUNT_AUTHORITY_SCOPE_UNVERIFIED")
    return {
        "env": env,
        "account": account_fingerprint.lower(),
        "epoch": trading_epoch_id,
    }


def _positive_decimal(value: Any) -> Decimal:
    try:
        d = Decimal(str(value))
    except (InvalidOperation, TypeError):
        raise ReservationConflict("broker_cash_evidence_invalid") from None
    if not d.is_finite() or d < 0:
        raise ReservationConflict("broker_cash_evidence_invalid")
    return d


@dataclass(frozen=True)
class Lease:
    holder_id: str
    fencing_token: int


class AccountExecutionAuthority:
    """Serialize reservations through the account's *fenced lease row*.

    All methods use a single short PostgreSQL transaction. Holding a lease is
    insufficient to submit; broker truth, execution claim, journal and KIS
    permission checks remain independently mandatory in the canonical router.
    """

    def __init__(self, engine: Any, *, env: str, account_fingerprint: str,
                 trading_epoch_id: str):
        if engine.dialect.name != "postgresql":
            raise AuthorityUnavailable("US_ACCOUNT_AUTHORITY_POSTGRES_REQUIRED")
        self.engine = engine
        self.scope = _scope(env, account_fingerprint, trading_epoch_id)

    def acquire_lease(self, *, holder_id: str, ttl_seconds: int = 30) -> Lease:
        if not holder_id or not (5 <= int(ttl_seconds) <= 120):
            raise ValueError("holder and bounded lease TTL required")
        with self.engine.begin() as conn:
            token = conn.execute(text("""
                INSERT INTO us_execution_authority_leases AS lease
                    (env,account_fingerprint,trading_epoch_id,holder_id,
                     fencing_token,expires_at)
                VALUES (:env,:account,:epoch,:holder,1,
                        clock_timestamp() + :ttl * INTERVAL '1 second')
                ON CONFLICT (env,account_fingerprint,trading_epoch_id)
                DO UPDATE SET
                    holder_id=EXCLUDED.holder_id,
                    fencing_token=CASE
                        WHEN lease.holder_id=EXCLUDED.holder_id
                         AND lease.expires_at>clock_timestamp()
                        THEN lease.fencing_token ELSE lease.fencing_token+1 END,
                    expires_at=EXCLUDED.expires_at,
                    updated_at=clock_timestamp()
                WHERE lease.expires_at<=clock_timestamp()
                   OR lease.holder_id=EXCLUDED.holder_id
                RETURNING fencing_token
            """), {**self.scope, "holder": holder_id,
                   "ttl": int(ttl_seconds)}).scalar_one_or_none()
        if token is None:
            raise AuthorityUnavailable("US_ACCOUNT_AUTHORITY_LEASE_HELD")
        return Lease(holder_id, int(token))

    def _guard(self, conn: Any, lease: Lease) -> None:
        token = conn.execute(text("""
            SELECT fencing_token FROM us_execution_authority_leases
            WHERE env=:env AND account_fingerprint=:account
              AND trading_epoch_id=:epoch AND holder_id=:holder
              AND fencing_token=:token AND expires_at>clock_timestamp()
            FOR UPDATE
        """), {**self.scope, "holder": lease.holder_id,
               "token": lease.fencing_token}).scalar_one_or_none()
        if token is None:
            raise AuthorityUnavailable("US_ACCOUNT_AUTHORITY_FENCED")

    def reserve(self, *, lease: Lease, action_key: str, strategy_owner: str,
                symbol: str, side: str, qty: int, price_usd: Any,
                broker_cash_usd: Any = None, broker_available_shares: int | None = None
                ) -> dict:
        """Account-global held exposure, not independent per-strategy budgets.

        Caller must pass freshly queried account-wide broker quantities.
        Never use an in-memory quote as proof of cash or sell availability.
        """
        if not action_key or not strategy_owner or not symbol or int(qty) <= 0:
            raise ReservationConflict("reservation_identity_invalid")
        side = side.upper()
        if side not in {"BUY", "SELL"}:
            raise ReservationConflict("reservation_side_invalid")
        qty = int(qty)
        if side == "BUY":
            price = _positive_decimal(price_usd)
            if price <= 0 or broker_cash_usd is None:
                raise ReservationConflict("broker_cash_evidence_missing")
            total_capacity = _positive_decimal(broker_cash_usd)
            cash = price * qty
            shares = 0
        else:
            if broker_available_shares is None:
                raise ReservationConflict("broker_position_evidence_missing")
            total_capacity = int(broker_available_shares)
            if total_capacity < 0:
                raise ReservationConflict("broker_position_evidence_invalid")
            cash = Decimal("0")
            shares = qty
        with self.engine.begin() as conn:
            self._guard(conn, lease)  # row lock serializes every strategy
            old = conn.execute(text("""
                SELECT strategy_owner,symbol,side,requested_qty,
                       reserved_cash_usd,reserved_shares,state
                FROM us_execution_account_reservations
                WHERE action_key=:key FOR UPDATE
            """), {"key": action_key}).mappings().first()
            if old is not None:
                if (str(old["strategy_owner"]) != strategy_owner
                        or str(old["symbol"]) != symbol.upper()
                        or str(old["side"]) != side
                        or int(old["requested_qty"]) != qty
                        or Decimal(str(old["reserved_cash_usd"])) != cash
                        or int(old["reserved_shares"]) != shares):
                    raise ReservationConflict("reservation_idempotency_collision")
                if old["state"] not in {"RESERVED", "UNCERTAIN"}:
                    raise ReservationConflict("terminal_action_key_cannot_be_reused")
                return {"action_key": action_key, "state": old["state"],
                        "idempotent": True, "fencing_token": lease.fencing_token}
            held = conn.execute(text("""
                SELECT COALESCE(SUM(reserved_cash_usd),0) AS cash,
                       COALESCE(SUM(reserved_shares),0) AS shares
                FROM us_execution_account_reservations
                WHERE env=:env AND account_fingerprint=:account
                  AND trading_epoch_id=:epoch
                  AND state IN ('RESERVED','UNCERTAIN')
                  AND side=:side
                  AND (:side='BUY' OR symbol=:symbol)
            """), {**self.scope, "side": side,
                   "symbol": symbol.upper()}).mappings().one()
            needed = cash + Decimal(str(held["cash"])) if side == "BUY" else shares + int(held["shares"])
            if needed > total_capacity:
                raise ReservationConflict("account_capacity_reserved")
            conn.execute(text("""
                INSERT INTO us_execution_account_reservations
                  (action_key,env,account_fingerprint,trading_epoch_id,
                   strategy_owner,symbol,side,requested_qty,reserved_cash_usd,
                   reserved_shares,state,holder_id,fencing_token)
                VALUES (:key,:env,:account,:epoch,:owner,:symbol,:side,
                        :qty,:cash,:shares,'RESERVED',:holder,:token)
            """), {**self.scope, "key": action_key, "owner": strategy_owner,
                   "symbol": symbol.upper(), "side": side, "qty": qty,
                   "cash": cash, "shares": shares, "holder": lease.holder_id,
                   "token": lease.fencing_token})
        return {"action_key": action_key, "state": "RESERVED",
                "idempotent": False, "fencing_token": lease.fencing_token}

    def terminalize(self, *, lease: Lease, action_key: str,
                    terminal_evidence: str, filled: bool = False) -> bool:
        """Only authoritative terminal broker proof may clear a reservation.

        ACK or locally inferred timeout is not terminal evidence.
        KIS truth must be reconciled before invoking this method.
        """
        if not action_key or not str(terminal_evidence or "").startswith("KIS_TERMINAL:"):
            raise ReservationConflict("authoritative_terminal_evidence_required")
        with self.engine.begin() as conn:
            self._guard(conn, lease)
            result = conn.execute(text("""
                UPDATE us_execution_account_reservations
                SET state=:new_state,terminal_evidence=:evidence,
                    updated_at=clock_timestamp(),holder_id=:holder,
                    fencing_token=:token
                WHERE action_key=:key AND env=:env
                  AND account_fingerprint=:account
                  AND trading_epoch_id=:epoch
                  AND state IN ('RESERVED','UNCERTAIN')
            """), {**self.scope, "key": action_key,
                   "new_state": "CONSUMED" if filled else "RELEASED",
                   "evidence": terminal_evidence,
                   "holder": lease.holder_id,
                   "token": lease.fencing_token})
            return result.rowcount == 1
