"""Fenced, account-wide US order reservations (PostgreSQL only).

Infrastructure only: there is deliberately NO implicit trading-mode switch.
Only a verified broker snapshot may supply cash/available shares. No timeout
ever releases an unresolved KIS order; callers need terminal broker evidence.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import os
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
        # One PostgreSQL transaction advisory lock PER BROKER ACCOUNT
        # (not per strategy, date or epoch). Otherwise AM/Afternoon
        # overlapping epochs could both reserve the same broker cash.
        # Acquire before the lease row lock for consistent lock ordering.
        conn.execute(text("""
            SELECT pg_advisory_xact_lock(
              hashtextextended('US_ACCOUNT_RESERVE:' || :env || ':' || :account, 0)
            )
        """), self.scope)
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
                broker_cash_usd: Any = None, broker_available_shares: int | None = None,
                _conn: Any = None) -> dict:
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
        if _conn is None:
            with self.engine.begin() as conn:
                return self.reserve(
                    lease=lease, action_key=action_key,
                    strategy_owner=strategy_owner, symbol=symbol, side=side,
                    qty=qty, price_usd=price_usd,
                    broker_cash_usd=broker_cash_usd,
                    broker_available_shares=broker_available_shares,
                    _conn=conn,
                )
        conn = _conn
        self._guard(conn, lease)  # row lock serializes every strategy
        old = conn.execute(text("""
            SELECT env,account_fingerprint,trading_epoch_id,
                   strategy_owner,symbol,side,requested_qty,
                   reserved_cash_usd,reserved_shares,state
            FROM us_execution_account_reservations
            WHERE action_key=:key FOR UPDATE
        """), {"key": action_key}).mappings().first()
        if old is not None:
            if (str(old["env"]) != self.scope["env"]
                    or str(old["account_fingerprint"]) != self.scope["account"]
                    or str(old["trading_epoch_id"]) != self.scope["epoch"]
                    or str(old["strategy_owner"]) != strategy_owner
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
              -- An active or UNKNOWN reservation in an earlier epoch still
              -- consumes the same broker-account capacity until reconciled.
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

    def assert_reserved_submit_authority(
        self, *, attempt_id: str, client_order_key: str,
        strategy_owner: str, symbol: str, side: str, qty: int,
    ) -> None:
        """Fail closed at the *native HTTP boundary*, not just order planning.

        Acquire the account-wide advisory transaction lock so a changed
        reservation/claim cannot be mistaken for an authorized submission.
        No KIS call occurs here. An unresolved/terminalized reservation, stale
        Attempt, or cross-account/owner identity cannot submit.
        """
        if not attempt_id or not client_order_key or not strategy_owner:
            raise AuthorityUnavailable("US_SUBMIT_AUTHORITY_IDENTITY_MISSING")
        with self.engine.begin() as conn:
            conn.execute(text("""
                SELECT pg_advisory_xact_lock(
                  hashtextextended('US_ACCOUNT_RESERVE:' || :env || ':' || :account, 0)
                )
            """), self.scope)
            row = conn.execute(text("""
                SELECT r.state, r.strategy_owner, r.symbol, r.side,
                       r.requested_qty,
                       a.client_order_key, a.requested_qty AS attempt_qty,
                       c.active_attempt_id, c.strategy_owner AS claim_owner,
                       c.action_state
                FROM us_execution_account_reservations r
                JOIN us_execution_attempts a ON a.attempt_id=r.action_key
                JOIN us_execution_claims c ON c.action_key=a.action_key
                WHERE r.action_key=:attempt
                  AND r.env=:env AND r.account_fingerprint=:account
                  AND r.trading_epoch_id=:epoch
                  AND c.env=:env AND c.account_id=:account
                  AND c.trading_epoch_id=:epoch
                FOR SHARE OF r, a, c
            """), {**self.scope, "attempt": attempt_id}).mappings().one_or_none()
            if (
                row is None
                or str(row["state"]) != "RESERVED"
                or str(row["strategy_owner"]) != strategy_owner.upper()
                or str(row["symbol"]) != symbol.upper()
                or str(row["side"]) != side.upper()
                or int(row["requested_qty"]) != int(qty)
                or int(row["attempt_qty"]) != int(qty)
                or str(row["client_order_key"]) != client_order_key
                or str(row["active_attempt_id"]) != attempt_id
                or str(row["claim_owner"]) != strategy_owner.upper()
                or str(row["action_state"]) not in {"IN_FLIGHT", "OPEN"}
            ):
                raise AuthorityUnavailable("US_RESERVED_SUBMIT_AUTHORITY_STALE")

    def release_lease(self, *, lease: Lease) -> None:
        """Release only the current holder's write lease, never its holds.

        Broker-open and UNCERTAIN reservations survive process restarts and
        subsequent fencing-token generations until KIS terminal reconciliation.
        """
        with self.engine.begin() as conn:
            self._guard(conn, lease)
            conn.execute(text("""
                UPDATE us_execution_authority_leases
                SET expires_at=clock_timestamp(),updated_at=clock_timestamp()
                WHERE env=:env AND account_fingerprint=:account
                  AND trading_epoch_id=:epoch AND holder_id=:holder
                  AND fencing_token=:token
            """), {**self.scope, "holder": lease.holder_id,
                   "token": lease.fencing_token})

    def terminalize(self, *, lease: Lease, action_key: str,
                    terminal_evidence: str, filled: bool = False) -> bool:
        """Only authoritative terminal broker proof may clear a reservation.

        ACK or locally inferred timeout is not terminal evidence.
        KIS truth must be reconciled before invoking this method.
        """
        # Kept for isolated legacy regression fixtures only. A caller's
        # string is NOT authoritative broker proof; real workers must instead
        # call terminalize_verified_attempt() under the same DB fencing row.
        if not (
            os.getenv("PYTEST_CURRENT_TEST")
            and os.getenv("PBCORE_TEST_POSTGRES_URL")
        ):
            raise ReservationConflict("unverified_terminalization_forbidden")
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

    def terminalize_verified_attempt(
        self, *, lease: Lease, attempt_id: str,
        client_order_key: str, trade_date: str,
    ) -> dict:
        """Release *only* a KIS-proven terminal Attempt's reservation.

        This query is intended for a fenced reconciliation writer *after*
        broker evidence has atomically updated us_orders and execution Claims.
        Arbitrary 'KIS_TERMINAL:' strings and local cancel requests cannot
        authorize this transition. A later superseding Attempt is independent.
        """
        if not attempt_id or not client_order_key or not trade_date:
            raise ReservationConflict("terminal_reservation_identity_missing")
        with self.engine.begin() as conn:
            self._guard(conn, lease)
            reserved = conn.execute(text("""
                SELECT * FROM us_execution_account_reservations
                WHERE action_key=:attempt_id AND env=:env
                  AND account_fingerprint=:account AND trading_epoch_id=:epoch
                FOR UPDATE
            """), {**self.scope, "attempt_id": attempt_id}).mappings().first()
            if reserved is None:
                raise ReservationConflict("terminal_reservation_missing")
            if reserved["state"] in {"RELEASED", "CONSUMED"}:
                return {
                    "state": reserved["state"], "idempotent": True,
                    "attempt_id": attempt_id,
                }
            if reserved["state"] not in {"RESERVED", "UNCERTAIN"}:
                raise ReservationConflict("terminal_reservation_not_active")
            attempt = conn.execute(text("""
                SELECT action_key, attempt_state, cumulative_filled_qty,
                       authoritative, requested_qty, client_order_key
                FROM us_execution_attempts
                WHERE attempt_id=:attempt_id FOR UPDATE
            """), {"attempt_id": attempt_id}).mappings().first()
            if (
                attempt is None or not attempt["authoritative"]
                or str(attempt["client_order_key"] or "") != client_order_key
                or int(attempt["requested_qty"]) != int(reserved["requested_qty"])
            ):
                raise ReservationConflict("terminal_attempt_proof_unverified")
            action = conn.execute(text("""
                SELECT env, account_id, trading_epoch_id, strategy_owner,
                       active_attempt_id
                FROM us_execution_claims WHERE action_key=:key FOR UPDATE
            """), {"key": attempt["action_key"]}).mappings().first()
            if (
                action is None
                or action["env"] != self.scope["env"]
                or action["account_id"] != self.scope["account"]
                or action["trading_epoch_id"] != self.scope["epoch"]
                or action["strategy_owner"] != reserved["strategy_owner"]
                or action["active_attempt_id"] == attempt_id
            ):
                raise ReservationConflict("terminal_claim_not_proven")
            rows = conn.execute(text("""
                SELECT status, qty_requested, qty_filled, order_no,
                       symbol, side, meta
                FROM us_orders
                WHERE client_order_key=:client_order_key
                  AND trade_date=CAST(:trade_date AS DATE)
                  AND env=:env AND trading_epoch_id=:epoch
                FOR UPDATE
            """), {
                "client_order_key": client_order_key,
                "trade_date": trade_date,
                "env": self.scope["env"], "epoch": self.scope["epoch"],
            }).mappings().all()
            if len(rows) != 1:
                raise ReservationConflict("terminal_original_order_not_unique")
            order = dict(rows[0])
            meta = order.get("meta") or {}
            if not isinstance(meta, dict):
                raise ReservationConflict("terminal_order_meta_invalid")
            cumulative = int(order["qty_filled"] or 0)
            if (
                str(meta.get("execution_action_key") or "") != attempt["action_key"]
                or str(meta.get("submit_attempt_id") or "") != attempt_id
                or str(order["symbol"]).upper() != str(reserved["symbol"]).upper()
                or str(order["side"]).upper() != str(reserved["side"]).upper()
                or int(order["qty_requested"]) != int(reserved["requested_qty"])
                or int(attempt["cumulative_filled_qty"] or 0) != cumulative
                or not order["order_no"]
            ):
                raise ReservationConflict("terminal_order_attempt_mismatch")
            state = str(order["status"] or "").upper()
            attempt_state = str(attempt["attempt_state"] or "").upper()
            if state == "FILLED":
                if (
                    cumulative != int(order["qty_requested"])
                    or attempt_state != "FILLED"
                    or str(meta.get("fill_evidence_type") or "")
                        != "KIS_ORDER_CUMULATIVE_ACTUAL"
                ):
                    raise ReservationConflict("terminal_fill_proof_missing")
            elif state == "CANCELLED":
                row = meta.get("broker_raw_row") or {}
                if (
                    attempt_state not in {"CANCELLED_ZERO_FILL", "CANCELLED_PARTIAL_FILL"}
                    or meta.get("broker_open_qty") != 0
                    or not meta.get("broker_observed_at")
                    or not isinstance(row, dict)
                    or str(row.get("status") or "").upper() not in {"CANCELLED", "CANCELED"}
                    or row.get("filled_qty_present") is not True
                    or row.get("remaining_qty_present") is not True
                    or int(row["filled_qty"] if row.get("filled_qty") is not None else -1) != cumulative
                ):
                    raise ReservationConflict("terminal_cancel_proof_missing")
            else:
                raise ReservationConflict("terminal_order_not_final")
            new_state = "CONSUMED" if cumulative > 0 else "RELEASED"
            changed = conn.execute(text("""
                UPDATE us_execution_account_reservations
                SET state=:state, terminal_evidence=:evidence,
                    holder_id=:holder, fencing_token=:token,
                    updated_at=clock_timestamp()
                WHERE action_key=:attempt_id
                  AND env=:env AND account_fingerprint=:account
                  AND trading_epoch_id=:epoch
                  AND state IN ('RESERVED','UNCERTAIN')
            """), {
                **self.scope, "attempt_id": attempt_id, "state": new_state,
                "evidence": "KIS_DB_PROVEN:" + str(order["order_no"]),
                "holder": lease.holder_id, "token": lease.fencing_token,
            })
            if changed.rowcount != 1:
                raise ReservationConflict("terminal_reservation_update_race")
            return {
                "state": new_state, "idempotent": False,
                "attempt_id": attempt_id, "broker_order_no": str(order["order_no"]),
            }

    def release_proven_pre_submit_abort(
        self, *, lease: Lease, attempt_id: str, client_order_key: str,
    ) -> bool:
        """Release a reservation only after durable pre-HTTP abandonment.

        This is not a KIS cancellation. The exact Attempt must be marked
        ABANDONED_PRE_SUBMIT and authoritative by the existing Router before
        submission, and the Claim must not still own the Attempt. A generic
        timeout or local cancel request never satisfies these conditions.
        """
        if not attempt_id or not client_order_key:
            raise ReservationConflict("pre_submit_abort_identity_missing")
        with self.engine.begin() as conn:
            self._guard(conn, lease)
            row = conn.execute(text("""
                SELECT r.state, a.attempt_state, a.authoritative,
                       a.client_order_key, c.active_attempt_id
                FROM us_execution_account_reservations AS r
                JOIN us_execution_attempts AS a ON a.attempt_id=r.action_key
                JOIN us_execution_claims AS c ON c.action_key=a.action_key
                WHERE r.action_key=:attempt_id AND r.env=:env
                  AND r.account_fingerprint=:account
                  AND r.trading_epoch_id=:epoch
                  AND c.env=:env AND c.account_id=:account
                  AND c.trading_epoch_id=:epoch
                FOR UPDATE OF r,a,c
            """), {**self.scope, "attempt_id": attempt_id}).mappings().one_or_none()
            if row is None:
                raise ReservationConflict("pre_submit_abort_attempt_not_found")
            if row["state"] == "RELEASED":
                return False
            if (
                row["state"] != "RESERVED"
                or str(row["attempt_state"]) != "ABANDONED_PRE_SUBMIT"
                or row["authoritative"] is not True
                or str(row["client_order_key"] or "") != client_order_key
                or row["active_attempt_id"] == attempt_id
            ):
                raise ReservationConflict("pre_submit_abort_not_authoritatively_proven")
            result = conn.execute(text("""
                UPDATE us_execution_account_reservations
                SET state='RELEASED',
                    terminal_evidence='DURABLE_ABANDONED_PRE_SUBMIT',
                    holder_id=:holder, fencing_token=:token,
                    updated_at=clock_timestamp()
                WHERE action_key=:attempt_id AND state='RESERVED'
                  AND env=:env AND account_fingerprint=:account
                  AND trading_epoch_id=:epoch
            """), {
                **self.scope, "attempt_id": attempt_id,
                "holder": lease.holder_id, "token": lease.fencing_token,
            })
            if result.rowcount != 1:
                raise ReservationConflict("pre_submit_abort_release_race")
            return True
