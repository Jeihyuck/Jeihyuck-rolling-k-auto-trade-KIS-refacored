"""Fail-closed, immutable US broker/portfolio/position provenance.

This is a validation layer, not a new trading policy.  KIS proves its
account/day/order; portfolio epoch and position cycle are ORIGINAL local
submit/fill facts.  Never transfer a historical order to a current cycle.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass

from sqlalchemy import text


class USRecoveryIdentityError(RuntimeError):
    pass


OWNERS = frozenset({"US_STANDARD", "TQQQ_INFINITE"})


@dataclass(frozen=True)
class USRecoveryScope:
    env: str
    account_id: str  # SHA256 of authenticated KIS account key
    trading_epoch_id: str
    portfolio_epoch_id: str
    position_cycle_id: str
    strategy_owner: str

    def as_dict(self) -> dict[str, str]:
        return {key: getattr(self, key) for key in (
            "env", "account_id", "trading_epoch_id", "portfolio_epoch_id",
            "position_cycle_id", "strategy_owner",
        )}


def require_us_recovery_engine():
    from trader.us.db.repos import _get_engine_or_none
    engine = _get_engine_or_none()
    if engine is None:
        raise USRecoveryIdentityError("US_SCOPE_DATABASE_UNAVAILABLE")
    return engine


def _verified_source(value: dict, *keys: str) -> str:
    present = [str(value.get(k) or "").strip() for k in keys if value.get(k) not in (None, "")]
    if not present:
        return ""
    if len(set(present)) != 1:
        raise USRecoveryIdentityError("ORIGINAL_IDENTITY_CONFLICT")
    return present[0]


def current_us_portfolio_epoch(
    conn, *, env: str, account_key: str, trading_epoch_id: str,
    strategy_owner: str,
) -> str:
    """One explicitly started US owner epoch, never a KR epoch or newest row."""
    owner = str(strategy_owner or "").upper()
    if owner not in OWNERS or not all((env, account_key, trading_epoch_id)):
        raise USRecoveryIdentityError("PORTFOLIO_EPOCH_LOOKUP_SCOPE_UNVERIFIED")
    rows = conn.execute(text("""
        SELECT portfolio_epoch_id FROM portfolio_epochs
        WHERE env=:env AND account_id=:account
          AND trading_epoch_id=:epoch AND status='ACTIVE'
          AND sid=0 AND mode=0 AND strategy=:owner
        LIMIT 2
    """), {"env": env, "account": account_key,
           "epoch": trading_epoch_id, "owner": owner}).all()
    if len(rows) != 1 or not str(rows[0][0] or "").strip():
        raise USRecoveryIdentityError(
            "PORTFOLIO_EPOCH_MISSING" if not rows else "PORTFOLIO_EPOCH_AMBIGUOUS"
        )
    return str(rows[0][0])


def position_cycle_from_order(
    intent: dict, *, account_id: str, trading_epoch_id: str,
    portfolio_epoch_id: str, owner: str,
) -> str:
    """Use actual lifecycle; first NEW_POSITION_BUY gets one stable cycle.

    Retries proven unsent on the same symbol/trade day share their logical
    parent BUY cycle. Later trade days get a different cycle. Current PB1
    policy already fences a second new-position BUY on one day/symbol.
    """
    meta = intent.get("meta") if isinstance(intent.get("meta"), dict) else {}
    lifecycle = _verified_source(
        {"a": intent.get("position_lifecycle_id"),
         "b": meta.get("position_lifecycle_id"),
         "c": intent.get("position_cycle_id"),
         "d": meta.get("position_cycle_id")},
        "a", "b", "c", "d",
    )
    side = str(intent.get("side") or "").upper()
    action = str(intent.get("position_action") or meta.get("position_action") or "").upper()
    if owner == "US_STANDARD" and side == "BUY" and action == "NEW_POSITION_BUY":
        symbol = str(intent.get("symbol") or "").strip().upper()
        day = str(intent.get("trade_date") or "").strip()
        if not symbol or len(day) != 10:
            raise USRecoveryIdentityError("POSITION_CYCLE_ENTRY_PROVENANCE_MISSING")
        material = "|".join((account_id, trading_epoch_id,
                             portfolio_epoch_id, owner, symbol, day, "NEW_POSITION_BUY"))
        original_cycle = hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]
        # The existing PB1 producer stores ENTRY:US_STANDARD:SYMBOL as a
        # pre-submit *action family placeholder*. It is NOT an opened holding
        # cycle; replace only that exact placeholder with immutable new cycle.
        producer_placeholder = f"ENTRY:{owner}:{symbol}"
        if lifecycle and lifecycle not in {original_cycle, producer_placeholder}:
            raise USRecoveryIdentityError("POSITION_CYCLE_NEW_ENTRY_REUSE_MISMATCH")
        return original_cycle
    if not lifecycle:
        raise USRecoveryIdentityError("POSITION_CYCLE_UNVERIFIED")
    return lifecycle


def bind_original_us_order_scope(
    intent: dict, *, conn, env: str, account_key: str, trading_epoch_id: str,
) -> USRecoveryScope:
    """Check and stamp the immutable scope BEFORE the broker/Claim boundary."""
    meta = intent.get("meta") if isinstance(intent.get("meta"), dict) else {}
    owner = str(intent.get("strategy_owner") or meta.get("strategy_owner") or "").strip().upper()
    if owner not in OWNERS:
        raise USRecoveryIdentityError("STRATEGY_OWNER_UNVERIFIED")
    if env not in {"practice", "real"} or not account_key or not trading_epoch_id:
        raise USRecoveryIdentityError("KIS_ACCOUNT_OR_TRADING_EPOCH_UNVERIFIED")
    account_hash = hashlib.sha256(account_key.encode("utf-8")).hexdigest()
    portfolio_id = current_us_portfolio_epoch(
        conn, env=env, account_key=account_key,
        trading_epoch_id=trading_epoch_id, strategy_owner=owner,
    )
    cycle_id = position_cycle_from_order(
        intent, account_id=account_hash, trading_epoch_id=trading_epoch_id,
        portfolio_epoch_id=portfolio_id, owner=owner,
    )
    scope = USRecoveryScope(env, account_hash, str(trading_epoch_id),
                            portfolio_id, cycle_id, owner)
    # Existing PB1 position snapshots keep the original account key in
    # metadata. Replacing that with a hash would break lifecycle matching.
    # Only the independent broker_account_hash is canonical on new orders.
    for key, expected in scope.as_dict().items():
        if key == "account_id":
            raw = _verified_source({
                "top": intent.get("account_id"), "meta": meta.get("account_id"),
            }, "top", "meta")
            if raw and raw not in {account_key, account_hash}:
                raise USRecoveryIdentityError("ACCOUNT_ID_MISMATCH")
            proof = _verified_source({
                "top": intent.get("broker_account_hash"),
                "meta": meta.get("broker_account_hash"),
            }, "top", "meta")
            if proof and proof != account_hash:
                raise USRecoveryIdentityError("ACCOUNT_ID_MISMATCH")
            continue
        actual = _verified_source(
            {"top": intent.get(key), "meta": meta.get(key)}, "top", "meta",
        )
        if actual and actual != expected:
            raise USRecoveryIdentityError(key.upper() + "_MISMATCH")
    # Never mutate the original strategy's raw account key.
    new_meta = dict(meta)
    new_meta.update({k: v for k, v in scope.as_dict().items() if k != "account_id"})
    new_meta["broker_account_hash"] = account_hash
    new_meta["position_lifecycle_id"] = cycle_id
    intent.update({k: v for k, v in scope.as_dict().items() if k != "account_id"})
    intent["broker_account_hash"] = account_hash
    intent["position_lifecycle_id"] = cycle_id
    intent["meta"] = new_meta
    return scope


def assert_recovery_scope(original: dict, current: dict, *,
                          original_meta: dict | None = None,
                          current_meta: dict | None = None) -> None:
    """No implicit old-order adoption; missing values fail closed."""
    orig = original_meta or {}
    now = current_meta or {}
    for field in ("env", "account_id", "trading_epoch_id",
                  "portfolio_epoch_id", "position_cycle_id", "strategy_owner"):
        key = "broker_account_hash" if field == "account_id" else field
        old = _verified_source({"top": original.get(key), "meta": orig.get(key)},
                               "top", "meta")
        active = _verified_source({"top": current.get(key), "meta": now.get(key)},
                                  "top", "meta")
        if not old or not active:
            raise USRecoveryIdentityError(field.upper() + "_UNVERIFIED")
        if old != active:
            raise USRecoveryIdentityError(field.upper() + "_MISMATCH")
    old_life = _verified_source(
        {"life": original.get("position_lifecycle_id"), "cycle": original.get("position_cycle_id"),
         "meta_life": orig.get("position_lifecycle_id")}, "life","cycle","meta_life",
    )
    new_life = _verified_source(
        {"life": current.get("position_lifecycle_id"), "cycle": current.get("position_cycle_id"),
         "meta_life": now.get("position_lifecycle_id")}, "life","cycle","meta_life",
    )
    if old_life != original.get("position_cycle_id") or old_life != new_life:
        raise USRecoveryIdentityError("POSITION_LIFECYCLE_MISMATCH")


def assert_no_crossday_us_order_conflict(conn, *, env: str, account_id: str,
                                         trade_date: str, symbol: str, side: str) -> None:
    """Keep only relevant historical claims fenced without a KIS recovery call."""
    conflict = conn.execute(text("""
        SELECT 1
        FROM us_execution_claims c
        JOIN us_execution_attempts a ON a.action_key=c.action_key
             AND a.attempt_id=c.active_attempt_id
        LEFT JOIN us_order_events e ON e.submit_attempt_id=a.attempt_id
             AND e.trade_date=c.trade_date AND e.client_order_key=a.client_order_key
             AND e.event_type IN ('BROKER_SUBMIT_STARTED','BROKER_CLAIM_PREPARED')
        LEFT JOIN us_orders o ON o.client_order_key=a.client_order_key
             AND o.trade_date=c.trade_date AND o.trading_epoch_id=c.trading_epoch_id
             AND o.env=c.env AND o.meta->>'submit_attempt_id'=a.attempt_id
        WHERE c.env=:env AND c.account_id=:account AND c.market='US'
          AND (c.trade_date < CAST(:trade_date AS date) OR c.trade_date IS NULL)
          AND (
              (upper(coalesce(nullif(e.symbol,''),o.symbol))=:symbol
               AND upper(coalesce(nullif(e.side,''),o.side))=:side)
              OR (a.attempt_state <> 'CREATED'
                  AND (coalesce(nullif(e.symbol,''),o.symbol) IS NULL
                       OR coalesce(nullif(e.side,''),o.side) IS NULL))
          )
        LIMIT 1
    """), {"env": env, "account": account_id, "trade_date": trade_date,
           "symbol": symbol, "side": side}).first()
    if conflict is not None:
        raise USRecoveryIdentityError("CROSSDAY_ORDER_CONFLICT_UNRESOLVED")


def assert_preexisting_us_claim_scope(conn, *, action_key: str,
                                      scope: USRecoveryScope) -> None:
    """Never adopt an unscoped historic claim under a new portfolio generation."""
    if not action_key:
        raise USRecoveryIdentityError("EXECUTION_ACTION_UNVERIFIED")
    row = conn.execute(text("""
        SELECT env,account_id,trading_epoch_id,strategy_owner,lifecycle_id,
               portfolio_epoch_id,position_cycle_id
        FROM us_execution_claims WHERE action_key=:key FOR UPDATE
    """), {"key": action_key}).mappings().first()
    if row is None:
        return
    old = dict(row)
    required = scope.as_dict()
    for column, expected in required.items():
        if column == "position_cycle_id":
            original = str(old.get(column) or old.get("lifecycle_id") or "")
            if not old.get("position_cycle_id"):
                raise USRecoveryIdentityError("POSITION_CYCLE_UNVERIFIED")
        else:
            original = str(old.get(column) or "")
        if not original or original != expected:
            raise USRecoveryIdentityError(column.upper() + "_MISMATCH")


def persist_us_claim_scope(conn, *, action_key: str,
                           scope: USRecoveryScope) -> None:
    """Same original action can only carry one owner portfolio and cycle."""
    if not action_key:
        raise USRecoveryIdentityError("EXECUTION_ACTION_UNVERIFIED")
    result = conn.execute(text("""
        UPDATE us_execution_claims
        SET portfolio_epoch_id=:portfolio,
            position_cycle_id=:cycle
        WHERE action_key=:key AND env=:env AND account_id=:account
          AND trading_epoch_id=:epoch AND strategy_owner=:owner
          AND lifecycle_id=:cycle
          AND (portfolio_epoch_id IS NULL OR portfolio_epoch_id=:portfolio)
          AND (position_cycle_id IS NULL OR position_cycle_id=:cycle)
        RETURNING action_key
    """), {
        "key": action_key, "env": scope.env, "account": scope.account_id,
        "epoch": scope.trading_epoch_id, "owner": scope.strategy_owner,
        "portfolio": scope.portfolio_epoch_id, "cycle": scope.position_cycle_id,
    })
    if result.first() is None:
        raise USRecoveryIdentityError("EXECUTION_CLAIM_ORIGINAL_SCOPE_MISMATCH")
