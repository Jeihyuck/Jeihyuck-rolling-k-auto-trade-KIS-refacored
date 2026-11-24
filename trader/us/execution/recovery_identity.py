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
    # The PB1 producer signed the placeholder lifecycle. Never simply recompute
    # its hash after replacing the ID: verify the ORIGINAL immutable signal
    # first, and allow only the exact placeholder -> deterministic cycle.
    original_timing_intent = None
    if (owner == "US_STANDARD"
            and str(intent.get("side") or "").upper() == "BUY"
            and str(intent.get("position_action") or meta.get("position_action") or "").upper()
                == "NEW_POSITION_BUY"
            and isinstance(meta.get("us_entry_opportunity_v1"), dict)):
        from trader.us.entry_timing.opportunity import _verify_timing_signal_and_binding
        original_timing_intent = {**intent, "meta": dict(meta)}
        original_proof = meta["us_entry_opportunity_v1"]
        if not _verify_timing_signal_and_binding(original_timing_intent, original_proof):
            raise USRecoveryIdentityError("US_ENTRY_TIMING_ORIGINAL_PROOF_UNVERIFIED")
        placeholder = f"ENTRY:{owner}:{str(intent.get('symbol') or '').strip().upper()}"
        if original_proof.get("submission_binding_version") is None:
            if str(intent.get("position_lifecycle_id") or "") != placeholder:
                raise USRecoveryIdentityError("US_ENTRY_TIMING_SCOPE_TRANSITION_FORBIDDEN")
        elif str(intent.get("position_lifecycle_id") or "") != cycle_id:
            raise USRecoveryIdentityError("US_ENTRY_TIMING_ALREADY_SCOPED_ID_MISMATCH")
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
    if original_timing_intent is not None:
        from trader.us.entry_timing.opportunity import finalize_verified_buy_scope_binding
        if original_proof.get("submission_binding_version") is None:
            finalize_verified_buy_scope_binding(
                intent, original=original_timing_intent, placeholder=placeholder,
            )
        elif not _verify_timing_signal_and_binding(intent, original_proof):
            raise USRecoveryIdentityError("US_ENTRY_TIMING_SCOPED_PROOF_INVALID")
    return scope


def bind_existing_standard_order_buy_source(
    intent: dict, *, conn, scope: USRecoveryScope, raw_account: str,
) -> dict:
    """Revalidate canonical ADD/SELL against the open original native BUY.

    The original contract remains distinct from an approved forward
    management or stop-policy revision. This read-only proof is not a
    strategy signal, a broker balance, or permission to submit an order.
    """
    from trader.us.original_buy_lineage import prove_open_standard_buy_source

    if scope.strategy_owner != "US_STANDARD":
        raise USRecoveryIdentityError("US_SST_OWNER_SCOPE_UNVERIFIED")
    if hashlib.sha256(raw_account.encode()).hexdigest() != scope.account_id:
        raise USRecoveryIdentityError("US_SST_ACCOUNT_SCOPE_UNVERIFIED")
    identity = {**scope.as_dict(), "account_id": raw_account,
                "position_lifecycle_id": scope.position_cycle_id}
    proof = prove_open_standard_buy_source(
        conn=conn, symbol=str(intent.get("symbol") or "").strip().upper(),
        trade_date=str(intent.get("trade_date") or ""), identity=identity,
    )
    contract = proof["contract"]
    meta = intent.get("meta") if isinstance(intent.get("meta"), dict) else {}
    side = str(intent.get("side") or "").upper()
    for field, expected in (
        ("source_entry_contract_sha256", contract["sha256"]),
        ("source_entry_contract_version", contract["version"]),
    ):
        for source in (intent, meta):
            if source.get(field) not in (None, "", expected):
                raise USRecoveryIdentityError("US_SST_ORIGINAL_CONTRACT_CONFLICT")
    if side == "BUY":
        # Existing ADD policy already rejects mixed lots. The independently
        # loaded original must also match the parent the producer selected.
        if (meta.get("entry_exit_contract") != contract
                or meta.get("parent_entry_contract_sha256") != contract["sha256"]):
            raise USRecoveryIdentityError("US_SST_ADD_PARENT_CONTRACT_CONFLICT")
    elif side != "SELL":
        raise USRecoveryIdentityError("US_SST_SIDE_UNVERIFIED")
    # Missing diagnostic copies (e.g. defense trim) may be attached from
    # proof; a conflicting caller is never rewritten to appear legitimate.
    receipt = {
        "source_buy_trade_date": proof["original_buy_trade_date"],
        "source_buy_client_order_key": proof["source_order_key"],
        "source_entry_contract_sha256": contract["sha256"],
        "position_cycle_id": scope.position_cycle_id,
        "portfolio_epoch_id": scope.portfolio_epoch_id,
    }
    if meta.get("us_original_buy_sst_v1") not in (None, receipt):
        raise USRecoveryIdentityError("US_SST_ORIGINAL_RECEIPT_CONFLICT")
    meta.update(source_entry_contract_sha256=contract["sha256"],
                source_entry_contract_version=contract["version"],
                us_original_buy_sst_v1=receipt)
    intent.update(source_entry_contract_sha256=contract["sha256"],
                  source_entry_contract_version=contract["version"], meta=meta)
    if side == "SELL":
        # The original BUY proof above is the common authority for every
        # canonical exit family. Decision attribution is audit-only: a
        # missing legacy audit or diagnostic error never changes SELL policy.
        binding = proof.get("original_buy_decision_binding")
        if binding:
            try:
                import os
                from datetime import datetime, timezone
                from trader.us.decision_evidence_5w1h import attach_original_buy_bound_sell_audit
                status = attach_original_buy_bound_sell_audit(
                    intent, contract=contract, binding=binding,
                    decision_at=str(meta.get("decision_ts_et") or meta.get("decision_at")
                                    or datetime.now(timezone.utc).isoformat()),
                    code_sha=str(os.getenv("US_PINNED_CODE_SHA") or os.getenv("GITHUB_SHA") or "UNKNOWN"),
                )
                meta["exit_decision_evidence_status"] = (
                    "VERIFIED" if status == "ALREADY_VERIFIED" else status
                )
            except Exception:
                meta["exit_decision_evidence_status"] = "AUDIT_UNAVAILABLE"
        elif (contract.get("entry_provenance") or {}).get("entry_decision_evidence_5w1h"):
            meta["exit_decision_evidence_status"] = "ORIGINAL_DECISION_BINDING_UNVERIFIED"
    return receipt


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




def assert_no_conflicting_us_legacy_stop_claim(
    conn, *, scope: USRecoveryScope, prospective_action_key: str,
) -> None:
    """Block a *new* legacy stop action while another action is unresolved.

    Historic V2 DHR soft-stop actions used fluctuating PnL/tick explanation
    text as the semantic key. A fresh stable STOP action MUST NOT bypass an
    earlier UNCERTAIN attempt simply by having a different action_key.
    Current/same-action retries still use the native claim/outbox fence.
    Only the exact verified account+epoch+owner+management cycle participates;
    this gate never cancels, resubmits, or promotes an uncertain broker order.
    """
    if (
        scope.strategy_owner != "US_STANDARD"
        or scope.env != "practice"
        or not scope.account_id or len(scope.account_id) != 64
        or not scope.trading_epoch_id or not scope.position_cycle_id
        or not prospective_action_key
    ):
        raise USRecoveryIdentityError("LEGACY_STOP_CLAIM_FENCE_SCOPE_UNVERIFIED")
    rows = conn.execute(text("""
        SELECT action_key,action_state FROM us_execution_claims
        WHERE env=:env AND account_id=:account AND market='US'
          AND trading_epoch_id=:epoch AND strategy_owner='US_STANDARD'
          AND position_cycle_id=:cycle
          AND action_key<>:prospective_key
          AND action_state IN (
            'OPEN', 'IN_FLIGHT', 'UNCERTAIN',
            'PARTIALLY_SATISFIED', 'RETRYABLE'
          )
        LIMIT 1 FOR UPDATE
    """), {
        "env": scope.env, "account": scope.account_id,
        "epoch": scope.trading_epoch_id, "cycle": scope.position_cycle_id,
        "prospective_key": prospective_action_key,
    }).mappings().all()
    if rows:
        raise USRecoveryIdentityError("US_LEGACY_STOP_OTHER_ACTION_UNRESOLVED")

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
