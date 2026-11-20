"""Read-only, exact original BUY contract fallback for broken runtime copies.

Only a *previously signed and native-fill-proven* original may replace an
invalid derivative in an in-memory US_STANDARD position. Never re-sign,
mutate stored records, infer a different trade or bypass KIS SELL fences.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date
from typing import Callable



def _no_intervening_order_for_original(
    *, symbol: str, epoch: str, account: str, portfolio: str, cycle: str,
    original_trade_date: str, current_trade_date: str,
    original_client_key: str, original_order_no: str,
) -> bool:
    """Read-only PostgreSQL proof of a single untouched BUY cycle.

    Never infer zero intervening activity from current share count alone:
    an ADD and a SELL could cancel numerically while changing source lots.
    """
    import hashlib
    from sqlalchemy import text
    from trader.us.db.repos import _get_engine_or_none

    engine = _get_engine_or_none()
    if engine is None:
        return False
    hashed = hashlib.sha256(account.encode("utf-8")).hexdigest()
    with engine.connect() as conn:
        amount = conn.execute(text("""
            SELECT COUNT(*)
              FROM us_orders
             WHERE trading_epoch_id=:epoch AND account_id=:account_hash
               AND portfolio_epoch_id=:portfolio AND position_cycle_id=:cycle
               AND symbol=:symbol AND side IN ('BUY','SELL')
               AND trade_date BETWEEN CAST(:opened AS date) AND CAST(:today AS date)
               AND (
                   client_order_key IS DISTINCT FROM :source_key
                   OR order_no IS DISTINCT FROM :source_no
               )
        """), {
            "epoch": epoch, "account_hash": hashed, "portfolio": portfolio,
            "cycle": cycle, "symbol": symbol, "opened": original_trade_date,
            "today": current_trade_date,
            "source_key": original_client_key, "source_no": original_order_no,
        }).scalar_one()
    return int(amount) == 0


def restore_verified_us_original_contract(
    position: dict, *, identity: dict[str, str],
    current_trade_date: str,
    original_lookup: Callable[..., dict | None] | None = None,
    no_intervening_lookup: Callable[..., bool] | None = None,
) -> tuple[dict | None, str]:
    """Return an ephemeral corrected copy, or fail closed with a reason.

    Conservative scope: the original BUY has exactly the presently held
    quantity (no ADD, partial SELL or unknown share delta). Other lifecycle
    paths continue with their existing original policy and KIS checks.
    """
    from trader.us.entry_exit_contract import (
        us_entry_exit_contract_integrity_state, verify_us_entry_exit_contract,
    )
    from trader.us.original_buy_lineage import (
        _exact_positive_shares, load_verified_us_original_buy_scope,
    )

    if not isinstance(position, dict) or not isinstance(identity, dict):
        return None, "INVALID_INPUT"
    meta = position.get("meta") if isinstance(position.get("meta"), dict) else {}
    symbol = str(position.get("symbol") or "").strip().upper()
    owner = str(position.get("strategy_owner") or meta.get("strategy_owner") or "").strip().upper()
    if symbol == "TQQQ" or owner != "US_STANDARD":
        return None, "OWNER_NOT_STANDARD"
    if us_entry_exit_contract_integrity_state(position, meta) != "INVALID":
        return None, "NO_INVALID_RUNTIME_COPY"

    raw_account = str(identity.get("account_id") or "")
    epoch = str(identity.get("trading_epoch_id") or "")
    environment = str(identity.get("env") or "").lower()
    portfolio = str(position.get("portfolio_epoch_id") or meta.get("portfolio_epoch_id") or "")
    cycle = str(position.get("position_cycle_id") or meta.get("position_cycle_id") or "")
    lifecycle = str(position.get("position_lifecycle_id") or meta.get("position_lifecycle_id") or "")
    qty = _exact_positive_shares(position.get("qty"))
    opened = str(
        position.get("opened_trade_date") or meta.get("opened_trade_date") or ""
    )
    try:
        original_day = date.fromisoformat(opened)
        current_day = date.fromisoformat(current_trade_date)
    except (ValueError, TypeError):
        return None, "ORIGINAL_TRADE_DAY_UNVERIFIED"
    if (not symbol or not raw_account or not epoch or environment != "practice"
            or not portfolio or not cycle or not lifecycle or cycle != lifecycle
            or qty is None or original_day > current_day):
        return None, "ORIGINAL_BUY_SCOPE_UNVERIFIED"
    # Every claimed identity must agree before any DB lookup. The account
    # dimension is canonicalized separately by the trusted scope adapter.
    for key, expected in (
        ("trading_epoch_id", epoch),
        ("portfolio_epoch_id", portfolio),
        ("position_cycle_id", cycle),
        ("position_lifecycle_id", lifecycle),
    ):
        for source in (position, meta):
            found = source.get(key)
            if found not in (None, "") and str(found) != expected:
                return None, "CONFLICTING_POSITION_SCOPE"
    if (str(position.get("trading_epoch_id") or meta.get("trading_epoch_id") or "")
            != epoch):
        return None, "TRADING_EPOCH_MISMATCH"

    reader = original_lookup if original_lookup is not None else load_verified_us_original_buy_scope
    proof = reader(
        symbol=symbol, trade_date=opened, qty=qty,
        identity={"env": environment, "account_id": raw_account,
                  "trading_epoch_id": epoch, "strategy_owner": owner},
    )
    if not isinstance(proof, dict):
        return None, "ORIGINAL_NATIVE_BUY_UNVERIFIED"
    source_meta = proof.get("meta") if isinstance(proof.get("meta"), dict) else {}
    signed = source_meta.get("entry_exit_contract")
    if not isinstance(signed, dict) or not verify_us_entry_exit_contract(signed):
        return None, "ORIGINAL_POLICY_HASH_INVALID"
    if not (
        proof.get("symbol") == symbol and proof.get("strategy_owner") == owner
        and proof.get("account_id") == raw_account
        and str(proof.get("trading_epoch_id") or "") == epoch
        and str(proof.get("portfolio_epoch_id") or "") == portfolio
        and str(proof.get("position_cycle_id") or "") == cycle
        and str(proof.get("position_lifecycle_id") or "") == lifecycle
        and _exact_positive_shares(proof.get("qty")) == qty
        and proof.get("side") == "BUY"
        and proof.get("order_no") and proof.get("client_order_key")
        and proof.get("filled_at")
        and source_meta.get("lineage_resolution") == "EXACT_ORIGINAL_KIS_CUMULATIVE_FULL_BUY"
    ):
        return None, "ORIGINAL_BUY_PROOF_SCOPE_MISMATCH"

    # An identical current quantity is not sufficient after an ADD/SELL
    # round-trip or open submit attempt. Require zero other local orders in
    # the same verified original lifecycle after the source native BUY.
    checker = no_intervening_lookup or _no_intervening_order_for_original
    try:
        no_other_order = checker(
            symbol=symbol, epoch=epoch, account=raw_account,
            portfolio=portfolio, cycle=cycle,
            original_trade_date=opened, current_trade_date=current_trade_date,
            original_client_key=str(proof["client_order_key"]),
            original_order_no=str(proof["order_no"]),
        )
    except Exception:
        no_other_order = False
    if no_other_order is not True:
        return None, "ORIGINAL_CYCLE_INTERVENING_ORDER_UNVERIFIED"

    digest = signed["sha256"]
    for source in (position, meta):
        declared = source.get("entry_exit_contract_sha256")
        declared_version = source.get("entry_exit_contract_version")
        derivative = source.get("entry_exit_contract")
        if declared not in (None, "", digest):
            return None, "DECLARED_ORIGINAL_HASH_CONFLICT"
        if declared_version not in (None, "", signed["version"]):
            return None, "DECLARED_ORIGINAL_VERSION_CONFLICT"
        if isinstance(derivative, dict):
            if (derivative.get("sha256") != digest
                    or derivative.get("version") != signed["version"]):
                return None, "DERIVATIVE_ORIGINAL_IDENTITY_CONFLICT"

    restored = deepcopy(position)
    restored_meta = deepcopy(meta)
    restored_meta.update(
        entry_exit_contract=deepcopy(signed),
        entry_exit_contract_sha256=digest,
        entry_exit_contract_version=signed["version"],
    )
    restored.update(
        meta=restored_meta,
        entry_exit_contract=deepcopy(signed),
        entry_exit_contract_sha256=digest,
        entry_exit_contract_version=signed["version"],
        _verified_original_policy_source="EXACT_ORIGINAL_KIS_CUMULATIVE_FULL_BUY",
        _runtime_derivative_policy_anomaly="INVALID_ORIGINAL_COPY_NOT_PERSISTED",
    )
    if us_entry_exit_contract_integrity_state(restored, restored_meta) != "VALID":
        return None, "RESTORED_ORIGINAL_POLICY_UNVERIFIED"
    return restored, "RESTORED_FROM_VERIFIED_NATIVE_ORIGINAL"
