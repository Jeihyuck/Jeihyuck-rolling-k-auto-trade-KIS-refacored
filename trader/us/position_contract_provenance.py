"""Preserve signed BUY policy when carrying derived position state.

Only the exact, valid original payload may replace a null-elided derivative
with the same signature. No re-signing, policy fallback or identity adoption.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any


def _without_nulls(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _without_nulls(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [_without_nulls(v) for v in value]
    return value


def restore_null_elided_policy(derived: dict, original: dict) -> dict | None:
    """Return a corrected derivative only for an exactly proven null loss."""
    from trader.us.entry_exit_contract import verify_us_entry_exit_contract

    old = derived.get("entry_exit_contract")
    source = original.get("entry_exit_contract")
    if not isinstance(old, dict) or not isinstance(source, dict):
        return None
    if verify_us_entry_exit_contract(old) or not verify_us_entry_exit_contract(source):
        return None
    digest = source.get("sha256")
    if (not digest or old.get("sha256") != digest
            or any(p.get("entry_exit_contract_sha256") not in (None, "", digest)
                   for p in (derived, original))
            or any(p.get("entry_exit_contract_version") not in
                   (None, "", source.get("version")) for p in (derived, original))
            or _without_nulls(source) != old):
        return None
    # Keep all non-contract derived fields; this is not a policy migration.
    restored = deepcopy(derived)
    restored.update({
        "entry_exit_contract": deepcopy(source),
        "entry_exit_contract_sha256": digest,
        "entry_exit_contract_version": source["version"],
    })
    return restored


def carry_entry_policy(derived: dict, original: dict) -> dict:
    return restore_null_elided_policy(derived, original) or deepcopy(derived)


def merge_original_order_contract_metadata(source: dict, incoming: dict) -> dict:
    """Preserve original execution-order BUY attribution across fill replay.

    KIS observations supply quantities, prices and execution status, never a
    replacement strategy contract. Null or absent derivative copies cannot
    erase durable original values. Explicit competing original evidence is
    a reconciliation conflict, not permission to re-sign or adopt it.
    """
    result = {**source, **incoming}
    immutable = (
        "entry_exit_contract", "entry_exit_contract_sha256",
        "entry_exit_contract_version", "parent_entry_contract_sha256",
        "source_entry_contract_sha256", "source_entry_contract_version",
        "us_original_buy_sst_v1",
    )
    for field in immutable:
        original = source.get(field)
        observed = incoming.get(field)
        if original not in (None, ""):
            if observed not in (None, "", original):
                # Broker/journal serialization may elide null leaves. This
                # is not a changed signed payload if exactly provable.
                if not (field == "entry_exit_contract"
                        and isinstance(original, dict)
                        and isinstance(observed, dict)
                        and _without_nulls(original) == observed):
                    raise ValueError("FILL_ORIGINAL_CONTRACT_CONFLICT:" + field)
            result[field] = deepcopy(original)
        elif field == "us_original_buy_sst_v1" and observed not in (None, ""):
            # A fill observer cannot manufacture the pre-submit receipt.
            raise ValueError("FILL_ORIGINAL_CONTRACT_CONFLICT:" + field)
    receipt = source.get("us_original_buy_sst_v1")
    if receipt is not None:
        if (not isinstance(receipt, dict)
                or not receipt.get("source_entry_contract_sha256")
                or receipt["source_entry_contract_sha256"] != result.get("source_entry_contract_sha256")
                or receipt.get("position_cycle_id") != result.get("position_cycle_id")
                or receipt.get("portfolio_epoch_id") != result.get("portfolio_epoch_id")):
            raise ValueError("FILL_ORIGINAL_CONTRACT_CONFLICT:receipt")
    return result
