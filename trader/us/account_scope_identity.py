"""Exact raw/broker-hash identity comparison for US_STANDARD account scopes.

Never infer account type from length; never log or persist the raw key.
This adapter compares current trusted account identity to pre-existing claims.
It is not proof of original BUY, policy, epoch or KIS execution by itself.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class VerifiedAccountScope:
    raw_account: str
    broker_account_hash: str


def verify_us_account_scope(
    *, trusted_raw_account: str, claims: Iterable[str],
    declared_hashes: Iterable[str] = (),
) -> VerifiedAccountScope | None:
    raw = str(trusted_raw_account or "").strip()
    if not raw:
        return None
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    values = [str(v).strip() for v in claims if v not in (None, "")]
    hashes = [str(v).strip() for v in declared_hashes if v not in (None, "")]
    # A matching digest establishes only same-account identity; other
    # dimensions remain mandatory at the existing execution/stop gates.
    if not values or any(value not in {raw, digest} for value in values):
        return None
    if any(value != digest for value in hashes):
        return None
    return VerifiedAccountScope(raw_account=raw, broker_account_hash=digest)
