"""Broker order-number identity helpers.

KIS can return an ACK number padded with zeroes and subsequently return the
same number without padding in its fill inquiry API.  Keep the raw value for
audit, but use this value for identity and idempotency comparisons.
"""
from __future__ import annotations


def canonical_order_no(value: object) -> str | None:
    """Canonical comparison key while callers retain the unmodified raw value."""
    raw = str(value or "").strip()
    if not raw:
        return None
    return (raw.lstrip("0") or "0") if raw.isdigit() else raw


def normalize_us_order_no(value: object) -> str:
    """Return the stable KIS order number, with leading zeroes removed."""
    return canonical_order_no(value) or ""
