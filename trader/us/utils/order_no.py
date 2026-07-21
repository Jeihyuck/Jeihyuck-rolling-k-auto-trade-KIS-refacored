"""Broker order-number identity helpers.

KIS can return an ACK number padded with zeroes and subsequently return the
same number without padding in its fill inquiry API.  Keep the raw value for
audit, but use this value for identity and idempotency comparisons.
"""
from __future__ import annotations


def normalize_us_order_no(value: object) -> str:
    """Return the stable KIS order number, with leading zeroes removed."""
    text = str(value or "").strip().replace(" ", "")
    if not text:
        return ""
    return text.lstrip("0") or "0"
