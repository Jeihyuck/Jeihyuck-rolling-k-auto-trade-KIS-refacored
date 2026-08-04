"""Fail-closed, decimal take-profit calculations shared by strategy and router."""
from __future__ import annotations

from decimal import Decimal, InvalidOperation


def as_decimal(value: object, *, name: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"invalid_{name}") from exc
    if not result.is_finite() or result <= 0:
        raise ValueError(f"invalid_{name}")
    return result


def calc_return_rate(executable_price: Decimal, broker_avg_price: Decimal) -> Decimal:
    """Return a fraction (``.03`` means 3%), never a price delta."""
    if broker_avg_price <= 0:
        raise ValueError("invalid_broker_avg_price")
    if executable_price <= 0:
        raise ValueError("invalid_executable_price")
    return (executable_price - broker_avg_price) / broker_avg_price

