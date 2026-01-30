"""
time_coerce.py - Date/Datetime coercion utilities

Ensures consistent date handling across the codebase,
particularly for pb1_watchlist.as_of (DATE column).
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any


def to_date(x: Any) -> date:
    """
    Coerce x into datetime.date.
    Accepts date, datetime, 'YYYY-MM-DD' string.
    
    Args:
        x: Input value (date, datetime, or ISO string)
        
    Returns:
        datetime.date object
        
    Raises:
        ValueError: If x is None
        TypeError: If x type is unsupported
    """
    if x is None:
        raise ValueError("to_date(None) is not allowed")
    if isinstance(x, date) and not isinstance(x, datetime):
        return x
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, str):
        # allow 'YYYY-MM-DD' or longer like ISO string
        return datetime.strptime(x[:10], "%Y-%m-%d").date()
    raise TypeError(f"Unsupported date type: {type(x)} value={x!r}")
