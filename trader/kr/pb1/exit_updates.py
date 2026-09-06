from __future__ import annotations

from typing import Any


def build_exit_position_update_fields(
    *,
    current_max_price: float,
    mark: float,
    stop_price: float | None,
    stop_price_missing: bool,
) -> dict[str, Any]:
    new_max = max(float(current_max_price or 0.0), float(mark or 0.0))
    fields: dict[str, Any] = {"max_price": new_max}
    if stop_price is not None and stop_price_missing:
        fields["stop_price"] = stop_price
    return fields
