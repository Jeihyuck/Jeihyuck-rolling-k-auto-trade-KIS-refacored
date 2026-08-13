from __future__ import annotations

from dataclasses import replace
from datetime import date
from decimal import Decimal
from typing import Iterable, Mapping
from .models import CycleStatus, SleeveState


def rebuild_from_fills(state: SleeveState, fills: Iterable[Mapping], broker_quantity: int) -> SleeveState:
    """Rebuild accounting from authoritative fills; an ACK is intentionally ignored."""
    buys = sells = Decimal(0); bought = sold = 0; last_buy = last_sell = None
    for fill in fills:
        qty, price = int(fill["qty"]), Decimal(str(fill["price"]))
        fee, tax = Decimal(str(fill.get("fee", 0))), Decimal(str(fill.get("tax", 0)))
        when = fill.get("trade_date")
        when = date.fromisoformat(when) if isinstance(when, str) else when
        if str(fill["side"]).upper() == "BUY":
            bought += qty; buys += price * qty + fee; last_buy = max(filter(None, (last_buy, when)), default=None)
        else:
            sold += qty; sells += price * qty - fee - tax; last_sell = max(filter(None, (last_sell, when)), default=None)
    db_qty = bought - sold
    if db_qty != broker_quantity:
        return replace(state, status=CycleStatus.RECONCILE_PENDING, reconciled=False)
    avg = buys / bought if bought else Decimal(0)
    complete = broker_quantity == 0 and bought > 0 and sold >= bought
    return replace(state, status=CycleStatus.COMPLETE if complete else (CycleStatus.ACTIVE if broker_quantity else CycleStatus.READY),
                   filled_quantity=broker_quantity, buy_notional=buys, sell_notional=sells,
                   average_price=avg, last_buy_date=last_buy, last_sell_date=last_sell,
                   pending_order_key=None, reconciled=True)


def ownership_check(broker_quantity: int, known_fill_quantity: int, adoption_approved: bool = False) -> CycleStatus | None:
    if broker_quantity and not adoption_approved and broker_quantity != known_fill_quantity:
        return CycleStatus.OWNERSHIP_CONFLICT
    return None
