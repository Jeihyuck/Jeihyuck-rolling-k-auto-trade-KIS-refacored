"""Read-only, exact cross-owner proof for KR_INFINITE KIS daily executions.

The PB1 account reconciler must not invent PB1 orders/fills for another sleeve.
An authenticated broker order can be *classified* as already owned by the
KR_INFINITE durable journal only when all independent identities agree.
Unknown, ambiguous, stale-epoch or contradictory evidence remains fail-closed.
"""
from __future__ import annotations

from datetime import date
from typing import Any

import sqlalchemy as sa


def _positive_int(value: Any) -> int | None:
    try:
        result = int(str(value).replace(",", "").strip())
        return result if result > 0 else None
    except (TypeError, ValueError):
        return None


def _native_int(row: dict, *keys: str) -> tuple[bool, int | None]:
    for key in keys:
        if key in row and row[key] not in ("", None):
            return True, _positive_int(row[key])
    return False, None


def prove_kr_infinite_daily_owner(
    *,
    engine: Any,
    scope: Any,
    broker_row: dict,
    code: str,
    side: str,
    kis_odno: str | None,
    order_date: date,
    broker_authenticated: bool,
) -> dict | None:
    """Return attested owner evidence, *never* authorization or a synthetic order.

    The KIS response must be authenticated for this exact account. The active
    trading epoch is resolved externally from the same authenticated account;
    there is no matching by symbol/order-number alone and no DB writes.
    """
    if (
        not broker_authenticated or scope is None
        or str(getattr(scope, "env", "")) not in {"practice", "real", "live"}
        or not getattr(scope, "account_key", None)
        or not getattr(scope, "trading_epoch_id", None)
        or not isinstance(order_date, date)
        or str(side) not in {"BUY", "SELL"}
        or not str(code).isdigit() or len(str(code)) != 6
        or not str(kis_odno or "").strip().isdigit()
        or not isinstance(broker_row, dict)
    ):
        return None
    odno = str(kis_odno).strip().zfill(10)
    if len(odno) != 10:
        return None

    order_present, native_order_qty = _native_int(
        broker_row, "ord_qty", "ord_qty_sum", "qty"
    )
    fill_present, native_fill_qty = _native_int(
        broker_row, "tot_ccld_qty", "ccld_qty", "filled_qty"
    )
    # A durable FILLED alone cannot substitute for a matching native KIS
    # order quantity. If KIS explicitly reports 0/invalid fills, do not hide it.
    if not order_present or native_order_qty is None:
        return None
    if not fill_present or native_fill_qty is None:
        return None

    query = sa.text("""
        SELECT i.id, i.strategy_id, i.cycle_id, i.requested_qty,
               i.filled_qty, i.status, i.trading_epoch_id,
               s.strategy_id AS state_owner, s.trading_epoch_id AS state_epoch
          FROM kr_infinite_order_intents AS i
          JOIN kr_infinite_state AS s
            ON s.strategy_id = i.strategy_id
           AND s.symbol = i.symbol
           AND s.cycle_id = i.cycle_id
           AND s.trading_epoch_id = i.trading_epoch_id
         WHERE i.strategy_id = 'KR_INFINITE_V1'
           AND i.symbol = :code
           AND i.side = :side
           AND i.broker_order_id = :odno
           AND i.trade_date = :trade_date
           AND i.trading_epoch_id = :epoch
         LIMIT 2
    """)
    with engine.connect() as conn:
        rows = conn.execute(query, {
            "code": code,
            "side": side,
            "odno": odno,
            "trade_date": order_date,
            "epoch": str(scope.trading_epoch_id),
        }).mappings().all()
    if len(rows) != 1:
        return None
    intent = rows[0]
    requested = _positive_int(intent["requested_qty"])
    filled = _positive_int(intent["filled_qty"])
    if (
        intent["strategy_id"] != "KR_INFINITE_V1"
        or intent["state_owner"] != "KR_INFINITE_V1"
        or str(intent["state_epoch"]) != str(scope.trading_epoch_id)
        or str(intent["trading_epoch_id"]) != str(scope.trading_epoch_id)
        or str(intent["status"]) != "FILLED"
        or requested is None or filled is None
        or requested != native_order_qty
        or filled != requested
        or (fill_present and native_fill_qty != filled)
    ):
        return None
    return {
        "owner": "KR_INFINITE_V1",
        "intent_id": int(intent["id"]),
        "code": code,
        "side": side,
        "kis_odno": odno,
        "trade_date": order_date.isoformat(),
        "filled_qty": filled,
        "trading_epoch_id": str(scope.trading_epoch_id),
        "account_verified_by": "ACTIVE_EPOCH_AND_AUTHENTICATED_KIS_ADAPTER",
    }
