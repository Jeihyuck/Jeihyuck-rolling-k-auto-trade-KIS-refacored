"""Central symbol-to-strategy ownership contract for US trading."""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

TQQQ_SYMBOL = "TQQQ"
TQQQ_OWNER = "TQQQ_INFINITE"
STANDARD_OWNER = "US_STANDARD"


def owner_for_symbol(symbol: object) -> str:
    return TQQQ_OWNER if str(symbol or "").upper().strip() == TQQQ_SYMBOL else STANDARD_OWNER


def is_standard_owned_position(row: dict) -> bool:
    """Broker position belongs to PB1 only when both symbol and durable owner agree.

    Legacy non-dedicated positions may omit owner, but an explicit different
    owner (or dedicated symbol) cannot cross into US_STANDARD exit policy.
    """
    if not isinstance(row, dict):
        return False
    sym = row.get("symbol") or row.get("code")
    if owner_for_symbol(sym) != STANDARD_OWNER:
        return False
    meta = row.get("meta") if isinstance(row.get("meta"), dict) else {}
    asserted = str(row.get("strategy_owner") or meta.get("strategy_owner") or STANDARD_OWNER).upper()
    return asserted == STANDARD_OWNER


def is_standard_owned_fill(row: dict) -> bool:
    return is_standard_owned_position(row)


def is_standard_symbol(symbol: object, *, log: bool = False) -> bool:
    standard = owner_for_symbol(symbol) == STANDARD_OWNER
    if not standard and log:
        logger.info("[US_STANDARD][SYMBOL_EXCLUDED] symbol=TQQQ owner=TQQQ_INFINITE")
    return standard


def exclude_non_standard(rows: list[dict] | None) -> list[dict]:
    result: list[dict] = []
    for row in rows or []:
        symbol = row.get("symbol") or row.get("code")
        if is_standard_symbol(symbol, log=True):
            result.append(row)
    return result


def is_valid_tqqq_intent(intent: dict) -> bool:
    """TQQQ may cross the broker boundary only from its dedicated sleeve."""
    if owner_for_symbol(intent.get("symbol")) != TQQQ_OWNER:
        return True
    return (
        intent.get("strategy_owner") == TQQQ_OWNER
        and intent.get("sleeve_id") == TQQQ_OWNER
    )
