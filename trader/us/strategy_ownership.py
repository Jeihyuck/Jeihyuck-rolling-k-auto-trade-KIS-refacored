"""Central symbol-to-strategy ownership contract for US trading."""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

TQQQ_SYMBOL = "TQQQ"
TQQQ_OWNER = "TQQQ_INFINITE"
STANDARD_OWNER = "US_STANDARD"


def owner_for_symbol(symbol: object) -> str:
    return TQQQ_OWNER if str(symbol or "").upper().strip() == TQQQ_SYMBOL else STANDARD_OWNER


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
