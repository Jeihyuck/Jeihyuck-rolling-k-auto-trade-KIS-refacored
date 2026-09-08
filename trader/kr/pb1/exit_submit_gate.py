from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def resolve_exit_submit_gate_reasons(
    *,
    order_precheck_gate_reasons: list[str],
    display_code: str,
    order_allowed: bool,
    trading_day: bool,
    force_block_live: bool,
    exit_holdings_source: str,
) -> list[str]:
    reasons = list(order_precheck_gate_reasons)
    logger.info(
        "[EXIT][SUBMIT_GATE] code=%s order_allowed=%s trading_day=%s force_block_live=%s source=%s action=%s",
        display_code,
        int(bool(order_allowed)),
        int(bool(trading_day)),
        int(bool(force_block_live)),
        str(exit_holdings_source or "unknown"),
        "skip_before_precheck" if reasons else "allow",
    )
    return reasons
