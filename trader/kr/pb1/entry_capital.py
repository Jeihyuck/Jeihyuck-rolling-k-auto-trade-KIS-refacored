from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def resolve_entry_capital(
    *,
    env: str | None,
    intended_live: bool,
    base_cash_krw: int | None = None,
    available_cash_krw: int | None = None,
    override_capital: float | None,
    reserve_pct: float,
    paper_max_capital_krw: int,
    cap_cap: float,
) -> tuple[int, int, dict]:
    if base_cash_krw is None:
        base_cash_krw = int(available_cash_krw or 0)
    raw_base_cash_krw = int(base_cash_krw or 0)
    effective_base_cash_krw = raw_base_cash_krw
    clamp_meta = {}
    if (env or "").strip().lower() in {"paper", "practice"}:
        cap = int(paper_max_capital_krw)
        if cap > 0 and effective_base_cash_krw > cap:
            before = effective_base_cash_krw
            effective_base_cash_krw = cap
            clamp_meta = {"before": before, "cap": cap, "after": effective_base_cash_krw}
            logger.info(
                "[PB1][CAPITAL][CLAMP] before=%s cap=%s after=%s reason=paper_limit",
                before,
                cap,
                effective_base_cash_krw,
            )
    use_override = override_capital is not None and int(override_capital) > 0
    usable = max(int(effective_base_cash_krw * (1 - reserve_pct)), 0)
    entry_capital = usable
    if use_override:
        entry_capital = min(entry_capital, int(override_capital))
    cap_limit = None
    cap_applied = False
    if intended_live and cap_cap and cap_cap > 0:
        cap_limit = int(effective_base_cash_krw * cap_cap) if cap_cap <= 1 else int(cap_cap)
        if cap_limit > 0 and entry_capital > cap_limit:
            entry_capital = cap_limit
            cap_applied = True
    meta = {
        "use_override": use_override,
        "reserve_pct": reserve_pct,
        "cap_limit": cap_limit,
        "cap_applied": cap_applied,
        "clamp": clamp_meta,
        "base_cash": effective_base_cash_krw,
        "raw_base_cash": raw_base_cash_krw,
    }
    return entry_capital, usable, meta
