from __future__ import annotations

from typing import Any


def resolve_run_context_state(
    *,
    today: str,
    as_of: str | None,
    trade_date: str | None | Any,
    run_ctx: Any,
    derived_as_of: str | None,
) -> dict[str, str]:
    run_ctx_as_of = None
    if isinstance(run_ctx, dict):
        run_ctx_as_of = run_ctx.get("derived_as_of") or run_ctx.get("as_of")
    else:
        run_ctx_as_of = getattr(run_ctx, "derived_as_of", None) or getattr(run_ctx, "as_of", None)
    resolved_as_of = str(as_of or run_ctx_as_of or derived_as_of or today)
    resolved_trade_date = trade_date
    if resolved_trade_date is None:
        if isinstance(run_ctx, dict):
            resolved_trade_date = run_ctx.get("trade_date")
        else:
            resolved_trade_date = getattr(run_ctx, "trade_date", None)
    if as_of:
        as_of_source = "explicit_as_of"
    elif run_ctx_as_of:
        as_of_source = "run_ctx"
    elif derived_as_of:
        as_of_source = "derived_as_of"
    else:
        as_of_source = "fallback_resolver"
    return {
        "as_of": resolved_as_of,
        "trade_date": str(resolved_trade_date or today),
        "as_of_source": as_of_source,
    }
