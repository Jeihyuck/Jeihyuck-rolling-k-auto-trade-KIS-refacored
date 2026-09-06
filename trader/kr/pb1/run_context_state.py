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


def initialize_run_context_state(
    *,
    today: str,
    as_of: str | None,
    trade_date: str | None | Any,
    run_ctx: Any,
    derived_as_of: str | None,
) -> dict[str, str]:
    return resolve_run_context_state(
        today=today,
        as_of=as_of,
        trade_date=trade_date,
        run_ctx=run_ctx,
        derived_as_of=derived_as_of,
    )


def resolve_as_of_state(
    *,
    today: str,
    run_ctx: Any,
    derived_as_of: str | None,
    current_as_of: str | None,
    current_trade_date: str | None,
    current_source: str | None,
) -> dict[str, str]:
    if current_as_of:
        return {
            "as_of": str(current_as_of),
            "trade_date": str(current_trade_date or today),
            "as_of_source": str(current_source or "backfill"),
            "backfill": "0",
        }
    if isinstance(run_ctx, dict):
        run_ctx_as_of = run_ctx.get("derived_as_of") or run_ctx.get("as_of")
        run_ctx_trade_date = run_ctx.get("trade_date")
    else:
        run_ctx_as_of = getattr(run_ctx, "derived_as_of", None) or getattr(run_ctx, "as_of", None)
        run_ctx_trade_date = getattr(run_ctx, "trade_date", None)
    backfill_value = str(run_ctx_as_of or derived_as_of or today)
    if backfill_value:
        return {
            "as_of": backfill_value,
            "trade_date": str(current_trade_date or run_ctx_trade_date or today),
            "as_of_source": str(current_source or "backfill"),
            "backfill": "1",
        }
    raise RuntimeError("engine_as_of_missing")
