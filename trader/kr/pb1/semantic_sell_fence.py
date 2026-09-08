from __future__ import annotations

from typing import Any

from trader.kr.pb1_stability import same_day_semantic_sell_exists


def same_day_semantic_sell_blocked(*, orders_repo: Any, env: str, code: str, strategy_owner: str,
                                   reason_family: str, lifecycle_id: str, logger: Any | None = None) -> bool:
    try:
        rows = orders_repo.list_today_orders(env, side="SELL", code=str(code).zfill(6), status_exclude=())
    except Exception as exc:
        if logger is not None:
            logger.warning("[PB1][SEMANTIC_SELL_FENCE][LOOKUP_WARN] code=%s err=%s", str(code).zfill(6), exc)
        return False
    return same_day_semantic_sell_exists(
        rows=rows,
        symbol=code,
        strategy_owner=str(strategy_owner or "KR_STANDARD"),
        reason_family=reason_family,
        lifecycle_id=lifecycle_id,
    )
