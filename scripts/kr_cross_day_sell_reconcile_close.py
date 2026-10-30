"""Post-trading KR PB1 historical SELL settlement, no order submission.

Invoked by WSL close wrapper only *after* the trader child has returned
successfully. This is a separate maintenance child, not a critical trading tick.
No broker BUY/SELL/cancel API is called. Old unproven orders remain pending.
"""
from __future__ import annotations

import json
import os
from uuid import uuid4

from trader.time_utils import now_kst


def main() -> int:
    if not (
        os.getenv("KIS_ENV", "").lower() == "practice"
        and os.getenv("STRATEGY_ENV", "").lower() == "practice"
        and os.getenv("PB1_SESSION", "").lower() == "close"
        and os.getenv("WSL_RUN_SESSION", "").lower() == "close"
        and os.getenv("DISABLE_REAL_TRADING") == "1"
        and os.getenv("NO_TRADE") == "1"
        and os.getenv("WSL_RUN_MARKET", "").upper() == "KR"
    ):
        print("[KR_CROSS_DAY_SELL][BLOCK] practice_close_read_only_contract_missing")
        return 3

    # kis_wrapper uses settings.API_BASE_URL, not KIS_REST_URL; validate the
    # actual endpoint before constructing KisAPI (which can request a token).
    from settings import API_BASE_URL, KIS_ENV as EFFECTIVE_KIS_ENV
    if EFFECTIVE_KIS_ENV != "practice" or not str(API_BASE_URL).startswith(
        "https://openapivts.koreainvestment.com"
    ):
        print("[KR_CROSS_DAY_SELL][BLOCK] effective_KIS_endpoint_not_practice")
        return 3

    from trader.db.engine import get_engine
    from trader.kis_wrapper import KisAPI
    from trader.run_context import RunContext
    from trader.kr.cross_day_sell_auto_reconcile import run_all_historical_sell_recovery

    engine = get_engine()
    try:
        kis = KisAPI(kis_env="practice")
        if str(getattr(kis, "env", "")).lower() != "practice":
            raise RuntimeError("KR_CROSS_DAY_SELL_NOT_PRACTICE_KIS")
        ctx = RunContext(
            run_id=str(uuid4()), env="practice", strategy="pb1_pullback_close",
            started_at=now_kst(), dry_run=False,
        )
        outcome = run_all_historical_sell_recovery(
            engine=engine, kis=kis, env="practice",
            strategy="pb1_pullback_close", tick_ts=now_kst(), ctx=ctx,
        )
        print("[KR_CROSS_DAY_SELL][RESULT] " + json.dumps(
            outcome, ensure_ascii=False, default=str, sort_keys=True,
        ))
        if outcome.get("state") in {"BATCH_COMPLETE", "NO_PRIOR_DAY_PENDING_SELL"}:
            return 0
        if outcome.get("state") == "BATCH_WITH_PENDING":
            return 2
        return 3
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
