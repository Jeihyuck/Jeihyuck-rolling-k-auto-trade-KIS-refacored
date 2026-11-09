"""CLOSE historical settlement boundary, distinct from AM/PM SELL-first tick.

This module is only called by the separately fenced CLOSE owner.  It performs
full order/fill reconciliation, including historical contract and position
repair, and returns the original reconciliation evidence unchanged.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any


def reconcile_close_historical_once(
    *, engine: Any, kis: Any, env: str, run_id: str,
    tick_ts: datetime, runtime_root_dir: str | Path,
) -> dict[str, Any]:
    from trader.reconcile_kis import reconcile_kis
    return reconcile_kis(
        engine=engine, kis=kis, env=env, run_id=run_id,
        strategy="pb1_pullback_close", tick_ts=tick_ts,
        runtime_dir=str(runtime_root_dir), historical_recovery=True,
    )
