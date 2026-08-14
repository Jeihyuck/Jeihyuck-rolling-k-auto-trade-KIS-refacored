"""Read-only adapter for the canonical dual-agent KR regime snapshot."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class InfiniteRegimeView:
    state: str | None
    data_quality: str
    available: bool


def from_canonical_snapshot(snapshot: Any) -> InfiniteRegimeView:
    if snapshot is None:
        return InfiniteRegimeView(None, "BLOCKED", False)
    if hasattr(snapshot, "market_states"):
        market = (snapshot.market_states or {}).get("KOSPI")
        state = getattr(market, "state", None)
        quality = str(getattr(market, "data_quality", None) or getattr(snapshot, "data_quality", "BLOCKED"))
    elif isinstance(snapshot, dict):
        market = (snapshot.get("market_states") or {}).get("KOSPI") or {}
        state = market.get("state")
        quality = str(market.get("data_quality") or snapshot.get("data_quality") or "BLOCKED")
    else:
        return InfiniteRegimeView(None, "BLOCKED", False)
    return InfiniteRegimeView(str(state) if state else None, quality, bool(state))


def load_canonical_snapshot(path: str | Path = "artifacts/kr_regime_snapshot.json") -> InfiniteRegimeView:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        expected = os.getenv("KR_TRADE_DATE")
        as_of = payload.get("as_of") if isinstance(payload, dict) else None
        if expected and as_of and datetime.fromisoformat(str(as_of).replace("Z", "+00:00")).date() != date.fromisoformat(expected):
            return InfiniteRegimeView(None, "BLOCKED", False)
        return from_canonical_snapshot(payload)
    except (OSError, ValueError, TypeError):
        return InfiniteRegimeView(None, "BLOCKED", False)
