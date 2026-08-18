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
        state = market.get("state") or snapshot.get("market_state") or snapshot.get("market_regime")
        quality = str(market.get("data_quality") or snapshot.get("data_quality") or "BLOCKED")
        if snapshot.get("regime_quality"):
            quality = str(snapshot["regime_quality"])
    else:
        return InfiniteRegimeView(None, "BLOCKED", False)
    return InfiniteRegimeView(str(state) if state else None, quality, bool(state))


def load_canonical_snapshot(path: str | Path | None = None) -> InfiniteRegimeView:
    expected = os.getenv("KR_TRADE_DATE")
    candidates = [Path(path)] if path else []
    if not candidates:
        if expected:
            candidates.append(Path("runtime/kr/watchlist") / expected / "prep_contract.json")
        candidates.extend((Path("signals/kr/latest_prep_contract.json"), Path("artifacts/kr_regime_snapshot.json")))
    for candidate in candidates:
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
            as_of = payload.get("as_of") if isinstance(payload, dict) else None
            trade_date = payload.get("trade_date") if isinstance(payload, dict) else None
            if expected and ((as_of and datetime.fromisoformat(str(as_of).replace("Z", "+00:00")).date() != date.fromisoformat(expected)) or (trade_date and str(trade_date) != expected)):
                continue
            view = from_canonical_snapshot(payload)
            if view.available:
                return view
        except (OSError, ValueError, TypeError):
            continue
    return InfiniteRegimeView(None, "BLOCKED", False)
