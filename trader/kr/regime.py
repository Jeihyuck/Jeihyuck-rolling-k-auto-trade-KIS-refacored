"""Single source of truth for Korean market regime and entry policy.

The engine deliberately consumes already-normalised observations.  Fetching is kept
outside this module so the same calculation is used by PREP, intraday and tests.
Missing mandatory observations are *blocked*, never silently replaced by an ETF.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
from typing import Any, Mapping

logger = logging.getLogger(__name__)

MARKETS = ("KOSPI", "KOSDAQ")
STATE_ORDER = (
    "KR_DEFENSE_CRASH", "KR_DEFENSE_RISK_OFF", "KR_DEFENSE_CAUTION",
    "KR_NORMAL", "KR_RISK_ON", "KR_STRONG_RISK_ON",
)


@dataclass(frozen=True)
class KRMarketState:
    market: str
    score: float
    state: str
    data_quality: str
    components: dict[str, float] = field(default_factory=dict)
    reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class KRExecutionPolicy:
    budget_multiplier: float
    max_new_positions: int | None
    allow_new_buy: bool
    allow_add_to_existing: bool
    gross_exposure_cap: float = .95
    single_position_cap: float = .10
    sector_cap: float = .35
    high_beta_sector_cap: float = .45


@dataclass(frozen=True)
class KRRegimeSnapshot:
    as_of: str
    global_state: str
    market_states: dict[str, KRMarketState]
    data_quality: str
    execution_policy: KRExecutionPolicy
    sector_leaders: tuple[str, ...] = ()
    source: str = "PREP"
    schema_version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class KRRegimeStabilizer:
    """Stateful intraday anti-flap gate (crash immediate, upgrades conservative)."""

    def __init__(self, initial_state: str = "KR_NORMAL") -> None:
        self.state = initial_state
        self._candidate: str | None = None
        self._count = 0
        self._last_change_at: datetime | None = None

    def update(self, proposed: str, now: datetime) -> str:
        current_rank, proposed_rank = STATE_ORDER.index(self.state), STATE_ORDER.index(proposed)
        if proposed == "KR_DEFENSE_CRASH":
            return self._accept(proposed, now)
        if proposed == self.state:
            self._candidate, self._count = None, 0
            return self.state
        if self._candidate != proposed:
            self._candidate, self._count = proposed, 1
        else:
            self._count += 1
        elapsed = (now - self._last_change_at).total_seconds() if self._last_change_at else float("inf")
        if proposed_rank < current_rank:  # ordinary downgrade: two consecutive ticks
            return self._accept(proposed, now) if self._count >= 2 else self.state
        # Upgrade cooldown, three ticks (or a candidate held for ten minutes), and
        # never jump more than one level inside the cooldown window.
        if elapsed < 600:
            proposed = STATE_ORDER[min(current_rank + 1, proposed_rank)]
        return self._accept(proposed, now) if self._count >= 3 and elapsed >= 600 else self.state

    def _accept(self, state: str, now: datetime) -> str:
        self.state, self._candidate, self._count, self._last_change_at = state, None, 0, now
        return state


def _number(data: Mapping[str, Any], key: str) -> float | None:
    value = data.get(key)
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def state_for_score(score: float) -> str:
    if score <= -60: return STATE_ORDER[0]
    if score <= -30: return STATE_ORDER[1]
    if score <= -10: return STATE_ORDER[2]
    if score < 20: return STATE_ORDER[3]
    if score < 50: return STATE_ORDER[4]
    return STATE_ORDER[5]


def calculate_market_state(market: str, observations: Mapping[str, Any], *,
                           sector_data_suspect: bool = False,
                           account_kill_switch: bool = False) -> KRMarketState:
    """Calculate the documented -100..100 score for one independent market."""
    market = market.upper()
    if market not in MARKETS:
        raise ValueError(f"unsupported Korean market: {market}")
    required = ("close", "ma20", "ma50", "ma200", "ma20_slope_5d",
                "breadth_ma20", "breadth_ma50", "advance_ratio",
                "median_return_5d", "return_5d", "return_20d")
    missing = tuple(key for key in required if _number(observations, key) is None)
    stale = bool(observations.get("stale"))
    if missing or stale:
        reasons = (("stale_required_data",) if stale else ()) + tuple(f"missing_{x}" for x in missing)
        return KRMarketState(market, -100.0, STATE_ORDER[0], "BLOCKED", {}, reasons)

    v = {key: _number(observations, key) for key in observations}
    trend = sum((10 if condition else -10) for condition in (
        v["close"] > v["ma20"], v["ma20"] > v["ma50"],
        v["ma50"] > v["ma200"], v["ma20_slope_5d"] > 0,
    ))
    # Breadth: four independent votes, scaled to the documented +/-30 maximum.
    breadth_votes = (
        v["breadth_ma20"] >= .55, v["breadth_ma50"] >= .50,
        v["advance_ratio"] >= .50, v["median_return_5d"] > 0,
    )
    breadth = sum(7.5 if x else -7.5 for x in breadth_votes)
    momentum = (10 if v["return_5d"] > 0 else -10) + (10 if v["return_20d"] > 0 else -10)
    risk = 0.0
    drawdown = abs(min(0.0, _number(observations, "drawdown_20d") or 0.0))
    risk -= min(4.0, drawdown / .10 * 4.0)
    if observations.get("volatility_spike"): risk -= 2.0
    if (_number(observations, "intraday_return") or 0.0) <= -.02: risk -= 2.0
    if (_number(observations, "gap_return") or 0.0) <= -.015: risk -= 2.0
    score = max(-100.0, min(100.0, trend + breadth + momentum + risk))
    state = state_for_score(score)
    independent_strength = sum((trend > 0, breadth > 0, momentum > 0))
    if state == "KR_STRONG_RISK_ON" and (sector_data_suspect or account_kill_switch or independent_strength < 2):
        state = "KR_RISK_ON"
    if account_kill_switch:
        state = STATE_ORDER[max(0, STATE_ORDER.index(state) - 1)]
    return KRMarketState(market, score, state, "OK", {"trend": trend, "breadth": breadth, "momentum": momentum, "risk": risk})


def execution_policy(state: str, *, data_quality: str = "OK") -> KRExecutionPolicy:
    blocked = data_quality == "BLOCKED"
    table = {
        STATE_ORDER[0]: (0.0, 0, False), STATE_ORDER[1]: (.20, 2, False),
        STATE_ORDER[2]: (.50, 3, False), STATE_ORDER[3]: (.80, None, True),
        STATE_ORDER[4]: (1.0, None, True), STATE_ORDER[5]: (1.10, None, True),
    }
    budget, positions, add = table[state]
    risk_off = state in STATE_ORDER[:2]
    sector_cap = .20 if risk_off else (.45 if state in STATE_ORDER[4:] else .35)
    return KRExecutionPolicy(0.0 if blocked else budget, 0 if blocked else positions,
                             not blocked and state != STATE_ORDER[0], not blocked and add,
                             sector_cap=sector_cap, high_beta_sector_cap=.15 if risk_off else .45)


def build_kr_regime_snapshot(observations: Mapping[str, Mapping[str, Any]], *,
                             as_of: str | None = None, source: str = "PREP",
                             sector_leaders: list[str] | tuple[str, ...] = (),
                             sector_data_suspect: bool = False,
                             account_kill_switch: bool = False) -> KRRegimeSnapshot:
    states = {m: calculate_market_state(m, observations.get(m, {}),
              sector_data_suspect=sector_data_suspect,
              account_kill_switch=account_kill_switch) for m in MARKETS}
    quality = "BLOCKED" if any(x.data_quality == "BLOCKED" for x in states.values()) else "OK"
    # The weaker market constrains common capital, while per-market gates remain independent.
    global_state = min((x.state for x in states.values()), key=STATE_ORDER.index)
    policy = execution_policy(global_state, data_quality=quality)
    snap = KRRegimeSnapshot(as_of or datetime.now(timezone.utc).isoformat(), global_state,
                            states, quality, policy, tuple(sector_leaders), source.upper())
    logger.info("[KR_REGIME][SNAPSHOT] as_of=%s source=%s global_state=%s data_quality=%s", snap.as_of, snap.source, snap.global_state, snap.data_quality)
    for market, value in states.items():
        logger.info("[KR_REGIME][MARKET_STATE] market=%s state=%s score=%.1f data_quality=%s", market, value.state, value.score, value.data_quality)
    logger.info("[KR_REGIME][EXECUTION_POLICY] budget_multiplier=%.2f allow_new_buy=%s max_new_positions=%s", policy.budget_multiplier, policy.allow_new_buy, policy.max_new_positions)
    return snap


def market_allows_buy(snapshot: KRRegimeSnapshot, market: str) -> bool:
    market = str(market or "").upper()
    return bool(snapshot.execution_policy.allow_new_buy and market in snapshot.market_states
                and snapshot.market_states[market].data_quality == "OK"
                and STATE_ORDER.index(snapshot.market_states[market].state) >= STATE_ORDER.index("KR_DEFENSE_CAUTION"))


def write_snapshot(snapshot: KRRegimeSnapshot, path: str | Path = "artifacts/kr_regime_snapshot.json") -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(json.dumps(snapshot.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(target)
    return target
