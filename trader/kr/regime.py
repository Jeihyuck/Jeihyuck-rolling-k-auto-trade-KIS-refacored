"""Single source of truth for Korean market regime and entry policy.

The engine deliberately consumes already-normalised observations.  Fetching is kept
outside this module so the same calculation is used by PREP, intraday and tests.
Missing mandatory observations are *blocked*, never silently replaced by an ETF.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
from typing import Any, Mapping

logger = logging.getLogger(__name__)

MARKETS = ("KOSPI", "KOSDAQ")
KR_MARKET_ETFS = {"KOSPI": "069500", "KOSPI_CONFIRM": "226490", "KOSDAQ": "229200"}
KR_MARKET_LEADERS = {"KOSPI": ("091160", "005930", "000660"), "KOSDAQ": ()}
KR_LEADER_SYMBOLS = KR_MARKET_LEADERS["KOSPI"]
KR_REGIME_REQUIRED_SYMBOLS = ("069500", "226490", "229200", "091160", "005930", "000660")
STATE_ORDER = (
    "KR_DEFENSE_CRASH", "KR_DEFENSE_RISK_OFF", "KR_DEFENSE_CAUTION",
    "KR_SHOCK_REBOUND_PENDING", "KR_SHOCK_REBOUND_CONFIRMED",
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
class KRMarketExecutionPolicy:
    market: str
    data_quality: str
    allow_new_buy: bool
    budget_multiplier: float
    max_new_positions: int | None


@dataclass(frozen=True)
class KRRegimeSnapshot:
    as_of: str
    global_state: str
    market_states: dict[str, KRMarketState]
    data_quality: str
    execution_policy: KRExecutionPolicy
    market_policies: dict[str, KRMarketExecutionPolicy] = field(default_factory=dict)
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
        if proposed == self.state:
            self._candidate, self._count = None, 0
            return self.state
        if proposed in {"KR_DEFENSE_CRASH", "KR_SHOCK_REBOUND_PENDING"}:
            return self._accept(proposed, now)
        if proposed == "KR_SHOCK_REBOUND_CONFIRMED" and self.state == "KR_SHOCK_REBOUND_PENDING" and self._last_change_at and (now - self._last_change_at).total_seconds() >= 600:
            return self._accept(proposed, now)
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


def normalize_kr_market(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if raw in {"KOSPI", "KS", "P"}: return "KOSPI"
    if raw in {"KOSDAQ", "KQ", "Q"}: return "KOSDAQ"
    return "UNKNOWN"


def state_for_score(score: float) -> str:
    if score <= -60: return "KR_DEFENSE_CRASH"
    if score <= -30: return "KR_DEFENSE_RISK_OFF"
    if score <= -10: return "KR_DEFENSE_CAUTION"
    if score < 20: return "KR_NORMAL"
    if score < 50: return "KR_RISK_ON"
    return "KR_STRONG_RISK_ON"


def structural_regime_cap(observations: Mapping[str, Any]) -> str:
    """MA structure is a hard ceiling, not another additive score component."""
    close, ma20, ma50 = (_number(observations, key) for key in ("close", "ma20", "ma50"))
    if None in (close, ma20, ma50):
        return "KR_DEFENSE_CRASH"
    if close < ma20 and close < ma50:
        return "KR_DEFENSE_CAUTION"
    if close >= ma20 and close < ma50:
        return "KR_NORMAL"
    if (_number(observations, "ma20_slope_5d") or 0) <= 0:
        return "KR_NORMAL"
    breadth_strong = (_number(observations, "breadth_ma20") or 0) >= .60 and (_number(observations, "breadth_ma50") or 0) >= .55
    return "KR_STRONG_RISK_ON" if close >= ma50 and breadth_strong else "KR_RISK_ON"


def _shock_rebound_state(observations: Mapping[str, Any]) -> str | None:
    market = str(observations.get("market") or "")
    positive = sum((
        (_number(observations, "intraday_return_positive") or 0) >= .02,
        (_number(observations, "gap_up_return") or 0) >= .01,
        bool(observations.get("above_open")), bool(observations.get("above_vwap")),
        (_number(observations, "advance_ratio_intraday") or 0) >= .65,
        (_number(observations, "turnover_expansion") or 0) >= 1.2,
        (_number(observations, "distance_from_intraday_high") or -1) >= -.015,
        int(observations.get("leader_confirmation_count") or 0) >= 2,
    ))
    if positive < 4:
        return None
    held = int(observations.get("shock_consecutive_ticks") or 0) >= 3 and float(observations.get("shock_minutes") or 0) >= 10
    market_confirmation = (bool(observations.get("kospi_confirmation_ok")) and int(observations.get("leader_confirmation_count") or 0) >= 2) if market == "KOSPI" else (
        market == "KOSDAQ" and (_number(observations, "advance_ratio_intraday") or 0) >= .70
        and (_number(observations, "median_return_1d") or 0) > 0)
    confirmed = (held and bool(observations.get("above_open")) and bool(observations.get("above_vwap"))
                 and (_number(observations, "advance_ratio_intraday") or 0) >= .65
                 and (_number(observations, "turnover_expansion") or 0) >= 1.2
                 and market_confirmation
                 and (_number(observations, "distance_from_intraday_high") or -1) >= -.03)
    return "KR_SHOCK_REBOUND_CONFIRMED" if confirmed else "KR_SHOCK_REBOUND_PENDING"


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
    score = max(-100.0, min(100.0, breadth + momentum + risk))
    state = state_for_score(score)
    cap = structural_regime_cap(observations)
    if STATE_ORDER.index(state) > STATE_ORDER.index(cap):
        state = cap
    shock = _shock_rebound_state(observations)
    below_structure = v["close"] < v["ma20"] and v["close"] < v["ma50"]
    if shock and below_structure:
        state = shock
    independent_strength = sum((trend > 0, breadth > 0, momentum > 0))
    if state == "KR_STRONG_RISK_ON" and (sector_data_suspect or account_kill_switch or independent_strength < 2):
        state = "KR_RISK_ON"
    if account_kill_switch:
        state = "KR_DEFENSE_CRASH"
    return KRMarketState(market, score, state, str(observations.get("input_data_quality") or "OK"), {"trend": trend, "breadth": breadth, "momentum": momentum, "risk": risk})


def execution_policy(state: str, *, data_quality: str = "OK") -> KRExecutionPolicy:
    blocked = data_quality == "BLOCKED"
    table = {
        "KR_DEFENSE_CRASH": (0.0, 0, False), "KR_DEFENSE_RISK_OFF": (.20, 2, False),
        "KR_DEFENSE_CAUTION": (.50, 3, False), "KR_SHOCK_REBOUND_PENDING": (.10, 1, False),
        "KR_SHOCK_REBOUND_CONFIRMED": (.25, 3, False), "KR_NORMAL": (.80, None, True),
        "KR_RISK_ON": (1.0, None, True), "KR_STRONG_RISK_ON": (1.10, None, True),
    }
    budget, positions, add = table[state]
    risk_off = state in {"KR_DEFENSE_CRASH", "KR_DEFENSE_RISK_OFF"}
    sector_cap = .20 if risk_off else (.45 if state in {"KR_RISK_ON", "KR_STRONG_RISK_ON"} else .35)
    return KRExecutionPolicy(0.0 if blocked else budget, 0 if blocked else positions,
                             not blocked and state != "KR_DEFENSE_CRASH", not blocked and add,
                             sector_cap=sector_cap, high_beta_sector_cap=.15 if risk_off else .45)


def market_execution_policies(states: Mapping[str, KRMarketState], quality: str) -> dict[str, KRMarketExecutionPolicy]:
    result = {}
    for market, value in states.items():
        local = execution_policy(value.state, data_quality=value.data_quality)
        multiplier = local.budget_multiplier * (.8 if quality == "DEGRADED" else 1.0)
        result[market] = KRMarketExecutionPolicy(market, value.data_quality, local.allow_new_buy, multiplier, local.max_new_positions)
    return result


def constrain_global_policy(policy: KRExecutionPolicy, market_policies: Mapping[str, KRMarketExecutionPolicy]) -> KRExecutionPolicy:
    active = [p for p in market_policies.values() if p.data_quality != "BLOCKED"]
    if not active:
        return replace(policy, budget_multiplier=0.0, max_new_positions=0, allow_new_buy=False, allow_add_to_existing=False)
    finite_limits = [p.max_new_positions for p in active if p.max_new_positions is not None]
    return replace(policy, budget_multiplier=min(p.budget_multiplier for p in active),
                   max_new_positions=min(finite_limits) if finite_limits else None,
                   allow_new_buy=any(p.allow_new_buy for p in active))


def build_kr_regime_snapshot(observations: Mapping[str, Mapping[str, Any]], *,
                             as_of: str | None = None, source: str = "PREP",
                             sector_leaders: list[str] | tuple[str, ...] = (),
                             sector_data_suspect: bool = False,
                             account_kill_switch: bool = False) -> KRRegimeSnapshot:
    states = {m: calculate_market_state(m, observations.get(m, {}),
              sector_data_suspect=sector_data_suspect,
              account_kill_switch=account_kill_switch) for m in MARKETS}
    healthy = [x for x in states.values() if x.data_quality != "BLOCKED"]
    quality = "BLOCKED" if not healthy else "OK" if len(healthy) == len(states) and all(x.data_quality == "OK" for x in healthy) else "DEGRADED"
    # A failed market is excluded rather than zeroing a healthy market's budget.
    global_state = min((x.state for x in healthy), key=STATE_ORDER.index) if healthy else "KR_DEFENSE_CRASH"
    policy = execution_policy(global_state, data_quality=quality)
    if quality == "DEGRADED":
        policy = replace(policy, budget_multiplier=policy.budget_multiplier * .8)
    market_policies = market_execution_policies(states, quality)
    active_policies = [p for p in market_policies.values() if p.data_quality != "BLOCKED"]
    if active_policies:
        policy = replace(policy, budget_multiplier=max(p.budget_multiplier for p in active_policies), allow_new_buy=any(p.allow_new_buy for p in active_policies))
    snap = KRRegimeSnapshot(as_of or datetime.now(timezone.utc).isoformat(), global_state,
                            states, quality, policy, market_policies, tuple(sector_leaders), source.upper())
    logger.info("[KR_REGIME][SNAPSHOT] as_of=%s source=%s global_state=%s data_quality=%s", snap.as_of, snap.source, snap.global_state, snap.data_quality)
    for market, value in states.items():
        logger.info("[KR_REGIME][MARKET_STATE] market=%s state=%s score=%.1f data_quality=%s", market, value.state, value.score, value.data_quality)
    logger.info("[KR_REGIME][EXECUTION_POLICY] budget_multiplier=%.2f allow_new_buy=%s max_new_positions=%s", policy.budget_multiplier, policy.allow_new_buy, policy.max_new_positions)
    for market, value in market_policies.items():
        logger.info("[KR_REGIME][MARKET_POLICY] market=%s quality=%s allow_new_buy=%s budget_multiplier=%.2f max_new_positions=%s", market, value.data_quality, value.allow_new_buy, value.budget_multiplier, value.max_new_positions)
    return snap


def market_allows_buy(snapshot: KRRegimeSnapshot, market: str) -> bool:
    market = str(market or "").upper()
    local = snapshot.market_policies.get(market)
    return bool(snapshot.execution_policy.allow_new_buy and local and local.allow_new_buy and market in snapshot.market_states
                and snapshot.market_states[market].data_quality != "BLOCKED"
                and STATE_ORDER.index(snapshot.market_states[market].state) >= STATE_ORDER.index("KR_DEFENSE_CAUTION"))


def calculate_market_budgets(snapshot: KRRegimeSnapshot, base_tick_budget: float, available_cash: float) -> dict[str, float]:
    return {market: min(float(available_cash), float(base_tick_budget) * policy.budget_multiplier)
            if policy.allow_new_buy else 0.0 for market, policy in snapshot.market_policies.items()}


def candidate_allows_buy(candidate: Mapping[str, Any], state: str) -> tuple[bool, str | None]:
    """Final stock-level MA/VWAP/liquidity/RS gate; SELL never calls this gate."""
    close = _number(candidate, "last_price") or _number(candidate, "close")
    ma20 = _number(candidate, "ma20")
    if close is None or ma20 is None or close <= ma20:
        return False, "KR_STOCK_BELOW_MA20"
    if not bool(candidate.get("above_vwap", candidate.get("vwap_ok", False))):
        return False, "KR_STOCK_BELOW_VWAP"
    if not bool(candidate.get("liq_ok", False)):
        return False, "KR_STOCK_LIQUIDITY_BLOCK"
    rs = _number(candidate, "rs_percentile") or _number(candidate, "rs_pctile") or 0.0
    if rs > 1: rs /= 100.0
    if rs < .70:
        return False, "KR_STOCK_RS_BLOCK"
    if state in {"KR_SHOCK_REBOUND_PENDING", "KR_SHOCK_REBOUND_CONFIRMED"}:
        if not (bool(candidate.get("is_market_leader")) or bool(candidate.get("is_sector_leader")) or rs >= .85):
            return False, "KR_SHOCK_STOCK_LEADERSHIP_BLOCK"
        if not bool(candidate.get("above_open")) or (_number(candidate, "turnover_expansion") or 0) < 1.2:
            return False, "KR_SHOCK_STOCK_CONFIRMATION_BLOCK"
        if (_number(candidate, "distance_from_intraday_high") or -1) < -.05:
            return False, "KR_SHOCK_STOCK_HIGH_DISTANCE_BLOCK"
    elif (_number(candidate, "ma20_slope_5d") or 0) <= 0 and not bool(candidate.get("breakout_ok")):
        return False, "KR_STOCK_MA20_SLOPE_BLOCK"
    return True, None


def write_snapshot(snapshot: KRRegimeSnapshot, path: str | Path = "artifacts/kr_regime_snapshot.json") -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(json.dumps(snapshot.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(target)
    return target
