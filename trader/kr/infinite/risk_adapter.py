from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from trader.kr.regime import STATE_ORDER


@dataclass(frozen=True)
class RegimeAction:
    allow_buy: bool
    decision: str
    recovery_probe: bool = False


REGIME_ACTIONS = {
    "KR_DEFENSE_CRASH": RegimeAction(False, "BLOCK_BUY_CRASH"),
    "KR_DEFENSE_RISK_OFF": RegimeAction(False, "BLOCK_BUY_RISK_OFF"),
    "KR_DEFENSE_CAUTION": RegimeAction(False, "BLOCK_BUY_CAUTION"),
    "KR_SHOCK_REBOUND_PENDING": RegimeAction(False, "BLOCK_BUY_REBOUND_PENDING"),
    "KR_SHOCK_REBOUND_CONFIRMED": RegimeAction(True, "ALLOW_RECOVERY_PROBE", True),
    "KR_NORMAL": RegimeAction(True, "ALLOW_ROUTINE_BUY"),
    "KR_RISK_ON": RegimeAction(True, "ALLOW_ROUTINE_BUY"),
    "KR_STRONG_RISK_ON": RegimeAction(True, "ALLOW_ROUTINE_BUY"),
}

assert set(REGIME_ACTIONS) == set(STATE_ORDER), "KR regime mapping must remain exhaustive"


def _get(obj: Any, name: str, default: Any = None) -> Any:
    return obj.get(name, default) if isinstance(obj, dict) else getattr(obj, name, default)


def _parse_as_of(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def assess(snapshot: Any, trade_date: date, *, account_loss_kill_switch: bool = False,
           max_age_seconds: int = 36 * 3600) -> RegimeAction:
    if snapshot is None:
        return RegimeAction(False, "BLOCK_BUY_DATA_QUALITY")
    as_of = _parse_as_of(_get(snapshot, "as_of"))
    now = datetime.now(timezone.utc)
    if as_of is None or as_of.date() != trade_date or as_of > now or (now - as_of).total_seconds() > max_age_seconds:
        return RegimeAction(False, "BLOCK_BUY_STALE_REGIME")
    states, policies = _get(snapshot, "market_states", {}) or {}, _get(snapshot, "market_policies", {}) or {}
    local, local_policy = states.get("KOSPI"), policies.get("KOSPI")
    state = str(_get(local, "state", ""))
    quality = str(_get(local, "data_quality", "BLOCKED")).upper()
    if account_loss_kill_switch or bool(_get(snapshot, "account_loss_kill_switch", False)):
        return RegimeAction(False, "BLOCK_BUY_ACCOUNT_KILL_SWITCH")
    if not local or not local_policy or quality == "BLOCKED" or str(_get(snapshot, "data_quality", "BLOCKED")).upper() == "BLOCKED":
        return RegimeAction(False, "BLOCK_BUY_DATA_QUALITY")
    if state not in REGIME_ACTIONS:
        return RegimeAction(False, "BLOCK_BUY_UNKNOWN_STATE")
    action = REGIME_ACTIONS[state]
    local_allow = bool(_get(local_policy, "allow_new_buy", False))
    add_allow = bool(_get(_get(snapshot, "execution_policy", {}), "allow_add_to_existing", False))
    if action.allow_buy and (not local_allow or not add_allow):
        return RegimeAction(False, "BLOCK_BUY_POLICY_MISMATCH")
    return action
