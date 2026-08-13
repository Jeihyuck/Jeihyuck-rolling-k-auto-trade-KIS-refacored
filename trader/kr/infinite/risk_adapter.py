from __future__ import annotations

from datetime import datetime, timedelta, timezone
from trader.kr.regime import STATE_ORDER
from .models import MarketInput

REGIME_POLICY = {
    STATE_ORDER[0]: (0, 0), STATE_ORDER[1]: (0, 0), STATE_ORDER[2]: (.5, 8),
    STATE_ORDER[3]: (.5, 12), STATE_ORDER[4]: (1, 20), STATE_ORDER[5]: (1, 28),
    STATE_ORDER[6]: (1, 36), STATE_ORDER[7]: (1, 40),
}
if set(REGIME_POLICY) != set(STATE_ORDER):
    raise RuntimeError("incomplete canonical KR regime mapping")


def validate_regime(value: MarketInput, now: datetime, *, max_age: timedelta = timedelta(minutes=30)) -> str | None:
    if value.regime_state not in STATE_ORDER: return "UNKNOWN_REGIME"
    if value.data_quality.upper() == "BLOCKED": return "DATA_QUALITY_BLOCKED"
    if value.regime_as_of != value.trade_date: return "REGIME_TRADE_DATE_MISMATCH"
    current = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
    stamp = value.regime_at if value.regime_at.tzinfo else value.regime_at.replace(tzinfo=timezone.utc)
    if stamp > current: return "REGIME_FUTURE_TIMESTAMP"
    if current - stamp > max_age: return "REGIME_SNAPSHOT_STALE"
    if not value.policy_matches: return "EXECUTION_POLICY_MISMATCH"
    if value.kill_switch: return "ACCOUNT_KILL_SWITCH"
    return None
