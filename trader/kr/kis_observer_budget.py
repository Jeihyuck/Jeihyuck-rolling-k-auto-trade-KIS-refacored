"""Cooperative, cross-process KR *observer-only* KIS request budget.

The regular trading engine is NEVER granted/denied a broker call by this
module.  Its critical order/balance requests publish a best-effort notification
to a same-account sidecar; the read-only minute observer fails closed if its
reserved request budget is unavailable.  No shared lock is held during HTTP.
This reduces extra collector calls competing with safety-critical KIS work,
without claiming to enforce the broker's full quota across all processes.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path
import time

from trader.runtime_paths import runtime_path

logger = logging.getLogger(__name__)

MAX_CLOCK_SKEW_SEC = 30.0


def _path(account_key: str, environment: str) -> Path:
    scope = f"{environment}:{account_key}"
    digest = hashlib.sha256(scope.encode("utf-8")).hexdigest()[:24]
    return runtime_path("runtime", "kr_kis_observer_budget", f"{digest}.json")


def _read(file) -> dict:
    file.seek(0)
    try:
        payload = json.loads(file.read() or "{}")
        return payload if isinstance(payload, dict) else {}
    except (ValueError, UnicodeError):
        return {}


def _write(file, state: dict) -> None:
    file.seek(0)
    file.truncate()
    json.dump(state, file, sort_keys=True)
    file.flush()


def _current_clock(value=None) -> float:
    return float(time.monotonic() if value is None else value)


def _valid_past(stamp: float, now: float) -> bool:
    return 0 <= now - stamp <= 86400


def _secured_file(path: Path):
    # No raw account number or credentials in the filename or contents.
    path.parent.mkdir(parents=True, exist_ok=True)
    # a+ preserves previous state and creates with private mode by default
    fd = os.open(str(path), os.O_CREAT | os.O_RDWR, 0o600)
    return os.fdopen(fd, "r+", encoding="utf-8")


def note_critical_kr_call(*, environment: str, account_key: str,
                          now: float | None = None) -> bool:
    """Nonblocking: a failed observer notification MUST NOT stop an order."""
    stamp = _current_clock(now)
    try:
        with _secured_file(_path(account_key, environment)) as file:
            try:
                fcntl.flock(file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return False
            try:
                state = _read(file)
                state["last_critical"] = stamp
                _write(file, state)
                return True
            finally:
                fcntl.flock(file.fileno(), fcntl.LOCK_UN)
    except (OSError, ValueError, TypeError) as exc:
        logger.warning(
            "[KR][KIS_BUDGET][CRITICAL_NOTICE_UNAVAILABLE] type=%s "
            "action=ALLOW_PROTECTED_BROKER_REQUEST", type(exc).__name__,
        )
        return False


def acquire_observer_slot(*, environment: str, account_key: str,
                          now: float | None = None,
                          rps: float | None = None,
                          priority_cooldown_sec: float = 2.0) -> tuple[bool, str]:
    """Nonblocking, fail-closed for LOW-priority observer REST calls.

    No sleep is performed. Caller must defer this one data request, allowing
    other symbols to be evaluated and protective SELL to proceed independently.
    """
    stamp = _current_clock(now)
    try:
        rate = float(rps if rps is not None else
                     os.getenv("KR_PB1_OBSERVER_KIS_MAX_RPS", "0.5"))
        if not (0 < rate <= 1.5):
            return False, "INVALID_OBSERVER_BUDGET"
        with _secured_file(_path(account_key, environment)) as file:
            try:
                fcntl.flock(file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return False, "LOCK_BUSY"
            try:
                state = _read(file)
                critical = float(state.get("last_critical") or -1)
                if _valid_past(critical, stamp) and stamp - critical < max(
                    0.0, priority_cooldown_sec
                ):
                    return False, "PRIORITY_BROKER_TRAFFIC"
                next_at = float(state.get("next_observer_at") or -1)
                if stamp < next_at and next_at - stamp < MAX_CLOCK_SKEW_SEC:
                    return False, "OBSERVER_RPS_LIMIT"
                # Stale files from a reboot or clock skew do not block the
                # current day forever.
                state["next_observer_at"] = stamp + 1.0 / rate
                _write(file, state)
                return True, "GRANTED"
            finally:
                fcntl.flock(file.fileno(), fcntl.LOCK_UN)
    except (OSError, ValueError, TypeError) as exc:
        logger.warning(
            "[KR][KIS_BUDGET][OBSERVER_DEFER] type=%s action=NO_COLLECTOR_HTTP",
            type(exc).__name__,
        )
        return False, "BUDGET_STATE_UNAVAILABLE"
