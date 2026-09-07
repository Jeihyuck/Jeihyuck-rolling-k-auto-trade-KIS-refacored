"""Independent session runtime for the KR Infinite strategy.

This process is deliberately not owned by PB1.  The Windows/WSL KR session
wrapper may start it alongside PB1, but PB1 success, failure, locks, prechecks,
and result files do not control this loop.

Shared infrastructure is limited to the broker account, the database, and the
canonical KR regime snapshot.  Order/state ownership remains KR_INFINITE only.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterator
from zoneinfo import ZoneInfo

from trader.kr.calendar import resolve_kr_trade_date

from .models import Action
from .repository import InfiniteRepository
from .runner import RunResult, run_canonical_session

logger = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parents[3]
KST = ZoneInfo("Asia/Seoul")

_DEFAULT_END = {
    "am": "13:00",
    "afternoon": "15:10",
    "close": "15:30",
}


def _now_kst() -> datetime:
    return datetime.now(KST)


def _resolve_session_end(session: str, now: datetime) -> datetime:
    key = {
        "am": "KR_INFINITE_AM_SESSION_END",
        "afternoon": "KR_INFINITE_PM_SESSION_END",
        "close": "KR_INFINITE_CLOSE_SESSION_END",
    }[session]
    raw = str(os.getenv(key) or _DEFAULT_END[session]).strip()
    try:
        hh, mm = (int(part) for part in raw.split(":", 1))
        if not (0 <= hh <= 23 and 0 <= mm <= 59):
            raise ValueError(raw)
    except Exception:
        fallback = _DEFAULT_END[session]
        hh, mm = (int(part) for part in fallback.split(":", 1))
        logger.warning(
            "[KR_INF][SESSION_LOOP][END_FALLBACK] session=%s raw=%s fallback=%s",
            session,
            raw,
            fallback,
        )
    return now.replace(hour=hh, minute=mm, second=0, microsecond=0)


def _health_path(session: str, now: datetime) -> Path:
    return ROOT / "runtime" / "health" / f"kr-infinite-{session}-{now.date().isoformat()}.json"


def _write_health(
    *,
    session: str,
    status: str,
    reason: str,
    decision: str | None = None,
    submitted: bool | None = None,
    now: datetime | None = None,
) -> None:
    current = now or _now_kst()
    target = _health_path(session, current)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "market": "KR",
        "strategy": "KR_INFINITE_V1",
        "runtime_owner": "KR_INFINITE_INDEPENDENT",
        "session": session,
        "status": status,
        "reason": reason,
        "decision": decision,
        "submitted": submitted,
        "updated_at": current.isoformat(),
        "pid": os.getpid(),
    }
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(target)


@contextmanager
def _tick_lock() -> Iterator[bool]:
    """Serialize Infinite ticks across overlapping AM/PM wrappers.

    The lock is intentionally independent from every PB1 lock.  A boundary
    overlap (for example AM ending while afternoon starts) merely skips one
    Infinite tick; it never blocks the PB1 process and never creates a second
    order race.
    """
    import fcntl

    path = ROOT / "runtime" / "locks" / "kr-infinite-runtime.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+", encoding="utf-8")
    acquired = False
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
        except BlockingIOError:
            acquired = False
        yield acquired
    finally:
        if acquired:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def _ensure_infinite_schema() -> None:
    """Own the Infinite schema bootstrap instead of relying on PB1 startup."""
    repo = InfiniteRepository()
    try:
        repo.ensure_schema()
        return
    except RuntimeError as exc:
        if str(exc) != "KR_INF_DB_UNAVAILABLE":
            raise
    from trader.db.engine import get_engine
    from trader.db.migrate import run_migrations

    logger.warning("[KR_INF][SCHEMA] missing=1 action=run_migrations")
    run_migrations(get_engine())
    InfiniteRepository().ensure_schema()


def run_independent_tick(
    *,
    session: str,
    env: str,
    allow_entry: bool,
    run_fn: Callable[..., RunResult] = run_canonical_session,
) -> RunResult | None:
    """Run one Infinite tick with no PB1 state or control dependency."""
    with _tick_lock() as acquired:
        if not acquired:
            logger.info("[KR_INF][SESSION_LOOP][SKIP_LOCKED] session=%s", session)
            _write_health(
                session=session,
                status="SKIP_LOCKED",
                reason="KR_INF_RUNTIME_LOCK_HELD",
            )
            return None
        try:
            result = run_fn(session=session, env=env, allow_entry=allow_entry)
        except Exception as exc:
            logger.exception(
                "[KR_INF][SESSION_LOOP][TICK_EXCEPTION] session=%s allow_entry=%s err=%s",
                session,
                int(allow_entry),
                exc,
            )
            _write_health(
                session=session,
                status="FAIL_SOFT",
                reason=f"{type(exc).__name__}:{exc}",
            )
            return None

        action = result.decision.action.value
        reason = result.decision.reason
        status = "BLOCK" if result.decision.action == Action.BLOCK else "OK"
        logger.info(
            "[KR_INF][SESSION_LOOP][TICK] session=%s allow_entry=%s decision=%s reason=%s submitted=%s",
            session,
            int(allow_entry),
            action,
            reason,
            int(bool(result.submitted)),
        )
        _write_health(
            session=session,
            status=status,
            reason=reason,
            decision=action,
            submitted=bool(result.submitted),
        )
        return result


def run_session_loop(
    *,
    session: str,
    env: str,
    interval_sec: int | None = None,
    now_fn: Callable[[], datetime] = _now_kst,
    sleep_fn: Callable[[float], None] = time.sleep,
    run_fn: Callable[..., RunResult] = run_canonical_session,
) -> int:
    session = str(session or "").strip().lower()
    if session not in {"am", "afternoon", "close"}:
        raise ValueError(f"unsupported Infinite session={session!r}")

    now_for_trade_date = now_fn()
    trade_date = resolve_kr_trade_date(now_for_trade_date)
    os.environ["KR_TRADE_DATE"] = trade_date.isoformat()

    try:
        _ensure_infinite_schema()
    except Exception as exc:
        logger.exception("[KR_INF][SCHEMA][FATAL] err=%s", exc)
        _write_health(
            session=session,
            status="FAIL",
            reason=f"KR_INF_SCHEMA_BOOTSTRAP:{type(exc).__name__}:{exc}",
            now=now_for_trade_date,
        )
        return 2

    interval = max(5, int(interval_sec or os.getenv("KR_INFINITE_LOOP_INTERVAL_SEC", "60")))
    allow_entry = session in {"am", "afternoon"} and os.getenv(
        "KR_INFINITE_ENTRY_ENABLED", "1"
    ).strip().lower() not in {"0", "false", "no", "off"}

    now = now_fn()
    end = _resolve_session_end(session, now)
    logger.info(
        "[KR_INF][SESSION_LOOP][START] session=%s env=%s allow_entry=%s interval=%s end=%s owner=KR_INFINITE_INDEPENDENT",
        session,
        env,
        int(allow_entry),
        interval,
        end.isoformat(),
    )

    # CLOSE is always one exit-only safety tick.  It must never create a new
    # Infinite buy, regardless of external PB1 environment flags.
    if session == "close":
        run_independent_tick(
            session=session,
            env=env,
            allow_entry=False,
            run_fn=run_fn,
        )
        logger.info("[KR_INF][SESSION_LOOP][END] session=close ticks=1")
        return 0

    ticks = 0
    while True:
        now = now_fn()
        if now >= end:
            break
        run_independent_tick(
            session=session,
            env=env,
            allow_entry=allow_entry,
            run_fn=run_fn,
        )
        ticks += 1
        remaining = max(0.0, (end - now_fn()).total_seconds())
        if remaining <= 0:
            break
        sleep_fn(min(float(interval), remaining))

    logger.info(
        "[KR_INF][SESSION_LOOP][END] session=%s ticks=%s end=%s",
        session,
        ticks,
        end.isoformat(),
    )
    _write_health(
        session=session,
        status="SESSION_DONE",
        reason="KR_INF_INDEPENDENT_SESSION_DONE",
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    parser = argparse.ArgumentParser(description="Independent KR Infinite session runner")
    parser.add_argument("--session", required=True, choices=["am", "afternoon", "close"])
    parser.add_argument("--env", default=os.getenv("KIS_ENV", "practice"))
    parser.add_argument("--interval-sec", type=int, default=None)
    args = parser.parse_args(argv)
    return run_session_loop(
        session=args.session,
        env=args.env,
        interval_sec=args.interval_sec,
    )


if __name__ == "__main__":
    raise SystemExit(main())
