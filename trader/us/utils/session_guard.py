# -*- coding: utf-8 -*-
"""US file-based session guard with .done and atomic .running locks."""
from __future__ import annotations

import json
import logging
import os
import socket
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)
NY_TZ = ZoneInfo("America/New_York")
_GUARD_BASE = Path("runtime/session_guard/us")
_STALE_DEFAULTS = {"prep": 60, "am": 240, "afternoon": 240, "close": 30, "trader": 480}


def _done_path(trade_date: str, session: str, run_type: str = "schedule") -> Path:
    suffix = ".done" if str(run_type or "schedule") == "schedule" else f".{run_type}.done"
    return _GUARD_BASE / trade_date / f"{session}{suffix}"


def _running_path(trade_date: str, session: str) -> Path:
    return _GUARD_BASE / trade_date / f"{session}.running"


def _guard_path(trade_date: str, session: str) -> Path:  # backward compatible .done path
    return _done_path(trade_date, session)


def now_et_iso() -> str:
    return datetime.now(tz=NY_TZ).isoformat()


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)).astimezone(NY_TZ)
    except Exception:
        return None


def _is_stale(payload: dict, stale_minutes: int) -> bool:
    ref = _parse_dt(payload.get("heartbeat_at_et")) or _parse_dt(payload.get("started_at_et"))
    if ref is None:
        return True
    return datetime.now(tz=NY_TZ) - ref > timedelta(minutes=stale_minutes)


def _read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _atomic_write_new(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def acquire_us_session_running_lock(trade_date: str, session: str, run_id: str, stale_minutes: int | None = None) -> dict:
    if os.getenv("US_BYPASS_RUNNING_LOCK_FOR_TEST") == "1":
        return {"acquired": True, "reason": "BYPASS_FOR_TEST", "path": str(_running_path(trade_date, session))}
    stale_minutes = int(stale_minutes or _STALE_DEFAULTS.get(session, 240))
    path = _running_path(trade_date, session)
    payload = {
        "market": "US", "session": session, "trade_date": trade_date, "run_id": run_id,
        "pid": os.getpid(), "started_at_et": now_et_iso(), "heartbeat_at_et": now_et_iso(),
        "host": socket.gethostname(), "command": " ".join(sys.argv),
    }
    while True:
        try:
            _atomic_write_new(path, payload)
            logger.info("[US_SESSION_GUARD][RUNNING_LOCK_ACQUIRED] session=%s trade_date=%s path=%s pid=%s run_id=%s", session, trade_date, path, os.getpid(), run_id)
            return {"acquired": True, "reason": "RUNNING_LOCK_ACQUIRED", "path": str(path), "payload": payload}
        except FileExistsError:
            old = _read_json(path)
            old_pid = int(old.get("pid") or 0)
            if _pid_alive(old_pid) and not _is_stale(old, stale_minutes):
                logger.warning("[US_SESSION_GUARD][SKIP_DUPLICATE_RUNNING] session=%s trade_date=%s path=%s pid=%s run_id=%s", session, trade_date, path, old_pid, old.get("run_id"))
                return {"acquired": False, "reason": "SKIP_DUPLICATE_RUNNING", "path": str(path), "payload": old}
            stale_path = path.with_name(f"{path.name}.stale.{datetime.now(tz=NY_TZ).strftime('%Y%m%d%H%M%S%f')}")
            try:
                path.rename(stale_path)
                logger.warning("[US_SESSION_GUARD][STALE_LOCK_REPLACED] session=%s trade_date=%s old_path=%s stale_path=%s old_pid=%s", session, trade_date, path, stale_path, old_pid)
            except FileNotFoundError:
                continue
            except Exception as exc:
                return {"acquired": False, "reason": f"RUNNING_LOCK_REPLACE_FAILED:{exc}", "path": str(path), "payload": old}


def release_us_session_running_lock(trade_date: str, session: str, run_id: str | None = None) -> dict:
    path = _running_path(trade_date, session)
    payload = _read_json(path)
    if run_id and payload and str(payload.get("run_id")) != str(run_id):
        logger.warning("[US_SESSION_GUARD][RUNNING_LOCK_RELEASE_SKIP] session=%s trade_date=%s reason=run_id_mismatch path=%s", session, trade_date, path)
        return {"released": False, "reason": "RUN_ID_MISMATCH", "path": str(path)}
    try:
        path.unlink()
        logger.info("[US_SESSION_GUARD][RUNNING_LOCK_RELEASED] session=%s trade_date=%s path=%s run_id=%s", session, trade_date, path, run_id or "")
        return {"released": True, "reason": "RUNNING_LOCK_RELEASED", "path": str(path)}
    except FileNotFoundError:
        return {"released": False, "reason": "NOT_FOUND", "path": str(path)}


def check_us_session_file_guard(trade_date: str, session: str) -> dict:
    path = _done_path(trade_date, session, "schedule")
    if not path.exists():
        return {"already_ran": False, "guard_status": "NOT_FOUND", "payload": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        accepted = str(payload.get("run_type") or "schedule") == "schedule" and not bool(payload.get("offline")) and not bool(payload.get("dry_run")) and not bool(payload.get("force_now"))
        logger.info("[US_FILE_GUARD][DONE_FOUND] session=%s trade_date=%s path=%s status=%s accepted_as_schedule_done=%s", session, trade_date, str(path), payload.get("status"), accepted)
        if accepted:
            return {"already_ran": True, "guard_status": "DONE_FILE_FOUND", "payload": payload}
        return {"already_ran": False, "guard_status": "IGNORED_NON_SCHEDULE_DONE", "payload": payload}
    except Exception as exc:
        logger.warning("[US_FILE_GUARD][READ_ERROR] session=%s trade_date=%s error=%s", session, trade_date, exc)
        return {"already_ran": False, "guard_status": "READ_ERROR", "payload": {}}


def write_us_session_done_file(trade_date: str, session: str, run_id: str, started_at_et: str, finished_at_et: str, status: str, ticks: int = 0, extra: dict | None = None) -> Path | None:
    extra = extra or {}
    event_name = str(extra.get("event_name") or os.getenv("GITHUB_EVENT_NAME") or "")
    offline = bool(extra.get("offline") or os.getenv("OFFLINE") == "1")
    dry_run = bool(extra.get("dry_run") or os.getenv("DRY_RUN") == "1")
    force_now = bool(extra.get("force_now") or os.getenv("US_FORCE_NOW") or os.getenv("FORCE_NOW"))
    max_ticks = int(extra.get("max_ticks") or os.getenv("MAX_TICKS") or os.getenv("US_MAX_TICKS") or 0)
    run_type = str(extra.get("run_type") or ("schedule" if event_name == "schedule" and not offline and not dry_run and not force_now and max_ticks <= 0 else ("offline" if offline else "manual")))
    path = _done_path(trade_date, session, run_type)
    payload = {"market": "US", "session": session, "trade_date": trade_date, "run_id": run_id, "started_at_et": started_at_et, "finished_at_et": finished_at_et, "status": status, "ticks": ticks, "event_name": event_name, "workflow": extra.get("workflow") or os.getenv("GITHUB_WORKFLOW"), "offline": offline, "dry_run": dry_run, "force_now": force_now, "max_ticks": max_ticks, "run_type": run_type, "expected_min_ticks": extra.get("expected_min_ticks", 0), "wall_elapsed_sec": extra.get("wall_elapsed_sec", 0)}
    payload.update(extra)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("[US_FILE_GUARD][WRITE_DONE] session=%s trade_date=%s path=%s status=%s", session, trade_date, str(path), status)
        logger.info("[US_SESSION_GUARD][DONE_WRITTEN] session=%s trade_date=%s path=%s status=%s", session, trade_date, str(path), status)
        return path
    except Exception as exc:
        logger.warning("[US_FILE_GUARD][WRITE_ERROR] session=%s trade_date=%s error=%s", session, trade_date, exc)
        return None
