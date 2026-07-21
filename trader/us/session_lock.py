"""US session duplicate lock helpers shared by tests and WSL wrappers."""
from __future__ import annotations

import argparse
import json
import os
import socket
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass
class SessionLockResult:
    ok: bool
    reason: str = "ok"
    path: str = ""
    stale_pid: int = 0


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


def acquire_session_lock(
    market: str,
    session: str,
    *,
    trade_date: str,
    min_interval_sec: int = 60,
    lock_dir: str | Path = "runtime/locks",
) -> SessionLockResult:
    root = Path(lock_dir)
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{str(market).lower()}-{str(session).lower()}-{trade_date}.json"
    now = datetime.now(timezone.utc).timestamp()
    stale_pid = 0
    stale_removed = False
    if path.exists():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            last = float(payload.get("timestamp", 0) or 0)
            if payload.get("status") == "running":
                stale_pid = int(payload.get("pid") or 0)
                if _pid_alive(stale_pid):
                    return SessionLockResult(False, "session_already_running", str(path), stale_pid)
                path.unlink()
                stale_removed = True
            elif now - last < min_interval_sec:
                return SessionLockResult(False, "duplicate_recent_run", str(path))
        except Exception:
            pass
    path.write_text(
        json.dumps(
            {
                "market": market,
                "session": session,
                "trade_date": trade_date,
                "timestamp": now,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "pid": os.getpid(),
                "hostname": socket.gethostname(),
                "cmdline": " ".join(sys.argv),
                "status": "running",
            }
        ),
        encoding="utf-8",
    )
    return SessionLockResult(True, "stale_lock_removed" if stale_removed else "ok", str(path), stale_pid)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Acquire a US duplicate-session guard lock")
    parser.add_argument("--market", default="us")
    parser.add_argument("--session", required=True)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--min-interval-sec", type=int, default=60)
    parser.add_argument("--lock-dir", default="runtime/locks")
    args = parser.parse_args(argv)
    result = acquire_session_lock(
        args.market,
        args.session,
        trade_date=args.trade_date,
        min_interval_sec=args.min_interval_sec,
        lock_dir=args.lock_dir,
    )
    print(json.dumps(result.__dict__, ensure_ascii=False))
    return 0 if result.ok else 10


if __name__ == "__main__":
    raise SystemExit(main())
