"""US session duplicate lock helpers shared by tests and WSL wrappers."""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass
class SessionLockResult:
    ok: bool
    reason: str = "ok"
    path: str = ""


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
    if path.exists():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            last = float(payload.get("timestamp", 0) or 0)
            if now - last < min_interval_sec:
                return SessionLockResult(False, "duplicate_recent_run", str(path))
            if payload.get("status") == "running":
                return SessionLockResult(False, "session_already_running", str(path))
        except Exception:
            pass
    path.write_text(
        json.dumps(
            {
                "market": market,
                "session": session,
                "trade_date": trade_date,
                "timestamp": now,
                "status": "running",
            }
        ),
        encoding="utf-8",
    )
    return SessionLockResult(True, "ok", str(path))


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
