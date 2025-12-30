from __future__ import annotations

import json
import os
import subprocess
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")


def _run(cmd: list[str], cwd: Path | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=cwd, check=True, text=True, capture_output=True)


def setup_worktree(base_dir: Path, worktree_dir: Path, target_branch: str = "bot-state") -> None:
    worktree_dir.mkdir(parents=True, exist_ok=True)
    try:
        _run(["git", "worktree", "add", "-B", target_branch, str(worktree_dir), target_branch], cwd=base_dir)
    except subprocess.CalledProcessError:
        _run(["git", "fetch", "origin", f"{target_branch}:{target_branch}"], cwd=base_dir)
        _run(["git", "worktree", "add", "-B", target_branch, str(worktree_dir), target_branch], cwd=base_dir)
    _run(["git", "pull", "--rebase"], cwd=worktree_dir)


def _lock_path(worktree_dir: Path) -> Path:
    return worktree_dir / "bot_state" / "locks" / "trader.lock.json"


def acquire_lock(worktree_dir: Path, owner: str, run_id: str, ttl_sec: int = 900) -> bool:
    lock_path = _lock_path(worktree_dir)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(tz=KST)
    if lock_path.exists():
        try:
            payload = json.loads(lock_path.read_text())
            ts = datetime.fromisoformat(payload.get("ts"))
            ttl = int(payload.get("ttl_sec") or ttl_sec)
            if ts + timedelta(seconds=ttl) > now:
                logger.warning("[BOTSTATE][LOCKED] owner=%s run_id=%s until=%s", payload.get("owner"), payload.get("run_id"), ts + timedelta(seconds=ttl))
                return False
        except Exception:
            pass
    lock_payload = {
        "owner": owner,
        "run_id": run_id,
        "ts": now.isoformat(),
        "ttl_sec": ttl_sec,
    }
    lock_path.write_text(json.dumps(lock_payload))
    _run(["git", "add", str(lock_path)], cwd=worktree_dir)
    push_retry(worktree_dir, message=f"lock run_id={run_id}")
    logger.info("[BOTSTATE][LOCK-ACQUIRED] owner=%s run_id=%s", owner, run_id)
    return True


def release_lock(worktree_dir: Path, run_id: str) -> None:
    lock_path = _lock_path(worktree_dir)
    if lock_path.exists():
        lock_path.unlink()
        _run(["git", "add", "-u"], cwd=worktree_dir)
        push_retry(worktree_dir, message=f"unlock run_id={run_id}")
        logger.info("[BOTSTATE][LOCK-RELEASED] run_id=%s", run_id)


def persist_run_files(worktree_dir: Path, new_files: Iterable[Path], message: str) -> None:
    files = list(new_files)
    for path in files:
        try:
            if path.resolve().is_relative_to(worktree_dir.resolve()):
                target = path
            else:
                if "bot_state" in path.parts:
                    idx = path.parts.index("bot_state")
                    rel = Path(*path.parts[idx:])
                else:
                    rel = Path("bot_state") / Path(*path.parts[-4:])
                target = worktree_dir / rel
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(path.read_bytes())
            _run(["git", "add", str(target)], cwd=worktree_dir)
        except Exception:
            continue
    push_retry(worktree_dir, message=message)
    logger.info("[BOTSTATE][PERSIST] files=%s message=%s", len(files), message)


def push_retry(worktree_dir: Path, message: str, retries: int = 3) -> None:
    for attempt in range(1, retries + 1):
        try:
            _run(["git", "commit", "-m", message], cwd=worktree_dir)
        except subprocess.CalledProcessError:
            pass
        try:
            _run(["git", "pull", "--rebase"], cwd=worktree_dir)
            _run(["git", "push"], cwd=worktree_dir)
            logger.info("[BOTSTATE][PUSH] message=%s attempt=%s", message, attempt)
            return
        except subprocess.CalledProcessError as e:
            if attempt == retries:
                raise
            time.sleep(2 * attempt)
