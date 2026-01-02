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

DEFAULT_BOTSTATE_WORKTREE_DIR = "_botstate"
BOTSTATE_WORKTREE_DIR_ENV = "BOTSTATE_WORKTREE_DIR"


def resolve_botstate_worktree_dir() -> Path:
    return Path(os.getenv(BOTSTATE_WORKTREE_DIR_ENV, DEFAULT_BOTSTATE_WORKTREE_DIR)).resolve()


def _run(cmd: list[str], cwd: Path | None = None) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(cmd, cwd=cwd, check=True, text=True, capture_output=True)
    except subprocess.CalledProcessError as exc:
        logger.error(
            "[BOTSTATE][CMD-ERROR] cmd=%s cwd=%s returncode=%s stdout=%s stderr=%s",
            exc.cmd,
            cwd,
            exc.returncode,
            exc.stdout,
            exc.stderr,
        )
        raise


def _git(worktree_dir: Path, *args: str) -> subprocess.CompletedProcess:
    return _run(["git", "-C", str(worktree_dir), *args])


def _configure_safe_directories(base_dir: Path, worktree_dir: Path) -> None:
    base_dir_resolved = base_dir.resolve()
    worktree_dir_resolved = worktree_dir.resolve()
    for path in {base_dir_resolved, worktree_dir_resolved}:
        _run(["git", "config", "--global", "--add", "safe.directory", str(path)])


def setup_worktree(base_dir: Path, worktree_dir: Path, target_branch: str = "bot-state") -> None:
    base_dir = base_dir.resolve()
    worktree_dir = worktree_dir.resolve()
    worktree_dir.mkdir(parents=True, exist_ok=True)
    _configure_safe_directories(base_dir, worktree_dir)
    try:
        _run(["git", "worktree", "add", "-B", target_branch, str(worktree_dir), target_branch], cwd=base_dir)
    except subprocess.CalledProcessError:
        _run(["git", "fetch", "origin", f"{target_branch}:{target_branch}"], cwd=base_dir)
        _run(["git", "worktree", "add", "-B", target_branch, str(worktree_dir), target_branch], cwd=base_dir)
    _git(worktree_dir, "pull", "--rebase")


def _lock_path(worktree_dir: Path) -> Path:
    return worktree_dir / "bot_state" / "locks" / "trader.lock.json"


def acquire_lock(worktree_dir: Path, owner: str, run_id: str, ttl_sec: int = 900) -> bool:
    worktree_dir = worktree_dir.resolve()
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
    temp_path = lock_path.with_name(f"{lock_path.name}.tmp")
    temp_path.write_text(json.dumps(lock_payload))
    temp_path.replace(lock_path)
    lock_rel_path = lock_path.relative_to(worktree_dir)
    _git(worktree_dir, "add", str(lock_rel_path))
    push_retry(worktree_dir, message=f"lock run_id={run_id}")
    logger.info("[BOTSTATE][LOCK-ACQUIRED] owner=%s run_id=%s", owner, run_id)
    return True


def release_lock(worktree_dir: Path, run_id: str) -> None:
    worktree_dir = worktree_dir.resolve()
    lock_path = _lock_path(worktree_dir)
    if lock_path.exists():
        lock_path.unlink()
        lock_rel_path = lock_path.relative_to(worktree_dir)
        _git(worktree_dir, "add", "-u", str(lock_rel_path))
        push_retry(worktree_dir, message=f"unlock run_id={run_id}")
        logger.info("[BOTSTATE][LOCK-RELEASED] run_id=%s", run_id)


def persist_run_files(worktree_dir: Path, new_files: Iterable[Path], message: str) -> None:
    worktree_dir = worktree_dir.resolve()
    files = list(new_files)
    for path in files:
        try:
            if path.resolve().is_relative_to(worktree_dir.resolve()):
                target = path.resolve()
            else:
                if "bot_state" in path.parts:
                    idx = path.parts.index("bot_state")
                    rel = Path(*path.parts[idx:])
                else:
                    rel = Path("bot_state") / Path(*path.parts[-4:])
                target = (worktree_dir / rel).resolve()
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(path.read_bytes())
            rel_target = target.relative_to(worktree_dir)
            _git(worktree_dir, "add", str(rel_target))
        except Exception:
            continue
    push_retry(worktree_dir, message=message)
    logger.info("[BOTSTATE][PERSIST] files=%s message=%s", len(files), message)


def push_retry(worktree_dir: Path, message: str, retries: int = 3) -> None:
    worktree_dir = worktree_dir.resolve()
    for attempt in range(1, retries + 1):
        try:
            _git(worktree_dir, "commit", "-m", message)
        except subprocess.CalledProcessError:
            pass
        try:
            _git(worktree_dir, "pull", "--rebase")
            _git(worktree_dir, "push")
            logger.info("[BOTSTATE][PUSH] message=%s attempt=%s", message, attempt)
            return
        except subprocess.CalledProcessError as e:
            if attempt == retries:
                raise
            time.sleep(2 * attempt)
