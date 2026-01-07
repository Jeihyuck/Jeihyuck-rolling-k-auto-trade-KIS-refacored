from __future__ import annotations

import json
import os
import subprocess
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")

DEFAULT_BOTSTATE_WORKTREE_DIR = "_botstate"
BOTSTATE_WORKTREE_DIR_ENV = "BOTSTATE_WORKTREE_DIR"
DEFAULT_LOCK_TTL_SEC = 240
DEFAULT_LOCK_RETRY_SEC = 60
LOCK_RETRY_STEP_SEC = 10


def _lock_ttl_sec() -> int:
    try:
        return int(os.getenv("BOTSTATE_LOCK_TTL_SEC", str(DEFAULT_LOCK_TTL_SEC)))
    except Exception:
        return DEFAULT_LOCK_TTL_SEC


def _lock_retry_total_sec() -> int:
    try:
        return int(os.getenv("BOTSTATE_LOCK_RETRY_TOTAL_SEC", str(DEFAULT_LOCK_RETRY_SEC)))
    except Exception:
        return DEFAULT_LOCK_RETRY_SEC


def resolve_botstate_worktree_dir() -> Path:
    return Path(os.getenv(BOTSTATE_WORKTREE_DIR_ENV, DEFAULT_BOTSTATE_WORKTREE_DIR)).resolve()


def _run(cmd: list[str], cwd: Path | None = None, *, check: bool = True) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(cmd, cwd=cwd, check=check, text=True, capture_output=True)
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


def git_porcelain(worktree_dir: Path) -> str:
    return _git(worktree_dir, "status", "--porcelain").stdout


def stage_all(worktree_dir: Path) -> None:
    _git(worktree_dir, "add", "-A")


def commit_if_staged(worktree_dir: Path, message: str) -> bool:
    diff_proc = _git(worktree_dir, "diff", "--cached", "--quiet", check=False)
    if diff_proc.returncode == 0:
        return False
    _git(worktree_dir, "commit", "-m", message)
    return True


def ensure_clean_before_rebase(worktree_dir: Path, message_for_autosave: str) -> None:
    status = git_porcelain(worktree_dir)
    files = [line.strip() for line in status.splitlines() if line.strip()]
    dirty = bool(files)
    logger.info("[BOTSTATE][GIT] dirty=%s files=%s", dirty, files)
    if not dirty:
        return
    stage_all(worktree_dir)
    committed = commit_if_staged(worktree_dir, message_for_autosave)
    logger.info("[BOTSTATE][GIT] committed=%s msg=%s", committed, message_for_autosave)


def _git(worktree_dir: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    return _run(["git", "-C", str(worktree_dir), *args], check=check)


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


def acquire_lock(worktree_dir: Path, owner: str, run_id: str, ttl_sec: int | None = None) -> bool:
    worktree_dir = worktree_dir.resolve()
    lock_path = _lock_path(worktree_dir)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    ttl_env = _lock_ttl_sec()
    ttl_sec = min(ttl_sec, ttl_env) if ttl_sec is not None else ttl_env
    retry_total_sec = max(0, _lock_retry_total_sec())
    attempts = max(1, (retry_total_sec // LOCK_RETRY_STEP_SEC) + 1)

    for attempt in range(1, attempts + 1):
        now = datetime.now(tz=KST)
        locked = False
        stale_takeover = False
        if lock_path.exists():
            try:
                payload = json.loads(lock_path.read_text())
                ts_raw = payload.get("ts")
                ts = datetime.fromisoformat(ts_raw) if ts_raw else None
                ttl = int(payload.get("ttl_sec") or ttl_sec)
                if ts is None:
                    stale_takeover = True
                else:
                    until = ts + timedelta(seconds=ttl)
                    if until > now:
                        locked = True
                        logger.warning(
                            "[BOTSTATE][LOCKED] owner=%s run_id=%s until=%s",
                            payload.get("owner"),
                            payload.get("run_id"),
                            until,
                        )
                    else:
                        stale_takeover = True
                if stale_takeover:
                    logger.warning(
                        "[BOTSTATE][STALE_TAKEOVER] owner=%s run_id=%s",
                        payload.get("owner"),
                        payload.get("run_id"),
                    )
            except Exception as exc:
                logger.warning("[BOTSTATE][STALE_TAKEOVER] reason=parse_error err=%s", exc)
                stale_takeover = True

        if not locked:
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
            logger.info("[BOTSTATE][LOCK_ACQUIRED] owner=%s run_id=%s ttl_sec=%s", owner, run_id, ttl_sec)
            return True

        if attempt < attempts:
            logger.info("[BOTSTATE][RETRY] attempt=%s total=%s sleep=%s", attempt, attempts, LOCK_RETRY_STEP_SEC)
            time.sleep(LOCK_RETRY_STEP_SEC)

    return False


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


def _pull_with_autostash(worktree_dir: Path, branch: str) -> bool:
    try:
        _git(worktree_dir, "pull", "--rebase", "--autostash", "origin", branch)
        return True
    except subprocess.CalledProcessError as exc:
        stdout = exc.stdout or ""
        stderr = exc.stderr or ""
        if "--autostash" in stderr or "unknown option" in stderr or "unknown option" in stdout:
            logger.warning("[BOTSTATE][GIT] --autostash unsupported fallback -> manual stash")
            _git(worktree_dir, "stash", "push", "-u", "-m", "autostash botstate")
            try:
                _git(worktree_dir, "pull", "--rebase", "origin", branch)
                return True
            finally:
                try:
                    _git(worktree_dir, "stash", "pop")
                except subprocess.CalledProcessError:
                    logger.warning("[BOTSTATE][GIT] stash pop failed after manual autostash")
                    raise
        raise


def push_retry(worktree_dir: Path, message: str, retries: int = 3) -> None:
    worktree_dir = worktree_dir.resolve()
    branch = _git(worktree_dir, "rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
    for attempt in range(1, retries + 1):
        pull_ok = False
        push_ok = False
        retry_reason = ""
        try:
            _git(worktree_dir, "fetch", "origin", branch)
            committed = commit_if_staged(worktree_dir, message)
            logger.info("[BOTSTATE][GIT] committed=%s msg=%s", committed, message)
            ensure_clean_before_rebase(worktree_dir, message_for_autosave=f"autosave before {message}")
            rev_list = _git(worktree_dir, "rev-list", "--left-right", "--count", f"origin/{branch}...HEAD").stdout.strip()
            try:
                behind, ahead = (int(x) for x in rev_list.split())
            except Exception:
                behind, ahead = 0, 0
            logger.info("[BOTSTATE][GIT] behind=%s ahead=%s", behind, ahead)

            if behind == 0 and ahead > 0:
                _git(worktree_dir, "push")
                push_ok = True
            elif behind > 0:
                pull_ok = _pull_with_autostash(worktree_dir, branch)
                _git(worktree_dir, "push")
                push_ok = True
            else:
                _git(worktree_dir, "push")
                push_ok = True

            logger.info("[BOTSTATE][GIT] push_ok=%s pull_ok=%s attempt=%s", push_ok, pull_ok, attempt)
            return
        except subprocess.CalledProcessError as exc:
            retry_reason = (exc.stderr or "").strip() or (exc.stdout or "").strip() or str(exc)
            logger.warning("[BOTSTATE][GIT] push_ok=%s pull_ok=%s retry_reason=%s attempt=%s", push_ok, pull_ok, retry_reason, attempt)
            if "non-fast-forward" in retry_reason or "fetch first" in retry_reason.lower():
                ensure_clean_before_rebase(worktree_dir, message_for_autosave=f"autosave before {message}")
                try:
                    pull_ok = _pull_with_autostash(worktree_dir, branch)
                except subprocess.CalledProcessError as pull_exc:
                    retry_reason = (pull_exc.stderr or "").strip() or (pull_exc.stdout or "").strip() or str(pull_exc)
            if attempt == retries:
                raise
            time.sleep(2 * attempt)
