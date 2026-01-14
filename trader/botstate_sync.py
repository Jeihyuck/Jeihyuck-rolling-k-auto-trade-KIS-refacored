from __future__ import annotations

import json
import os
import subprocess
import time
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional, Tuple
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")

DEFAULT_BOTSTATE_WORKTREE_DIR = "_botstate"
BOTSTATE_WORKTREE_DIR_ENV = "BOTSTATE_WORKTREE_DIR"
SYNC_MODE_FETCH_RESET = "FETCH_RESET"
BOTSTATE_SYNC_MODE_ENV = "BOTSTATE_SYNC_MODE"  # optional
DEFAULT_LOCK_TTL_SEC = 240
DEFAULT_LOCK_RETRY_SEC = 55
DEFAULT_LOCK_RETRY_SLEEP_SEC = 5


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


def _lock_retry_sleep_sec() -> int:
    try:
        return int(os.getenv("BOTSTATE_LOCK_RETRY_SLEEP_SEC", str(DEFAULT_LOCK_RETRY_SLEEP_SEC)))
    except Exception:
        return DEFAULT_LOCK_RETRY_SLEEP_SEC


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


@dataclass
class GitSyncStatus:
    remote: str
    branch: str
    remote_ref: str
    ahead: int
    behind: int


def _run_git(cmd: list[str], cwd: Optional[str] = None, check: bool = True) -> Tuple[int, str]:
    p = subprocess.run(
        cmd,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    out = (p.stdout or "").strip()
    if check and p.returncode != 0:
        raise RuntimeError(f"git command failed: {' '.join(cmd)}\n{out}")
    return p.returncode, out


def git_fetch_reset(remote: str, branch: str, cwd: str) -> GitSyncStatus:
    """
    Deterministic sync:
      fetch origin <branch>
      reset --hard origin/<branch>
    Then compute ahead/behind vs origin/<branch>.
    """
    remote_ref = f"{remote}/{branch}"
    _run_git(["git", "fetch", remote, branch], cwd=cwd, check=True)
    _run_git(["git", "reset", "--hard", remote_ref], cwd=cwd, check=True)
    _, cnt = _run_git(
        ["git", "rev-list", "--left-right", "--count", f"HEAD...{remote_ref}"],
        cwd=cwd,
        check=True,
    )
    left, right = cnt.split()
    ahead = int(left)
    behind = int(right)
    return GitSyncStatus(remote=remote, branch=branch, remote_ref=remote_ref, ahead=ahead, behind=behind)


def git_push(remote: str, branch: str, cwd: str) -> Tuple[bool, str]:
    rc, out = _run_git(["git", "push", remote, f"HEAD:{branch}"], cwd=cwd, check=False)
    ok = rc == 0
    return ok, out


def is_non_fast_forward(push_output: str) -> bool:
    s = (push_output or "").lower()
    return ("non-fast-forward" in s) or ("fetch first" in s) or ("rejected" in s)


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
    git_fetch_reset("origin", target_branch, cwd=str(worktree_dir))


def _lock_path(worktree_dir: Path) -> Path:
    return worktree_dir / "bot_state" / "locks" / "trader.lock.json"


def acquire_lock(worktree_dir: Path, owner: str, run_id: str, ttl_sec: int | None = None) -> bool:
    worktree_dir = worktree_dir.resolve()
    lock_path = _lock_path(worktree_dir)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    branch = _git(worktree_dir, "rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
    remote = "origin"
    sync_mode = SYNC_MODE_FETCH_RESET
    ttl_env = _lock_ttl_sec()
    ttl_sec = ttl_sec if ttl_sec is not None else ttl_env
    retry_total_sec = max(0, _lock_retry_total_sec())
    retry_sleep_sec = max(1, _lock_retry_sleep_sec())
    deadline_ts = time.time() + retry_total_sec
    attempt = 0

    logger.info(
        "[BOTSTATE][SYNC] SYNC_MODE=%s step=pre_lock_sync remote=%s branch=%s",
        sync_mode,
        remote,
        branch,
    )

    while time.time() < deadline_ts:
        attempt += 1
        try:
            st = git_fetch_reset(remote=remote, branch=branch, cwd=str(worktree_dir))
            logger.info(
                "[BOTSTATE][SYNC] SYNC_MODE=%s step=post_sync remote_ref=%s behind=%d ahead=%d attempt=%d",
                sync_mode,
                st.remote_ref,
                st.behind,
                st.ahead,
                attempt,
            )
        except Exception as exc:
            logger.warning(
                "[BOTSTATE][SYNC][ERR] SYNC_MODE=%s step=sync_failed attempt=%d err=%s",
                sync_mode,
                attempt,
                str(exc),
            )
            time.sleep(retry_sleep_sec)
            continue

        now = datetime.now(tz=KST)
        locked = False
        stale_takeover = False
        locked_until = None
        current_owner = None
        current_run_id = None
        if lock_path.exists():
            try:
                payload = json.loads(lock_path.read_text())
                current_owner = payload.get("owner")
                current_run_id = payload.get("run_id")
                ts_raw = payload.get("ts")
                ts = datetime.fromisoformat(ts_raw) if ts_raw else None
                ttl = int(payload.get("ttl_sec") or ttl_sec)
                if ts is None:
                    stale_takeover = True
                else:
                    locked_until = ts + timedelta(seconds=ttl)
                    if locked_until > now:
                        locked = True
                    else:
                        stale_takeover = True
                if stale_takeover:
                    logger.warning(
                        "[BOTSTATE][STALE_TAKEOVER] prev_owner=%s prev_run_id=%s prev_until=%s",
                        current_owner,
                        current_run_id,
                        locked_until,
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
            committed = commit_if_staged(worktree_dir, message=f"lock run_id={run_id}")
            logger.info(
                "[BOTSTATE][GIT] SYNC_MODE=%s step=lock_commit committed=%s attempt=%d",
                sync_mode,
                committed,
                attempt,
            )
            push_ok, out = git_push(remote=remote, branch=branch, cwd=str(worktree_dir))
            if push_ok:
                logger.info(
                    "[BOTSTATE][PUSH] SYNC_MODE=%s push_ok=True attempt=%d",
                    sync_mode,
                    attempt,
                )
                logger.info(
                    "[BOTSTATE][LOCK_ACQUIRED] owner=%s run_id=%s ttl_sec=%d",
                    owner,
                    run_id,
                    ttl_sec,
                )
                return True

            logger.warning(
                "[BOTSTATE][PUSH][FAIL] SYNC_MODE=%s push_ok=False attempt=%d non_ff=%s out=%s",
                sync_mode,
                attempt,
                is_non_fast_forward(out),
                (out[-400:] if out else ""),
            )
            try:
                st2 = git_fetch_reset(remote=remote, branch=branch, cwd=str(worktree_dir))
                logger.info(
                    "[BOTSTATE][SYNC] SYNC_MODE=%s step=resync_after_push_fail remote_ref=%s behind=%d ahead=%d attempt=%d",
                    sync_mode,
                    st2.remote_ref,
                    st2.behind,
                    st2.ahead,
                    attempt,
                )
            except Exception as exc:
                logger.warning(
                    "[BOTSTATE][SYNC][ERR] SYNC_MODE=%s step=resync_failed attempt=%d err=%s",
                    sync_mode,
                    attempt,
                    str(exc),
                )
            time.sleep(retry_sleep_sec)
            continue

        logger.info(
            "[BOTSTATE][RETRY] wait=%s locked_until=%s attempt=%s",
            retry_sleep_sec,
            locked_until.isoformat() if locked_until else "unknown",
            attempt,
        )
        time.sleep(retry_sleep_sec)

    logger.error("[BOTSTATE][LOCK_ACQUIRE][TIMEOUT] SYNC_MODE=%s attempts=%d", sync_mode, attempt)
    return False


def release_lock(worktree_dir: Path, owner: str, run_id: str) -> None:
    worktree_dir = worktree_dir.resolve()
    lock_path = _lock_path(worktree_dir)
    branch = _git(worktree_dir, "rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
    remote = "origin"
    sync_mode = SYNC_MODE_FETCH_RESET
    retry_total_sec = max(0, _lock_retry_total_sec())
    retry_sleep_sec = max(1, _lock_retry_sleep_sec())
    deadline_ts = time.time() + retry_total_sec
    attempt = 0

    while time.time() < deadline_ts:
        attempt += 1
        try:
            st = git_fetch_reset(remote=remote, branch=branch, cwd=str(worktree_dir))
            logger.info(
                "[BOTSTATE][SYNC] SYNC_MODE=%s step=post_sync remote_ref=%s behind=%d ahead=%d attempt=%d",
                sync_mode,
                st.remote_ref,
                st.behind,
                st.ahead,
                attempt,
            )
        except Exception as exc:
            logger.warning(
                "[BOTSTATE][SYNC][ERR] SYNC_MODE=%s step=sync_failed attempt=%d err=%s",
                sync_mode,
                attempt,
                str(exc),
            )
            time.sleep(retry_sleep_sec)
            continue

        if not lock_path.exists():
            return
        try:
            payload = json.loads(lock_path.read_text())
        except Exception as exc:
            logger.warning("[BOTSTATE][LOCK_RELEASE_SKIP] reason=parse_error err=%s", exc)
            return
        if payload.get("owner") != owner or payload.get("run_id") != run_id:
            logger.warning(
                "[BOTSTATE][LOCK_RELEASE_SKIP] owner=%s run_id=%s current_owner=%s current_run_id=%s",
                owner,
                run_id,
                payload.get("owner"),
                payload.get("run_id"),
            )
            return
        lock_path.unlink()
        lock_rel_path = lock_path.relative_to(worktree_dir)
        _git(worktree_dir, "add", "-u", str(lock_rel_path))
        committed = commit_if_staged(worktree_dir, message=f"unlock run_id={run_id}")
        logger.info(
            "[BOTSTATE][GIT] SYNC_MODE=%s step=unlock_commit committed=%s attempt=%d",
            sync_mode,
            committed,
            attempt,
        )
        push_ok, out = git_push(remote=remote, branch=branch, cwd=str(worktree_dir))
        if push_ok:
            logger.info(
                "[BOTSTATE][PUSH] SYNC_MODE=%s push_ok=True attempt=%d",
                sync_mode,
                attempt,
            )
            logger.info("[BOTSTATE][LOCK_RELEASED] owner=%s run_id=%s", owner, run_id)
            return
        logger.warning(
            "[BOTSTATE][PUSH][FAIL] SYNC_MODE=%s push_ok=False attempt=%d non_ff=%s out=%s",
            sync_mode,
            attempt,
            is_non_fast_forward(out),
            (out[-400:] if out else ""),
        )
        try:
            st2 = git_fetch_reset(remote=remote, branch=branch, cwd=str(worktree_dir))
            logger.info(
                "[BOTSTATE][SYNC] SYNC_MODE=%s step=resync_after_push_fail remote_ref=%s behind=%d ahead=%d attempt=%d",
                sync_mode,
                st2.remote_ref,
                st2.behind,
                st2.ahead,
                attempt,
            )
        except Exception as exc:
            logger.warning(
                "[BOTSTATE][SYNC][ERR] SYNC_MODE=%s step=resync_failed attempt=%d err=%s",
                sync_mode,
                attempt,
                str(exc),
            )
        time.sleep(retry_sleep_sec)


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
    branch = _git(worktree_dir, "rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
    remote = "origin"
    sync_mode = SYNC_MODE_FETCH_RESET
    for attempt in range(1, retries + 1):
        try:
            st = git_fetch_reset(remote=remote, branch=branch, cwd=str(worktree_dir))
            logger.info(
                "[BOTSTATE][SYNC] SYNC_MODE=%s step=post_sync remote_ref=%s behind=%d ahead=%d attempt=%d",
                sync_mode,
                st.remote_ref,
                st.behind,
                st.ahead,
                attempt,
            )
        except Exception as exc:
            logger.warning(
                "[BOTSTATE][SYNC][ERR] SYNC_MODE=%s step=sync_failed attempt=%d err=%s",
                sync_mode,
                attempt,
                str(exc),
            )
            if attempt == retries:
                raise
            time.sleep(2 * attempt)
            continue

        committed = commit_if_staged(worktree_dir, message)
        logger.info(
            "[BOTSTATE][GIT] SYNC_MODE=%s step=commit committed=%s msg=%s attempt=%d",
            sync_mode,
            committed,
            message,
            attempt,
        )
        push_ok, out = git_push(remote=remote, branch=branch, cwd=str(worktree_dir))
        if push_ok:
            logger.info(
                "[BOTSTATE][PUSH] SYNC_MODE=%s push_ok=True attempt=%d",
                sync_mode,
                attempt,
            )
            return

        logger.warning(
            "[BOTSTATE][PUSH][FAIL] SYNC_MODE=%s push_ok=False attempt=%d non_ff=%s out=%s",
            sync_mode,
            attempt,
            is_non_fast_forward(out),
            (out[-400:] if out else ""),
        )
        try:
            st2 = git_fetch_reset(remote=remote, branch=branch, cwd=str(worktree_dir))
            logger.info(
                "[BOTSTATE][SYNC] SYNC_MODE=%s step=resync_after_push_fail remote_ref=%s behind=%d ahead=%d attempt=%d",
                sync_mode,
                st2.remote_ref,
                st2.behind,
                st2.ahead,
                attempt,
            )
        except Exception as exc:
            logger.warning(
                "[BOTSTATE][SYNC][ERR] SYNC_MODE=%s step=resync_failed attempt=%d err=%s",
                sync_mode,
                attempt,
                str(exc),
            )
        if attempt == retries:
            raise RuntimeError(f"git push failed after retries: {out}")
        time.sleep(2 * attempt)
