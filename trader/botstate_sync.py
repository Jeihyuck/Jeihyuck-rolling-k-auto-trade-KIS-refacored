from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional, Tuple
from zoneinfo import ZoneInfo

from trader.utils.env import env_bool

logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")

BOTSTATE_WORKTREE_DIR_ENV = "BOTSTATE_WORKTREE_DIR"
SYNC_MODE_FETCH_RESET = "FETCH_RESET"
BOTSTATE_SYNC_MODE_ENV = "BOTSTATE_SYNC_MODE"  # optional
DEFAULT_LOCK_TTL_SEC = 240
DEFAULT_LOCK_RETRY_SEC = 55
DEFAULT_LOCK_RETRY_SLEEP_SEC = 5
DEFAULT_LOCK_BUFFER_SEC = 180
DEFAULT_LOCK_GRACE_SEC = 60
BOTSTATE_GITIGNORE_TEXT = """\
# --- botstate (tracked artifacts) ---
# Keep universe artifacts, diagnostics, db, and minimal runtime meta.

# Large caches MUST NOT be committed
bot_state/runtime/ohlcv_cache/
bot_state/runtime/ohlcv_cache/**

# Optional: other noisy runtime temp (필요시 켜기)
# bot_state/runtime/tmp/
# bot_state/runtime/tmp/**

# Python / OS noise
.DS_Store
__pycache__/
*.pyc
"""


def ensure_botstate_gitignore(workdir: str) -> None:
    # workdir is the botstate worktree root
    path = os.path.join(workdir, "bot_state", ".gitignore")
    os.makedirs(os.path.dirname(path), exist_ok=True)

    current = None
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            current = f.read()

    if current != BOTSTATE_GITIGNORE_TEXT:
        with open(path, "w", encoding="utf-8") as f:
            f.write(BOTSTATE_GITIGNORE_TEXT)


def ensure_writable_path(path: str | Path, *, is_dir: bool) -> None:
    target = Path(path)
    if is_dir:
        target.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(target, 0o700)
        except OSError:
            logger.warning("[DB][PERM][DIR_CHMOD_FAIL] path=%s", target, exc_info=True)
    else:
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            try:
                os.chmod(target, 0o600)
            except OSError:
                logger.warning("[DB][PERM][FILE_CHMOD_FAIL] path=%s", target, exc_info=True)
    try:
        subprocess.run(
            ["sudo", "chown", "-R", "runner:runner", str(target)],
            check=False,
            text=True,
            capture_output=True,
        )
    except Exception:
        logger.info("[DB][PERM][CHOWN_SKIP] path=%s", target, exc_info=True)


def ensure_sqlite_writable(db_path: str | Path) -> None:
    db_path = Path(db_path)
    ensure_writable_path(db_path.parent, is_dir=True)
    ensure_writable_path(db_path, is_dir=False)

    import sqlite3

    conn = sqlite3.connect(str(db_path), timeout=30)
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("CREATE TABLE IF NOT EXISTS __writetest (k TEXT PRIMARY KEY, v TEXT);")
        cur.execute(
            "INSERT OR REPLACE INTO __writetest(k,v) VALUES ('t', ?);",
            (str(time.time()),),
        )
        conn.commit()
    finally:
        conn.close()

    try:
        db_stat = os.stat(db_path)
        dir_stat = os.stat(db_path.parent)
        logger.info(
            "[DB][PERM] path=%s mode=%o uid=%s gid=%s",
            db_path,
            db_stat.st_mode & 0o777,
            db_stat.st_uid,
            db_stat.st_gid,
        )
        logger.info(
            "[DB][DIR_PERM] dir=%s mode=%o uid=%s gid=%s",
            db_path.parent,
            dir_stat.st_mode & 0o777,
            dir_stat.st_uid,
            dir_stat.st_gid,
        )
    except OSError:
        logger.warning("[DB][PERM][STAT_FAIL] path=%s", db_path, exc_info=True)


def hard_reset_bot_state(bot_state_dir: Path) -> list[str]:
    deleted: list[str] = []
    targets: list[Path] = []
    db_path = bot_state_dir / "db" / "pbcore.sqlite3"
    targets.extend(
        [
            db_path,
            db_path.with_name(f"{db_path.name}-wal"),
            db_path.with_name(f"{db_path.name}-shm"),
        ]
    )
    targets.append(bot_state_dir / "universe_lkg")
    runtime_dir = bot_state_dir / "runtime"
    locks_dir = bot_state_dir / "locks"

    for path in targets:
        try:
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
                deleted.append(str(path))
            elif path.exists():
                path.unlink()
                deleted.append(str(path))
        except Exception:
            logger.warning("[BOTSTATE][HARD_RESET][SKIP] path=%s", path, exc_info=True)

    if runtime_dir.exists():
        for child in runtime_dir.iterdir():
            if child.name in {".gitkeep", ".gitignore"}:
                continue
            try:
                if child.is_dir():
                    shutil.rmtree(child, ignore_errors=True)
                else:
                    child.unlink()
                deleted.append(str(child))
            except Exception:
                logger.warning("[BOTSTATE][HARD_RESET][SKIP] path=%s", child, exc_info=True)

    if locks_dir.exists():
        for lock_path in locks_dir.glob("*.json"):
            try:
                lock_path.unlink()
                deleted.append(str(lock_path))
            except Exception:
                logger.warning("[BOTSTATE][HARD_RESET][SKIP] path=%s", lock_path, exc_info=True)

    if runtime_dir.exists():
        for lock_path in runtime_dir.glob("lock*.json"):
            try:
                lock_path.unlink()
                deleted.append(str(lock_path))
            except Exception:
                logger.warning("[BOTSTATE][HARD_RESET][SKIP] path=%s", lock_path, exc_info=True)

    logger.info("[BOTSTATE][HARD_RESET] deleted=%s", deleted)
    return deleted


def compute_lock_ttl(max_seconds: int) -> tuple[int, int]:
    try:
        buffer_sec = int(os.getenv("BOTSTATE_LOCK_TTL_BUFFER_SEC", str(DEFAULT_LOCK_BUFFER_SEC)))
    except Exception:
        buffer_sec = DEFAULT_LOCK_BUFFER_SEC
    try:
        base_sec = int(os.getenv("BOTSTATE_LOCK_TTL_SEC", str(DEFAULT_LOCK_TTL_SEC)))
    except Exception:
        base_sec = DEFAULT_LOCK_TTL_SEC
    ttl_base = max_seconds if max_seconds > 0 else base_sec
    ttl_sec = ttl_base + buffer_sec
    return ttl_sec, buffer_sec


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


def _lock_grace_sec() -> int:
    try:
        return int(os.getenv("BOTSTATE_LOCK_GRACE_SEC", str(DEFAULT_LOCK_GRACE_SEC)))
    except Exception:
        return DEFAULT_LOCK_GRACE_SEC


def resolve_botstate_worktree_dir(repo_dir: Path) -> Path:
    """
    Priority:
    1) BOTSTATE_WORKTREE_DIR env
    2) GitHub Actions: $RUNNER_TEMP/botstate_worktree_<GITHUB_RUN_ID>
    3) local: repo_dir / "_botstate"
    """
    raw = os.getenv(BOTSTATE_WORKTREE_DIR_ENV, "").strip()
    if raw:
        return Path(raw)

    if os.getenv("GITHUB_ACTIONS", "").lower() == "true":
        tmp = os.getenv("RUNNER_TEMP", "").strip() or "/tmp"
        run_id = os.getenv("GITHUB_RUN_ID", "").strip() or "0"
        return Path(tmp) / f"botstate_worktree_{run_id}"

    return repo_dir / "_botstate"


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
    return _git_worktree(worktree_dir, "status", "--porcelain").stdout


def stage_all(worktree_dir: Path) -> None:
    _git_worktree(worktree_dir, "add", "-A")


def stage_runtime_universe(worktree_dir: Path) -> None:
    pathspecs = [
        "bot_state/runtime/universe",
        "bot_state/runtime/diagnostics",
        "bot_state/runtime/universe_build_done_*.flag",
        "bot_state/runtime/universe_sanitize_*.json",
        "bot_state/runtime/diagnostics/universe_drop_*.json",
        "bot_state/universe_lkg",
        "bot_state/runtime/schema_version.txt",
        "bot_state/runtime/runtime_meta.json",
        "bot_state/runtime/balance_snapshot.json",
    ]
    for spec in pathspecs:
        _git_worktree(worktree_dir, "add", "-A", "--", spec, check=False)


def _cached_diff_names(worktree_dir: Path) -> list[str]:
    output = _git_worktree(worktree_dir, "diff", "--cached", "--name-only", check=False).stdout.strip()
    return [line for line in output.splitlines() if line.strip()]


def commit_if_staged(worktree_dir: Path, message: str) -> bool:
    diff_proc = _git_worktree(worktree_dir, "diff", "--cached", "--quiet", check=False)
    if diff_proc.returncode == 0:
        return False
    _git_worktree(worktree_dir, "commit", "-m", message)
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


def _git_worktree(worktree_dir: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    return _run(["git", "-C", str(worktree_dir), *args], check=check)


@dataclass
class GitSyncStatus:
    remote: str
    branch: str
    remote_ref: str
    ahead: int
    behind: int


@dataclass
class BotStateContext:
    worktree_dir: Path
    bot_state_dir: Path
    runtime_dir: Path
    branch: str
    ref: str


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


def get_dirty_status_excluding(base_dir: Path, paths_to_exclude: Iterable[str] | None = None) -> list[str]:
    excludes = [path.strip().rstrip("/") for path in (paths_to_exclude or []) if path and str(path).strip()]
    exclude_specs = [f":(exclude){path}" for path in excludes]
    pathspec = ["--", ".", *exclude_specs]
    tracked_out = _run(
        ["git", "-C", str(base_dir), "diff", "--name-status", "HEAD", *pathspec],
        check=True,
    ).stdout.strip()
    untracked_out = _run(
        ["git", "-C", str(base_dir), "ls-files", "--others", "--exclude-standard", *pathspec],
        check=True,
    ).stdout.strip()
    lines: list[str] = []
    if tracked_out:
        lines.extend([line for line in tracked_out.splitlines() if line.strip()])
    if untracked_out:
        lines.extend([f"?? {line}" for line in untracked_out.splitlines() if line.strip()])
    return lines


def _git(repo_dir: Path, cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=repo_dir, check=check, text=True, capture_output=True)


def _is_valid_git_worktree(path: Path) -> bool:
    try:
        proc = subprocess.run(
            ["git", "-C", str(path), "rev-parse", "--is-inside-work-tree"],
            check=False,
            text=True,
            capture_output=True,
        )
    except Exception:
        return False
    return proc.returncode == 0 and proc.stdout.strip().lower() == "true"


def get_worktree_paths(repo_dir: Path) -> set[Path]:
    """
    Parse: git worktree list --porcelain
    Return: set of worktree paths
    """
    out = _git(repo_dir, ["git", "worktree", "list", "--porcelain"], check=True).stdout.splitlines()
    paths: set[Path] = set()
    for line in out:
        if line.startswith("worktree "):
            p = line.split(" ", 1)[1].strip()
            if p:
                paths.add(Path(p).resolve())
    return paths


def ensure_worktree(repo_dir: Path, worktree_dir: Path, branch: str, remote_ref: str) -> None:
    """
    Guarantee worktree_dir is usable.
    - If dir exists but not registered: rmtree
    - If registered: reuse
    - If registered but broken: remove --force then recreate
    """
    wt = worktree_dir.resolve()
    registered = get_worktree_paths(repo_dir)

    if wt.exists() and wt not in registered:
        if _is_valid_git_worktree(wt):
            logger.warning("[BOTSTATE][WORKTREE][DECISION] action=adopt dir=%s registered=0", wt)
            return
        logger.warning("[BOTSTATE][WORKTREE][DECISION] action=rmtree dir=%s registered=0", wt)
        shutil.rmtree(wt, ignore_errors=True)

    registered = get_worktree_paths(repo_dir)
    if wt in registered:
        logger.info("[BOTSTATE][WORKTREE][DECISION] action=reuse dir=%s", wt)
        return

    _git(repo_dir, ["git", "worktree", "prune"], check=False)
    logger.info("[BOTSTATE][WORKTREE][DECISION] action=create dir=%s branch=%s ref=%s", wt, branch, remote_ref)
    proc = _git(repo_dir, ["git", "worktree", "add", "-B", branch, str(wt), remote_ref], check=False)
    if proc.returncode == 0:
        logger.info("[BOTSTATE][WORKTREE][CREATE_OK] dir=%s", wt)
        return

    logger.error(
        "[BOTSTATE][WORKTREE][CREATE_FAIL] rc=%s stderr=%s",
        proc.returncode,
        (proc.stderr or "").strip(),
    )
    _git(repo_dir, ["git", "worktree", "remove", "--force", str(wt)], check=False)
    shutil.rmtree(wt, ignore_errors=True)
    _git(repo_dir, ["git", "worktree", "prune"], check=False)

    _git(repo_dir, ["git", "worktree", "add", "-B", branch, str(wt), remote_ref], check=True)
    logger.info("[BOTSTATE][WORKTREE][CREATE_OK] dir=%s retried=1", wt)


def setup_worktree(base_dir: Path, worktree_dir: Path, target_branch: str = "bot-state") -> BotStateContext:
    base_dir = base_dir.resolve()
    worktree_dir = worktree_dir.resolve()
    _configure_safe_directories(base_dir, worktree_dir)
    dirty_files = get_dirty_status_excluding(base_dir, paths_to_exclude=["bot_state", "runtime"])
    if dirty_files:
        raise RuntimeError(f"code worktree dirty; refusing botstate sync: {dirty_files}")

    remote = "origin"
    remote_ref = f"{remote}/{target_branch}"
    _run(["git", "fetch", remote, "--prune"], cwd=base_dir)
    ensure_worktree(base_dir, worktree_dir, target_branch, remote_ref)
    _run(["git", "-C", str(worktree_dir), "reset", "--hard", remote_ref], cwd=base_dir)
    bot_state_dir = worktree_dir / "bot_state"
    ensure_sqlite_writable(bot_state_dir / "db" / "pbcore.sqlite3")
    ensure_writable_path(bot_state_dir / "runtime", is_dir=True)
    ensure_writable_path(bot_state_dir / "trader_ledger", is_dir=True)
    os.environ["BOTSTATE_ROOT"] = str(bot_state_dir)
    return BotStateContext(
        worktree_dir=worktree_dir,
        bot_state_dir=bot_state_dir,
        runtime_dir=bot_state_dir / "runtime",
        branch=target_branch,
        ref=remote_ref,
    )


def _lock_path(worktree_dir: Path) -> Path:
    return worktree_dir / "bot_state" / "locks" / "trader.lock.json"


def acquire_lock(worktree_dir: Path, owner: str, run_id: str, ttl_sec: int | None = None) -> bool:
    worktree_dir = worktree_dir.resolve()
    lock_path = _lock_path(worktree_dir)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    branch = _git_worktree(worktree_dir, "rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
    remote = "origin"
    sync_mode = SYNC_MODE_FETCH_RESET
    if ttl_sec is None:
        ttl_sec, _ = compute_lock_ttl(0)
    retry_total_sec = max(0, _lock_retry_total_sec())
    retry_sleep_sec = max(1, _lock_retry_sleep_sec())
    grace_sec = max(0, _lock_grace_sec())
    hard_reset_requested = env_bool("BOT_STATE_HARD_RESET", False)
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
                    if locked_until + timedelta(seconds=grace_sec) > now:
                        locked = True
                    else:
                        stale_takeover = True
                if stale_takeover:
                    logger.warning(
                        "[BOTSTATE][LOCK_OVERRIDE] event=lock_stale_override prev_owner=%s prev_run_id=%s prev_until=%s grace_sec=%s now=%s",
                        current_owner,
                        current_run_id,
                        locked_until,
                        grace_sec,
                        now.isoformat(),
                    )
            except Exception as exc:
                logger.warning("[BOTSTATE][LOCK_OVERRIDE] event=lock_stale_override reason=parse_error err=%s", exc)
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
            _git_worktree(worktree_dir, "add", str(lock_rel_path))
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
                if hard_reset_requested and os.getenv("BOT_STATE_HARD_RESET_DONE") != "1":
                    deleted = hard_reset_bot_state(worktree_dir / "bot_state")
                    os.environ["BOT_STATE_HARD_RESET_DONE"] = "1"
                    logger.info(
                        "[BOTSTATE][HARD_RESET][DONE] deleted=%s stale_takeover=%s",
                        deleted,
                        stale_takeover,
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
    branch = _git_worktree(worktree_dir, "rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
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
        _git_worktree(worktree_dir, "add", "-u", str(lock_rel_path))
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


def persist_run_files(worktree_dir: Path, new_files: Iterable[Path], message: str, retries: int = 3) -> None:
    worktree_dir = worktree_dir.resolve()
    ensure_botstate_gitignore(str(worktree_dir))
    files = list(new_files)
    branch = _git_worktree(worktree_dir, "rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
    remote = "origin"
    sync_mode = SYNC_MODE_FETCH_RESET
    retry_sleep_sec = max(1, _lock_retry_sleep_sec())

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
            time.sleep(retry_sleep_sec)
            continue

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
                _git_worktree(worktree_dir, "add", str(rel_target))
            except Exception:
                continue

        _git_worktree(worktree_dir, "add", "-A", "bot_state")
        if _git_worktree(worktree_dir, "diff", "--cached", "--quiet", check=False).returncode == 0:
            logger.info("[BOTSTATE][PERSIST] no staged changes after add -A bot_state -> skip")
            post = git_porcelain(worktree_dir)
            if post.strip():
                raise RuntimeError(
                    "botstate worktree dirty after persist (A plan). Remaining changes:\n" + post
                )
            return
        status = git_porcelain(worktree_dir)
        status_lines = [line for line in status.splitlines() if line.strip()]
        stage_runtime_universe(worktree_dir)
        logger.info("[BOTSTATE][PERSIST][STATUS] lines=%s", status_lines[:50])
        stage_all(worktree_dir)
        status2 = git_porcelain(worktree_dir)
        status2_lines = [line for line in status2.splitlines() if line.strip()]
        staged_files = _cached_diff_names(worktree_dir)
        logger.info("[BOTSTATE][PERSIST][CACHED] files=%s", staged_files)
        if not status2_lines:
            logger.info("[PERSIST] no changes -> skip")
            post = git_porcelain(worktree_dir)
            if post.strip():
                raise RuntimeError(
                    "botstate worktree dirty after persist (A plan). Remaining changes:\n" + post
                )
            return

        committed = commit_if_staged(worktree_dir, message)
        logger.info(
            "[BOTSTATE][GIT] SYNC_MODE=%s step=commit committed=%s msg=%s attempt=%d",
            sync_mode,
            committed,
            message,
            attempt,
        )
        if not committed:
            logger.info("[PERSIST] no changes -> skip")
            post = git_porcelain(worktree_dir)
            if post.strip():
                raise RuntimeError(
                    "botstate worktree dirty after persist (A plan). Remaining changes:\n" + post
                )
            return

        try:
            push_retry(worktree_dir, message=message, retries=1, sync_before_commit=False)
            logger.info(
                "[BOTSTATE][PUSH] SYNC_MODE=%s push_ok=True attempt=%d",
                sync_mode,
                attempt,
            )
            logger.info("[BOTSTATE][PERSIST] files=%s message=%s", len(files), message)
            post = git_porcelain(worktree_dir)
            if post.strip():
                raise RuntimeError(
                    "botstate worktree dirty after persist (A plan). Remaining changes:\n" + post
                )
            return
        except RuntimeError as exc:
            logger.warning(
                "[BOTSTATE][PUSH][FAIL] SYNC_MODE=%s push_ok=False attempt=%d err=%s",
                sync_mode,
                attempt,
                str(exc),
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
            raise RuntimeError("git push failed after retries")
        time.sleep(retry_sleep_sec)

    logger.info("[BOTSTATE][PERSIST] files=%s message=%s", len(files), message)


def push_retry(
    worktree_dir: Path,
    message: str,
    retries: int = 3,
    *,
    sync_before_commit: bool = True,
) -> None:
    worktree_dir = worktree_dir.resolve()
    branch = _git_worktree(worktree_dir, "rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
    remote = "origin"
    sync_mode = SYNC_MODE_FETCH_RESET
    for attempt in range(1, retries + 1):
        if sync_before_commit:
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
        if sync_before_commit:
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
