from __future__ import annotations

import fnmatch
import json
import logging
import os
import shutil
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional, Tuple, List
from zoneinfo import ZoneInfo

from trader.utils.env import env_bool

logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")

BOTSTATE_WORKTREE_DIR_ENV = "BOTSTATE_WORKTREE_DIR"
SYNC_MODE_FETCH_RESET = "FETCH_RESET"
BOTSTATE_SYNC_MODE_ENV = "BOTSTATE_SYNC_MODE"  # optional
DEFAULT_LOCK_TTL_SEC = 240
DEFAULT_LOCK_RETRY_SEC = 70
DEFAULT_LOCK_RETRY_SLEEP_SEC = 3
DEFAULT_LOCK_BUFFER_SEC = 180
DEFAULT_LOCK_GRACE_SEC = 60
BOTSTATE_GITIGNORE_TEXT = """\
# --- botstate (tracked artifacts) ---
# Keep minimal runtime artifacts only.

# Ignore runtime by default
bot_state/runtime/
bot_state/runtime/**

# Allowlist runtime artifacts
!bot_state/runtime/
!bot_state/runtime/state.json
!bot_state/runtime/lot_state.json
!bot_state/runtime/universe/
!bot_state/runtime/universe/**
!bot_state/runtime/diagnostics/
!bot_state/runtime/diagnostics/*.flag

# Large caches MUST NOT be committed
bot_state/runtime/ohlcv_cache/
bot_state/runtime/ohlcv_cache/**

# Keep only latest universe snapshots
bot_state/universe_lkg/**
!bot_state/universe_lkg/**/latest.json

# Never commit archive/ or readonly backups
bot_state/archive/

# Runtime DB artifacts (should never be created)
db/
db/**
*.sqlite3
*.sqlite3.*
*.db

# Python / OS noise
.DS_Store
__pycache__/
*.pyc
"""

ALLOWLIST_PATTERNS = [
    "bot_state/runtime/universe/*.json",
    "bot_state/universe_lkg/**/latest.json",
    "bot_state/runtime/state.json",
    "bot_state/runtime/lot_state.json",
    "bot_state/runtime/diagnostics/*.flag",
]

CLEAN_PATTERNS = [
    "bot_state/archive/**",
    "bot_state/runtime/ohlcv_cache/**",
]


@dataclass(frozen=True)
class DirtyStatSnapshot:
    events: tuple[int, float | None, int]
    universe: tuple[int, float | None, int]
    last_seen_positions: tuple[float, int] | None
    positions_snapshot: tuple[float, int] | None


class BotStatePersistError(RuntimeError):
    def __init__(self, message: str, *, require_persist: bool, dirty_by_stat: bool) -> None:
        super().__init__(message)
        self.require_persist = require_persist
        self.dirty_by_stat = dirty_by_stat


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


def _is_live_trading_env() -> bool:
    env = (os.getenv("KIS_ENV") or "").strip().lower()
    mode = (os.getenv("STRATEGY_MODE") or "").strip().upper()
    live_flag = (os.getenv("LIVE_TRADING_ENABLED") or "").strip()
    return env in {"live", "real", "prod", "production"} or mode == "LIVE" or live_flag == "1"


def _reset_flag_path(bot_state_dir: Path, as_of: str) -> Path:
    return bot_state_dir / "runtime" / "reset_flags" / f"{as_of}.json"


def _ensure_gitkeep(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    gitkeep = path / ".gitkeep"
    if not gitkeep.exists():
        gitkeep.write_text("", encoding="utf-8")


def hard_reset_bot_state(bot_state_dir: Path, *, reason: str = "manual") -> dict:
    deleted: list[str] = []
    skipped = False
    now = datetime.now(tz=KST)
    as_of = now.date().isoformat()
    reset_flag_path = _reset_flag_path(bot_state_dir, as_of)
    is_live = _is_live_trading_env()

    if is_live and reset_flag_path.exists():
        logger.warning("[RESET][HARD][SKIP] reason=already_reset_today flag=%s", reset_flag_path)
        return {"skipped": True, "reason": "already_reset_today", "flag": str(reset_flag_path)}

    runtime_dir = bot_state_dir / "runtime"
    if runtime_dir.exists():
        try:
            shutil.rmtree(runtime_dir, ignore_errors=True)
            deleted.append(str(runtime_dir))
            logger.info("[RESET][HARD] removed runtime/")
        except Exception:
            logger.warning("[RESET][HARD][SKIP] path=%s", runtime_dir, exc_info=True)

    for ledger_dir in [bot_state_dir / "ledger", bot_state_dir / "trader_ledger"]:
        if ledger_dir.exists():
            try:
                shutil.rmtree(ledger_dir, ignore_errors=True)
                deleted.append(str(ledger_dir))
                logger.info("[RESET][HARD] removed %s/", ledger_dir.name)
            except Exception:
                logger.warning("[RESET][HARD][SKIP] path=%s", ledger_dir, exc_info=True)

    for extra in [bot_state_dir / "universe_lkg", bot_state_dir / "locks"]:
        if extra.exists():
            try:
                shutil.rmtree(extra, ignore_errors=True)
                deleted.append(str(extra))
            except Exception:
                logger.warning("[RESET][HARD][SKIP] path=%s", extra, exc_info=True)

    _ensure_gitkeep(bot_state_dir / "runtime")
    _ensure_gitkeep(bot_state_dir / "trader_ledger")
    _ensure_gitkeep(bot_state_dir / "ledger")
    reset_flag_path.parent.mkdir(parents=True, exist_ok=True)
    reset_flag_path.write_text(
        json.dumps(
            {
                "ts": now.isoformat(),
                "reason": reason,
                "live": is_live,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    logger.info("[BOTSTATE][HARD_RESET] deleted=%s skipped=%s", deleted, skipped)
    return {"deleted": deleted, "skipped": skipped, "flag": str(reset_flag_path)}


def compute_lock_ttl(max_seconds: int) -> tuple[int, int]:
    try:
        buffer_sec = int(
            os.getenv(
                "BOTSTATE_LOCK_BUFFER_SEC",
                os.getenv("BOTSTATE_LOCK_TTL_BUFFER_SEC", str(DEFAULT_LOCK_BUFFER_SEC)),
            )
        )
    except Exception:
        buffer_sec = DEFAULT_LOCK_BUFFER_SEC
    try:
        base_sec = int(os.getenv("BOTSTATE_LOCK_TTL_SEC", str(DEFAULT_LOCK_TTL_SEC)))
    except Exception:
        base_sec = DEFAULT_LOCK_TTL_SEC
    if max_seconds > 0:
        ttl_min = max_seconds + buffer_sec + 300
        ttl_sec = max(base_sec, ttl_min)
    else:
        ttl_sec = base_sec + buffer_sec
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


def _run_git_logged(args: list[str], cwd: Path, *, check: bool = True, label: str) -> subprocess.CompletedProcess:
    proc = subprocess.run(["git", *args], cwd=cwd, text=True, capture_output=True)
    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    logger.info(
        "[BOTSTATE][PERSIST][GIT] label=%s cmd=%s rc=%s stdout=%s stderr=%s",
        label,
        " ".join(args),
        proc.returncode,
        stdout[-800:],
        stderr[-800:],
    )
    if check and proc.returncode != 0:
        raise RuntimeError(f"git command failed ({label}): {' '.join(args)}\n{stdout}\n{stderr}")
    return proc


def git_porcelain(worktree_dir: Path) -> str:
    return _git_worktree(worktree_dir, "status", "--porcelain").stdout


def stage_all(worktree_dir: Path) -> None:
    _git_worktree(worktree_dir, "add", "-A")


def stage_runtime_universe(worktree_dir: Path) -> None:
    for spec in ALLOWLIST_PATTERNS:
        _git_worktree(worktree_dir, "add", "-A", "--", spec, check=False)


def _safe_rm(path: Path) -> None:
    try:
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        elif path.exists():
            path.unlink()
    except Exception:
        logger.info("[BOTSTATE][PERSIST][CLEANUP_SKIP] path=%s", path, exc_info=True)


def _stage_allowlist(worktree_dir: Path, allow_patterns: List[str]) -> None:
    _git_worktree(worktree_dir, "reset", check=False)
    to_add: list[str] = []
    status = _run_git_logged(
        ["status", "--porcelain"], worktree_dir, check=True, label="status_porcelain_allowlist"
    ).stdout
    for line in status.splitlines():
        if not line.strip():
            continue
        _, rel = _parse_porcelain_path(line)
        if not rel:
            continue
        if any(fnmatch.fnmatch(rel, pattern) for pattern in allow_patterns):
            to_add.append(rel)
    if to_add:
        _git_worktree(worktree_dir, "add", "--", *sorted(set(to_add)), check=False)

    _safe_rm(worktree_dir / "bot_state" / "archive")


def _parse_porcelain_path(line: str) -> tuple[str, str]:
    status = line[:2]
    path = line[3:].strip()
    if "->" in path:
        path = path.split("->")[-1].strip()
    return status, path


def _clean_non_allowlisted_changes(
    worktree_dir: Path,
    status_lines: list[str],
    patterns: list[str],
) -> tuple[int, int]:
    reverted_deleted = 0
    cleaned_untracked = 0
    for line in status_lines:
        if not line.strip():
            continue
        status, path = _parse_porcelain_path(line)
        if not path:
            continue
        if not any(fnmatch.fnmatch(path, pattern) for pattern in patterns):
            continue
        if status == "??":
            _git_worktree(worktree_dir, "clean", "-fd", "--", path, check=False)
            cleaned_untracked += 1
        else:
            _git_worktree(worktree_dir, "restore", "--staged", "--worktree", "--", path, check=False)
            reverted_deleted += 1
    return reverted_deleted, cleaned_untracked


def _cached_diff_names(worktree_dir: Path) -> list[str]:
    output = _git_worktree(worktree_dir, "diff", "--cached", "--name-only", check=False).stdout.strip()
    return [line for line in output.splitlines() if line.strip()]


def _safe_stat(path: Path) -> tuple[float, int] | None:
    try:
        stat = path.stat()
        return stat.st_mtime, stat.st_size
    except FileNotFoundError:
        return None


def _latest_event_mtime(events_dir: Path) -> float | None:
    if not events_dir.exists():
        return None
    latest = None
    for entry in events_dir.glob("*.jsonl"):
        try:
            mtime = entry.stat().st_mtime
        except FileNotFoundError:
            continue
        latest = mtime if latest is None else max(latest, mtime)
    return latest


def _collect_dir_stats(base_dir: Path, pattern: str) -> tuple[int, float | None, int]:
    count = 0
    latest_mtime = None
    total_size = 0
    if not base_dir.exists():
        return 0, None, 0
    for entry in base_dir.glob(pattern):
        try:
            stat = entry.stat()
        except FileNotFoundError:
            continue
        count += 1
        total_size += stat.st_size
        latest_mtime = stat.st_mtime if latest_mtime is None else max(latest_mtime, stat.st_mtime)
    return count, latest_mtime, total_size


def _collect_dirty_stats(worktree_dir: Path) -> DirtyStatSnapshot:
    bot_state_dir = worktree_dir / "bot_state"
    events_stats = _collect_dir_stats(bot_state_dir / "runtime" / "events", "*.jsonl")
    universe_stats = _collect_dir_stats(bot_state_dir / "runtime" / "universe", "*.json")
    last_seen_positions = _safe_stat(bot_state_dir / "runtime" / "last_seen_positions.json")
    positions_snapshot = _safe_stat(bot_state_dir / "runtime" / "positions_snapshot.json")
    return DirtyStatSnapshot(
        events=events_stats,
        universe=universe_stats,
        last_seen_positions=last_seen_positions,
        positions_snapshot=positions_snapshot,
    )


def _force_stage_dirty_files(worktree_dir: Path) -> None:
    to_add: list[str] = []
    bot_state_dir = worktree_dir / "bot_state"
    for entry in (bot_state_dir / "runtime" / "events").glob("*.jsonl"):
        to_add.append(entry.relative_to(worktree_dir).as_posix())
    for entry in (bot_state_dir / "runtime" / "universe").glob("*.json"):
        to_add.append(entry.relative_to(worktree_dir).as_posix())
    for name in ("last_seen_positions.json", "positions_snapshot.json"):
        path = bot_state_dir / "runtime" / name
        if path.exists():
            to_add.append(path.relative_to(worktree_dir).as_posix())
    if to_add:
        _git_worktree(worktree_dir, "add", "-f", "--", *sorted(set(to_add)), check=False)


def evaluate_persist_guard(*, require_persist: bool, dirty_by_stat: bool) -> str:
    if not require_persist:
        return "ok"
    if dirty_by_stat:
        raise BotStatePersistError(
            "PERSIST_REQUIRED_BUT_EMPTY_STAGE",
            require_persist=require_persist,
            dirty_by_stat=dirty_by_stat,
        )
    return "warn"


def _normalize_botstate_rel(path: Path) -> str | None:
    parts = path.parts
    if "bot_state" in parts:
        idx = parts.index("bot_state")
        return Path(*parts[idx:]).as_posix()
    if parts and parts[0] == "bot_state":
        return Path(*parts).as_posix()
    return None


def _allowlist_changed(new_files: Iterable[Path], patterns: list[str]) -> bool:
    for path in new_files:
        rel = _normalize_botstate_rel(path)
        if not rel:
            continue
        if any(fnmatch.fnmatch(rel, pattern) for pattern in patterns):
            return True
    return False


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

        if locked and current_owner == owner and current_run_id == run_id:
            logger.info(
                "[BOTSTATE][LOCK_REUSE] owner=%s run_id=%s locked_until=%s",
                owner,
                run_id,
                locked_until.isoformat() if locked_until else "unknown",
            )
            return True

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
                    reset_result = hard_reset_bot_state(worktree_dir / "bot_state", reason="lock_acquired")
                    os.environ["BOT_STATE_HARD_RESET_DONE"] = "1"
                    logger.info(
                        "[BOTSTATE][HARD_RESET][DONE] result=%s stale_takeover=%s",
                        reset_result,
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
    allowlist_changed = _allowlist_changed(files, ALLOWLIST_PATTERNS)

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

        events_dir = worktree_dir / "bot_state" / "runtime" / "events"
        pre_stats = _collect_dirty_stats(worktree_dir)
        events_latest = _latest_event_mtime(events_dir)
        status_pre = _run_git_logged(
            ["status", "--porcelain"], worktree_dir, check=True, label="status_porcelain_pre"
        ).stdout
        logger.info(
            "[BOTSTATE][PERSIST][PRE] allowlist_changed=%s events_latest=%s dirty_stats=%s status_lines=%s",
            allowlist_changed,
            events_latest,
            pre_stats,
            [line for line in status_pre.splitlines() if line.strip()][:50],
        )

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

        _stage_allowlist(worktree_dir, ALLOWLIST_PATTERNS)
        post_stats = _collect_dirty_stats(worktree_dir)
        dirty_by_stat = pre_stats != post_stats
        if dirty_by_stat:
            _force_stage_dirty_files(worktree_dir)
        logger.info(
            "[BOTSTATE][PERSIST][STAT] dirty_by_stat=%s pre=%s post=%s",
            dirty_by_stat,
            pre_stats,
            post_stats,
        )
        status = _run_git_logged(["status", "--porcelain"], worktree_dir, check=True, label="status_porcelain").stdout
        status_lines = [line for line in status.splitlines() if line.strip()]
        reverted_deleted, cleaned_untracked = _clean_non_allowlisted_changes(
            worktree_dir,
            status_lines,
            CLEAN_PATTERNS,
        )
        if reverted_deleted or cleaned_untracked:
            logger.info(
                "[BOTSTATE][PERSIST][CLEAN] reverted_deleted=%s cleaned_untracked=%s patterns=%s",
                reverted_deleted,
                cleaned_untracked,
                CLEAN_PATTERNS,
            )
        status = _run_git_logged(["status", "--porcelain"], worktree_dir, check=True, label="status_porcelain_post").stdout
        cached_names = _run_git_logged(
            ["diff", "--cached", "--name-only"], worktree_dir, check=True, label="cached_names"
        ).stdout
        if _run_git_logged(
            ["diff", "--cached", "--quiet"], worktree_dir, check=False, label="cached_quiet"
        ).returncode == 0:
            has_recent_events = False
            if events_latest is not None:
                has_recent_events = (time.time() - events_latest) < 600
            require_persist = allowlist_changed or has_recent_events or dirty_by_stat
            logger.info(
                "[BOTSTATE][PERSIST] no staged changes after allowlist staging -> skip require_persist=%s",
                require_persist,
            )
            post = git_porcelain(worktree_dir)
            if post.strip():
                logger.warning(
                    "[BOTSTATE][PERSIST][SOFT-FAIL] remaining changes after allowlist:\n%s",
                    post,
                )
            if require_persist:
                logger.warning(
                    "[BOTSTATE][PERSIST][EMPTY_STAGE] require_persist=1 dirty_by_stat=%s",
                    dirty_by_stat,
                )
                outcome = evaluate_persist_guard(require_persist=require_persist, dirty_by_stat=dirty_by_stat)
                if outcome == "warn":
                    logger.warning("[BOTSTATE][PERSIST][EMPTY_STAGE] dirty_by_stat=0 -> skip")
                    return
            return
        status_lines = [line for line in status.splitlines() if line.strip()]
        logger.info("[BOTSTATE][PERSIST][STATUS] lines=%s", status_lines[:50])
        status2 = git_porcelain(worktree_dir)
        status2_lines = [line for line in status2.splitlines() if line.strip()]
        staged_files = [line for line in cached_names.splitlines() if line.strip()]
        logger.info("[BOTSTATE][PERSIST][CACHED] files=%s", staged_files)
        if not status2_lines:
            logger.info("[PERSIST] no changes -> skip")
            post = git_porcelain(worktree_dir)
            if post.strip():
                logger.warning(
                    "[BOTSTATE][PERSIST][SOFT-FAIL] remaining changes after allowlist:\n%s",
                    post,
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
                logger.warning(
                    "[BOTSTATE][PERSIST][SOFT-FAIL] remaining changes after allowlist:\n%s",
                    post,
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
                _safe_rm(worktree_dir / "bot_state" / "archive")
                post = git_porcelain(worktree_dir)
            if post.strip():
                logger.warning(
                    "[BOTSTATE][PERSIST][SOFT-FAIL] remaining changes after cleanup:\n%s",
                    post,
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


def persist_or_fail(worktree_dir: Path, new_files: Iterable[Path], message: str, retries: int = 3) -> None:
    persist_run_files(worktree_dir, new_files, message, retries=retries)


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
