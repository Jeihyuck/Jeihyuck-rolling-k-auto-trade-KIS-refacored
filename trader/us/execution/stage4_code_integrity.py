"""Fail-closed Stage4 worker pin validation (KIS practice; no order I/O).

Only the launched worker itself can prove its source code equals the current
market/day immutable pin. An inherited environment variable by itself is not
sufficient: repository HEAD and pin file must agree with it.
"""
from __future__ import annotations

import re
import subprocess
from datetime import date
from pathlib import Path
from typing import Callable


_SHA = re.compile(r"^[0-9a-f]{40}$")
_ROOT = Path(__file__).resolve().parents[3]


def assert_stage4_worker_code_pin(
    *, trade_date: str, expected_sha: str,
    repo_root: Path | None = None,
    git_read: Callable[..., str] | None = None,
) -> str:
    """Return the verified SHA or raise a non-sensitive operational error."""
    try:
        date.fromisoformat(str(trade_date))
    except (TypeError, ValueError) as exc:
        raise RuntimeError("US_STAGE4_CODE_PIN_DATE_INVALID") from exc
    target = Path(repo_root) if repo_root is not None else _ROOT
    expected = str(expected_sha or "").lower().strip()
    if not _SHA.fullmatch(expected):
        raise RuntimeError("US_STAGE4_CODE_PIN_EXPECTED_MISSING")
    pin = target / "runtime" / "code-pins" / f"US-{trade_date}.sha"
    try:
        disk = pin.read_text(encoding="ascii").strip().lower()
    except OSError as exc:
        raise RuntimeError("US_STAGE4_CODE_PIN_FILE_UNAVAILABLE") from exc
    if not _SHA.fullmatch(disk) or disk != expected:
        raise RuntimeError("US_STAGE4_CODE_PIN_FILE_MISMATCH")

    def _default_git(*args: str) -> str:
        return subprocess.run(
            ["git", *args], cwd=target, check=True, text=True,
            capture_output=True, timeout=8,
        ).stdout.strip()

    run = git_read or _default_git
    try:
        branch = str(run("branch", "--show-current")).strip()
        head = str(run("rev-parse", "HEAD")).strip().lower()
    except (OSError, subprocess.SubprocessError, ValueError) as exc:
        raise RuntimeError("US_STAGE4_CODE_PIN_GIT_UNAVAILABLE") from exc
    if branch != "dual-agent":
        raise RuntimeError("US_STAGE4_CODE_PIN_BRANCH_MISMATCH")
    if head != expected:
        raise RuntimeError("US_STAGE4_CODE_PIN_HEAD_MISMATCH")
    return head
