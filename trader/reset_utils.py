from __future__ import annotations

import logging
import shutil
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


def detect_account_fp(kis_env: str, cano: str, acnt_prdt_cd: str, api_base_url: str) -> str:
    return f"{kis_env}|{api_base_url}|{cano}|{acnt_prdt_cd}"


def _unique_archive_path(archive_dir: Path, name: str) -> Path:
    candidate = archive_dir / name
    if not candidate.exists():
        return candidate
    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    return archive_dir / f"{name}_{stamp}"


def purge_bot_state(bot_state_dir: Path, archive_dir: Path, reason: str) -> None:
    archive_dir.mkdir(parents=True, exist_ok=True)
    targets: list[Path] = []
    for rel in ("runtime", "trader_ledger", "db"):
        targets.append(bot_state_dir / rel)
    for pattern in ("positions*", "*state*"):
        targets.extend(sorted(bot_state_dir.glob(pattern)))

    seen: set[Path] = set()
    for src in targets:
        if src in seen:
            continue
        seen.add(src)
        if not src.exists():
            continue
        dest = _unique_archive_path(archive_dir, src.name)
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.move(str(src), str(dest))
            logger.warning(
                "[STATE][PURGE] reason=%s src=%s archive=%s moved=1",
                reason,
                src,
                dest,
            )
        except Exception:
            logger.exception(
                "[STATE][PURGE] reason=%s src=%s archive=%s moved=0",
                reason,
                src,
                dest,
            )
