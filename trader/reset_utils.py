from __future__ import annotations

import json
import logging
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

RESET_GUARD_FILE = Path("runtime") / "reset_guard.json"
RESET_GUARD_COOLDOWN_MIN = 60


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
    runtime_dir = bot_state_dir / "runtime"
    purge_universe = os.getenv("PURGE_UNIVERSE") == "1"
    if runtime_dir.exists():
        for child in sorted(runtime_dir.iterdir()):
            if child.name == "universe" and not purge_universe:
                continue
            targets.append(child)
    for rel in ("trader_ledger", "db"):
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


def _reset_guard_path(bot_state_dir: Path) -> Path:
    return bot_state_dir / RESET_GUARD_FILE


def load_reset_guard(bot_state_dir: Path) -> dict:
    path = _reset_guard_path(bot_state_dir)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("[STATE][RESET_GUARD][LOAD_FAIL] path=%s", path)
        return {}


def save_reset_guard(bot_state_dir: Path, payload: dict) -> Path:
    path = _reset_guard_path(bot_state_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def update_reset_guard_from_balance(
    bot_state_dir: Path,
    *,
    now_kst: datetime,
    kis_output1: list,
) -> Path:
    guard = load_reset_guard(bot_state_dir)
    guard.update(
        {
            "last_balance_ts": now_kst.isoformat(),
            "last_balance_had_positions": bool(kis_output1),
            "last_seen_positions_count": len(kis_output1),
        }
    )
    return save_reset_guard(bot_state_dir, guard)


def record_purge_event(
    bot_state_dir: Path,
    *,
    now_kst: datetime,
    reason: str,
) -> Path:
    guard = load_reset_guard(bot_state_dir)
    guard.update(
        {
            "last_purge_ts": now_kst.isoformat(),
            "last_purge_reason": reason,
        }
    )
    return save_reset_guard(bot_state_dir, guard)


def should_purge_on_empty_kis_holdings(
    *,
    kis_output1: list,
    ledger_positions_count: int,
    last_seen_positions_count: int | None,
    last_balance_had_positions: bool | None,
    now_kst: datetime,
    reason_ctx: dict,
) -> tuple[bool, str]:
    kis_holdings_count = len(kis_output1 or [])
    if kis_holdings_count > 0:
        return False, "kis_has_positions"

    last_purge_ts_raw = reason_ctx.get("last_purge_ts")
    last_purge_ts = None
    if last_purge_ts_raw:
        try:
            last_purge_ts = datetime.fromisoformat(str(last_purge_ts_raw))
        except ValueError:
            last_purge_ts = None
    if last_purge_ts:
        if last_purge_ts.tzinfo is None:
            last_purge_ts = last_purge_ts.replace(tzinfo=now_kst.tzinfo)
        if now_kst - last_purge_ts < timedelta(minutes=RESET_GUARD_COOLDOWN_MIN):
            return False, "cooldown"

    last_seen = last_seen_positions_count or 0
    has_ledger_positions = ledger_positions_count > 0
    had_positions_before = bool(last_balance_had_positions)
    if has_ledger_positions or last_seen > 0 or had_positions_before:
        return True, "positions_missing_mismatch"

    return False, "normal_start_no_holdings"
