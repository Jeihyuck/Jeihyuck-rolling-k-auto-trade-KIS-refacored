from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
TRADER_DIR = REPO_ROOT / "trader"
STATE_DIR = TRADER_DIR / "state"
FILLS_DIR = TRADER_DIR / "fills"
LOG_DIR = TRADER_DIR / "logs"
BOT_STATE_MIRROR_DIR = REPO_ROOT / "bot_state" / "trader_state" / "trader"


def ensure_dirs() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    FILLS_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
