# FILE: trader/rebalance_engine.py
from __future__ import annotations
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from .scoring import build_2w_stats, rank_and_pick

log = logging.getLogger(__name__)

SNAP_DIR = Path(__file__).parent / "reports" / "universe"
SNAP_DIR.mkdir(parents=True, exist_ok=True)


def save_snapshot(snap: dict) -> Path:
    ts = datetime.now().strftime("%Y-%m-%d_%H%M")
    path = SNAP_DIR / f"top10_{ts}.json"
    with path.open("w", encoding="utf-8") as fp:
        json.dump(snap, fp, ensure_ascii=False, indent=2)
    log.info(f"[SNAPSHOT] saved: {path}")
    return path


def load_latest_snapshot(date: datetime | None = None) -> dict | None:
    date = date or datetime.now()
    prefix = date.strftime("top10_%Y-%m-%d_")
    cands = sorted(SNAP_DIR.glob(f"{prefix}*.json"))
    if not cands:
        # 당일 없으면 마지막 파일 fallback
        cands = sorted(SNAP_DIR.glob("top10_*.json"))
        if not cands:
            return None
    with cands[-1].open("r", encoding="utf-8") as fp:
        return json.load(fp)


def run_top10_snapshot(log_dir: str, window_days: int = 10, created_at: datetime | None = None,
                        prev_core: List[str] | None = None) -> dict:
    stats = build_2w_stats(Path(log_dir), lookback_days=window_days)
    core, bench = rank_and_pick(stats, k=10, bench=4, hysteresis_prev=prev_core)
    snap = {
        "created_at": (created_at or datetime.now()).strftime("%Y-%m-%d %H:%M:%S"),
        "window": f"{window_days}D",
        "universe": {"core": core, "bench": bench},
        "scoring": "WilsonWin+Expectancy+PF+Liq-Spread",
        "hysteresis": True,
    }
    save_snapshot(snap)
    return snap


if __name__ == "__main__":
    # CLI: python -m trader.rebalance_engine
    run_top10_snapshot(log_dir=str((Path(__file__).parent / "logs").resolve()))
