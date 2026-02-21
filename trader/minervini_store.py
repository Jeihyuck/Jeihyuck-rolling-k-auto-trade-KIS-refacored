from __future__ import annotations

import json
from pathlib import Path

from trader.time_utils import now_kst


def write_minervini_signals(base_dir: Path, env: str, run_id: str, signals: dict, buyable_report: dict) -> Path:
    as_of = str(signals.get("as_of") or now_kst().date().isoformat())
    out_dir = base_dir / "trader_ledger" / "signals_minervini" / as_of
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"run_{run_id}.jsonl"

    payload = {
        "env": env,
        "run_id": run_id,
        "as_of": as_of,
        "signals": signals,
        "buyable_report": buyable_report,
        "final30": list(signals.get("final30") or []),
        "timestamp_kst": now_kst().isoformat(),
    }
    with path.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(payload, ensure_ascii=False) + "\n")
    return path
