import os
from datetime import datetime

from trader.db.json_safe import json_dumps_safe


def spool_event(kind: str, data: dict, base_dir: str = "trader/state/spool") -> None:
    os.makedirs(base_dir, exist_ok=True)
    filename = os.path.join(base_dir, f"{kind}_{datetime.utcnow().date().isoformat()}.jsonl")
    payload = {
        "ts": datetime.utcnow().isoformat(),
        "kind": kind,
        "data": data,
    }
    with open(filename, "a", encoding="utf-8") as handle:
        handle.write(json_dumps_safe(payload) + "\n")
