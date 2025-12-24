from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict


def _flush_and_sync(fobj) -> None:
    fobj.flush()
    try:
        os.fsync(fobj.fileno())
    except Exception:
        # fsync may be unavailable on some platforms; ignore to avoid hard failure.
        pass


def atomic_write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=None)
        _flush_and_sync(f)
    os.replace(tmp, path)


def append_jsonl(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")
        _flush_and_sync(f)
