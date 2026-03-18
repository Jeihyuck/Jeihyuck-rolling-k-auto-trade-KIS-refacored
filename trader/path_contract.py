from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path
from typing import Any


def _as_of_str(as_of: str | date) -> str:
    if isinstance(as_of, date):
        return as_of.isoformat()
    return str(as_of).strip()


def resolve_repo_root() -> Path:
    explicit = (os.getenv("TRADER_REPO_ROOT") or "").strip()
    if explicit:
        return Path(explicit).expanduser().resolve()
    return Path(__file__).resolve().parents[1]


def build_final30_paths(repo_root: Path | None, env: str, as_of: str | date) -> dict[str, Path]:
    root = (repo_root or resolve_repo_root()).expanduser().resolve()
    env_n = (env or "").strip().lower()
    as_of_s = _as_of_str(as_of)
    return {
        "runtime": root / "runtime" / "watchlist" / as_of_s / "final30_scored.json",
        "ledger": root / "bot_state" / "trader_ledger" / "final30" / env_n / as_of_s / "final30_scored.json",
        "signals": root / "signals" / "final30.json",
    }


def build_watchlist_paths(repo_root: Path | None, as_of: str | date) -> dict[str, Path]:
    root = (repo_root or resolve_repo_root()).expanduser().resolve()
    as_of_s = _as_of_str(as_of)
    return {
        "watchlist_dir": root / "runtime" / "watchlist" / as_of_s,
        "snapshot": root / "runtime" / "snapshots" / "final30.json",
    }


def serialize_path_map(path_map: dict[str, Any]) -> dict[str, str]:
    return {str(key): str(value) for key, value in (path_map or {}).items()}


def _coerce_final30_rows(rows: list[dict[str, Any]] | Any) -> list[dict[str, Any]]:
    if not isinstance(rows, list):
        return []
    normalized: list[dict[str, Any]] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        row = dict(item)
        code = str(row.get("code") or "").strip()
        if code:
            row["code"] = code.zfill(6)
        normalized.append(row)
    return normalized


def read_final30_file_rows(path: Path) -> tuple[list[dict[str, Any]], bool]:
    if not path.exists():
        return [], False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return [], False
    if isinstance(payload, dict):
        return _coerce_final30_rows(payload.get("items") or []), True
    if isinstance(payload, list):
        return _coerce_final30_rows(payload), True
    return [], True


def verify_final30_mirrors(
    *,
    repo_root: Path | None,
    env: str,
    as_of: str | date,
    expected_rows: int | None = None,
) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    for label, path in build_final30_paths(repo_root, env, as_of).items():
        exists = path.exists()
        rows, json_ok = read_final30_file_rows(path)
        bytes_written = 0
        if exists:
            try:
                bytes_written = int(path.stat().st_size)
            except OSError:
                bytes_written = 0
        results[label] = {
            "path": path,
            "exists": bool(exists),
            "bytes": bytes_written,
            "rows": len(rows),
            "json_ok": bool(json_ok),
            "ok": bool(
                exists
                and bytes_written > 0
                and json_ok
                and (expected_rows is None or len(rows) == int(expected_rows))
            ),
        }
    return results


def write_final30_mirrors(
    *,
    repo_root: Path | None,
    env: str,
    as_of: str | date,
    rows: list[dict[str, Any]] | Any,
    source: str = "final30_scored",
) -> dict[str, dict[str, Any]]:
    normalized_rows = _coerce_final30_rows(rows)
    as_of_s = _as_of_str(as_of)
    env_n = (env or "").strip().lower()
    file_payload = {
        "as_of": as_of_s,
        "env": env_n,
        "count": len(normalized_rows),
        "source": source,
        "items": normalized_rows,
    }
    path_map = build_final30_paths(repo_root, env_n, as_of_s)
    for label, path in path_map.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = file_payload if label == "signals" else normalized_rows
        tmp_path = path.parent / f".{path.name}.tmp"
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp_path, path)
    return verify_final30_mirrors(repo_root=repo_root, env=env_n, as_of=as_of_s, expected_rows=len(normalized_rows) or None)