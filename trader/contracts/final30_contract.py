from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Iterable

import pandas as pd

logger = logging.getLogger(__name__)

KR_REGIME_REQUIRED_FINAL30_FIELDS = {"code", "market", "close", "ma20", "ma50", "return_1d", "return_5d"}
FINAL30_REQUIRED_COLUMNS = {"code", "name", "as_of", "rank", "rank_final30", "final_score"} | KR_REGIME_REQUIRED_FINAL30_FIELDS


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        if isinstance(value, str) and not value.strip():
            return default
        return int(float(value))
    except Exception:
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, str) and not value.strip():
            return default
        return float(value)
    except Exception:
        return default


def stable_final30_hash(rows: list[dict[str, Any]]) -> str:
    payload = json.dumps(rows, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def normalize_final30_rows(
    rows: Iterable[dict[str, Any]] | pd.DataFrame,
    *,
    as_of: str | None = None,
    env: str | None = None,
    source: str = "unknown",
    require_count: int = 30,
    reassign_rank: bool = True,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if isinstance(rows, pd.DataFrame):
        raw_rows = rows.to_dict(orient="records")
    else:
        raw_rows = [dict(r or {}) for r in rows]

    normalized: list[dict[str, Any]] = []
    for r in raw_rows:
        row = dict(r)
        meta = row.get("meta")
        if not isinstance(meta, dict):
            meta = {}
        code = str(row.get("code") or meta.get("code") or "").strip()
        if code and code.isdigit():
            code = code.zfill(6)
        row["code"] = code
        row["name"] = str(row.get("name") or meta.get("name") or code or "").strip()
        if as_of:
            row["as_of"] = str(as_of)
        elif row.get("as_of") is not None:
            row["as_of"] = str(row.get("as_of"))
        if env:
            row["env"] = str(env)
        elif row.get("env") is not None:
            row["env"] = str(row.get("env"))
        row["final_score"] = _safe_float(row.get("final_score") or meta.get("final_score") or row.get("score_final") or meta.get("score_final") or row.get("score") or meta.get("score") or 0.0)
        row["_rank_sort_key"] = _safe_int(row.get("rank_final30") or row.get("rank") or meta.get("rank_final30") or meta.get("rank") or 999999, default=999999)
        row["meta"] = meta
        normalized.append(row)

    normalized.sort(key=lambda r: (_safe_int(r.get("_rank_sort_key"), 999999), -_safe_float(r.get("final_score"), 0.0), str(r.get("code") or "")))

    if require_count and len(normalized) != require_count:
        return normalized, {"ok": False, "reason": "count_mismatch", "source": source, "rows": len(normalized), "expected": require_count}

    if reassign_rank:
        for idx, row in enumerate(normalized, start=1):
            row["rank"] = idx
            row["rank_final30"] = idx
            meta = dict(row.get("meta") or {})
            meta.update({"rank": idx, "rank_final30": idx, "code": row.get("code"), "name": row.get("name"), "final_score": row.get("final_score")})
            if row.get("as_of") is not None:
                meta["as_of"] = row.get("as_of")
            if row.get("env") is not None:
                meta["env"] = row.get("env")
            row["meta"] = meta
            row.pop("_rank_sort_key", None)

    ranks = [_safe_int(r.get("rank_final30"), 0) for r in normalized]
    rank_ok = ranks == list(range(1, require_count + 1)) if require_count else len(set(ranks)) == len(ranks)
    missing_required = []
    for i, r in enumerate(normalized, start=1):
        for col in FINAL30_REQUIRED_COLUMNS:
            if r.get(col) in (None, ""):
                missing_required.append({"idx": i, "code": r.get("code"), "column": col})
    ok = bool(rank_ok and not missing_required and (not require_count or len(normalized) == require_count))
    contract_hash = stable_final30_hash(normalized)
    return normalized, {"ok": ok, "source": source, "rows": len(normalized), "expected": require_count, "rank_ok": rank_ok, "rank_min": min(ranks) if ranks else None, "rank_max": max(ranks) if ranks else None, "rank_unique": len(set(ranks)), "missing_required": missing_required, "contract_hash": contract_hash}


def assert_final30_contract(rows: Iterable[dict[str, Any]] | pd.DataFrame, *, as_of: str | None = None, env: str | None = None, source: str, require_count: int = 30) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    normalized, info = normalize_final30_rows(rows, as_of=as_of, env=env, source=source, require_count=require_count, reassign_rank=True)
    if not info.get("ok"):
        logger.error("[FINAL30_CONTRACT][FAIL] source=%s info=%s", source, info)
        raise ValueError(f"FINAL30_CONTRACT_FAIL source={source} info={info}")
    logger.info("[FINAL30_CONTRACT][OK] source=%s rows=%s rank_min=%s rank_max=%s unique=%s hash=%s", source, info.get("rows"), info.get("rank_min"), info.get("rank_max"), info.get("rank_unique"), info.get("contract_hash"))
    return normalized, info
