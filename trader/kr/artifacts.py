# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
import os
import shutil
import hashlib
from dataclasses import dataclass, field
from datetime import date, datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")
ROOT = Path(__file__).resolve().parents[2]
logger = logging.getLogger(__name__)

LEGACY_PATHS = (
    Path("signals/final30.json"),
    Path("signals/watchlist.json"),
    Path("signals/latest.json"),
    Path("signals/kr/final30_scored.json"),
    Path("signals/kr/prep_contract.json"),
)

@dataclass
class KrArtifactValidationResult:
    ok: bool
    fatal: bool
    reason: str
    source: str | None = None
    rows: int = 0
    db_exact_rows: int = 0
    legacy_blocked: bool = False
    legacy_paths: list[str] = field(default_factory=list)
    canonical_path: str | None = None
    detail: str | None = None
    trade_date: date | None = None
    expected_as_of: date | None = None
    artifact_as_of: date | None = None
    details: dict = field(default_factory=dict)
    final30_rows_payload: list[dict[str, Any]] = field(default_factory=list)

    @property
    def final30_rows(self) -> int:
        return self.rows


@dataclass
class _CandidateValidation:
    ok: bool
    reason: str
    path: Path | None = None
    rows: int = 0
    db_exact_rows: int = 0
    details: dict = field(default_factory=dict)



def _rel(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except Exception:
        return str(path)


def _load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_tmp(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".tmp.{os.getpid()}")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    _load(tmp)
    return tmp


def _rows(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        val = payload.get("rows") or payload.get("items") or payload.get("data") or payload.get("final30") or payload.get("final30_scored") or []
        return val if isinstance(val, list) else []
    return []


def _payload_asof(payload: Any) -> str | None:
    if isinstance(payload, dict):
        for k in ("expected_as_of", "actual_as_of", "as_of", "base_date", "scoring_date"):
            if payload.get(k):
                return str(payload[k])[:10]
        rows = _rows(payload)
        if rows and isinstance(rows[0], dict):
            return _payload_asof(rows[0])
    return None


def _payload_hash(payload: Any) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _code_list(rows: list[Any]) -> list[str]:
    return [str((row or {}).get("code") or (row or {}).get("symbol") or "").zfill(6) for row in rows if isinstance(row, dict)]


def _rank_list(rows: list[Any]) -> list[Any] | None:
    ranks = []
    for row in rows:
        if not isinstance(row, dict):
            return None
        val = row.get("rank_final30", row.get("rank"))
        if val is None:
            return None
        ranks.append(val)
    return ranks

def _reject(reason: str, *, trade_date: date, expected_as_of: date, path: Path | None = None, detail: str | None = None, **details: Any) -> KrArtifactValidationResult:
    if path:
        logger.error("[KR_ARTIFACT][REJECT] reason=%s path=%s", reason, _rel(path))
    else:
        logger.error("[KR_ARTIFACT][REJECT] reason=%s", reason)
    return KrArtifactValidationResult(False, True, reason, rows=0, db_exact_rows=0, detail=detail, trade_date=trade_date, expected_as_of=expected_as_of, details=details)


def _find_legacy_artifacts(expected_as_of: date | None = None) -> list[Path]:
    rels = [
        Path("signals/final30.json"),
        Path("signals/watchlist.json"),
        Path("signals/latest.json"),
        Path("signals/kr/final30_scored.json"),
        Path("signals/kr/prep_contract.json"),
    ]
    if expected_as_of is not None:
        rels.extend([
            Path("runtime/watchlist") / expected_as_of.isoformat() / "final30.json",
            Path("runtime/watchlist") / expected_as_of.isoformat() / "final30_scored.json",
        ])
    found = []
    for rel in rels:
        p = ROOT / rel
        if p.exists():
            found.append(p)
    return found


def _quarantine_or_warn_legacy(paths: list[Path], *, trade_date: date, reason: str) -> None:
    if not paths:
        return
    if os.getenv("KR_QUARANTINE_STALE_ARTIFACT", "1") != "1":
        for p in paths:
            logger.warning("[KR_ARTIFACT][LEGACY_IGNORED] path=%s reason=%s", _rel(p), reason)
        return
    ts = datetime.now(KST).strftime("%Y%m%dT%H%M%S%z")
    qdir = ROOT / "runtime/quarantine/kr" / trade_date.isoformat() / ts
    for p in paths:
        if not p.exists():
            continue
        rel = Path(_rel(p))
        dest = qdir / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.move(str(p), str(dest))
            logger.warning("[KR_ARTIFACT][QUARANTINE] path=%s to=%s reason=%s", rel, _rel(dest), reason)
        except Exception as exc:
            logger.warning("[KR_ARTIFACT][LEGACY_IGNORED] path=%s reason=%s quarantine_error=%s", rel, reason, exc)


def _contract_rows(c: dict[str, Any]) -> int:
    return int(c.get("rows") or c.get("final30_rows") or c.get("final30_scored_rows") or c.get("artifact_rows") or 0)


def _validate_canonical_prep_artifact(*, trade_date: date, expected_as_of: date, env: str, require_db_exact: bool = True) -> _CandidateValidation:
    tds, exp = trade_date.isoformat(), expected_as_of.isoformat()
    latest_c = ROOT / "signals/kr/latest_prep_contract.json"
    latest_f = ROOT / "signals/kr/latest_final30_scored.json"
    runtime_c = ROOT / "runtime/kr/watchlist" / tds / "prep_contract.json"
    runtime_f = ROOT / "runtime/kr/watchlist" / tds / "final30_scored.json"
    contracts = [p for p in (runtime_c, latest_c) if p.exists()]
    finals = [p for p in (runtime_f, latest_f) if p.exists()]
    if not contracts:
        return _CandidateValidation(False, "CONTRACT_MISSING")
    if not finals:
        return _CandidateValidation(False, "FINAL30_MISSING")
    loaded_contracts: list[tuple[Path, dict[str, Any]]] = []
    loaded_finals: list[tuple[Path, Any]] = []
    try:
        for p in contracts:
            payload = _load(p)
            if not isinstance(payload, dict):
                return _CandidateValidation(False, "CONTRACT_INVALID", p)
            loaded_contracts.append((p, payload))
        for p in finals:
            loaded_finals.append((p, _load(p)))
    except Exception as exc:
        return _CandidateValidation(False, "JSON_INVALID", details={"error": str(exc)})
    db_exact = 0
    for p, c in loaded_contracts:
        if str(c.get("trade_date") or "")[:10] != tds:
            return _CandidateValidation(False, "TRADE_DATE_MISMATCH", p)
        if str(c.get("expected_as_of") or c.get("as_of") or "")[:10] != exp:
            return _CandidateValidation(False, "ASOF_MISMATCH", p)
        if str(c.get("as_of") or c.get("actual_as_of") or c.get("expected_as_of") or "")[:10] != exp:
            return _CandidateValidation(False, "ASOF_MISMATCH", p)
        if str(c.get("env") or "").lower() != env.lower():
            return _CandidateValidation(False, "ENV_MISMATCH", p)
        if str(c.get("market") or "KR").upper() not in {"KR", "KRX"}:
            return _CandidateValidation(False, "MARKET_MISMATCH", p)
        if _contract_rows(c) != 30:
            return _CandidateValidation(False, "ROWS_NOT_30", p, rows=_contract_rows(c))
        db_exact = max(db_exact, int(c.get("db_exact_rows") or 0))
        if require_db_exact and int(c.get("db_exact_rows") or 0) != 30:
            return _CandidateValidation(False, "DB_EXACT_ROWS_NOT_30", p, rows=30, db_exact_rows=int(c.get("db_exact_rows") or 0))
        if c.get("trade_can_proceed", 1) in (0, False, "0", "false", "False"):
            return _CandidateValidation(False, "TRADE_CAN_PROCEED_FALSE", p)
        if c.get("contract_ok", True) is not True:
            return _CandidateValidation(False, "CONTRACT_NOT_OK", p)
    first_rows: list[Any] | None = None
    first_path: Path | None = None
    for p, payload in loaded_finals:
        rows = _rows(payload)
        if len(rows) != 30:
            return _CandidateValidation(False, "ROWS_NOT_30", p, rows=len(rows), db_exact_rows=db_exact)
        if (_payload_asof(payload) or exp)[:10] != exp:
            return _CandidateValidation(False, "ASOF_MISMATCH", p, rows=len(rows), db_exact_rows=db_exact)
        if not all(isinstance(r, dict) and (r.get("code") or r.get("symbol")) for r in rows):
            return _CandidateValidation(False, "CRITICAL_COLUMNS_MISSING", p, rows=len(rows), db_exact_rows=db_exact)
        if first_rows is None:
            first_rows, first_path = rows, p
            continue
        if _code_list(first_rows) != _code_list(rows):
            return _CandidateValidation(False, "CODE_LIST_MISMATCH", p, rows=30, db_exact_rows=db_exact)
        ranks1, ranks2 = _rank_list(first_rows), _rank_list(rows)
        if ranks1 is not None and ranks2 is not None and ranks1 != ranks2:
            return _CandidateValidation(False, "PAYLOAD_MISMATCH", p, rows=30, db_exact_rows=db_exact, details={"detail":"rank_list_mismatch"})
        if _payload_hash({"rows": first_rows}) != _payload_hash({"rows": rows}):
            return _CandidateValidation(False, "PAYLOAD_MISMATCH", p, rows=30, db_exact_rows=db_exact)
    keep = {"payload_match": True}
    for p in (runtime_f, latest_f, runtime_c, latest_c):
        if p.exists():
            keep[p.name + "_" + str(len(keep))] = _rel(p)
    return _CandidateValidation(True, "CANONICAL_OK", first_path or finals[0], rows=30, db_exact_rows=db_exact or 30, details=keep)



def load_kr_canonical_final30_rows(path: Path | str | None) -> list[dict[str, Any]]:
    """Load validated canonical final30 rows for direct PB1 injection."""
    if not path:
        return []
    try:
        payload = _load(Path(path))
        return [dict(r) for r in _rows(payload) if isinstance(r, dict)]
    except Exception as exc:
        logger.warning("[KR_ARTIFACT][FINAL30_LOAD_FAIL] path=%s err=%s", path, exc)
        return []

def _validate_legacy_artifact(paths: list[Path], *, expected_as_of: date, env: str) -> _CandidateValidation:
    for p in paths:
        try:
            payload = _load(p)
        except Exception:
            continue
        rows = _rows(payload)
        if len(rows) == 30 and ((_payload_asof(payload) or expected_as_of.isoformat())[:10] == expected_as_of.isoformat()):
            return _CandidateValidation(True, "LEGACY_OK", p, rows=30)
    return _CandidateValidation(False, "LEGACY_INVALID")


def validate_kr_prep_artifact(*, trade_date: date, expected_as_of: date, env: str, require_db_exact: bool = True, strict: bool = True, allow_legacy_fallback: bool = False, allow_legacy: bool | None = None) -> KrArtifactValidationResult:
    if allow_legacy is not None:
        allow_legacy_fallback = allow_legacy
    canonical = _validate_canonical_prep_artifact(trade_date=trade_date, expected_as_of=expected_as_of, env=env, require_db_exact=require_db_exact)
    if canonical.ok:
        legacy_paths = _find_legacy_artifacts(expected_as_of)
        if legacy_paths:
            _quarantine_or_warn_legacy(legacy_paths, trade_date=trade_date, reason="LEGACY_IGNORED_CANONICAL_OK")
            logger.warning("[KR_ARTIFACT][LEGACY_IGNORED] canonical_ok=1 paths=%s", [_rel(p) for p in legacy_paths])
        logger.info("[KR_ARTIFACT][VALIDATE_OK] trade_date=%s expected_as_of=%s rows=%s db_exact_rows=%s source=canonical", trade_date, expected_as_of, canonical.rows, canonical.db_exact_rows)
        return KrArtifactValidationResult(True, False, "CANONICAL_OK", source="canonical", rows=canonical.rows, db_exact_rows=canonical.db_exact_rows, legacy_blocked=bool(legacy_paths), legacy_paths=[_rel(p) for p in legacy_paths], canonical_path=_rel(canonical.path) if canonical.path else None, trade_date=trade_date, expected_as_of=expected_as_of, artifact_as_of=expected_as_of, details=canonical.details, final30_rows_payload=load_kr_canonical_final30_rows(canonical.path))

    legacy_paths = _find_legacy_artifacts(expected_as_of)
    if legacy_paths and strict and not allow_legacy_fallback:
        _quarantine_or_warn_legacy(legacy_paths, trade_date=trade_date, reason="LEGACY_SOURCE_NOT_ALLOWED_CANONICAL_MISSING")
        logger.error("[KR_ARTIFACT][REJECT] reason=CANONICAL_MISSING_LEGACY_NOT_ALLOWED trade_date=%s expected_as_of=%s legacy_paths=%s", trade_date, expected_as_of, [_rel(p) for p in legacy_paths])
        return KrArtifactValidationResult(False, True, "CANONICAL_MISSING", rows=0, db_exact_rows=0, legacy_blocked=True, legacy_paths=[_rel(p) for p in legacy_paths], detail="canonical_missing_legacy_not_allowed", trade_date=trade_date, expected_as_of=expected_as_of)
    if legacy_paths and allow_legacy_fallback:
        legacy = _validate_legacy_artifact(legacy_paths, expected_as_of=expected_as_of, env=env)
        if legacy.ok:
            logger.warning("[KR_ARTIFACT][VALIDATE_OK_LEGACY_FALLBACK] rows=%s path=%s", legacy.rows, _rel(legacy.path) if legacy.path else None)
            return KrArtifactValidationResult(True, False, "LEGACY_FALLBACK_OK", source="legacy", rows=legacy.rows, legacy_paths=[_rel(p) for p in legacy_paths], trade_date=trade_date, expected_as_of=expected_as_of, artifact_as_of=expected_as_of)
    logger.error("[KR_ARTIFACT][REJECT] reason=CANONICAL_PREP_ARTIFACT_INVALID trade_date=%s expected_as_of=%s canonical_reason=%s", trade_date, expected_as_of, canonical.reason)
    return KrArtifactValidationResult(False, True, canonical.reason if canonical.reason not in {"CONTRACT_MISSING", "FINAL30_MISSING"} else "CANONICAL_PREP_ARTIFACT_INVALID", rows=canonical.rows, db_exact_rows=canonical.db_exact_rows, detail=canonical.reason, trade_date=trade_date, expected_as_of=expected_as_of)


def publish_kr_prep_artifacts_atomic(*, trade_date: date, expected_as_of: date, actual_as_of: date, env: str, final30_rows: list[dict], db_exact_rows: int, metadata: dict) -> None:
    logger.info("[KR_ARTIFACT][PUBLISH_START] trade_date=%s expected_as_of=%s", trade_date, expected_as_of)
    if len(final30_rows) != 30: raise RuntimeError("FINAL30_NOT_READY")
    if int(db_exact_rows) != 30: raise RuntimeError("DB_EXACT_ROWS_NOT_30")
    if expected_as_of != actual_as_of: raise RuntimeError("ASOF_MISMATCH")
    tds, exp = trade_date.isoformat(), expected_as_of.isoformat()
    runtime_f = ROOT/"runtime/kr/watchlist"/tds/"final30_scored.json"; runtime_c = runtime_f.with_name("prep_contract.json")
    latest_f = ROOT/"signals/kr/latest_final30_scored.json"; latest_c = ROOT/"signals/kr/latest_prep_contract.json"
    created_at = datetime.now(KST).isoformat()
    payload = {"schema_version":"kr_final30_scored_v1","market":"KR","env":env,"trade_date":tds,"expected_as_of":exp,"as_of":exp,"rows":final30_rows,"created_at_kst":created_at}
    _RESERVED_CONTRACT_KEYS = {
        "schema_version",
        "market",
        "env",
        "trade_date",
        "expected_as_of",
        "actual_as_of",
        "final30_rows",
        "final30_scored_rows",
        "db_exact_rows",
        "artifact_rows",
        "created_at_kst",
        "source",
        "canonical",
        "rows",
        "trade_can_proceed",
        "source_paths",
        "contract_ok",
    }
    safe_metadata = {k: v for k, v in (metadata or {}).items() if k not in _RESERVED_CONTRACT_KEYS}
    dropped_metadata_keys = sorted(set((metadata or {}).keys()) - set(safe_metadata.keys()))
    if dropped_metadata_keys:
        logger.warning("[KR_ARTIFACT][METADATA_RESERVED_KEYS_DROPPED] keys=%s", dropped_metadata_keys)
    contract = {"schema_version":"kr_prep_contract_v1","market":"KR","env":env,"trade_date":tds,"expected_as_of":exp,"actual_as_of":actual_as_of.isoformat(),"final30_rows":30,"db_exact_rows":int(db_exact_rows),"artifact_rows":30,"created_at_kst":datetime.now(KST).isoformat(),"source":"fresh_build","canonical":True,"rows":30,"trade_can_proceed":1,"source_paths":{"runtime_final30":_rel(runtime_f),"latest_final30":_rel(latest_f)},"contract_ok":True, **safe_metadata}
    tmps = [( _write_tmp(p, payload if "final30_scored" in p.name else contract), p) for p in (runtime_f, runtime_c, latest_f, latest_c)]
    for tmp, final in tmps: os.replace(tmp, final)
    res = validate_kr_prep_artifact(trade_date=trade_date, expected_as_of=expected_as_of, env=env, require_db_exact=True, strict=True, allow_legacy_fallback=False)
    if not res.ok: raise RuntimeError(res.reason or "ARTIFACT_VALIDATE_FAILED")
    logger.info("[KR_ARTIFACT][PUBLISH_OK] trade_date=%s expected_as_of=%s rows=30 latest=1 runtime=1", tds, exp)


def quarantine_stale_kr_artifacts(*, trade_date: date, expected_as_of: date, env: str) -> list[Path]:
    logger.info("[KR_PREP][ARTIFACT_CLEAN_START] trade_date=%s expected_as_of=%s", trade_date, expected_as_of)
    tds = trade_date.isoformat(); ts = datetime.now(KST).strftime("%Y%m%dT%H%M%S%z")
    targets = list(LEGACY_PATHS) + [Path("signals/kr/latest_final30_scored.json"), Path("signals/kr/latest_prep_contract.json"), Path("runtime/kr/watchlist")/tds/"final30_scored.json", Path("runtime/kr/watchlist")/tds/"prep_contract.json"]
    valid = validate_kr_prep_artifact(trade_date=trade_date, expected_as_of=expected_as_of, env=env, allow_legacy=True)
    keep = {str(p) for p in (valid.details.values() if valid.ok else []) if isinstance(p, str)}
    moved=[]; kept=0
    qdir = ROOT/"runtime/quarantine/kr"/tds/ts
    for rel in targets:
        p = ROOT/rel
        if not p.exists(): continue
        if valid.ok and _rel(p) in keep and rel not in LEGACY_PATHS:
            kept += 1; continue
        dest = qdir/rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(p), str(dest)); moved.append(dest)
        logger.warning("[KR_ARTIFACT][QUARANTINE] path=%s to=%s reason=STALE_OR_UNVERIFIED", rel, _rel(dest))
    logger.info("[KR_PREP][ARTIFACT_CLEAN_DONE] moved=%s kept=%s", len(moved), kept)
    return moved
