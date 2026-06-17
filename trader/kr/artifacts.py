# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
import os
import shutil
from dataclasses import dataclass
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
    trade_date: date
    expected_as_of: date
    artifact_as_of: date | None
    final30_rows: int
    db_exact_rows: int | None
    reason: str | None
    details: dict


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


def _reject(reason: str, *, trade_date: date, expected_as_of: date, path: Path | None = None, **details: Any) -> KrArtifactValidationResult:
    if path:
        logger.error("[KR_ARTIFACT][REJECT] reason=%s path=%s", reason, _rel(path))
    else:
        logger.error("[KR_ARTIFACT][REJECT] reason=%s", reason)
    return KrArtifactValidationResult(False, trade_date, expected_as_of, None, 0, None, reason, details)


def validate_kr_prep_artifact(*, trade_date: date, expected_as_of: date, env: str, require_db_exact: bool = True, allow_legacy: bool = False) -> KrArtifactValidationResult:
    if not allow_legacy:
        for rel in LEGACY_PATHS:
            if (ROOT / rel).exists():
                logger.warning("[KR_ARTIFACT][LEGACY_BLOCKED] path=%s action=reject", rel)
                return _reject("LEGACY_SOURCE_NOT_ALLOWED", trade_date=trade_date, expected_as_of=expected_as_of, path=ROOT/rel)
    tds, exp = trade_date.isoformat(), expected_as_of.isoformat()
    latest_c = ROOT/"signals/kr/latest_prep_contract.json"; latest_f = ROOT/"signals/kr/latest_final30_scored.json"
    runtime_c = ROOT/"runtime/kr/watchlist"/tds/"prep_contract.json"; runtime_f = ROOT/"runtime/kr/watchlist"/tds/"final30_scored.json"
    for p, reason in ((latest_c,"CONTRACT_MISSING"),(runtime_c,"CONTRACT_MISSING"),(latest_f,"FINAL30_MISSING"),(runtime_f,"FINAL30_MISSING")):
        if not p.exists(): return _reject(reason, trade_date=trade_date, expected_as_of=expected_as_of, path=p)
    try:
        lc, rc, lf, rf = _load(latest_c), _load(runtime_c), _load(latest_f), _load(runtime_f)
    except Exception as exc:
        return _reject("JSON_INVALID", trade_date=trade_date, expected_as_of=expected_as_of, error=str(exc))
    for c in (lc, rc):
        if str(c.get("trade_date") or "")[:10] != tds: return _reject("TRADE_DATE_MISMATCH", trade_date=trade_date, expected_as_of=expected_as_of, expected=tds, actual=c.get("trade_date"))
        if str(c.get("expected_as_of") or c.get("as_of") or "")[:10] != exp: return _reject("ASOF_MISMATCH", trade_date=trade_date, expected_as_of=expected_as_of, expected=exp, actual=c.get("expected_as_of") or c.get("as_of"))
        if str(c.get("actual_as_of") or c.get("expected_as_of") or c.get("as_of") or "")[:10] != exp: return _reject("ASOF_MISMATCH", trade_date=trade_date, expected_as_of=expected_as_of)
        if str(c.get("env") or "").lower() != env.lower(): return _reject("ENV_MISMATCH", trade_date=trade_date, expected_as_of=expected_as_of)
        if str(c.get("market") or "KR").upper() not in {"KR","KRX"}: return _reject("MARKET_MISMATCH", trade_date=trade_date, expected_as_of=expected_as_of)
        if int(c.get("final30_rows") or c.get("final30_scored_rows") or 0) != 30: return _reject("ROWS_NOT_30", trade_date=trade_date, expected_as_of=expected_as_of, rows=c.get("final30_rows"))
        if require_db_exact and int(c.get("db_exact_rows") or 0) != 30: return _reject("DB_EXACT_ROWS_NOT_30", trade_date=trade_date, expected_as_of=expected_as_of, rows=c.get("db_exact_rows"))
        if c.get("contract_ok") is not True: return _reject("CONTRACT_NOT_OK", trade_date=trade_date, expected_as_of=expected_as_of)
        if any(str(v).startswith(("signals/final30.json","signals/watchlist.json","signals/latest.json","signals/kr/final30_scored.json","signals/kr/prep_contract.json")) for v in (c.get("source_paths") or {}).values()):
            return _reject("LEGACY_SOURCE_NOT_ALLOWED", trade_date=trade_date, expected_as_of=expected_as_of)
        ca = str(c.get("created_at_kst") or "")
        if ca:
            try:
                if datetime.fromisoformat(ca).astimezone(KST) < datetime.combine(trade_date, time.min, tzinfo=KST):
                    return _reject("STALE_CREATED_AT", trade_date=trade_date, expected_as_of=expected_as_of, created_at_kst=ca)
            except Exception: return _reject("STALE_CREATED_AT", trade_date=trade_date, expected_as_of=expected_as_of, created_at_kst=ca)
    lr, rr = _rows(lf), _rows(rf)
    if len(lr) != 30 or len(rr) != 30: return _reject("ROWS_NOT_30", trade_date=trade_date, expected_as_of=expected_as_of, rows={"latest":len(lr),"runtime":len(rr)})
    if (_payload_asof(lf) or exp)[:10] != exp or (_payload_asof(rf) or exp)[:10] != exp: return _reject("ASOF_MISMATCH", trade_date=trade_date, expected_as_of=expected_as_of)
    logger.info("[KR_ARTIFACT][VALIDATE_OK] trade_date=%s expected_as_of=%s rows=30 db_exact_rows=30 source=canonical", tds, exp)
    return KrArtifactValidationResult(True, trade_date, expected_as_of, expected_as_of, 30, 30, None, {"latest_final30":_rel(latest_f),"runtime_final30":_rel(runtime_f)})


def publish_kr_prep_artifacts_atomic(*, trade_date: date, expected_as_of: date, actual_as_of: date, env: str, final30_rows: list[dict], db_exact_rows: int, metadata: dict) -> None:
    logger.info("[KR_ARTIFACT][PUBLISH_START] trade_date=%s expected_as_of=%s", trade_date, expected_as_of)
    if len(final30_rows) != 30: raise RuntimeError("FINAL30_NOT_READY")
    if int(db_exact_rows) != 30: raise RuntimeError("DB_EXACT_ROWS_NOT_30")
    if expected_as_of != actual_as_of: raise RuntimeError("ASOF_MISMATCH")
    tds, exp = trade_date.isoformat(), expected_as_of.isoformat()
    runtime_f = ROOT/"runtime/kr/watchlist"/tds/"final30_scored.json"; runtime_c = runtime_f.with_name("prep_contract.json")
    latest_f = ROOT/"signals/kr/latest_final30_scored.json"; latest_c = ROOT/"signals/kr/latest_prep_contract.json"
    payload = {"schema_version":"kr_final30_scored_v1","market":"KR","env":env,"trade_date":tds,"expected_as_of":exp,"as_of":exp,"rows":final30_rows,"created_at_kst":datetime.now(KST).isoformat()}
    contract = {"schema_version":"kr_prep_contract_v1","market":"KR","env":env,"trade_date":tds,"expected_as_of":exp,"actual_as_of":actual_as_of.isoformat(),"final30_rows":30,"db_exact_rows":int(db_exact_rows),"artifact_rows":30,"created_at_kst":datetime.now(KST).isoformat(),"source":"prep_runner","source_paths":{"runtime_final30":_rel(runtime_f),"latest_final30":_rel(latest_f)},"contract_ok":True, **(metadata or {})}
    tmps = [( _write_tmp(p, payload if "final30_scored" in p.name else contract), p) for p in (runtime_f, runtime_c, latest_f, latest_c)]
    for tmp, final in tmps: os.replace(tmp, final)
    res = validate_kr_prep_artifact(trade_date=trade_date, expected_as_of=expected_as_of, env=env, require_db_exact=True, allow_legacy=True)
    if not res.ok: raise RuntimeError(res.reason or "ARTIFACT_VALIDATE_FAILED")
    logger.info("[KR_ARTIFACT][PUBLISH_OK] trade_date=%s expected_as_of=%s rows=30 latest=1 runtime=1", tds, exp)


def quarantine_stale_kr_artifacts(*, trade_date: date, expected_as_of: date, env: str) -> list[Path]:
    logger.info("[KR_PREP][ARTIFACT_CLEAN_START] trade_date=%s expected_as_of=%s", trade_date, expected_as_of)
    tds = trade_date.isoformat(); ts = datetime.now(KST).strftime("%Y%m%dT%H%M%S%z")
    targets = list(LEGACY_PATHS) + [Path("signals/kr/latest_final30_scored.json"), Path("signals/kr/latest_prep_contract.json"), Path("runtime/kr/watchlist")/tds/"final30_scored.json", Path("runtime/kr/watchlist")/tds/"prep_contract.json"]
    valid = validate_kr_prep_artifact(trade_date=trade_date, expected_as_of=expected_as_of, env=env, allow_legacy=True)
    keep = {Path(p) for p in (valid.details.values() if valid.ok else [])}
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
