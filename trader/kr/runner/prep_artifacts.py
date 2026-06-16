# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")
ROOT = Path(__file__).resolve().parents[3]

@dataclass
class ArtifactResult:
    ok: bool
    source: Path | None
    final30_path: Path | None
    contract_path: Path | None
    rows: int = 0
    contract: dict[str, Any] | None = None
    repaired: bool = False
    reason: str = ""


def rows_from_payload(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        rows = payload.get("rows") or payload.get("data") or payload.get("items") or payload.get("final30") or []
        return rows if isinstance(rows, list) else []
    return []


def _load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def previous_business_day(trade_date: str) -> str:
    d = date.fromisoformat(trade_date) - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.isoformat()


def _payload_as_of(payload: Any) -> str | None:
    if isinstance(payload, dict):
        for key in ("as_of", "base_date", "scoring_date"):
            if payload.get(key):
                return str(payload[key])[:10]
    return None


def _validate_contract(*, contract: dict[str, Any], rows: int, env: str, trade_date: str) -> str | None:
    if str(contract.get("trade_date") or "")[:10] != trade_date:
        return "STALE_PREP_ARTIFACT"
    if str(contract.get("env") or env).lower() != env.lower():
        return "STALE_PREP_ARTIFACT"
    if int(contract.get("final30_rows") or contract.get("final30_scored_rows") or rows or 0) != 30:
        return "REQUIRED_PREP_NOT_READY"
    if not bool(contract.get("trade_can_proceed")) or not bool(contract.get("contract_ok")):
        return "REQUIRED_PREP_NOT_READY"
    return None


def mirror_kr_prep_artifacts(*, final30_path: Path, env: str, as_of: str, trade_date: str, source_paths: dict | None = None) -> dict[str, Any]:
    payload = _load(final30_path)
    rows = rows_from_payload(payload)
    canonical = {
        "runtime_final30": str(ROOT / "runtime/kr/watchlist" / trade_date / "final30_scored.json"),
        "runtime_contract": str(ROOT / "runtime/kr/watchlist" / trade_date / "prep_contract.json"),
        "signals_final30": str(ROOT / "signals/kr/final30_scored.json"),
        "signals_latest_final30": str(ROOT / "signals/kr/latest_final30_scored.json"),
        "signals_contract": str(ROOT / "signals/kr/prep_contract.json"),
        "signals_latest_contract": str(ROOT / "signals/kr/latest_prep_contract.json"),
        "ledger_final30": str(ROOT / "bot_state/trader_ledger/final30" / env / trade_date / "final30_scored.json"),
    }
    contract = {
        "market": "KR", "env": env, "as_of": as_of, "trade_date": trade_date,
        "final30_rows": len(rows), "final30_scored_rows": len(rows),
        "contract_ok": len(rows) == 30, "trade_can_proceed": len(rows) == 30,
        "quality_status": "OK" if len(rows) == 30 else "OK_WITH_WARNINGS",
        "created_at_kst": datetime.now(KST).isoformat(),
        "source_paths": source_paths or {"final30": str(final30_path)},
        "canonical_paths": canonical,
    }
    for key in ("runtime_final30", "signals_final30", "signals_latest_final30", "ledger_final30"):
        _write(Path(canonical[key]), payload)
    for key in ("runtime_contract", "signals_contract", "signals_latest_contract"):
        _write(Path(canonical[key]), contract)
    return contract


def _result_from_canonical(name: str, cpath: Path, fpath: Path, *, env: str, trade_date: str) -> ArtifactResult | None:
    if not fpath.exists() or not cpath.exists():
        return None
    try:
        payload = _load(fpath)
        rows = rows_from_payload(payload)
        contract = _load(cpath)
    except Exception as exc:
        return ArtifactResult(False, fpath, fpath, cpath, reason=str(exc))
    reason = _validate_contract(contract=contract, rows=len(rows), env=env, trade_date=trade_date)
    if reason:
        return ArtifactResult(False, fpath, fpath, cpath, len(rows), contract, False, reason)
    return ArtifactResult(True, fpath, fpath, cpath, len(rows), contract, False, "OK")


def find_and_repair_kr_prep_artifact(*, env: str="practice", trade_date: str | None=None, as_of: str | None=None, allow_stale_canonical_fallback: bool=False) -> ArtifactResult:
    trade_date = trade_date or datetime.now(KST).strftime("%Y-%m-%d")
    requested_as_of = as_of

    # Canonical latest/current files are authoritative but must match requested env/date.
    for name, cpath, fpath in [
        ("signals_kr_latest", ROOT/"signals/kr/latest_prep_contract.json", ROOT/"signals/kr/latest_final30_scored.json"),
        ("signals_kr", ROOT/"signals/kr/prep_contract.json", ROOT/"signals/kr/final30_scored.json"),
        ("runtime_kr", ROOT/"runtime/kr/watchlist"/trade_date/"prep_contract.json", ROOT/"runtime/kr/watchlist"/trade_date/"final30_scored.json"),
    ]:
        res = _result_from_canonical(name, cpath, fpath, env=env, trade_date=trade_date)
        if res is not None:
            if allow_stale_canonical_fallback and not res.ok and res.reason == "STALE_PREP_ARTIFACT":
                continue
            return res

    # Legacy fallback should use explicit as_of, payload/contract as_of, or previous KRX business day.
    legacy_as_of = requested_as_of or previous_business_day(trade_date)
    legacy_candidates = [
        ROOT/"runtime/watchlist"/legacy_as_of/"final30_scored.json",
        ROOT/"signals/final30.json",
        ROOT/"bot_state/trader_ledger/final30"/env/legacy_as_of/"final30_scored.json",
    ]
    for fpath in legacy_candidates:
        if not fpath.exists():
            continue
        try:
            payload = _load(fpath)
            rows = rows_from_payload(payload)
        except Exception as exc:
            return ArtifactResult(False, fpath, None, None, reason=str(exc))
        if not rows:
            continue
        effective_as_of = requested_as_of or _payload_as_of(payload) or legacy_as_of
        contract = mirror_kr_prep_artifacts(final30_path=fpath, env=env, as_of=effective_as_of, trade_date=trade_date)
        reason = _validate_contract(contract=contract, rows=len(rows), env=env, trade_date=trade_date)
        return ArtifactResult(
            reason is None,
            fpath,
            ROOT/"signals/kr/latest_final30_scored.json",
            ROOT/"signals/kr/latest_prep_contract.json",
            len(rows),
            contract,
            True,
            "OK" if reason is None else reason,
        )
    return ArtifactResult(False, None, None, None, 0, None, False, "KR_PREP_ARTIFACT_MISSING")
