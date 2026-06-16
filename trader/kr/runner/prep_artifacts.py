# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
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
    if isinstance(payload, list): return payload
    if isinstance(payload, dict):
        rows = payload.get("rows") or payload.get("data") or payload.get("items") or payload.get("final30") or []
        return rows if isinstance(rows, list) else []
    return []

def _load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))

def _write(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

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

def find_and_repair_kr_prep_artifact(*, env: str="practice", trade_date: str | None=None, as_of: str | None=None) -> ArtifactResult:
    trade_date = trade_date or datetime.now(KST).strftime("%Y-%m-%d")
    as_of = as_of or trade_date
    candidates = [
        ("signals_kr_latest", ROOT/"signals/kr/latest_prep_contract.json", ROOT/"signals/kr/latest_final30_scored.json"),
        ("signals_kr", ROOT/"signals/kr/prep_contract.json", ROOT/"signals/kr/final30_scored.json"),
        ("runtime_kr", ROOT/"runtime/kr/watchlist"/trade_date/"prep_contract.json", ROOT/"runtime/kr/watchlist"/trade_date/"final30_scored.json"),
        ("legacy_runtime", None, ROOT/"runtime/watchlist"/as_of/"final30_scored.json"),
        ("legacy_signals", None, ROOT/"signals/final30.json"),
        ("legacy_ledger", None, ROOT/"bot_state/trader_ledger/final30"/env/as_of/"final30_scored.json"),
    ]
    for name, cpath, fpath in candidates:
        if not fpath.exists(): continue
        try:
            payload = _load(fpath); rows = rows_from_payload(payload)
            contract = _load(cpath) if cpath and cpath.exists() else {}
        except Exception as exc:
            return ArtifactResult(False, fpath, None, None, reason=str(exc))
        if not rows: continue
        repaired = False
        if name.startswith("legacy") or not cpath or not contract:
            contract = mirror_kr_prep_artifacts(final30_path=fpath, env=env, as_of=as_of, trade_date=trade_date)
            repaired = True
        ok = len(rows) == 30 and bool(contract.get("contract_ok", len(rows)==30)) and bool(contract.get("trade_can_proceed", len(rows)==30))
        return ArtifactResult(ok, fpath, ROOT/"signals/kr/latest_final30_scored.json", ROOT/"signals/kr/latest_prep_contract.json", len(rows), contract, repaired, "OK" if ok else "REQUIRED_PREP_NOT_READY")
    return ArtifactResult(False, None, None, None, 0, None, False, "KR_PREP_ARTIFACT_MISSING")
